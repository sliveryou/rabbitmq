package rabbitmq

import (
	"log"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

const (
	// DefaultDelaySeconds represents default delay retry seconds of reconnect or recreate
	DefaultDelaySeconds = 3
)

var (
	// defaultEnableDebug represents default whether enable debug flag
	defaultEnableDebug = false
)

// EnableDebug enables or not enables debug log.
func EnableDebug(isEnable bool) {
	defaultEnableDebug = isEnable
}

// Dial wraps amqp.Dial, which can dial and get a auto reconnect connection.
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
		delayer:    &delayer{delaySeconds: DefaultDelaySeconds},
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if connection is closed by developer
			if !ok {
				debug("connection closed")
				break
			}
			debugf("connection closed, reason: %v", reason)

			// reconnect if connection is not closed by developer
			for {
				// wait for connection reconnect
				wait(connection.delaySeconds)

				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Connection = conn
					debugf("connection reconnect success")
					break
				}
				debugf("connection reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}

// Connection is amqp.Connection wrapper.
type Connection struct {
	*amqp.Connection
	*delayer
}

// Channel wraps amqp.Connection.Channel, which can get a auto recreate channel.
func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
		delayer: &delayer{delaySeconds: c.delaySeconds},
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if channel is closed by developer
			if !ok || channel.IsClosed() {
				debug("channel closed")
				channel.Close() // ensure closed flag is set when channel is closed
				break
			}
			debugf("channel closed, reason: %v", reason)

			// recreate if channel is not closed by developer
			for {
				// wait for channel recreate
				wait(channel.delaySeconds)

				ch, err := c.Connection.Channel()
				if err == nil {
					debug("channel recreate success")
					channel.methodMap.Range(func(k, v interface{}) bool {
						methodName, _ := k.(string)
						channel.DoMethod(ch, methodName)
						debugf("channel do method %v success", methodName)
						return true
					})
					channel.Channel = ch
					break
				}
				debugf("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, nil
}

// Channel is amqp.Channel wrapper.
type Channel struct {
	*amqp.Channel
	*delayer
	closed    int32
	methodMap sync.Map
}

// RegisterMethod registers the channel method and params, when the channel is recreated, the method can be executed again.
func (ch *Channel) RegisterMethod(methodName string, params ...interface{}) {
	var values []reflect.Value
	for i := range params {
		values = append(values, reflect.ValueOf(params[i]))
	}
	ch.methodMap.Store(methodName, values)
}

// DoMethod executes the registered channel method and params by methodName.
func (ch *Channel) DoMethod(channel *amqp.Channel, methodName string) []reflect.Value {
	params, ok := ch.methodMap.Load(methodName)
	if !ok {
		return nil
	}
	vc := reflect.ValueOf(channel)
	ps, _ := params.([]reflect.Value)
	result := vc.MethodByName(methodName).Call(ps)
	return result
}

// IsClosed reports whether the channel is closed by developer.
func (ch *Channel) IsClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

// Close closes the channel and sets the closed flag.
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

// Consume warps amqp.Channel.Consume, the returned Delivery will end only when channel closed by developer.
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				debugf("consume failed, err: %v", err)
				wait(ch.delaySeconds)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			// wait, because channel closed flag may not set before waiting
			wait(ch.delaySeconds)

			if ch.IsClosed() {
				close(deliveries)
				debug("deliveries closed")
				break
			}
		}
	}()

	return deliveries, nil
}

// delayer represents a struct with delay retry seconds.
type delayer struct {
	delaySeconds int
}

// SetDelay sets delay retry seconds for the delayer.
func (d *delayer) SetDelay(seconds int) {
	d.delaySeconds = seconds
}

func wait(seconds int) {
	time.Sleep(time.Duration(seconds) * time.Second)
}

func debug(args ...interface{}) {
	if !defaultEnableDebug {
		return
	}
	log.Print(args...)
}

func debugf(format string, args ...interface{}) {
	if !defaultEnableDebug {
		return
	}
	log.Printf(format, args...)
}
