package rabbitmq

import (
	"reflect"

	"github.com/streadway/amqp"
)

// Consumer represents a AMQP consumer.
type Consumer struct {
	*executer
	handler    MessageHandler
	deliveries <-chan amqp.Delivery
}

// MessageHandler represents a AMQP delivery message handle function.
type MessageHandler func(delivery amqp.Delivery)

// Handler returns consumer message handler.
func (c *Consumer) Handler() MessageHandler {
	return c.handler
}

// Deliveries returns consumer AMQP message delivery chan.
func (c *Consumer) Deliveries() <-chan amqp.Delivery {
	return c.deliveries
}

// NewConsumer returns a new *Consumer which contains a new AMQP channel by session.
func (r *RabbitMQ) NewConsumer(session Session) (*Consumer, error) {
	err := r.Connect()
	if err != nil {
		return nil, err
	}

	channel, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}

	c := &Consumer{
		executer: &executer{
			channel: channel,
			session: &session,
		},
	}

	return c, nil
}

// Consume starts consuming AMQP delivery message by handler until the consumer channel closed by developer.
func (c *Consumer) Consume(handler MessageHandler) error {
	co := c.session.ConsumeOptions
	q := c.session.Queue

	deliveries, err := c.channel.Consume(
		q.Name,
		co.Tag,
		co.AutoAck,
		co.Exclusive,
		co.NoLocal,
		co.NoWait,
		co.Args,
	)
	if err != nil {
		return err
	}

	c.deliveries = deliveries
	c.handler = handler

	go func() {
		for delivery := range c.deliveries {
			handler(delivery)
		}
	}()

	return nil
}

// Get consumes a single AMQP delivery message from the head of a queue by handler.
func (c *Consumer) Get(handler MessageHandler) error {
	co := c.session.ConsumeOptions
	q := c.session.Queue

	delivery, ok, err := c.channel.Get(q.Name, co.AutoAck)
	if err != nil {
		return err
	}

	c.handler = handler

	if ok {
		handler(delivery)
	}

	return nil
}

// Qos controls how many messages the server will try to keep on
// the network for consumers before receiving delivery acks.
func (c *Consumer) Qos(prefetchCount int) error {
	c.RegisterMethod("Qos", prefetchCount, 0, false)
	result := c.DoMethod("Qos")
	err, _ := result[0].Interface().(error)
	return err
}

// RegisterMethod registers the consumer channel method and params,
// when the channel is recreated, the method can be executed again.
func (c *Consumer) RegisterMethod(methodName string, params ...interface{}) {
	c.channel.RegisterMethod(methodName, params...)
}

// DoMethod executes the consumer registered channel method and params by methodName.
func (c *Consumer) DoMethod(methodName string) []reflect.Value {
	return c.channel.DoMethod(methodName)
}
