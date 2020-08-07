package rabbitmq

import (
	"github.com/streadway/amqp"
)

// Producer represents a AMQP producer.
type Producer struct {
	*executer
	retryTimes     int
	returnNotifier ReturnNotifier
}

// ReturnNotifier represents a AMQP return message handle function.
type ReturnNotifier func(amqp.Return)

// RetryTimes returns producer publish retry times.
func (p *Producer) RetryTimes() int {
	return p.retryTimes
}

// SetRetryTimes sets publish retry times for the producer.
func (p *Producer) SetRetryTimes(times int) {
	p.retryTimes = times
}

// ReturnNotifier returns producer AMQP return message handler.
func (p *Producer) ReturnNotifier() ReturnNotifier {
	return p.returnNotifier
}

// NewProducer returns a new *Producer which contains a new AMQP channel by session.
func (r *RabbitMQ) NewProducer(session Session) (*Producer, error) {
	err := r.Connect()
	if err != nil {
		return nil, err
	}

	channel, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}

	return &Producer{
		executer: &executer{
			channel: channel,
			session: &session,
		},
	}, nil
}

// Publish sends a amqp.Publishing from the producer to an exchange on the server.
// If an error occurred, producer will retry to publish by retryTimes and delaySeconds,
// when retryTimes < 0, it will retry forever until publish successfully.
func (p *Producer) Publish(publishing amqp.Publishing) error {
	e := p.session.Exchange
	q := p.session.Queue
	po := p.session.PublishOptions

	routingKey := po.RoutingKey
	if e.Name == "" {
		routingKey = q.Name
	}

	var err error
	for i := 0; p.retryTimes < 0 || i <= p.retryTimes; i++ {
		err = p.channel.Publish(
			e.Name,
			routingKey,
			po.Mandatory,
			po.Immediate,
			publishing,
		)
		if err == nil {
			break
		}
		// debugf("publish failed, err: %v, retry %v", err, i)
		wait(p.channel.delaySeconds)
	}

	return err
}

// NotifyReturn registers a listener for AMQP return message by notifier.
// These can be sent from the server when a publish is undeliverable
// either from the mandatory or immediate flags.
func (p *Producer) NotifyReturn(notifier ReturnNotifier) {
	returns := make(chan amqp.Return)
	p.returnNotifier = notifier

	go func() {
		for {
			for r := range p.channel.NotifyReturn(make(chan amqp.Return)) {
				returns <- r
			}
			wait(p.channel.delaySeconds)
			if p.channel.IsClosed() {
				close(returns)
				debug("returns closed")
				break
			}
		}
	}()

	go func() {
		for message := range returns {
			notifier(message)
		}
	}()
}
