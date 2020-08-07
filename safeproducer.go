package rabbitmq

import (
	"github.com/streadway/amqp"
)

const (
	DefaultRetryTimes       = 3  // default retry times of safe publish
	DefaultRepublishRoutine = 10 // default number of quick republish goroutine
)

// SafeProducer represents a AMQP safe producer.
type SafeProducer struct {
	*Producer
	publishNotifier PublishNotifier
	deliveryCache   DeliveryCacher
}

// PublishNotifier represents a AMQP message confirmation handle function.
type PublishNotifier func(*SafeProducer, amqp.Publishing, amqp.Confirmation)

// PublishNotifier returns producer AMQP message confirmation handler.
func (p *SafeProducer) PublishNotifier() PublishNotifier {
	return p.publishNotifier
}

// NewSafeProducer returns a new *SafeProducer which contains a new AMQP channel by session.
// The *SafeProducer will set the retryTimes with DefaultRetryTimes, and run a publish listener
// with notifier for reliable publishing, if notifier is nil, it will use DefaultPublishNotifier.
func (r *RabbitMQ) NewSafeProducer(session Session, deliveryCache DeliveryCacher, notifier ...PublishNotifier) (*SafeProducer, error) {
	err := r.Connect()
	if err != nil {
		return nil, err
	}

	channel, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		executer: &executer{
			channel: channel,
			session: &session,
		},
		retryTimes: DefaultRetryTimes,
	}

	safeProducer := &SafeProducer{
		Producer:      producer,
		deliveryCache: deliveryCache,
	}

	if len(notifier) > 0 {
		safeProducer.notifyPublish(notifier[0])
	} else {
		safeProducer.notifyPublish(DefaultPublishNotifier)
	}

	return safeProducer, nil
}

// Publish sends a amqp.Publishing from the safe producer to an exchange on the server.
// If an error occurred, safe producer will retry to publish by retryTimes and delaySeconds,
// when retryTimes < 0, it will retry forever until publish successfully.
// Safe producer will record the publish message sent to the server to deliveryCache and
// each time a message is sent, the deliveryTag increases.
func (p *SafeProducer) Publish(publishing amqp.Publishing) error {
	err := p.Producer.Publish(publishing)
	if err != nil {
		return err
	}

	p.deliveryCache.Store(publishing)

	return nil
}

// notifyPublish registers a listener for AMQP message confirmation by notifier.
// It puts the safe producer channel into confirm mode, and after entering this
// mode, the server will send a ack or nak message with the delivery tag set to a 1
// based incremental index corresponding to every publishing received.
func (p *SafeProducer) notifyPublish(notifier PublishNotifier) {
	confirmations := make(chan amqp.Confirmation)
	p.publishNotifier = notifier

	go func() {
		for {
			err := p.channel.Confirm(false)
			if err != nil {
				wait(p.channel.delaySeconds)
				continue
			}

			go func(publishings <-chan amqp.Publishing) {
				semaphore := make(chan struct{}, DefaultRepublishRoutine)
				for publishing := range publishings {
					message := publishing
					semaphore <- struct{}{}
					go func() {
						defer func() { <-semaphore }()
						debugf("quick republish message: %v, %v", string(message.Body), message.Timestamp)
						p.Publish(message)
					}()
				}
			}(p.deliveryCache.Republish())

			for c := range p.channel.NotifyPublish(make(chan amqp.Confirmation)) {
				confirmations <- c
			}
			wait(p.channel.delaySeconds)
			if p.channel.IsClosed() {
				close(confirmations)
				debug("confirmations closed")
				break
			}
		}
	}()

	go func() {
		for confirm := range confirmations {
			dc, ok := p.deliveryCache.(*DeliveryMapCache)
			if ok {
				debug(dc.deliveryMap, dc.deliveryTag)
			}
			message := p.deliveryCache.Load(confirm.DeliveryTag)
			notifier(p, message, confirm)
		}
	}()
}

// DefaultPublishNotifier represents default AMQP message confirmation handler.
// If received nak confirmation, the message will be sent again.
func DefaultPublishNotifier(p *SafeProducer, message amqp.Publishing, confirm amqp.Confirmation) {
	if confirm.Ack {
		debugf("message ack: %v, %v, %v", confirm.DeliveryTag, string(message.Body), message.Timestamp)
	} else {
		debugf("message nak: %v, %v, %v", confirm.DeliveryTag, string(message.Body), message.Timestamp)
		go p.Publish(message)
	}
}
