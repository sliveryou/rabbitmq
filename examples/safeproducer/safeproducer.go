package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"

	"github.com/sliveryou/rabbitmq"
)

var (
	config = rabbitmq.Config{
		Host:     "localhost",
		Port:     5672,
		Username: "root",
		Password: "123123",
		Vhost:    "/",
	}
	exchange = rabbitmq.Exchange{
		Name:    "mq.delay.test",
		Type:    amqp.ExchangeDirect,
		Durable: true,
	}
	queue = rabbitmq.Queue{
		Name:    "delay.test.queue.one",
		Durable: true,
	}
	bindOptions = rabbitmq.BindOptions{
		BindingKey: "delay_test_one",
	}
	publishingOptions = rabbitmq.PublishOptions{
		RoutingKey: "delay_test_one",
	}
)

func main() {
	rabbitmq.EnableDebug(true)
	rmq, err := rabbitmq.New(config)
	if err != nil {
		log.Println(err)
		return
	}
	defer rmq.Close()
	rmq.SetDelay(3)

	queueArgs := make(amqp.Table)
	queueArgs["x-dead-letter-exchange"] = "mq.delay.test"
	queueArgs["x-dead-letter-routing-key"] = "delay_test_two"
	// queueArgs["x-message-ttl"] = 10 * 1000 // 10s
	queue.Args = queueArgs

	session := rabbitmq.Session{
		Exchange:       exchange,
		Queue:          queue,
		BindOptions:    bindOptions,
		PublishOptions: publishingOptions,
	}

	safeProducer, err := rmq.NewSafeProducer(session, rabbitmq.NewDeliveryMapCache(), rabbitmq.DefaultPublishNotifier)
	if err != nil {
		log.Println(err)
		return
	}
	defer safeProducer.Close()

	err = safeProducer.DeclareAndBind()
	if err != nil {
		log.Println(err)
		return
	}

	safeProducer.SetDelay(3)
	safeProducer.SetRetryTimes(3)

	safeProducer.NotifyReturn(func(message amqp.Return) {
		log.Println(message)
	})

	msg := amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte("Hello, RabbitMQ."),
		Expiration:   "10000", // 10s
	}

	ticker1 := time.NewTicker(500 * time.Millisecond)
	ticker2 := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker1.C:
			msg.Timestamp = time.Now()
			for i := 0; i < 5; i++ {
				go func() {
					err = safeProducer.Publish(msg)
					if err != nil {
						log.Println(err)
					}
				}()
			}
		case <-ticker2.C:
			time.Sleep(5 * time.Second)
			safeProducer.Close()
			rmq.Close()
			time.Sleep(5 * time.Second)
			return
		}
	}
}
