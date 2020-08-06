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
		Name:    "delay.test.queue.two",
		Durable: true,
	}
	bindOptions = rabbitmq.BindOptions{
		BindingKey: "delay_test_two",
	}
	publishingOptions = rabbitmq.PublishOptions{
		RoutingKey: "delay_test_two",
	}
)

func main() {
	rabbitmq.EnableDebug(true)

	rmq, err := rabbitmq.New(config)
	if err != nil {
		log.Panic(err)
	}
	defer rmq.Close()

	rmq.SetDelay(3)

	session := rabbitmq.Session{
		Exchange:       exchange,
		Queue:          queue,
		BindOptions:    bindOptions,
		PublishOptions: publishingOptions,
	}

	producer, err := rmq.NewProducer(session)
	if err != nil {
		log.Panic(err)
	}
	defer producer.Close()

	err = producer.DeclareAndBind()
	if err != nil {
		log.Panic(err)
	}

	producer.SetDelay(3)
	producer.SetRetryTimes(3)

	producer.NotifyReturn(func(message amqp.Return) {
		log.Println(message)
	})

	msg := amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte("Hello, RabbitMQ."),
	}

	ticker1 := time.NewTicker(500 * time.Millisecond)
	ticker2 := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-ticker1.C:
			msg.Timestamp = time.Now()
			err = producer.Publish(msg)
			if err != nil {
				log.Println(err)
			} else {
				log.Println("publish message success:", string(msg.Body), msg.Timestamp)
			}
		case <-ticker2.C:
			producer.Close()
			rmq.Close()
			time.Sleep(5 * time.Second)
			return
		}
	}
}
