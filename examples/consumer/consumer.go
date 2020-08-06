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
	consumeOptions = rabbitmq.ConsumeOptions{
		Tag: "consumer01",
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
		ConsumeOptions: consumeOptions,
	}

	consumer, err := rmq.NewConsumer(session)
	if err != nil {
		log.Panic(err)
	}
	defer consumer.Close()

	err = consumer.DeclareAndBind()
	if err != nil {
		log.Panic(err)
	}

	err = consumer.Qos(10)
	if err != nil {
		log.Panic(err)
	}

	var handler = func(delivery amqp.Delivery) {
		log.Println("consume message success:", string(delivery.Body), delivery.Timestamp, delivery.DeliveryTag)
		delivery.Ack(false)
		// delivery.Reject(true)
	}

	done := make(chan struct{})
	go func() {
		time.Sleep(5 * time.Second)
		consumer.Close()
		rmq.Close()
		time.Sleep(5 * time.Second)
		done <- struct{}{}
	}()

	err = consumer.Consume(handler)
	if err != nil {
		log.Println(err)
	}
	<-done
}
