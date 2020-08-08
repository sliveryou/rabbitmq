package main

import (
	"log"
	"time"

	"github.com/streadway/amqp"

	"github.com/sliveryou/rabbitmq"
)

func main() {
	rabbitmq.EnableDebug(true)

	conn, err := rabbitmq.Dial("amqp://root:123123@localhost:5672/")
	if err != nil {
		log.Panic(err)
	}

	producerCh, err := conn.Channel()
	if err != nil {
		log.Panic(err)
	}

	exchangeName := "exchange.test"
	queueName := "queue.test"

	err = producerCh.ExchangeDeclare(exchangeName, amqp.ExchangeDirect, true, false, false, false, nil)
	if err != nil {
		log.Panic(err)
	}

	_, err = producerCh.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		log.Panic(err)
	}

	if err := producerCh.QueueBind(queueName, "test", exchangeName, false, nil); err != nil {
		log.Panic(err)
	}

	go func() {
		for {
			msg := time.Now().Format("2006-01-02 15:04:05")
			err := producerCh.Publish(exchangeName, "test", false, false, amqp.Publishing{
				ContentType:  "text/plain",
				DeliveryMode: amqp.Persistent,
				Body:         []byte(msg),
			})
			log.Printf("publish msg: %v, err: %v", msg, err)
			time.Sleep(1 * time.Second)
		}
	}()

	consumerCh, err := conn.Channel()
	if err != nil {
		log.Panic(err)
	}

	go func() {
		d, err := consumerCh.Consume(queueName, "consumer-test", false, false, false, false, nil)
		if err != nil {
			log.Panic(err)
		}

		for msg := range d {
			log.Printf("consume msg: %s", string(msg.Body))
			msg.Ack(true)
		}
	}()

	<-time.After(10 * time.Second)
	producerCh.Close()
	consumerCh.Close()
	conn.Close()
}
