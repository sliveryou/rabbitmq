package rabbitmq

import (
	"sync"

	"github.com/streadway/amqp"
)

const (
	DefaultMapGC = 200 // default number of keys to start map garbage collection
)

type DeliveryCacher interface {
	Store(publishing amqp.Publishing)
	Load(deliveryTag uint64) amqp.Publishing
	Republish() <-chan amqp.Publishing
}

type DeliveryMapCache struct {
	sync.RWMutex
	deliveryTag uint64
	deliveryMap map[uint64]*amqp.Publishing
}

func NewDeliveryMapCache() *DeliveryMapCache {
	return &DeliveryMapCache{}
}

func (c *DeliveryMapCache) Store(publishing amqp.Publishing) {
	c.Lock()
	if c.deliveryTag%DefaultMapGC == 0 {
		deliveryMap := make(map[uint64]*amqp.Publishing)
		for deliverTag := range c.deliveryMap {
			deliveryMap[deliverTag] = c.deliveryMap[deliverTag]
		}
		c.deliveryMap = deliveryMap
		debug("deliverMap gc success")
	}

	c.deliveryMap[c.deliveryTag] = &publishing
	c.deliveryTag++
	c.Unlock()
}

func (c *DeliveryMapCache) Load(deliverTag uint64) amqp.Publishing {
	c.Lock()
	publishing, ok := c.deliveryMap[deliverTag]
	delete(c.deliveryMap, deliverTag)
	c.Unlock()

	if !ok {
		return amqp.Publishing{}
	}

	return *publishing
}

func (c *DeliveryMapCache) Republish() <-chan amqp.Publishing {
	c.Lock()
	deliveryMap := c.deliveryMap
	c.deliveryTag = 1
	c.deliveryMap = make(map[uint64]*amqp.Publishing)
	c.Unlock()

	publishings := make(chan amqp.Publishing)

	go func() {
		for deliverTag := range deliveryMap {
			publishings <- *deliveryMap[deliverTag]
		}
		close(publishings)
	}()

	return publishings
}
