package rabbitmq

import (
	"sync"

	"github.com/streadway/amqp"
)

const (
	// DefaultMapGC represents default number of keys to start map garbage collection
	DefaultMapGC = 200
)

// DeliveryCacher represents an object that can store and load AMQP publishing by deliveryTag,
// also when the channel is recreated, it can return publishings that not get ack confirmation and
// need to be republish in the delivery cache by AMQP publishing chan.
type DeliveryCacher interface {
	Store(deliveryTag uint64, publishing amqp.Publishing)
	Load(deliveryTag uint64) amqp.Publishing
	Republish() <-chan amqp.Publishing
}

// DeliveryMapCache implements the DeliveryCacher interface,
// and it can store, load and republish AMQP publishings by the thread-safe deliveryMap.
type DeliveryMapCache struct {
	sync.RWMutex
	deliveryMap map[uint64]*amqp.Publishing
}

// NewDeliveryMapCache creates a new *DeliveryMapCache.
func NewDeliveryMapCache() *DeliveryMapCache {
	return &DeliveryMapCache{}
}

// Store stores the AMQP publishing in deliveryMap by deliveryTag.
// If the number of deliveryTag keys reaches DefaultMapGC, deliveryMap will gc and recreate.
func (c *DeliveryMapCache) Store(deliveryTag uint64, publishing amqp.Publishing) {
	c.Lock()
	if deliveryTag%DefaultMapGC == 0 {
		deliveryMap := make(map[uint64]*amqp.Publishing)
		for tag := range c.deliveryMap {
			deliveryMap[tag] = c.deliveryMap[tag]
		}
		c.deliveryMap = deliveryMap
		debug("deliverMap gc success")
	}

	c.deliveryMap[deliveryTag] = &publishing
	c.Unlock()
}

// Load loads the AMQP publishing by deliveryTag and deletes the deliveryTag key in deliveryMap.
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

// Republish returns the AMQP publishing chan, then sends the publishing
// that not get ack confirmation and need to be republish in the deliveryMap.
func (c *DeliveryMapCache) Republish() <-chan amqp.Publishing {
	c.Lock()
	deliveryMap := c.deliveryMap
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
