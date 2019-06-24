package rabbitmq

import (
	"sync"

	"github.com/levpay/rabbitmq/base"
	"github.com/levpay/rabbitmq/consumer"
	"github.com/levpay/rabbitmq/publisher"
)

var (
	pM sync.Mutex
	p  *publisher.Publisher
	cM sync.Mutex
	c  *consumer.Consumer
)

func loadPublisher() (err error) {
	if p != nil {
		return
	}
	pM.Lock()
	defer pM.Unlock()
	if p != nil {
		return
	}
	err = base.Load()
	if err != nil {
		return
	}

	p, err = publisher.New()

	return
}

func loadConsumer() (err error) {
	if c != nil {
		return
	}
	cM.Lock()
	defer cM.Unlock()
	if c != nil {
		return
	}
	err = base.Load()
	if err != nil {
		return
	}

	c, err = consumer.New(50, 2)

	return
}

// SimplePublisher adds a message in the exchange without options
func SimplePublisher(exchangeName string, body []byte) (err error) {
	return Publisher(exchangeName, "", 0, 0, body)
}

// Publisher adds a message in the exchange
func Publisher(exchangeName string, typeName string, delay int64, priority uint8, body []byte) (err error) {
	err = loadPublisher()
	if err != nil {
		return
	}
	d := &publisher.Declare{
		Exchange: exchangeName,
		Type:     typeName,
		Delay:    delay,
		Body:     body,
		Priority: priority,
	}
	return p.Publish(d)
}

// PublisherWithDelay adds a message in the waiting exchange
func PublisherWithDelay(exchangeName string, delay int64, body []byte) (err error) {
	err = loadPublisher()
	if err != nil {
		return
	}
	d := &publisher.Declare{
		Exchange: exchangeName,
		Delay:    delay,
		Body:     body,
	}
	return p.Publish(d)
}

// PublisherWithPriority adds a message in the waiting exchange and with priority
func PublisherWithPriority(exchangeName string, delay int64, priority uint8, body []byte) (err error) {
	err = loadPublisher()
	if err != nil {
		return
	}
	d := &publisher.Declare{
		Exchange: exchangeName,
		Delay:    delay,
		Body:     body,
		Priority: priority,
	}
	return p.Publish(d)
}

// SimpleConsumer is a simple version of the Consumer that associates a function to receive messages from the queue
func SimpleConsumer(exchangeName, typeName string, actionFunction func([]byte) error) (err error) {
	err = loadConsumer()
	if err != nil {
		return
	}
	d := &consumer.Declare{
		Exchange:       exchangeName,
		Type:           typeName,
		ActionFunction: actionFunction,
	}
	return c.Consume(d)
}
