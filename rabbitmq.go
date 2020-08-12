package rabbitmq

import (
	"os"
	"sync"

	"github.com/levpay/rabbitmq/base"
	"github.com/levpay/rabbitmq/consumer"
	"github.com/levpay/rabbitmq/publisher"
)

var (
	pM sync.Mutex
	p  publisher.PublisherInterface
	cM sync.Mutex
	c  *consumer.Consumer
)

func loadPublisher() (err error) {
	if os.Getenv("NUVEO_ENVIRONMENT") == "3" {
		p, err = publisher.NewFakePublisher()
		return
	}
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

func loadConsumer(threads int) (err error) {
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
	c, err = consumer.New(threads, 2)
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
	return Publisher(exchangeName, "", delay, 0, body)
}

// PublisherWithPriority adds a message in the waiting exchange and with priority
func PublisherWithPriority(exchangeName string, delay int64, priority uint8, body []byte) (err error) {
	return Publisher(exchangeName, "", delay, priority, body)
}

// SimpleConsumer is a simple version of the Consumer that associates a function to receive messages from the queue
func SimpleConsumer(exchangeName, typeName string, actionFunction func([]byte) error) (err error) {
	err = loadConsumer(50)
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

// SimpleConsumerWithThreads is a simple version of the Consumer that associates a function to receive messages from the queue
func SimpleConsumerWithThreads(threads int, exchangeName, typeName string, actionFunction func([]byte) error) (err error) {
	err = loadConsumer(threads)
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
