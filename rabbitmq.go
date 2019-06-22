package rabbitmq

import (
	"github.com/levpay/rabbitmq/base"
	"github.com/levpay/rabbitmq/consumer"
	"github.com/levpay/rabbitmq/publisher"
)

var (
	p *publisher.Publisher
	c *consumer.Consumer
)

func loadPublisher() (err error) {
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
	err = base.Load()
	if err != nil {
		return
	}

	c, err = consumer.New(2, 2)

	return
}

// SimplePublisher TODO
func SimplePublisher(exchangeName string, body []byte) (err error) {
	return Publisher(exchangeName, "", body)
}

// Publisher TODO
func Publisher(exchangeName string, typeName string, body []byte) (err error) {
	err = loadPublisher()
	if err != nil {
		return
	}

	d := &publisher.Declare{
		Exchange: exchangeName,
		Type:     typeName,
		Body:     body,
	}
	return p.Publish(d)
}

// PublisherWithDelay TODO
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

// SimpleConsumer TODO
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
