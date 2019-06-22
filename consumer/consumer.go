package consumer

import (
	"fmt"

	"github.com/levpay/rabbitmq/base"
	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

// Consumer TODO
type Consumer struct {
	base.Base
	threads  int
	declares []*Declare
}

var errAcknowledgerNil = fmt.Errorf("Acknowledger is nil")

// New TODO
func New(threads, preFetchCount int) (c *Consumer, err error) {
	log.Println("New Consumer...")

	c = &Consumer{
		threads: threads,
	}
	c.Adapter = &adapter{
		preFetchCount: preFetchCount,
		consumer:      c,
	}
	return c, c.Config()
}

// Consume associates a function to receive messages from the queue.
func (c *Consumer) Consume(d *Declare) (err error) {
	c.declares = append(c.declares, d)
	err = c.Prepare(d)
	if err != nil {
		return
	}

	err = c.announceQueue(d)
	if err != nil {
		return
	}

	return c.handle(d)
}

func (c *Consumer) treatErrorToReconnect(err error) {
	if err == nil {
		return
	}

	log.Errorln("Consumer - Failed to consume the message ", err)
	switch err.Error() {
	case amqp.ErrClosed.Error(), amqp.ErrCommandInvalid.Error(), errAcknowledgerNil.Error():
		c.TryReconnect()
		c.WaitIfReconnecting()
	}

	return
}

func (c *Consumer) announceQueue(d *Declare) (err error) {
	log.Debugln("Consumer - Starting Consume  tag: ", d.consumerTag)
	d.deliveries, err = c.Channel.Consume(d.queueFullName, d.consumerTag, false, false, false, false, nil)
	if err != nil {
		log.Errorln("Consumer - Failed to register a consumer ", err)
		return
	}
	return
}

func (c *Consumer) handle(d *Declare) (err error) {
	log.Debugln("Consumer - Handling the messages")

	for i := 0; i < c.threads; i++ {
		go c.callingExternalFunc(d, i)
	}

	return
}

func (c *Consumer) callingExternalFunc(d *Declare, i int) {
	log.Debugln("Consumer - Calling the external func, thread: ", i)
	for {
		m := <-d.deliveries
		err := d.sendDelivery(m)
		if err != nil {
			log.Errorln("Consumer - Failed to consume the msg ", err)
			c.treatErrorToReconnect(err)
			continue
		}
		log.Println("Consumer - Consumed")
	}
}

type adapter struct {
	preFetchCount int
	consumer      *Consumer
}

func (a *adapter) PosCreateChannel(c *amqp.Channel) (err error) {
	err = c.Qos(a.preFetchCount, 0, false)
	if err != nil {
		log.Errorln("Error setting qos: ", err)
		return
	}
	return
}

func (a *adapter) PosReconnect() (err error) {
	log.Debugln("Consumer - reconnected")
	for _, d := range a.consumer.declares {
		err = a.consumer.announceQueue(d)
		if err != nil {
			return
		}
	}
	return
}

// Declare TODO
type Declare struct {
	Exchange         string
	QueueSuffix      string
	Type             string
	ActionFunction   func([]byte) error
	exchangeFullName string
	queueFullName    string
	consumerTag      string
	deliveries       <-chan amqp.Delivery
}

// Prepare TODO
func (d *Declare) Prepare() {
	d.exchangeFullName = base.GetExchangeFullName(d.Exchange, d.Type)
	d.queueFullName = base.GetQueueFullName(d.Exchange, d.QueueSuffix, d.Type)
	d.consumerTag = base.GetConsumerTag(d.Exchange, d.QueueSuffix, "")
}

func (d *Declare) sendDelivery(m amqp.Delivery) (err error) {
	if m.Acknowledger == nil {
		log.Errorln("Consumer - Failed to receive delivery ", errAcknowledgerNil)
		return errAcknowledgerNil
	}

	log.Debugln("Consumer - delivery intern", m)
	errActFunc := d.ActionFunction(m.Body)

	if errActFunc != nil {
		log.Errorln("Consumer - Failed to deliver the body ", errActFunc)
		err = m.Nack(false, true)
	} else {
		err = m.Ack(false)
	}
	return err
}

// GetExchangeFullName TODO
func (d *Declare) GetExchangeFullName() string {
	return d.exchangeFullName
}

// GetQueueFullName TODO
func (d *Declare) GetQueueFullName() string {
	return d.queueFullName
}

// GetQueueArgs TODO
func (d *Declare) GetQueueArgs() amqp.Table {
	return make(amqp.Table)
}
