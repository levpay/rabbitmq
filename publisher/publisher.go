package publisher

import (
	"fmt"
	"strconv"

	"github.com/levpay/rabbitmq/base"
	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

// Publisher contains the datas of the publisher as connection and channel
type Publisher struct {
	base.Base
	confirms chan amqp.Confirmation
}

// New creates a publisher
func New() (p *Publisher, err error) {
	log.Println("New Publisher ...")

	p = &Publisher{}
	p.Adapter = &adapter{
		publisher: p,
	}
	return p, p.Config()
}

func (p *Publisher) publishWithoutRetry(d *Declare) (err error) {
	err = p.Prepare(d)
	if err != nil {
		return
	}

	err = p.createExchangeAndQueueDLX(d)
	if err != nil {
		return
	}

	return p.handle(d)
}

// PublishWithDelay publish a message in the waiting exchange
func (p *Publisher) PublishWithDelay(d *Declare, delay int64) (err error) {
	if delay == 0 {
		delay = 10000
	}
	d.Delay = delay
	return p.Publish(d)
}

// Publish adds a message in the exchange
func (p *Publisher) Publish(d *Declare) (err error) {
	for i := 0; i < d.GetMaxRetries(); i++ {
		err = p.publishWithoutRetry(d)
		if err == nil {
			return
		}
	}
	return
}

func (p *Publisher) createExchangeAndQueueDLX(d *Declare) (err error) {
	if !d.wait {
		return
	}

	dDLX := d.getDeclareDLX()
	err = p.CreateExchangeAndQueue(dDLX)
	if err != nil {
		return
	}

	return
}

func (p *Publisher) handle(d *Declare) (err error) {

	log.Debugln("Publishing ", len(d.Body), "  body ", string(d.Body))

	msg := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		Body:            d.Body,
		DeliveryMode:    amqp.Persistent,
		Expiration:      d.expiration,
		Priority:        d.Priority,
	}

	err = p.Channel.Publish(d.exchangeFullName, "", true, false, msg)
	if err != nil {
		log.Errorln("Publisher - Failed to publish the message ", err)
		switch err.Error() {
		case amqp.ErrClosed.Error(), amqp.ErrCommandInvalid.Error():
			p.TryReconnect()
			p.WaitIfReconnecting()
		}
		return
	}
	log.Debugln("Publisher - waiting for confirmation of one publishing")

	confirmed := <-p.confirms
	if !confirmed.Ack {
		msg := fmt.Sprintf("Publisher - failed delivery of delivery tag: %d", confirmed.DeliveryTag)
		log.Errorln(msg)
		return fmt.Errorf(msg)
	}
	log.Debugln("Publisher - confirmed delivery with delivery tag: ", confirmed.DeliveryTag)

	return
}

type adapter struct {
	publisher *Publisher
}

func (a *adapter) PosCreateChannel(c *amqp.Channel) (err error) {

	go func() {
		for res := range c.NotifyReturn(make(chan amqp.Return)) {
			fmt.Println("Notify Return ", res)
		}
	}()

	a.publisher.confirms = c.NotifyPublish(make(chan amqp.Confirmation, 1))

	log.Debugln("Enabling publishing confirms")
	err = c.Confirm(false)
	if err != nil {
		log.Errorln("Channel could not be put into confirm mode ", err)
		return
	}
	return
}

func (a *adapter) PosReconnect() error {
	log.Debugln("Publisher - reconnected")
	return nil
}

// Declare contains the exchange and queue data that the publisher should publish
type Declare struct {
	Exchange         string
	Type             string
	Body             []byte
	Delay            int64
	MaxRetries       int
	Priority         uint8
	typeFullName     string
	exchangeFullName string
	queueFullName    string
	wait             bool
	expiration       string
	queueArgs        amqp.Table
}

// Prepare the metadata of the exchange and queue to be consumed
func (d *Declare) Prepare() {
	d.wait = d.Delay != 0

	d.typeFullName = d.Type
	if d.wait {

		priority := d.Priority
		if priority > base.MaxPriority {
			priority = base.MaxPriority
		}

		d.typeFullName = fmt.Sprintf("%v:PRIORITY_%02d:WAIT_%v", d.Type, priority, d.Delay)
	}

	d.expiration = strconv.FormatInt(d.Delay, 10)
	if d.expiration == "0" {
		d.expiration = ""
	}

	d.exchangeFullName = base.GetExchangeFullName(d.Exchange, d.typeFullName)
	d.queueFullName = base.GetQueueFullName(d.Exchange, "", d.typeFullName)
	d.setQueueArgs()
}

func (d *Declare) setQueueArgs() {
	d.queueArgs = make(amqp.Table)

	deadLetterExchange := d.getDLXExchangeName()
	if deadLetterExchange == "" {
		return
	}

	d.queueArgs["x-dead-letter-exchange"] = deadLetterExchange
}

func (d *Declare) getDeclareDLX() *Declare {
	if !d.wait {
		return nil
	}

	dDLX := &Declare{
		Exchange: d.Exchange,
		Type:     d.Type,
	}
	dDLX.Prepare()

	return dDLX
}

func (d *Declare) getDLXExchangeName() string {
	mDLX := d.getDeclareDLX()
	if mDLX == nil {
		return ""
	}

	return mDLX.exchangeFullName
}

// GetExchangeFullName returns the exchange full name
func (d *Declare) GetExchangeFullName() string {
	return d.exchangeFullName
}

// GetQueueFullName returns the queue full name
func (d *Declare) GetQueueFullName() string {
	return d.queueFullName
}

// GetTypeFullName returns the type full name
func (d *Declare) GetTypeFullName() string {
	return d.typeFullName
}

// GetQueueArgs returns the argument table of the queue
func (d *Declare) GetQueueArgs() amqp.Table {
	return d.queueArgs
}

// GetMaxRetries returns the max retries to publish
func (d *Declare) GetMaxRetries() int {
	if d.MaxRetries <= 0 {
		d.MaxRetries = 3
	}
	return d.MaxRetries
}
