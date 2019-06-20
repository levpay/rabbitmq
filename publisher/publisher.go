package publisher

import (
	"fmt"
	"strconv"

	"github.com/levpay/rabbitmq"
	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

//Publisher TODO
type Publisher struct {
	rabbitmq.Base
	confirms chan amqp.Confirmation
}

// New TODO
func New() (p *Publisher, err error) {
	log.Println("New Publisher ...")

	p = &Publisher{}
	p.Adapter = &adapter{
		publisher: p,
	}
	return p, p.Config()
}

func (p *Publisher) publishWithoutRetry(d *Declare) (err error) {
	p.WaitIfReconnecting()
	d.prepare()

	err = p.CreateExchangeAndQueue(d)
	if err != nil {
		return
	}

	err = p.createExchangeAndQueueDLX(d)
	if err != nil {
		return
	}

	return p.handle(d)
}

// PublishWithDelay TODO
func (p *Publisher) PublishWithDelay(d *Declare, delay int64) (err error) {
	if delay == 0 {
		delay = 10000
	}
	d.Delay = delay
	return p.Publish(d)
}

// Publish TODO
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
		Priority:        0,
	}

	err = p.Channel.Publish(d.exchangeFullName, "", true, false, msg)
	if err != nil {
		log.Errorln("Publisher - Failed to publish the message ", err)
		if err.Error() == amqp.ErrClosed.Error() {
			p.TryReconnect()
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

// Declare TODO
type Declare struct {
	Exchange         string
	Type             string
	Body             []byte
	Delay            int64
	MaxRetries       int
	exchangeFullName string
	queueFullName    string
	wait             bool
	expiration       string
	queueArgs        amqp.Table
}

func (d *Declare) prepare() {
	d.wait = d.Delay != 0

	if d.wait {
		d.Type = fmt.Sprintf("WAIT_%v", d.Delay)
	}

	d.expiration = strconv.FormatInt(d.Delay, 10)
	if d.expiration == "0" {
		d.expiration = ""
	}

	d.exchangeFullName = rabbitmq.GetExchangeFullName(d.Exchange, d.Type)
	d.queueFullName = rabbitmq.GetQueueFullName(d.Exchange, "", d.Type)
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
		Type:     "",
	}
	dDLX.prepare()

	return dDLX
}

func (d *Declare) getDLXExchangeName() string {
	mDLX := d.getDeclareDLX()
	if mDLX == nil {
		return ""
	}

	return mDLX.exchangeFullName
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
	return d.queueArgs
}

// GetMaxRetries TODO
func (d *Declare) GetMaxRetries() int {
	if d.MaxRetries <= 0 {
		d.MaxRetries = 3
	}
	return d.MaxRetries
}
