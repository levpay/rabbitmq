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
	err = p.Config()
	if err != nil {
		return
	}

	return p, p.createChannel()
}

// Publish TODO
func (p *Publisher) Publish(m *Message) (err error) {

	err = p.createExchangeAndQueue(m)
	if err != nil {
		return
	}

	err = p.createExchangeAndQueueDLX(m)
	if err != nil {
		return
	}

	return p.sendBody(m)
}

// PublishWithDelay TODO
func (p *Publisher) PublishWithDelay(m *Message, delay int64) (err error) {
	if delay == 0 {
		delay = 10000
	}
	m.Delay = delay
	return p.Publish(m)
}

func (p *Publisher) createChannel() (err error) {
	if p.Channel != nil {
		return
	}
	log.Debugln("Publisher - Getting channel")

	p.Channel, err = p.Conn.Channel()
	if err != nil {
		log.Errorln("Publisher - Failed to open a channel ", err)
		return
	}
	log.Debugln("Publisher - Got Channel")

	go func() { fmt.Printf("Publisher - Closing channel: %s", <-p.Channel.NotifyClose(make(chan *amqp.Error))) }()
	go func() {
		for res := range p.Channel.NotifyReturn(make(chan amqp.Return)) {
			fmt.Println("Publisher - result ", res)
		}
	}()

	p.confirms = p.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	log.Debugln("Publisher - Enabling publishing confirms.")
	if err = p.Channel.Confirm(false); err != nil {
		log.Errorln("Publisher - Channel could not be put into confirm mode ", err)
		return
	}

	return
}

func (p *Publisher) createExchangeAndQueue(m *Message) (err error) {

	if p.QueuesLoaded[m.getExchangeFullName()] {
		return
	}
	log.Debugln("Publisher - createExchangeAndQueue: ", m.getExchangeFullName())

	err = p.Channel.ExchangeDeclare(m.getExchangeFullName(), "fanout", true, false, false, false, nil)
	if err != nil {
		log.Errorln("Publisher - Failed to declare exchange ", err)
		return
	}

	err = p.createDefaultQueue(m)
	if err != nil {
		log.Errorln("Publisher - Failed to create default queue ", err)
		return
	}
	log.Debugln("Publisher - Declared exchange: ", m.getExchangeFullName())

	p.QueuesLoaded[m.getExchangeFullName()] = true
	return
}

func (p *Publisher) createExchangeAndQueueDLX(m *Message) (err error) {
	if !m.wait() {
		return
	}

	err = p.createExchangeAndQueue(m.getMessageDLX())
	if err != nil {
		return
	}

	return
}

func (p *Publisher) sendBody(m *Message) (err error) {

	log.Debugln("Publishing ", len(m.Body), "  body ", string(m.Body))

	msg := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		Body:            m.Body,
		DeliveryMode:    amqp.Persistent,
		Expiration:      m.getExpiration(),
		Priority:        0,
	}

	err = p.Channel.Publish(m.getExchangeFullName(), "", true, false, msg)
	if err != nil {
		return
	}
	log.Debugln("Publisher - waiting for confirmation of one publishing ", p.Conn.IsClosed)

	confirmed := <-p.confirms
	log.Println("teste-2")
	if !confirmed.Ack {
		msg := fmt.Sprintf("Publisher - failed delivery of delivery tag: %d", confirmed.DeliveryTag)
		log.Errorln(msg)
		return fmt.Errorf(msg)
	}
	log.Debugln("Publisher - confirmed delivery with delivery tag: ", confirmed.DeliveryTag)

	return
}

func (p *Publisher) createDefaultQueue(m *Message) (err error) {
	defaultQueueName := rabbitmq.GetQueueFullName(m.Exchange, "", m.getType())
	queue, err := p.Channel.QueueDeclare(defaultQueueName, true, false, false, false, m.getArgs())
	if err != nil {
		log.Errorln("Publisher - Failed to declare a queue ", err)
		return
	}
	bindingKey := fmt.Sprintf("%s-key", queue.Name)
	log.Debugln("Publisher - Declared Queue (",
		queue.Name, " ", queue.Messages, " messages, ", queue.Consumers,
		" consumers), binding to Exchange (key ", bindingKey, ")")
	return p.Channel.QueueBind(queue.Name, bindingKey, m.getExchangeFullName(), false, nil)
}

// Message TODO
type Message struct {
	Exchange string
	Type     string
	Body     []byte
	Delay    int64
}

func (m *Message) wait() bool {
	return m.Delay != 0
}

func (m *Message) getType() string {
	if m.wait() {
		m.Type = fmt.Sprintf("WAIT_%v", m.Delay)
	}
	return m.Type
}

func (m *Message) getExpiration() string {
	expiration := strconv.FormatInt(m.Delay, 10)
	if expiration == "0" {
		return ""
	}
	return expiration
}

func (m *Message) getArgs() (args amqp.Table) {
	args = make(amqp.Table)

	deadLetterExchange := m.getDeadLetterExchange()
	if deadLetterExchange == "" {
		return
	}

	args["x-dead-letter-exchange"] = deadLetterExchange
	return
}

func (m *Message) getExchangeFullName() string {
	return rabbitmq.GetExchangeFullName(m.Exchange, m.getType())
}

func (m *Message) getMessageDLX() *Message {
	if !m.wait() {
		return nil
	}

	return &Message{
		Exchange: m.Exchange,
		Type:     "",
	}
}

func (m *Message) getDeadLetterExchange() string {
	mDLX := m.getMessageDLX()
	if mDLX == nil {
		return ""
	}

	return mDLX.getExchangeFullName()
}
