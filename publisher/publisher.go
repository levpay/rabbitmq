package publisher

import (
	"fmt"
	"strconv"

	"github.com/levpay/rabbitmq"
	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

//Publisher iss
type Publisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	confirms     chan amqp.Confirmation
	queuesLoaded map[string]bool
}

// New iss
func New() (p *Publisher, err error) {
	log.Println("LoadPublisher ...")

	rabbitmq.Load()

	p = &Publisher{
		queuesLoaded: make(map[string]bool),
	}

	err = p.connect()
	if err != nil {
		return
	}

	return p, p.createChannel()
}

// Publish iss
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

// PublishWithDelay iss
func (p *Publisher) PublishWithDelay(m *Message, delay int64) (err error) {
	if delay == 0 {
		delay = 10000
	}
	m.Delay = delay
	return p.Publish(m)
}

func (p *Publisher) connect() (err error) {
	if p.conn != nil {
		return
	}

	log.Debugln("Publisher - connecting ", rabbitmq.Config.URL)
	p.conn, err = amqp.Dial(rabbitmq.Config.URL)
	if err != nil {
		log.Errorln("Publisher - Failed to connect to RabbitMQ ", err)
		return
	}
	log.Debugln("Publisher - Got connection")

	go func() { fmt.Printf("Publisher - Closing connection: %s", <-p.conn.NotifyClose(make(chan *amqp.Error))) }()

	return
}

func (p *Publisher) createChannel() (err error) {
	if p.channel != nil {
		return
	}
	log.Debugln("Publisher - Getting channel")

	p.channel, err = p.conn.Channel()
	if err != nil {
		log.Errorln("Publisher - Failed to open a channel ", err)
		return
	}
	log.Debugln("Publisher - Got Channel")

	go func() { fmt.Printf("Publisher - Closing channel: %s", <-p.channel.NotifyClose(make(chan *amqp.Error))) }()
	go func() {
		for res := range p.channel.NotifyReturn(make(chan amqp.Return)) {
			fmt.Println("Publisher - result ", res)
		}
	}()

	p.confirms = p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	log.Debugln("Publisher - Enabling publishing confirms.")
	if err = p.channel.Confirm(false); err != nil {
		log.Errorln("Publisher - Channel could not be put into confirm mode ", err)
		return
	}

	return
}

func (p *Publisher) createExchangeAndQueue(m *Message) (err error) {

	if p.queuesLoaded[m.getExchangeFullName()] {
		return
	}
	log.Debugln("Publisher - createExchangeAndQueue: ", m.getExchangeFullName())

	err = p.channel.ExchangeDeclare(m.getExchangeFullName(), "fanout", true, false, false, false, nil)
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

	p.queuesLoaded[m.getExchangeFullName()] = true
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

	err = p.channel.Publish(m.getExchangeFullName(), "", true, false, msg)
	if err != nil {
		return
	}
	log.Debugln("Publisher - waiting for confirmation of one publishing ", p.conn.IsClosed)

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
	queue, err := p.channel.QueueDeclare(defaultQueueName, true, false, false, false, m.getArgs())
	if err != nil {
		log.Errorln("Publisher - Failed to declare a queue ", err)
		return
	}
	bindingKey := fmt.Sprintf("%s-key", queue.Name)
	log.Debugln("Publisher - Declared Queue (",
		queue.Name, " ", queue.Messages, " messages, ", queue.Consumers,
		" consumers), binding to Exchange (key ", bindingKey, ")")
	return p.channel.QueueBind(queue.Name, bindingKey, m.getExchangeFullName(), false, nil)
}

// Message iss
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
