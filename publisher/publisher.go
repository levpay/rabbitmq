package publisher

import (
	"fmt"
	"strconv"

	"github.com/levpay/rabbitmq"
	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

// PublishWithDelay iss
func (p *Publisher) PublishWithDelay(m *Message, delay int64) (err error) {
	if delay == 0 {
		delay = 10000
	}
	m.delay = delay
	return p.Publish(m)
}

//Publisher iss
type Publisher struct {
	conn         *amqp.Connection
	channel      *amqp.Channel
	confirms     chan amqp.Confirmation
	queuesLoaded map[string]bool
	// queuesLoaded map[string]chan bool
	// exchangeName      string
	// queueSuffixName   string
	// typeName          string
	// consumerSuffixTag string
	// actionFunction    function
	// done              chan error

	// exchangeFullName string
	// queueFullName    string
	// consumerTag      string
	// bindingKey       string

	// load chan bool
	// threads     int
	// doneThreads []chan bool
}

// New iss
func New() (p *Publisher, err error) {
	log.Println("LoadPublisher ...")

	rabbitmq.Load()

	p = &Publisher{
		queuesLoaded: make(map[string]bool)}

	err = p.connect()
	if err != nil {
		return
	}

	return p, p.createChannel()
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

func (p *Publisher) createExchangeAndQueue(exchangeName, typeName string, delay int64) (exchangeFullName string, err error) {
	wait := false
	if delay != 0 {
		wait = true
		typeName = fmt.Sprintf("WAIT_%v", delay)
	}

	exchangeFullName = rabbitmq.GetExchangeFullName(exchangeName, typeName)

	log.Debugln("Publisher - createExchangeAndQueue: ", exchangeFullName)

	if p.queuesLoaded[exchangeFullName] {
		return
	}

	log.Debugln("Publisher - Declaring Exchange: ", exchangeFullName)

	err = p.channel.ExchangeDeclare(exchangeFullName, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Errorln("Publisher - Failed to declare exchange ", err)
		return
	}

	defaultQueueFullName := rabbitmq.GetQueueFullName(exchangeName, "", typeName)
	err = createDefaultQueue(p.channel, exchangeName, exchangeFullName, defaultQueueFullName, typeName, wait)
	if err != nil {
		log.Errorln("Publisher - Failed to create default queue ", err)
		return
	}
	log.Debugln("Publisher - Declared exchange: ", exchangeFullName)

	p.queuesLoaded[exchangeFullName] = true

	return
}

// Message iss
type Message struct {
	Exchange string
	Type     string
	delay    int64
	Body     []byte
}

// Publish iss
func (p *Publisher) Publish(m *Message) (err error) {

	exchangeFullName, err := p.createExchangeAndQueue(m.Exchange, m.Type, m.delay)
	if err != nil {
		return
	}

	log.Debugln("Publishing ", len(m.Body), "  body ", string(m.Body))

	msg := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		Body:            m.Body,
		DeliveryMode:    amqp.Persistent,
		Expiration:      getExpiration(m.delay),
		Priority:        0,
	}

	err = p.channel.Publish(exchangeFullName, "", true, false, msg)
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

func getExpiration(delay int64) (expiration string) {
	expiration = strconv.FormatInt(delay, 10)
	if expiration == "0" {
		return ""
	}
	return expiration
}

func createDefaultQueue(channel *amqp.Channel, exchangeName string, exchangeFullName string, defaultQueueName string, typeName string, wait bool) (err error) {
	args := make(amqp.Table)
	if wait {
		args["x-dead-letter-exchange"] = rabbitmq.GetExchangeFullName(exchangeName, "")
	}

	queue, err := channel.QueueDeclare(defaultQueueName, true, false, false, false, args)
	if err != nil {
		log.Errorln("Publisher - Failed to declare a queue ", err)
		return
	}
	bindingKey := fmt.Sprintf("%s-key", queue.Name)
	log.Debugln("Publisher - Declared Queue (",
		queue.Name, " ", queue.Messages, " messages, ", queue.Consumers,
		" consumers), binding to Exchange (key ", bindingKey, ")")
	return channel.QueueBind(queue.Name, bindingKey, exchangeFullName, false, nil)
}
