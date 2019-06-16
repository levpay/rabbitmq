package rabbitmq

import (
	"fmt"
	"strconv"

	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

// SimplePublisher adds a message in the exchange without options.
func SimplePublisher(exchangeName string, body []byte) (err error) {

	// action := func(uint) error {
	// var err error
	return publisherBase(exchangeName, "", 0, body)
	// }

	// // you can also combine multiple Breakers into one
	// interrupter := breaker.MultiplexTwo(
	// 	breaker.BreakByTimeout(time.Minute),
	// 	breaker.BreakBySignal(os.Interrupt),
	// )
	// defer interrupter.Close()

	// return retry.Try(interrupter, action, strategy.Limit(3))
}

// Publisher adds a message in the exchange.
func Publisher(exchangeName string, typeName string, body []byte) (err error) {
	return publisherBase(exchangeName, typeName, 0, body)
}

//PublisherWithDelay adds a message in the waiting exchange.
func PublisherWithDelay(exchangeName string, delay int64, body []byte) (err error) {
	if delay == 0 {
		delay = 10000
	}
	return publisherBase(exchangeName, "", delay, body)
}

// connection *

type publisher struct {
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

var p *publisher

// LoadPublisher iss
func LoadPublisher() (err error) {
	log.Println("LoadPublisher ...")

	Load()

	p = &publisher{
		queuesLoaded: make(map[string]bool),
		// queuesLoaded: make(map[string]chan bool),
	}

	err = p.connect()
	if err != nil {
		return
	}

	return p.createChannel()
}

func (p *publisher) connect() (err error) {
	if p.conn != nil {
		return
	}

	log.Debugln("Publisher - connecting ", Config.URL)
	p.conn, err = amqp.Dial(Config.URL)
	if err != nil {
		log.Errorln("Publisher - Failed to connect to RabbitMQ ", err)
		return
	}
	log.Debugln("Publisher - Got connection")

	go func() { fmt.Printf("Publisher - Closing connection: %s", <-p.conn.NotifyClose(make(chan *amqp.Error))) }()

	return
}

func (p *publisher) createChannel() (err error) {
	log.Debugln("Publisher - Getting channel")

	if p.channel != nil {
		return
	}

	p.channel, err = p.conn.Channel()
	if err != nil {
		log.Errorln("Publisher - Failed to open a channel ", err)
		return
	}
	// defer p.channel.Close()
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

func (p *publisher) createExchangeAndQueue(exchangeName, typeName string, delay int64) (exchangeFullName string, err error) {
	wait := false
	if delay != 0 {
		wait = true
		typeName = fmt.Sprintf("WAIT_%v", delay)
	}

	exchangeFullName = GetExchangeFullName(exchangeName, typeName)

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

	defaultQueueFullName := GetQueueFullName(exchangeName, "", typeName)
	err = createDefaultQueue(p.channel, exchangeName, exchangeFullName, defaultQueueFullName, typeName, wait)
	if err != nil {
		log.Errorln("Publisher - Failed to create default queue ", err)
		return
	}
	log.Debugln("Publisher - Declared exchange: ", exchangeFullName)

	p.queuesLoaded[exchangeFullName] = true

	return
}

func publisherBase(exchangeName string, typeName string, delay int64, body []byte) (err error) {

	err = p.connect()
	if err != nil {
		return
	}

	err = p.createChannel()
	if err != nil {
		return
	}

	exchangeFullName, err := p.createExchangeAndQueue(exchangeName, typeName, delay)
	if err != nil {
		return
	}

	log.Debugln("Publishing ", len(body), "  body ", string(body))

	msg := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		Body:            body,
		DeliveryMode:    amqp.Persistent,
		Expiration:      getExpiration(delay),
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
	var args amqp.Table
	if wait {
		args = make(amqp.Table)
		args["x-dead-letter-exchange"] = GetExchangeFullName(exchangeName, "")
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
