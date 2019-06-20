package rabbitmq

import (
	"errors"
	"fmt"
	"time"

	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

type consumer struct {
	conn              *amqp.Connection
	channel           *amqp.Channel
	exchangeName      string
	queueSuffixName   string
	typeName          string
	consumerSuffixTag string
	actionFunction    func([]byte) error
	done              chan error
	exchangeFullName  string
	queueFullName     string
	consumerTag       string
	bindingKey        string
}

// Consumer associates a function to receive messages from the queue.
func Consumer(exchangeName, queueSuffixName, typeName, consumerSuffixTag string, actionFunction func([]byte) error) (err error) {
	log.Println("Creating a new consumer")

	c := &consumer{
		exchangeName:      exchangeName,
		queueSuffixName:   queueSuffixName,
		typeName:          typeName,
		consumerSuffixTag: consumerSuffixTag,
		actionFunction:    actionFunction,
	}
	return c.connect()
}

// SimpleConsumer is a simple version of the Consumer that associates a function to receive messages from the queue
func SimpleConsumer(exchangeName string, typeName string, actionFunction func([]byte) error) (err error) {
	return Consumer(exchangeName, "", typeName, "", actionFunction)
}

func (c *consumer) connect() (err error) {
	log.Debugln("Connecting the consumer")

	if c.done == nil {
		c.done = make(chan error)
	}

	c.exchangeFullName = GetExchangeFullName(c.exchangeName, c.typeName)
	c.queueFullName = GetQueueFullName(c.exchangeName, c.queueSuffixName, c.typeName)
	c.consumerTag = GetConsumerTag(c.exchangeName, c.queueSuffixName, "")

	log.Debugln("dialing", Config.URL)
	c.conn, err = amqp.Dial(Config.URL)
	if err != nil {
		log.Errorln("Failed to connect to RabbitMQ ", err)
		return
	}
	go func() { fmt.Printf("Closing connection: %s", <-c.conn.NotifyClose(make(chan *amqp.Error))) }()

	log.Debugln("Got Connection, getting Channel")
	c.channel, err = c.conn.Channel()
	if err != nil {
		log.Errorln("Failed to open a channel ", err)
		return
	}

	log.Debugln("Got Channel, declaring Exchange", c.exchangeFullName)
	err = c.channel.ExchangeDeclare(c.exchangeFullName, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Errorln("Failed to declare exchange ", err)
		return
	}

	deliveries, err := c.announceQueue()
	if err != nil {
		return
	}

	return c.handle(deliveries)
}

func (c *consumer) reConnect() (deliveries <-chan amqp.Delivery, err error) {
	log.Printf("Reconnecting, waiting a few seconds.")
	time.Sleep(15 * time.Second)

	if err := c.connect(); err != nil {
		log.Printf("Could not connect in reconnect call: %v", err.Error())
	}

	deliveries, err = c.announceQueue()
	if err != nil {
		log.Errorln("Failed to reconnect: ", err)
		return nil, errors.New("Couldn't connect")
	}
	return
}

func (c *consumer) announceQueue() (deliveries <-chan amqp.Delivery, err error) {
	log.Debugln("Announcing the queue of the consumer")

	queue, err := c.channel.QueueDeclare(c.queueFullName, true, false, false, false, nil)
	if err != nil {
		log.Errorln("Failed to declare a queue: ", err)
		return
	}

	c.bindingKey = fmt.Sprintf("%s-key", queue.Name)
	log.Debugln("Declared Queue (", queue.Name, " ", queue.Messages,
		" messages, ", queue.Consumers, " consumers), binding to Exchange (key ", c.bindingKey, ")")
	err = c.channel.QueueBind(queue.Name, c.bindingKey, c.exchangeFullName, false, nil)
	if err != nil {
		log.Errorln("Failed to bind a queue ", err)
		return
	}

	log.Debugln("Queue bound to Exchange, starting Consume (consumer tag " + c.consumerTag + ")")
	deliveries, err = c.channel.Consume(queue.Name, c.consumerTag, false, false, false, false, nil)
	if err != nil {
		log.Errorln("Failed to register a consumer ", err)
		return
	}
	return
}

func (c *consumer) handle(deliveries <-chan amqp.Delivery) (err error) {
	log.Debugln("Handling the messages")

	for {
		go c.callingExternalFunc(deliveries)

		if <-c.done != nil {
			deliveries, err = c.reConnect()
			if err != nil {
				log.Errorln("Reconnecting Error: ", err)
				return err
			}
		}
		log.Println("Reconnected... possibly")
	}
}

func (c *consumer) callingExternalFunc(deliveries <-chan amqp.Delivery) {
	log.Debugln("Calling the external func")

	for d := range deliveries {
		err := c.actionFunction(d.Body)
		if err != nil {
			log.Errorln("Failed to deliver the body ", err)
			continue
		}
		d.Ack(false)
		log.Println("Committed")
	}
}
