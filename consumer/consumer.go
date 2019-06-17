package consumer

import (
	"errors"
	"fmt"
	"time"

	"github.com/levpay/rabbitmq"
	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

type consumer struct {
	conn          *amqp.Connection
	channel       *amqp.Channel
	queuesLoaded  map[string]bool
	done          chan error
	threads       int
	prefetchCount int

	// exchangeName      string
	// queueSuffixName   string
	// typeName          string
	// consumerSuffixTag string
	// actionFunction    function

	// exchangeFullName string
	// queueFullName    string
	// consumerTag      string
	// bindingKey       string

	doneThreads []chan bool
}

type function func([]byte) error

var c *consumer

// LoadConsumer iss
func LoadConsumer() (err error) {
	log.Println("LoadConsumer ...")

	rabbitmq.Load()

	c = &consumer{
		threads:       50,
		prefetchCount: 100,

		queuesLoaded: make(map[string]bool),
		done:         make(chan error),
	}

	err = c.connect()
	if err != nil {
		return
	}

	return c.createChannel()
}

// SimpleConsumer is a simple version of the Consumer that associates a function to receive messages from the queue
func SimpleConsumer(exchangeName string, typeName string, actionFunction function) (err error) {
	return Consumer(exchangeName, "", typeName, "", actionFunction)
}

func (c *consumer) connect() (err error) {
	log.Debugln("Consumer - connecting ", rabbitmq.Config.URL)

	if c.conn != nil {
		return
	}

	c.conn, err = amqp.Dial(rabbitmq.Config.URL)
	if err != nil {
		log.Errorln("Consumer - Failed to connect to RabbitMQ ", err)
		return
	}
	log.Debugln("Consumer - Got connection")

	go func() { fmt.Printf("Consumer - Closing connection: %s", <-c.conn.NotifyClose(make(chan *amqp.Error))) }()

	return
}

func (c *consumer) createChannel() (err error) {
	log.Debugln("Consumer - Getting channel")

	if c.channel != nil {
		return
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		log.Errorln("Consumer - Failed to open a channel ", err)
		return
	}
	log.Debugln("Consumer - Got Channel")

	go func() {
		fmt.Printf("Consumer - Closing channel: %s", <-c.channel.NotifyClose(make(chan *amqp.Error)))
	}()

	err = c.channel.Qos(c.prefetchCount, 0, false)
	if err != nil {
		log.Errorln("Consumer - Error setting qos: ", err)
		return
	}

	return
}

func (c *consumer) createExchangeAndQueue(exchangeName, typeName, queueSuffixName string) (exchangeFullName string, err error) {
	exchangeFullName = rabbitmq.GetExchangeFullName(exchangeName, typeName)
	queueFullName := rabbitmq.GetQueueFullName(exchangeName, queueSuffixName, typeName)

	log.Debugln("Consumer - createExchangeAndQueue: ", exchangeFullName)

	if c.queuesLoaded[exchangeFullName] {
		return
	}

	log.Debugln("Consumer - Declaring Exchange: ", exchangeFullName)
	err = c.channel.ExchangeDeclare(exchangeFullName, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Errorln("Consumer - Failed to declare exchange ", err)
		return
	}

	queue, err := c.channel.QueueDeclare(queueFullName, true, false, false, false, nil)
	if err != nil {
		log.Errorln("Consumer - Failed to declare a queue: ", err)
		return
	}

	bindingKey := fmt.Sprintf("%s-key", queueFullName)
	log.Debugln("Consumer - Declared Queue (", queueFullName, " ", queue.Messages,
		" messages, ", queue.Consumers, " consumers), binding to Exchange (key ", bindingKey, ")")
	err = c.channel.QueueBind(queueFullName, bindingKey, exchangeFullName, false, nil)
	if err != nil {
		log.Errorln("Consumer - Failed to bind a queue ", err)
		return
	}

	c.queuesLoaded[exchangeFullName] = true

	return
}

// Consumer associates a function to receive messages from the queue.
func Consumer(exchangeName, queueSuffixName, typeName, consumerSuffixTag string, actionFunction function) (err error) {
	log.Println("Consumer - Creating a new consumer")

	err = c.connect()
	if err != nil {
		return
	}

	err = c.createChannel()
	if err != nil {
		return
	}

	_, err = c.createExchangeAndQueue(exchangeName, typeName, queueSuffixName)
	if err != nil {
		return
	}

	deliveries, err := c.announceQueue(exchangeName, queueSuffixName, typeName)
	if err != nil {
		return
	}

	return c.handle(deliveries, exchangeName, queueSuffixName, typeName, actionFunction)
}

func (c *consumer) reConnect(exchangeName, queueSuffixName, typeName string) (deliveries <-chan amqp.Delivery, err error) {
	log.Printf("Consumer - Reconnecting, waiting a few seconds.")
	time.Sleep(30 * time.Second)

	if err = c.connect(); err != nil {
		log.Printf("Consumer - Could not connect in reconnect call: %v", err.Error())
		return nil, err
	}

	deliveries, err = c.announceQueue(exchangeName, queueSuffixName, typeName)
	if err != nil {
		log.Errorln("Consumer - Failed to reconnect: ", err)
		return nil, errors.New("Couldn't connect")
	}
	return
}

func (c *consumer) announceQueue(exchangeName, queueSuffixName, typeName string) (deliveries <-chan amqp.Delivery, err error) {
	log.Debugln("Consumer - Announcing the queue of the consumer")

	consumerTag := rabbitmq.GetConsumerTag(exchangeName, queueSuffixName, "")
	queueFullName := rabbitmq.GetQueueFullName(exchangeName, queueSuffixName, typeName)

	log.Debugln("Consumer - Starting Consume  tag: ", consumerTag)
	deliveries, err = c.channel.Consume(queueFullName, consumerTag, false, false, false, false, nil)
	if err != nil {
		log.Errorln("Consumer - Failed to register a consumer ", err)
		return
	}
	return
}

func (c *consumer) handle(deliveries <-chan amqp.Delivery, exchangeName, queueSuffixName, typeName string, actionFunction function) (err error) {
	log.Debugln("Consumer - Handling the messages")

	for {
		c.doneThreads = make([]chan bool, c.threads)
		for i := 0; i < c.threads; i++ {
			go c.callingExternalFunc(deliveries, i, actionFunction)
		}

		if <-c.done != nil {
			c.terminateOldWorkers()
			deliveries, err = c.reConnect(exchangeName, queueSuffixName, typeName)
			if err != nil {
				log.Errorln("Consumer - Reconnecting Error: ", err)
				return err
			}
		}
		log.Println("Consumer - Reconnected... possibly")
	}
}

func (c *consumer) terminateOldWorkers() {
	for i := 0; i < c.threads; i++ {
		log.Println("Consumer - ===============================================", i)
		log.Println("Consumer - ===============================================", i)
		log.Println("Consumer - ===============================================", i)
		c.doneThreads[i] <- true
	}
}

func (c *consumer) callingExternalFunc(delivery <-chan amqp.Delivery, i int, actionFunction function) {
	log.Debugln("Consumer - Calling the external func, thread: ", i)
	c.doneThreads[i] = make(chan bool)
	for {
		select {
		case <-c.doneThreads[i]:
			return
		case d := <-delivery:
			err := actionFunction(d.Body)
			if err != nil {
				log.Println("Consumer - Failed to deliver the body ", err)
			}

			err = d.Ack(false)
			if err != nil {
				log.Errorln("Consumer - Failed to ack the msg ", err)
				// c.doneThreads[i] <- true
				c.done <- err
				return
			}
			log.Println("Consumer - Committed")
		}
	}
}
