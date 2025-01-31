package base

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

// MaxPriority is the maximum possible priority number in the queue
const MaxPriority = 10

type queuesLoaded struct {
	sync.Mutex
	m map[string]bool
}

type iAdapter interface {
	PosCreateChannel(*amqp.Channel) error
	PosReconnect() error
}

type ideclare interface {
	GetExchangeFullName() string
	GetQueueFullName() string
	GetQueueArgs() amqp.Table
	Prepare()
}

// Base contains the base struct of publisher/consumer
type Base struct {
	Conn         *amqp.Connection
	Channel      *amqp.Channel
	Closed       bool
	ErrorConn    chan *amqp.Error
	ErrorChannel chan *amqp.Error
	Adapter      iAdapter
	queuesLoaded queuesLoaded
	reconnecting bool
}

// Config is the func that connect and create the channel with the rabbitmq server
func (b *Base) Config() (err error) {
	b.queuesLoaded = queuesLoaded{
		m: make(map[string]bool),
	}

	err = b.connect()
	if err != nil {
		return
	}
	go b.reconnector(b.ErrorConn)
	go b.reconnector(b.ErrorChannel)

	return
}

func (b *Base) connect() (err error) {
	log.Debugln("connecting ", Config.URL)
	b.Conn, err = amqp.Dial(Config.URL)
	if err != nil {
		log.Errorln("Failed to connect to RabbitMQ ", err)
		return
	}
	log.Debugln("Got connection")

	err = b.createChannel()
	if err != nil {
		return
	}

	b.ErrorConn = make(chan *amqp.Error)
	b.Conn.NotifyClose(b.ErrorConn)
	b.ErrorChannel = make(chan *amqp.Error)
	b.Channel.NotifyClose(b.ErrorChannel)

	return b.Adapter.PosCreateChannel(b.Channel)
}

//WaitIfReconnecting waits until reconnected with the server
func (b *Base) WaitIfReconnecting() {
	for {
		if !b.reconnecting {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (b *Base) reconnector(errConnChan chan *amqp.Error) {
	for {
		errConn := <-errConnChan
		if b.Closed {
			return
		}
		if errConn != nil {
			log.Errorln("Reconnecting after of the error: ", errConn)
			b.TryReconnect()
		}
	}
}

// TryReconnect attempts to reconnect when the connection drops
func (b *Base) TryReconnect() (err error) {
	if b.reconnecting {
		return
	}

	b.reconnecting = true
	defer func() {
		b.reconnecting = false
	}()

	log.Debugln("Waiting a few seconds to reconnecting...")
	time.Sleep(500 * time.Millisecond)

	err = b.connect()
	if err != nil {
		log.Errorln("Could not connect in reconnect call: ", err)
		return
	}
	log.Println("Reconnected: ", b.Conn.IsClosed())

	return b.Adapter.PosReconnect()
}

func (b *Base) createChannel() (err error) {

	log.Debugln("Getting channel")

	b.Channel, err = b.Conn.Channel()
	if err != nil {
		log.Errorln("Failed to open a channel ", err)
		return
	}
	log.Debugln("Got Channel")
	return
}

// Prepare the data to connect to the exchange and queue
func (b *Base) Prepare(d ideclare) (err error) {
	b.WaitIfReconnecting()
	d.Prepare()

	return b.CreateExchangeAndQueue(d)
}

// CreateExchangeAndQueue creates the exchange and queue
func (b *Base) CreateExchangeAndQueue(d ideclare) (err error) {

	if b.exchangeAlreadyCreated(d) {
		return
	}

	b.queuesLoaded.Lock()
	defer b.queuesLoaded.Unlock()

	if b.exchangeAlreadyCreated(d) {
		return
	}

	log.Debugln("createExchangeAndQueue: ", d.GetExchangeFullName())
	err = b.Channel.ExchangeDeclare(d.GetExchangeFullName(), "fanout", true, false, false, false, nil)
	if err != nil {
		log.Errorln("Failed to declare exchange ", err)
		return
	}

	err = b.createQueueAndBinding(d)
	if err != nil {
		log.Errorln("Failed to bind the queue ", err)
		return
	}
	log.Debugln("Declared exchange: ", d.GetExchangeFullName())

	b.queuesLoaded.m[d.GetExchangeFullName()] = true

	return
}

func (b *Base) createQueueAndBinding(d ideclare) (err error) {
	args := d.GetQueueArgs()
	args["x-max-priority"] = MaxPriority

	queue, err := b.Channel.QueueDeclare(d.GetQueueFullName(), true, false, false, false, args)
	if err != nil {
		log.Errorln("Failed to declare a queue: ", err)
		return
	}

	bindingKey := fmt.Sprintf("%s-key", d.GetQueueFullName())
	log.Debugln("Declared Queue (", d.GetQueueFullName(), " ", queue.Messages,
		" messages, ", queue.Consumers, " consumers), binding to Exchange (key ", bindingKey, ")")
	return b.Channel.QueueBind(d.GetQueueFullName(), bindingKey, d.GetExchangeFullName(), false, nil)
}

func (b *Base) exchangeAlreadyCreated(d ideclare) bool {
	return b.queuesLoaded.m[d.GetExchangeFullName()]
}

// Close all connections definitely
func (b *Base) Close() (err error) {
	log.Println("Closing connection")
	b.Closed = true
	err = b.Channel.Close()
	if err != nil {
		return
	}
	return b.Conn.Close()
}

// GetQueueFullName returns the queue name referencing the exchange and the environment
func GetQueueFullName(exchangeName string, queueSuffixName string, typeName string) string {
	if queueSuffixName == "" {
		queueSuffixName = "master"
	}

	if typeName != "" {
		typeName = fmt.Sprintf(":%s", typeName)
	}
	return fmt.Sprintf("%s.%s.%s-queue%s", Config.Env, exchangeName, queueSuffixName, typeName)
}

// GetExchangeFullName returns the exchange name referencing the environment
func GetExchangeFullName(exchangeName string, typeName string) string {
	if typeName != "" {
		typeName = fmt.Sprintf(":%s", typeName)
	}
	return fmt.Sprintf("%s.%s-exchange%s", Config.Env, exchangeName, typeName)
}

// GetConsumerTag returns the name of the consumer referencing the name of the exchange, queue, and environment
func GetConsumerTag(exchangeName string, queueSuffixName string, consumerSuffixTag string) string {
	if queueSuffixName == "" {
		queueSuffixName = "master"
	}
	if consumerSuffixTag == "" {
		consumerSuffixTag = uuid.New().String()
	}
	return fmt.Sprintf("%s.%s..%s.%s-consumer", Config.Env, exchangeName, queueSuffixName, consumerSuffixTag)
}
