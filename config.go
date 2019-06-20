package rabbitmq

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

type config struct {
	URL   string
	Env   string
	Debug bool
}

// Config contains configs infos
var Config *config

type queuesLoaded struct {
	sync.Mutex
	m map[string]bool
}

// Base TODO
type Base struct {
	Conn         *amqp.Connection
	Channel      *amqp.Channel
	Closed       bool
	ErrorConn    chan *amqp.Error
	ErrorChannel chan *amqp.Error
	isPublisher  bool
	Confirms     chan amqp.Confirmation
	queuesLoaded queuesLoaded
	reconnecting bool

	FnReconnected func() error
	PrefetchCount int
}

// Config TODO
func (b *Base) Config(isPublisher bool) (err error) {
	load()

	b.isPublisher = isPublisher
	b.queuesLoaded = queuesLoaded{
		m: make(map[string]bool),
	}

	err = b.Connect()
	if err != nil {
		return
	}
	go b.reconnector(b.ErrorConn)
	go b.reconnector(b.ErrorChannel)

	return
}

// Connect TODO
func (b *Base) Connect() (err error) {

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

	err = b.notifyPublish()
	if err != nil {
		return
	}

	return b.qos()
}

func (b *Base) notifyPublish() (err error) {
	if !b.isPublisher {
		return
	}
	go func() {
		for res := range b.Channel.NotifyReturn(make(chan amqp.Return)) {
			fmt.Println("Notify Return ", res)
		}
	}()

	b.Confirms = b.Channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	log.Debugln("Enabling publishing confirms")
	err = b.Channel.Confirm(false)
	if err != nil {
		log.Errorln("Channel could not be put into confirm mode ", err)
		return
	}
	return
}

//WaitIfReconnecting TODO
func (b *Base) WaitIfReconnecting() {
	for {
		if b.reconnecting {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return
	}
}

func (b *Base) qos() (err error) {
	if b.isPublisher {
		return
	}

	err = b.Channel.Qos(b.PrefetchCount, 0, false)
	if err != nil {
		log.Errorln("Error setting qos: ", err)
		return
	}
	return
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

// TryReconnect TODO
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

	err = b.Connect()
	if err != nil {
		log.Errorln("Could not connect in reconnect call: ", err)
		return
	}
	log.Println("Reconnected: ", b.Conn.IsClosed())

	return b.callFnReconnected()
}

func (b *Base) callFnReconnected() (err error) {

	if b.FnReconnected == nil {
		return
	}

	err = b.FnReconnected()
	if err != nil {
		log.Errorln("Error calling func of reconnected", err)
		return
	}

	return
}

// createChannel TODO
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

type ideclare interface {
	GetExchangeFullName() string
	GetQueueFullName() string
	GetQueueArgs() amqp.Table
}

// CreateExchangeAndQueue TODO
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
		return
	}
	log.Debugln("Declared exchange: ", d.GetExchangeFullName())

	b.queuesLoaded.m[d.GetExchangeFullName()] = true

	return
}

func (b *Base) createQueueAndBinding(d ideclare) (err error) {
	queue, err := b.Channel.QueueDeclare(d.GetQueueFullName(), true, false, false, false, d.GetQueueArgs())
	if err != nil {
		log.Errorln("Failed to declare a queue: ", err)
		return
	}

	bindingKey := fmt.Sprintf("%s-key", d.GetQueueFullName())
	log.Debugln("Declared Queue (", d.GetQueueFullName(), " ", queue.Messages,
		" messages, ", queue.Consumers, " consumers), binding to Exchange (key ", bindingKey, ")")
	err = b.Channel.QueueBind(d.GetQueueFullName(), bindingKey, d.GetExchangeFullName(), false, nil)
	if err != nil {
		log.Errorln("Failed to bind the queue ", err)
		return
	}
	return
}

func (b *Base) exchangeAlreadyCreated(d ideclare) bool {
	return b.queuesLoaded.m[d.GetExchangeFullName()]
}

// Close TODO
func (b *Base) Close() (err error) {
	log.Println("Closing connection")
	b.Closed = true
	err = b.Channel.Close()
	if err != nil {
		return
	}
	return b.Conn.Close()
}

// load sets the initial settings
func load() (err error) {
	if Config != nil {
		return
	}
	Config = &config{
		URL: os.Getenv("CLOUDAMQP_URL"),
		Env: os.Getenv("ENV"),
	}
	if Config.URL == "" {
		return errors.New("CLOUDAMQP_URL is missing in the environments variables")
	}
	if Config.Env == "" {
		return errors.New("ENV is missing in the environments variables")
	}

	Config.Debug, err = strconv.ParseBool(os.Getenv("DEBUG"))
	if err != nil {
		return fmt.Errorf("Failed to convert DEBUG value: %s", err)
	}
	log.DebugMode = Config.Debug

	return
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

// LoadEnv loads a file with the environment variables
func LoadEnv(filename string) {
	err := godotenv.Load(filename)
	if err != nil {
		log.Fatal("Error loading .env.testing file ", err)
	}

	load()
}
