package rabbitmq

import (
	"fmt"

	"github.com/nuveo/log"

	"github.com/streadway/amqp"
)

type function func([]byte) error

// SimpleConsumer is a simple version of the Consumer that associates a function to receive messages from the queue
func SimpleConsumer(exchangeName string, typeName string, actionFunction function) (err error) {
	return Consumer(exchangeName, "", typeName, "", actionFunction)
}

// Consumer associates a function to receive messages from the queue.
func Consumer(exchangeName string, queueSuffixName string, typeName string, consumerSuffixTag string, actionFunction function) (err error) {
	exchangeFullName := GetExchangeFullName(exchangeName, typeName)
	queueFullName := GetQueueFullName(exchangeName, queueSuffixName, typeName)
	consumerTag := GetConsumerTag(exchangeName, queueSuffixName, "")

	log.Debugln("dialing", Config.URL)
	conn, err := amqp.Dial(Config.URL)
	if err != nil {
		log.Errorln("Failed to connect to RabbitMQ: ", err)
		return
	}
	go func() {
		fmt.Printf("Closing connection: %s", <-conn.NotifyClose(make(chan *amqp.Error)))
	}()
	defer conn.Close()

	log.Debugln("Got Connection, getting Channel")
	channel, err := conn.Channel()
	if err != nil {
		log.Errorln("Failed to open a channel: ", err)
		return
	}
	defer channel.Close()

	log.Debugln("Got Channel, declaring Exchange", exchangeFullName)
	err = channel.ExchangeDeclare(
		exchangeFullName, // name of the exchange
		"fanout",         // type
		true,             // durable
		false,            // delete when complete
		false,            // internal
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		log.Errorln("Failed to declare exchange: ", err)
		return
	}

	queue, err := channel.QueueDeclare(
		queueFullName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Errorln("Failed to declare a queue: ", err)
		return
	}

	bindingKey := fmt.Sprintf("%s-key", queue.Name)
	log.Debugln("Declared Queue (", queue.Name, " ", queue.Messages, " messages, ", queue.Consumers,
		" consumers), binding to Exchange (key ", bindingKey, ")")
	err = channel.QueueBind(
		queue.Name,       // name of the queue
		bindingKey,       // bindingKey
		exchangeFullName, // sourceExchange
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		log.Errorln("Failed to bind a queue: ", err)
		return
	}

	log.Debugln("Queue bound to Exchange, starting Consume (consumer tag " + consumerTag + ")")
	deliveries, err := channel.Consume(
		queue.Name,  // queue
		consumerTag, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		log.Errorln("Failed to register a consumer: ", err)
		return
	}

	go handle(deliveries, actionFunction)

	forever := make(chan bool)
	<-forever

	return
}

func handle(deliveries <-chan amqp.Delivery, actionFunction function) (err error) {
	for d := range deliveries {
		err := actionFunction(d.Body)
		if err != nil {
			log.Errorln("Failed to deliver the body: ", err)
		}

		if err == nil {
			d.Ack(false)
			log.Debugln("Committed")
		}
	}

	log.Errorln("handle: deliveries channel closed")

	return
}
