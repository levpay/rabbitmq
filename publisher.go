package rabbitmq

import (
	"fmt"

	"github.com/nuveo/log"

	"github.com/streadway/amqp"
)

// SimplePublisher adds a message in the exchange without options.
func SimplePublisher(exchangeName string, body []byte) (err error) {
	return publisherBase(exchangeName, "", "", body)
}

// Publisher adds a message in the exchange.
func Publisher(exchangeName string, typeName string, body []byte) (err error) {
	return publisherBase(exchangeName, typeName, "", body)
}

//PublisherWithDelay adds a message in the waiting exchange.
func PublisherWithDelay(exchangeName string, delay string, body []byte) (err error) {
	if delay == "" {
		delay = "10000"
	}
	return publisherBase(exchangeName, "", delay, body)
}

func publisherBase(exchangeName string, typeName string, delay string, body []byte) (err error) {

	wait := true
	if delay == "" || delay == "0" {
		wait = false
	}

	if delay != "" {
		typeName = fmt.Sprintf("WAIT_%s", delay)
	}

	exchangeFullName := GetExchangeFullName(exchangeName, typeName)

	log.Debugln("Dialing ", Config.URL)
	connection, err := amqp.Dial(Config.URL)
	if err != nil {
		log.Errorln("Failed to connect to RabbitMQ: ", err)
		return
	}
	defer connection.Close()

	log.Debugln("Got connection, getting channel")
	channel, err := connection.Channel()
	if err != nil {
		log.Errorln("Failed to open a channel: ", err)
		return
	}
	defer channel.Close()

	log.Debugln("Got Channel, declaring Exchange: ", exchangeFullName)
	err = channel.ExchangeDeclare(
		exchangeFullName, // name
		"fanout",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // noWait
		nil)              // arguments

	if err != nil {
		log.Errorln("Failed to declare exchange: ", err)
		return
	}

	log.Debugln("Enabling publishing confirms.")
	err = channel.Confirm(false)
	if err != nil {
		log.Errorln("Channel could not be put into confirm mode: ", err)
		return
	}

	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	defer confirmOne(confirms)

	defaultQueueFullName := GetQueueFullName(exchangeName, "", typeName)
	err = createDefaultQueue(channel, exchangeName, exchangeFullName, defaultQueueFullName, typeName, wait)
	if err != nil {
		log.Errorln("Failed to create default queue: ", err)
		return
	}

	log.Debugln("Declared exchange [", exchangeFullName, "], publishing ", len(body), "  body ", string(body))
	err = channel.Publish(
		exchangeFullName, // exchange
		"",               // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "application/json",
			ContentEncoding: "UTF-8",
			Body:            body,
			DeliveryMode:    amqp.Persistent,
			Expiration:      delay,
			Priority:        0}) // 0-9

	if err != nil {
		log.Errorln("Failed to publish a message: ", err)
		return
	}

	return
}

func createDefaultQueue(channel *amqp.Channel, exchangeName string, exchangeFullName string, defaultQueueName string, typeName string, wait bool) (err error) {

	var args amqp.Table
	if wait {
		args = make(amqp.Table)
		args["x-dead-letter-exchange"] = GetExchangeFullName(exchangeName, "")
	}

	queue, err := channel.QueueDeclare(
		defaultQueueName, // name
		true,             // durable
		false,            // delete when unused
		false,            // exclusive
		false,            // no-wait
		args)             // arguments

	if err != nil {
		log.Errorln("Failed to declare a queue: ", err)
		return
	}

	bindingKey := fmt.Sprintf("%s-key", queue.Name)
	log.Debugln("Declared Queue (", queue.Name, " ", queue.Messages, " messages, ", queue.Consumers, " consumers), binding to Exchange (key ", bindingKey, ")")
	err = channel.QueueBind(
		queue.Name,       // name of the queue
		bindingKey,       // bindingKey
		exchangeFullName, // sourceExchange
		false,            // noWait
		nil)              // arguments

	if err != nil {
		log.Errorln("Failed to bind a queue: ", err)
	}

	return
}

func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Debugln("waiting for confirmation of one publishing")

	if confirmed := <-confirms; confirmed.Ack {
		log.Debugln("confirmed delivery with delivery tag: ", confirmed.DeliveryTag)
	} else {
		log.Debugln("failed delivery of delivery tag: ", confirmed.DeliveryTag)
	}
}
