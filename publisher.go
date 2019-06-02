package rabbitmq

import (
	"fmt"

	"github.com/nuveo/log"

	"github.com/streadway/amqp"
)

func Publisher(exchangeName string, queueSuffixName string, body []byte) (err error) {
	exchangeFullName := GetExchangeFullName(exchangeName)
	queueFullName := GetQueueFullName(exchangeName, queueSuffixName)

	log.Debugln("Dialing ", URL)
	connection, err := amqp.Dial(URL)
	if err != nil {
		log.Errorln("Failed to connect to RabbitMQ", err)
		return
	}
	defer connection.Close()

	log.Debugln("Got connection, getting channel")
	channel, err := connection.Channel()
	if err != nil {
		log.Errorln("Failed to open a channel", err)
		return
	}
	defer channel.Close()

	log.Debugln("Got Channel, declaring Exchange", exchangeFullName)
	err = channel.ExchangeDeclare(
		exchangeFullName, // name
		"fanout",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		log.Errorln("Failed to declare exchange", err)
		return
	}

	log.Debugln("Enabling publishing confirms.")
	err = channel.Confirm(false)
	if err != nil {
		log.Errorln("Channel could not be put into confirm mode", err)
		return
	}

	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	defer confirmOne(confirms)

	queue, err := channel.QueueDeclare(
		queueFullName, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Errorln("Failed to declare a queue", err)
		return
	}

	bindingKey := fmt.Sprintf("%s-key", queue.Name)
	log.Debugln("Declared Queue (", queue.Name, " ", queue.Messages, " messages, ", queue.Consumers, " consumers), binding to Exchange (key ", bindingKey, ")")
	err = channel.QueueBind(
		queue.Name,       // name of the queue
		bindingKey,       // bindingKey
		exchangeFullName, // sourceExchange
		false,            // noWait
		nil,              // arguments
	)
	if err != nil {
		log.Errorln("Failed to bind a queue", err)
		return
	}

	log.Debugln("Declared exchange, publishing ", len(body), "  body ", string(body))
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
			DeliveryMode:    2, // 1=non-persistent, 2=persistent
			Priority:        0, // 0-9
		})
	if err != nil {
		log.Errorln("Failed to publish a message", err)
		return
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
