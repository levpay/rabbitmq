package rabbitmq

import (
	"fmt"
	"strconv"

	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

// SimplePublisher adds a message in the exchange without options.
func SimplePublisher(exchangeName string, body []byte) (err error) {
	return publisherBase(exchangeName, "", 0, body)
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

func publisherBase(exchangeName string, typeName string, delay int64, body []byte) (err error) {

	wait := false
	if delay != 0 {
		wait = true
		typeName = fmt.Sprintf("WAIT_%v", delay)
	}

	exchangeFullName := GetExchangeFullName(exchangeName, typeName)

	log.Debugln("Dialing ", Config.URL)
	connection, err := amqp.Dial(Config.URL)
	if err != nil {
		log.Errorln("Failed to connect to RabbitMQ ", err)
		return
	}
	defer connection.Close()
	log.Debugln("Got connection, getting channel")
	channel, err := connection.Channel()
	if err != nil {
		log.Errorln("Failed to open a channel ", err)
		return
	}
	defer channel.Close()

	log.Debugln("Got Channel, declaring Exchange: ", exchangeFullName)
	err = channel.ExchangeDeclare(exchangeFullName, "fanout", true, false, false, false, nil)
	if err != nil {
		log.Errorln("Failed to declare exchange ", err)
		return
	}
	log.Debugln("Enabling publishing confirms.")
	if err = channel.Confirm(false); err != nil {
		log.Errorln("Channel could not be put into confirm mode ", err)
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
	msg := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		Body:            body,
		DeliveryMode:    amqp.Persistent,
		Expiration:      getExpiration(delay),
		Priority:        0,
	}
	return channel.Publish(exchangeFullName, "", false, false, msg)
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
		log.Errorln("Failed to declare a queue ", err)
		return
	}

	bindingKey := fmt.Sprintf("%s-key", queue.Name)
	log.Debugln("Declared Queue (",
		queue.Name, " ", queue.Messages, " messages, ", queue.Consumers,
		" consumers), binding to Exchange (key ", bindingKey, ")")
	return channel.QueueBind(queue.Name, bindingKey, exchangeFullName, false, nil)
}

func confirmOne(confirms <-chan amqp.Confirmation) {
	log.Debugln("waiting for confirmation of one publishing")
	confirmed := <-confirms
	if confirmed.Ack {
		log.Debugln("confirmed delivery with delivery tag: ", confirmed.DeliveryTag)
		return
	}
	log.Debugln("failed delivery of delivery tag: ", confirmed.DeliveryTag)
}
