package rabbitmq

import (
	"fmt"

	"github.com/nuveo/log"
	"github.com/streadway/amqp"
)

// Publisher adds a message in exchange.
func Publisher(exchangeName string, defaultQueueSuffixName string, body []byte) (err error) {
	exchangeFullName := GetExchangeFullName(exchangeName)
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
	log.Debugln("Got Channel, declaring Exchange", exchangeFullName)
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
	defaultQueueFullName := GetQueueFullName(exchangeName, defaultQueueSuffixName)
	err = createDefaultQueue(channel, exchangeFullName, defaultQueueFullName)
	if err != nil {
		log.Errorln("Failed to create default queue ", err)
		return
	}
	log.Debugln("Declared exchange, publishing ", len(body), "  body ", string(body))
	msg := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "UTF-8",
		Body:            body,
		DeliveryMode:    2,
		Priority:        0,
	}
	return channel.Publish(exchangeFullName, "", false, false, msg)
}

func createDefaultQueue(channel *amqp.Channel, exchangeFullName string, defaultQueueName string) (err error) {
	queue, err := channel.QueueDeclare(defaultQueueName, true, false, false, false, nil)
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
