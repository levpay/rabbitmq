package main

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/levpay/rabbitmq/consumer"
	"github.com/levpay/rabbitmq/publisher"
	"github.com/nuveo/log"
)

type testStruct struct {
	UUID    uuid.UUID
	Attempt int
}

var p *publisher.Publisher
var c *consumer.Consumer

func main() {
	log.DebugMode = true

	var err error
	c, err = consumer.New()
	if err != nil {
		log.Fatal("Failed to create consumer")
	}

	p, err = publisher.New()
	if err != nil {
		log.Fatal("Failed to create publisher")
	}

	go c.Consume("example", "", "", "", processMSG)

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")

	<-make(chan bool)
}

func processMSG(b []byte) (err error) {

	log.Println("Received a msg: ", string(b))

	var test testStruct
	err = json.Unmarshal(b, &test)
	if err != nil {
		return
	}

	test.Attempt++

	body, _ := json.Marshal(test)

	msg := &publisher.Message{
		Exchange: "example",
		Body:     body,
	}

	switch test.Attempt {
	case 1:
		log.Println("Queuing with a delay of 5 seconds -> ", test.UUID)
		err = p.PublishWithDelay(msg, 5000)
	case 2:
		log.Println("Queuing with a delay of 10 seconds. -> ", test.UUID)
		p.PublishWithDelay(msg, 10000)
	default:
		msg.Type = "SUCCESS"
		p.Publish(msg)
		log.Println("Success -> ", test.UUID)
	}

	return
}

func processMSGReturnSUCCESS(b []byte) (err error) {
	log.Println("Received a msg of SUCCESS: ", string(b))
	return
}
