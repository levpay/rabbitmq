package main

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/levpay/rabbitmq/base"
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

func init() {
	err := base.Load()
	if err != nil {
		return
	}
}

func main() {
	log.DebugMode = true

	var err error
	c, err = consumer.New(50, 3)
	if err != nil {
		log.Fatal("Failed to create consumer")
	}

	p, err = publisher.New()
	if err != nil {
		log.Fatal("Failed to create publisher")
	}

	d := &consumer.Declare{
		Exchange:       "example",
		ActionFunction: processMSG,
	}
	go c.Consume(d)

	success := &consumer.Declare{
		Exchange:       "example",
		Type:           "SUCCESS",
		ActionFunction: processMSGReturnSUCCESS,
	}
	go c.Consume(success)

	// go testConsumerReconnect()

	// go testPublisherReconnect()

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

	d := &publisher.Declare{
		Exchange: "example",
		Body:     body,
	}

	switch test.Attempt {
	case 1:
		log.Println("Queuing with a delay of 10 seconds -> ", test.UUID)
		d.Priority = 1
		err = p.PublishWithDelay(d, 10000)
	case 2:
		d.Priority = 2
		log.Println("Queuing with a delay of 30 seconds. -> ", test.UUID)
		p.PublishWithDelay(d, 30000)
	default:
		d.Type = "SUCCESS"
		p.Publish(d)
		log.Println("Success -> ", test.UUID)
	}

	return
}

func processMSGReturnSUCCESS(b []byte) (err error) {
	log.Println("Received a msg of SUCCESS: ", string(b))
	return
}

func testConsumerReconnect() {
	time.Sleep(1657 * time.Millisecond)
	for i := 0; i < 10; i++ {
		time.Sleep(2000 * time.Millisecond)
		c.Conn.Close()
		time.Sleep(2500 * time.Millisecond)
	}
}

func testPublisherReconnect() {
	time.Sleep(1657 * time.Millisecond)
	for i := 0; i < 10; i++ {
		time.Sleep(1250 * time.Millisecond)
		p.Conn.Close()
		time.Sleep(3250 * time.Millisecond)
	}
}
