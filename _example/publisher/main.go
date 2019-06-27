package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/levpay/rabbitmq/base"
	"github.com/levpay/rabbitmq/publisher"
	"github.com/nuveo/log"
)

type test struct {
	UUID    uuid.UUID
	Attempt int
	Item    int
}

var p *publisher.Publisher

func init() {
	err := base.Load()
	if err != nil {
		return
	}
}

func main() {
	log.DebugMode = true

	var err error
	p, err = publisher.New()
	if err != nil {
		log.Fatal("Failed to create publisher")
	}

	go generation()

	// go testReconnect()

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")

	<-make(chan bool)
}

func generation() {
	for e := 0; e < 30; e++ {
		go func(e int) {
			log.Println("generation: ", e)
			for i := 0; i < 100; i++ {
				publish(i)
				log.Println("publish at thread: ", e, i)
			}
		}(e)
	}
}

func publish(i int) {
	t := test{
		UUID: uuid.New(),
		Item: i,
	}

	body, _ := json.Marshal(t)
	msg := &publisher.Declare{
		Exchange: "example",
		Body:     body,
		Priority: uint8(i),
	}
	err := p.Publish(msg)
	if err != nil {
		log.Errorln("Message not sent: ", err)
	}
	log.Println(fmt.Sprintf("\n\nPublishing a msg %s.\n", t.UUID))
}

func testReconnect() {
	time.Sleep(1657 * time.Millisecond)
	for i := 0; i < 10; i++ {
		time.Sleep(1000 * time.Millisecond)
		p.Conn.Close()
		time.Sleep(2000 * time.Millisecond)
	}
}
