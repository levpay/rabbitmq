package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/levpay/rabbitmq/publisher"
	"github.com/nuveo/log"
)

type test struct {
	UUID    uuid.UUID
	Attempt int
}

var p *publisher.Publisher

func main() {
	log.DebugMode = true

	var err error
	p, err = publisher.New()
	if err != nil {
		log.Fatal("Failed to create publisher")
	}

	for i := 0; i < 15; i++ {
		go generation(i)
	}

	go func() {
		time.Sleep(1657 * time.Millisecond)
		for i := 0; i < 10; i++ {
			time.Sleep(1252 * time.Millisecond)
			p.Conn.Close()
			time.Sleep(2245 * time.Millisecond)
			// }
		}
	}()

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")

	<-make(chan bool)
}

func generation(e int) {
	log.Println("generation: ", e)
	for i := 0; i < 50; i++ {
		publish(i)
		log.Println("publish at thread: ", e, i)
	}
}

func publish(i int) {
	t := test{
		UUID: uuid.New(),
	}

	log.Println(fmt.Sprintf("\n\nPublishing a msg %s.\n", t.UUID))

	body, _ := json.Marshal(t)

	msg := &publisher.Declare{
		Exchange: "example",
		Body:     body,
	}
	err := p.Publish(msg)
	if err != nil {
		log.Errorln("Message not sent: ", err)
	}
}
