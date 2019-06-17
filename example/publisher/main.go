package main

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/levpay/rabbitmq/publisher"
	"github.com/nuveo/log"
)

type test struct {
	UUID    uuid.UUID
	Attempt int
}

func main() {
	log.DebugMode = true
	err := publisher.LoadPublisher()
	if err != nil {
		log.Fatal("Failed to load publisher")
	}

	for i := 0; i < 100; i++ {
		go generation(i)
		// time.Sleep(time.Millisecond * 100)
	}

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")

	<-make(chan bool)
}

func generation(e int) {
	log.Println("generation: ", e)
	for i := 0; i < 100; i++ {
		publish(i)
		// time.Sleep(time.Millisecond * 200)
		log.Println("publish at thread: ", e, i)
	}
}

func publish(i int) {
	t := test{
		UUID: uuid.New(),
	}

	log.Println(fmt.Sprintf("\n\nPublishing a msg %s.\n", t.UUID))

	body, _ := json.Marshal(t)

	// err := rabbitmq.PublisherWithDelay("example", 60000, body)
	err := publisher.SimplePublisher("example", body)
	if err != nil {
		log.Errorln("Message not sent: ", err)
	}
}
