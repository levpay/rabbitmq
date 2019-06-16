package main

import (
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/levpay/rabbitmq"
	"github.com/nuveo/log"
)

type test struct {
	UUID    uuid.UUID
	Attempt int
}

func main() {
	log.DebugMode = true
	err := rabbitmq.LoadPublisher()
	if err != nil {
		log.Fatal("Failed to load publisher")
	}

	for i := 0; i < 1; i++ {
		go generation(i)
		// time.Sleep(time.Millisecond * 10000)
	}

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")

	<-make(chan bool)
}

func generation(i int) {
	log.Println("generation: ", i)
	for i := 0; i < 1; i++ {
		publish(i)
		// time.Sleep(time.Millisecond * 200)
		log.Println("publish at thread: ", i)
	}
}

func publish(i int) {
	u, _ := uuid.NewUUID()
	t := test{
		UUID: u,
	}

	log.Println(fmt.Sprintf("\n\nPublishing a msg %s.\n", t.UUID))

	body, _ := json.Marshal(t)

	// err := rabbitmq.PublisherWithDelay("example", 60000, body)
	err := rabbitmq.SimplePublisher("example", body)
	if err != nil {
		log.Errorln("Message not sent: ", err)
	}
}
