package main

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/levpay/rabbitmq"
	"github.com/nuveo/log"
)

type testStruct struct {
	UUID    uuid.UUID
	Attempt int
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

	switch test.Attempt {
	case 1:
		log.Println("Queuing with a delay of 5 seconds -> ", test.UUID)
		err = rabbitmq.PublisherWithDelay("example", 5000, body)
	case 2:
		log.Println("Queuing with a delay of 10 seconds. -> ", test.UUID)
		err = rabbitmq.PublisherWithDelay("example", 10000, body)
	default:
		err = rabbitmq.Publisher("example", "SUCCESS", body)
		log.Println("Success -> ", test.UUID)
	}

	return
}

func main() {
	// log.DebugMode = true
	rabbitmq.Load()

	go rabbitmq.SimpleConsumer("example", "", processMSG)

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")

	forever := make(chan bool)
	<-forever
}
