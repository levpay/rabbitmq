package main

import (
	"encoding/json"
	"io"
	"strings"

	"github.com/google/uuid"
	"github.com/levpay/rabbitmq"
	"github.com/nuveo/log"
)

type testStruct struct {
	UUID    uuid.UUID
	Retrier int
	Attempt int
}

func convertJSONToTest(body string) (t *testStruct) {
	dec := json.NewDecoder(strings.NewReader(body))
	for {
		if err := dec.Decode(&t); err == io.EOF {
			break
		}
	}
	return
}

func processBoleto(b []byte) (err error) {

	bodyS := string(b)
	log.Println("Received a msg: ", string(bodyS))

	test := convertJSONToTest(bodyS)

	test.Attempt++

	body, _ := json.Marshal(test)

	switch test.Attempt {
	case 1:
		log.Println("Queuing with a delay of 5 seconds -> ", test.UUID)
		rabbitmq.PublisherWithDelay("example", "test", "5000", body)
	case 2:
		log.Println("Queuing with a delay of 10 seconds. -> ", test.UUID)
		rabbitmq.PublisherWithDelay("example", "test", "10000", body)
	default:
		log.Println("Success -> ", test.UUID)
	}

	return
}

func main() {
	// log.DebugMode = true
	rabbitmq.Load()

	go rabbitmq.Consumer("example", "test", "worker", processBoleto)

	log.Println(" [*] Waiting for messages. To exit press CTRL+C")

	forever := make(chan bool)
	<-forever
}
