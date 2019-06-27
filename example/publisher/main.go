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
	// log.DebugMode = true
	rabbitmq.Load()

	u, _ := uuid.NewUUID()
	t := test{
		UUID: u,
	}

	log.Println(fmt.Sprintf("\n\nPublishing a msg %s.\n", t.UUID))

	body, _ := json.Marshal(t)

	err := rabbitmq.SimplePublisher("example", body)
	if err != nil {
		log.Errorln("Message not sent: ", err)
	}
}
