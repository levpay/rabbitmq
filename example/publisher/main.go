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
	Retrier int
	Attempt int
}

func main() {
	// log.DebugMode = true
	rabbitmq.Load()

	u, _ := uuid.NewUUID()
	t := test{
		UUID:    u,
		Retrier: 3}

	log.Println(fmt.Sprintf("\n\nPublishing a msg %s with %d retrier.\n", t.UUID, t.Retrier))

	body, _ := json.Marshal(t)

	rabbitmq.Publisher("example", "test", body)
}
