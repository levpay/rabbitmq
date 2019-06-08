package rabbitmq

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/nuveo/log"
)

type config struct {
	URL string
	Env string
}

// Config contains configs infos
var Config *config

// Load sets the initial settings
func Load() {

	if Config != nil {
		return
	}
	Config = &config{
		URL: os.Getenv("CLOUDAMQP_URL"),
		Env: os.Getenv("ENV")}

	if Config.URL == "" {
		log.Fatal("CLOUDAMQP_URL is missing in the environments variables")
	}

	if Config.Env == "" {
		log.Fatal("ENV is missing in the environments variables")
	}
}

// GetQueueFullName returns the queue name referencing the exchange and the environment
func GetQueueFullName(exchangeName string, queueSuffixName string, typeName string) (queueFullName string) {

	if queueSuffixName == "" {
		queueSuffixName = "master"
	}

	if typeName != "" {
		typeName = fmt.Sprintf(":%s", typeName)
	}

	queueFullName = fmt.Sprintf("%s.%s.%s-queue%s", Config.Env, exchangeName, queueSuffixName, typeName)

	return
}

// GetExchangeFullName returns the exchange name referencing the environment
func GetExchangeFullName(exchangeName string, typeName string) (exchangeFullName string) {

	if typeName != "" {
		typeName = fmt.Sprintf(":%s", typeName)
	}

	exchangeFullName = fmt.Sprintf("%s.%s-exchange%s", Config.Env, exchangeName, typeName)

	return
}

// GetConsumerTag returns the name of the consumer referencing the name of the exchange, queue, and environment
func GetConsumerTag(exchangeName string, queueSuffixName string, consumerSuffixTag string) (consumerTag string) {

	if queueSuffixName == "" {
		queueSuffixName = "master"
	}

	if consumerSuffixTag == "" {
		u, _ := uuid.NewUUID()
		consumerSuffixTag = u.String()
	}

	consumerTag = fmt.Sprintf("%s.%s..%s.%s-consumer", Config.Env, exchangeName, queueSuffixName, consumerSuffixTag)

	return
}
