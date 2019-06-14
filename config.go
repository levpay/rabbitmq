package rabbitmq

import (
	"errors"
	"fmt"
	"os"

	"github.com/google/uuid"
)

type config struct {
	URL string
	Env string
}

// Config contains configs infos
var Config *config

// Load sets the initial settings
func Load() (err error) {
	if Config != nil {
		return
	}
	Config = &config{
		URL: os.Getenv("CLOUDAMQP_URL"),
		Env: os.Getenv("ENV"),
	}
	if Config.URL == "" {
		return errors.New("CLOUDAMQP_URL is missing in the environments variables")
	}
	if Config.Env == "" {
		return errors.New("ENV is missing in the environments variables")
	}
	return
}

// GetQueueFullName returns the queue name referencing the exchange and the environment
func GetQueueFullName(exchangeName string, queueSuffixName string, typeName string) string {
	if queueSuffixName == "" {
		queueSuffixName = "master"
	}

	if typeName != "" {
		typeName = fmt.Sprintf(":%s", typeName)
	}
	return fmt.Sprintf("%s.%s.%s-queue%s", Config.Env, exchangeName, queueSuffixName, typeName)
}

// GetExchangeFullName returns the exchange name referencing the environment
func GetExchangeFullName(exchangeName string, typeName string) string {
	if typeName != "" {
		typeName = fmt.Sprintf(":%s", typeName)
	}
	return fmt.Sprintf("%s.%s-exchange%s", Config.Env, exchangeName, typeName)
}

// GetConsumerTag returns the name of the consumer referencing the name of the exchange, queue, and environment
func GetConsumerTag(exchangeName string, queueSuffixName string, consumerSuffixTag string) string {
	if queueSuffixName == "" {
		queueSuffixName = "master"
	}
	if consumerSuffixTag == "" {
		u, _ := uuid.NewUUID()
		consumerSuffixTag = u.String()
	}
	return fmt.Sprintf("%s.%s..%s.%s-consumer", Config.Env, exchangeName, queueSuffixName, consumerSuffixTag)
}
