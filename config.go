package rabbitmq

import (
	"errors"
	"fmt"
	"os"
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
func GetQueueFullName(exchangeName string, queueSuffixName string) (queueFullName string) {
	return fmt.Sprintf("ENV_%s-EXCHANGE_%s-QUEUE_%s", Config.Env, exchangeName, queueSuffixName)
}

// GetExchangeFullName returns the exchange name referencing the environment
func GetExchangeFullName(exchangeName string) (exchangeFullName string) {
	return fmt.Sprintf("ENV_%s-EXCHANGE_%s", Config.Env, exchangeName)
}

// GetConsumerTag returns the name of the consumer referencing the name of the exchange, queue, and environment
func GetConsumerTag(exchangeName string, queueSuffixName string, consumerSuffixTag string) (consumerTag string) {
	return fmt.Sprintf("ENV_%s-EXCHANGE_%s-QUEUE_%s-CONSUMER_%s", Config.Env, exchangeName, queueSuffixName, consumerSuffixTag)
}
