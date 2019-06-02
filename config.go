package rabbitmq

import (
	"fmt"
	"os"

	"github.com/nuveo/log"
)

type config struct {
	URL string
	Env string
}

// Config contains configs infos
var Config *config

// Load sets the initial settings.
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
func GetQueueFullName(exchangeName string, queueSuffixName string) (queueFullName string) {

	queueFullName = fmt.Sprintf("EXCHANGE_%s-QUEUE_%s-ENV_%s", exchangeName, queueSuffixName, Config.Env)

	return
}

// GetExchangeFullName returns the exchange name referencing the environment
func GetExchangeFullName(exchangeName string) (exchangeFullName string) {

	exchangeFullName = fmt.Sprintf("EXCHANGE_%s-ENV_%s", exchangeName, Config.Env)

	return
}

// GetConsumerTag returns the name of the consumer referencing the name of the exchange, queue, and environment
func GetConsumerTag(exchangeName string, queueSuffixName string, consumerSuffixTag string) (consumerTag string) {

	consumerTag = fmt.Sprintf("CONSUMER_%s-EXCHANGE_%s-QUEUE_%s-ENV_%s", consumerSuffixTag, exchangeName, queueSuffixName, Config.Env)

	return
}
