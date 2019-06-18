package rabbitmq

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/nuveo/log"
)

type config struct {
	URL   string
	Env   string
	Debug bool
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

	Config.Debug, err = strconv.ParseBool(os.Getenv("DEBUG"))
	if err != nil {
		return fmt.Errorf("Failed to convert DEBUG value: %s", err)
	}
	log.DebugMode = Config.Debug

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
		consumerSuffixTag = uuid.New().String()
	}
	return fmt.Sprintf("%s.%s..%s.%s-consumer", Config.Env, exchangeName, queueSuffixName, consumerSuffixTag)
}

// LoadEnv loads a file with the environment variables
func LoadEnv(filename string) {
	err := godotenv.Load(filename)
	if err != nil {
		log.Fatal("Error loading .env.testing file ", err)
	}

	Load()
}
