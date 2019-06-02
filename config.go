package rabbitmq

import (
	"fmt"
	"os"
	"strconv"

	"github.com/joho/godotenv"
	"github.com/nuveo/log"
)

// Global variables
var (
	URL string
	env string
)

func init() {
	testingEnv()

	URL = os.Getenv("CLOUDAMQP_URL")
	if URL == "" {
		log.Fatal("CLOUDAMQP_URL is missing in the environments variables")
	}

	env = os.Getenv("ENV")
	if env == "" {
		log.Fatal("ENV is missing in the environments variables")
	}
}

func GetQueueFullName(exchangeName string, queueSuffixName string) (queueFullName string) {

	queueFullName = fmt.Sprintf("EXCHANGE_%s-QUEUE_%s-ENV_%s", exchangeName, queueSuffixName, env)

	return
}

func GetExchangeFullName(exchangeName string) (exchangeFullName string) {

	exchangeFullName = fmt.Sprintf("EXCHANGE_%s-ENV_%s", exchangeName, env)

	return
}

func GetConsumerTag(exchangeName string, queueSuffixName string, consumerSuffixTag string) (consumerTag string) {

	consumerTag = fmt.Sprintf("CONSUMER_%s-EXCHANGE_%s-QUEUE_%s-ENV_%s", consumerSuffixTag, exchangeName, queueSuffixName, env)

	return
}

func testingEnv() {
	testingS := os.Getenv("TESTING")

	if testingS == "" {
		return
	}

	testing, err := strconv.ParseBool(testingS)
	if err != nil {
		log.Fatal("Failed to convert TESTING value", err)
	}

	if !testing {
		return
	}

	err = godotenv.Load(".env.testing")
	if err != nil {
		log.Fatal("Error loading .env.testing file")
	}

	debugS := os.Getenv("DEBUG")

	if debugS != "" {
		debug, err := strconv.ParseBool(debugS)
		if err != nil {
			log.Errorln("Failed to convert DEBUG value", err)
			return
		}

		log.DebugMode = debug
	}

}
