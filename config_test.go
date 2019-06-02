package rabbitmq_test

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/joho/godotenv"
	"github.com/levpay/rabbitmq"
	"github.com/nuveo/log"
)

func TestMain(m *testing.M) {
	testingEnv()
	rabbitmq.Load()
	os.Exit(m.Run())
}

func testingEnv() {

	fmt.Printf("Closing teste")

	err := godotenv.Load(".env.testing")
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

func TestGetQueueFullName(t *testing.T) {

	t.Run("Test GetQueueFullName method with success", func(t *testing.T) {

		result := rabbitmq.GetQueueFullName("exchangeX", "rangeY")

		expected := "EXCHANGE_exchangeX-QUEUE_rangeY-ENV_testing"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetExchangeFullName(t *testing.T) {

	t.Run("Test GetExchangeFullName method with success", func(t *testing.T) {

		result := rabbitmq.GetExchangeFullName("exchangeX")

		expected := "EXCHANGE_exchangeX-ENV_testing"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetConsumerTag(t *testing.T) {

	t.Run("Test GetConsumerTag method with success", func(t *testing.T) {

		result := rabbitmq.GetConsumerTag("exchangeX", "rangeY", "consumerZ")

		expected := "CONSUMER_consumerZ-EXCHANGE_exchangeX-QUEUE_rangeY-ENV_testing"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}
