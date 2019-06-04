package rabbitmq_test

import (
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
	err := godotenv.Load(".env.testing")
	if err != nil {
		log.Fatal("Error loading .env.testing file")
	}

	debugS := os.Getenv("DEBUG")

	if debugS != "" {
		debug, err := strconv.ParseBool(debugS)
		if err != nil {
			log.Errorln("Failed to convert DEBUG value: ", err)
			return
		}

		log.DebugMode = debug
	}
}

func TestGetQueueFullName(t *testing.T) {

	t.Run("Test GetQueueFullName method with success", func(t *testing.T) {

		result := rabbitmq.GetQueueFullName("exchangeX", "work", "rangeY")

		expected := "testing.exchangeX.work.rangeY-queue"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetExchangeFullName(t *testing.T) {

	t.Run("Test GetExchangeFullName method with success", func(t *testing.T) {

		result := rabbitmq.GetExchangeFullName("exchangeX", "wait")

		expected := "testing.exchangeX.wait-exchange"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetConsumerTag(t *testing.T) {

	t.Run("Test GetConsumerTag method with success", func(t *testing.T) {

		result := rabbitmq.GetConsumerTag("exchangeX", "rangeY", "consumerZ")

		expected := "testing.exchangeX..rangeY.consumerZ-consumer"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}
