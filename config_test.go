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

func TestGetQueueFullNameWithReturn(t *testing.T) {

	t.Run("Test GetQueueFullName method with success", func(t *testing.T) {

		result := rabbitmq.GetQueueFullName("exchangeX", "rangeY", "ERROR")

		expected := "testing.exchangeX.rangeY-queue:ERROR"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetQueueFullName(t *testing.T) {

	t.Run("Test GetQueueFullName method with success", func(t *testing.T) {

		result := rabbitmq.GetQueueFullName("exchangeX", "rangeY", "")

		expected := "testing.exchangeX.rangeY-queue"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetExchangeFullName(t *testing.T) {

	t.Run("Test GetExchangeFullName method with success", func(t *testing.T) {

		result := rabbitmq.GetExchangeFullName("exchangeX", "WAIT_10000ms")

		expected := "testing.exchangeX-exchange:WAIT_10000ms"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetConsumerTag(t *testing.T) {

	t.Run("Test GetConsumerTag method with success", func(t *testing.T) {

		result := rabbitmq.GetConsumerTag("exchangeX", "", "consumerZ")

		expected := "testing.exchangeX..master.consumerZ-consumer"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}
