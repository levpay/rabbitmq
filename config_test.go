package rabbitmq_test

import (
	"os"
	"testing"

	"github.com/levpay/rabbitmq"
)

func TestMain(m *testing.M) {
	rabbitmq.LoadEnv("./.env.testing")
	os.Exit(m.Run())
}

func TestGetQueueFullNameWithReturn(t *testing.T) {
	t.Run("Test GetQueueFullName method with success", func(t *testing.T) {
		result := rabbitmq.GetQueueFullName("exchangeX", "rangeY", "ERROR")
		expected := "testing.exchangeX.rangeY-queue:ERROR"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})

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
		result := rabbitmq.GetExchangeFullName("exchangeX", "WAIT_10000")
		expected := "testing.exchangeX-exchange:WAIT_10000"
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
