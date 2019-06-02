package rabbitmq

import (
	"testing"
)

func TestGetQueueFullName(t *testing.T) {

	t.Run("Test GetQueueFullName method with success", func(t *testing.T) {

		result := GetQueueFullName("exchangeX", "rangeY")

		expected := "EXCHANGE_exchangeX-QUEUE_rangeY-ENV_testing"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetExchangeFullName(t *testing.T) {

	t.Run("Test GetExchangeFullName method with success", func(t *testing.T) {

		result := GetExchangeFullName("exchangeX")

		expected := "EXCHANGE_exchangeX-ENV_testing"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}

func TestGetConsumerTag(t *testing.T) {

	t.Run("Test GetConsumerTag method with success", func(t *testing.T) {

		result := GetConsumerTag("exchangeX", "rangeY", "consumerZ")

		expected := "CONSUMER_consumerZ-EXCHANGE_exchangeX-QUEUE_rangeY-ENV_testing"
		if expected != result {
			t.Fatalf("Expect %s, got: %s", expected, result)
		}
	})
}
