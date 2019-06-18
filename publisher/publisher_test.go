package publisher

import (
	"os"
	"testing"

	"github.com/levpay/rabbitmq"
)

func TestMain(m *testing.M) {
	rabbitmq.LoadEnv("../.env.testing")
	os.Exit(m.Run())
}

func TestMessageWait(t *testing.T) {
	t.Run("Test Message.wait with result true", func(t *testing.T) {
		m := &Message{
			Delay: 1000,
		}
		result := m.wait()
		expected := true
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Message.wait with result false", func(t *testing.T) {
		m := &Message{}
		result := m.wait()
		expected := false
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestMessageGetExpiration(t *testing.T) {
	t.Run("Test Message.getExpiration with result 1000", func(t *testing.T) {
		m := &Message{
			Delay: 1000,
		}
		result := m.getExpiration()
		expected := "1000"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Message.getExpiration with result empty", func(t *testing.T) {
		m := &Message{
			Delay: 0,
		}
		result := m.getExpiration()
		expected := ""
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestMessageGetArgs(t *testing.T) {
	t.Run("Test Message.getArgs with result with dlx", func(t *testing.T) {
		m := &Message{
			Exchange: "queue-of-destination",
			Delay:    1000,
		}
		result := m.getArgs()["x-dead-letter-exchange"]
		expected := "testing.queue-of-destination-exchange"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Message.getArgs with result without dlx", func(t *testing.T) {
		m := &Message{
			Exchange: "queue-of-destination",
		}
		result := m.getArgs()["x-dead-letter-exchange"]
		var expected interface{}
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestMessageGetType(t *testing.T) {
	t.Run("Test Message.getType = WAIT_1000", func(t *testing.T) {
		m := &Message{
			Type:  "SUCCESS",
			Delay: 1000,
		}
		result := m.getType()
		expected := "WAIT_1000"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Message.getType = SUCCESS", func(t *testing.T) {
		m := &Message{
			Type: "SUCCESS",
		}
		result := m.getType()
		expected := "SUCCESS"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Message.getType = empty", func(t *testing.T) {
		m := &Message{}
		result := m.getType()
		expected := ""
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestMessageGetMessageDLX(t *testing.T) {
	t.Run("Test Message.getMessageDLX = nil", func(t *testing.T) {
		m := &Message{}
		result := m.getMessageDLX()
		var expected *Message
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Message.getMessageDLX != nil", func(t *testing.T) {
		m := &Message{
			Exchange: "test",
			Delay:    100,
		}
		result := m.getMessageDLX().getExchangeFullName()
		expected := "testing.test-exchange"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestMessageGetDeadLetterExchange(t *testing.T) {
	t.Run("Test Message.getDeadLetterExchange = empty", func(t *testing.T) {
		m := &Message{
			Exchange: "test",
		}
		result := m.getDeadLetterExchange()
		expected := ""
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Message.getDeadLetterExchange != empty", func(t *testing.T) {
		m := &Message{
			Exchange: "test",
			Delay:    1000,
		}
		result := m.getDeadLetterExchange()
		expected := "testing.test-exchange"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}
