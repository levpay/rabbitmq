package publisher

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/levpay/rabbitmq/base"
	"github.com/nuveo/log"
)

func TestMain(m *testing.M) {
	err := base.LoadEnv("../.env.testing")
	if err != nil {
		log.Fatal("Erro to load ", err)
	}
	os.Exit(m.Run())
}

func TestDeclareWait(t *testing.T) {
	t.Run("Test Declare.wait with result true", func(t *testing.T) {
		m := &Declare{
			Delay: 1000,
		}
		m.Prepare()

		result := m.wait
		expected := true
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Declare.wait with result false", func(t *testing.T) {
		m := &Declare{}
		result := m.wait
		expected := false
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestDeclareGetExpiration(t *testing.T) {
	t.Run("Test Declare.getExpiration with result 1000", func(t *testing.T) {
		m := &Declare{
			Delay: 1000,
		}
		m.Prepare()

		result := m.expiration
		expected := "1000"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Declare.getExpiration with result empty", func(t *testing.T) {
		m := &Declare{
			Delay: 0,
		}
		m.Prepare()

		result := m.expiration
		expected := ""
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestDeclareGetArgs(t *testing.T) {
	t.Run("Test Declare.getArgs with result with dlx", func(t *testing.T) {
		m := &Declare{
			Exchange: "queue-of-destination",
			Delay:    1000,
		}
		m.Prepare()

		result := m.queueArgs["x-dead-letter-exchange"]
		expected := "testing.queue-of-destination-exchange"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Declare.getArgs with result without dlx", func(t *testing.T) {
		m := &Declare{
			Exchange: "queue-of-destination",
		}
		m.Prepare()

		result := m.queueArgs["x-dead-letter-exchange"]
		var expected interface{}
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestDeclareGetType(t *testing.T) {
	t.Run("Test Declare.getType = WAIT_1000", func(t *testing.T) {
		m := &Declare{
			Type:  "SUCCESS",
			Delay: 1000,
		}
		m.Prepare()
		assert.Equal(t, m.GetTypeFullName(), "SUCCESS:WAIT_1000")
	})
	t.Run("Test Declare.getType = SUCCESS", func(t *testing.T) {
		m := &Declare{
			Type: "SUCCESS",
		}
		m.Prepare()

		result := m.Type
		expected := "SUCCESS"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Declare.getType = empty", func(t *testing.T) {
		m := &Declare{}
		m.Prepare()

		result := m.Type
		expected := ""
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}

func TestDeclareGetDeclareDLX(t *testing.T) {
	t.Run("Test Declare.getDeclareDLX = nil", func(t *testing.T) {
		m := &Declare{}
		m.Prepare()

		result := m.getDeclareDLX()
		var expected *Declare
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Declare.getDeclareDLX != nil", func(t *testing.T) {
		m := &Declare{
			Exchange: "test",
			Delay:    100,
		}
		m.Prepare()
		assert.Equal(t, m.getDeclareDLX().exchangeFullName, "testing.test-exchange")
	})
	t.Run("Test Declare.getDeclareDLX != nil", func(t *testing.T) {
		m := &Declare{
			Exchange: "test",
			Type:     "SUCCESS",
			Delay:    100,
		}
		m.Prepare()
		assert.Equal(t, m.getDeclareDLX().exchangeFullName, "testing.test-exchange:SUCCESS")
	})
}

func TestDeclareGetDeadLetterExchange(t *testing.T) {
	t.Run("Test Declare.getDLXExchangeName = empty", func(t *testing.T) {
		m := &Declare{
			Exchange: "test",
		}
		m.Prepare()

		result := m.getDLXExchangeName()
		expected := ""
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
	t.Run("Test Declare.getDLXExchangeName != empty", func(t *testing.T) {
		m := &Declare{
			Exchange: "test",
			Delay:    1000,
		}
		m.Prepare()

		result := m.getDLXExchangeName()
		expected := "testing.test-exchange"
		if expected != result {
			t.Fatalf("Expect %v, got: %v", expected, result)
		}
	})
}
