package consumer

import (
	"os"
	"testing"

	"github.com/levpay/rabbitmq/base"
	"github.com/nuveo/log"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	err := base.LoadEnv("../.env.testing")
	if err != nil {
		log.Fatal("Erro to load ", err)
	}
	os.Exit(m.Run())
}

func Test_handle(t *testing.T) {
	t.Run("Test handle = SUCCESS", func(t *testing.T) {
		d := &Declare{
			Exchange:    "test",
			QueueSuffix: "q",
		}
		d.Prepare()
		assert.Equal(t, "testing.test-exchange", d.GetExchangeFullName())
		assert.Equal(t, "testing.test.q-queue", d.GetQueueFullName())
	})
}
