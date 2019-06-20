package consumer

import (
	"os"
	"testing"

	"github.com/levpay/rabbitmq"
)

func TestMain(m *testing.M) {
	rabbitmq.LoadEnv("../.env.testing")
	os.Exit(m.Run())
}

// func TestDeclareGetType(t *testing.T) {
// 	t.Run("Test Declare.prepare = SUCCESS", func(t *testing.T) {
// 		m := &Declare{
// 			Type: "SUCCESS",
// 		}

// 		expected := "SUCCESS"
// 		if expected != result {
// 			t.Fatalf("Expect %v, got: %v", expected, result)
// 		}
// 	})
// 	t.Run("Test Declare.getType = empty", func(t *testing.T) {
// 		m := &Declare{}
// 		result := m.getType()
// 		expected := ""
// 		if expected != result {
// 			t.Fatalf("Expect %v, got: %v", expected, result)
// 		}
// 	})
// }
