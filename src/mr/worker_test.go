package mr

import (
	"testing"
)

func HelloWorld(input string) string {
	return input
}

func TestHelloWorld(t *testing.T) {
	input := "Hello world"
	ans := HelloWorld(input)
	if ans != input {
		t.Errorf("input != output")
	}
}

func (c *Coordinator) TestRegisterWorker(b *testing.B) {
	c.server()

}
