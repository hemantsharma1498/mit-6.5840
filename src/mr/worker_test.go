package mr

import (
	"fmt"
	"reflect"
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

func TestRegisterWorker(t *testing.T) {
	files := make([]string, 0)
	files = append(files, "pg-being_ernest.txt", "pg-dorian_gray.txt")
	MakeCoordinator(files, 10)
	args := RegisterWorkerReq{}
	reply := RegisterWorkerRes{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		fmt.Println("worker id: ", reply.WorkerId)
		typ := reflect.TypeOf(reply.WorkerId)
		if typ.Kind() != reflect.Int {
			t.Error("Not an integer")
		}
	}
}
