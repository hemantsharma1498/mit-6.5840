package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RegisterWorkerReq struct {
	WorkerId int
}

type RegisterWorkerRes struct {
	WorkerId int
	NReduce  int
	Error    int
}

type JobStatusReq struct {
}

type SendPartitionsReq struct {
	IntermediateFiles []string
}

type SendPartitionsRes struct {
}

type SignalMapDoneReq struct {
	Filename          string
	WorkerId          int
	IntermediateFiles []string
}

type AssignFileReq struct {
	WorkerId int
}

type AssignFileRes struct {
	TaskId   int
	Filename string
}

type SignalMapDoneRes struct {
}

type JobStatusRes struct {
	IsFinished bool
}

type GetReduceTaskReq struct {
	WorkerId int
}

type GetReduceTaskRes struct {
	ReduceTaskId      int
	IntermediateFiles []string
	Message           int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
