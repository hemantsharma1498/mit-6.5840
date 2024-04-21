package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.

	nReduce int

	// map worker table
	mapPhaseMutex sync.Mutex
	mapJobCount   int
	mapPhase      map[string]string // file : workerID

	// reduce jobs list <reduceID> : <filelist>
	intermediateMutex    sync.Mutex
	intermediateFilelist map[int][]string
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerReq, reply *RegisterWorkerRes) error {
	reply.WorkerID = RandStringBytes(5)
	return nil
}

func (c *Coordinator) AssignFile(args *AssignFileReq, reply *AssignFileRes) error {
	c.mapPhaseMutex.Lock()
	defer c.mapPhaseMutex.Unlock()
	for k, v := range c.mapPhase {
		if v == "0" {
			c.mapPhase[k] = args.WorkerId
			reply.Filename = k
			fmt.Println("File given: ", reply.Filename)
			break
		}
	}
	return nil
}

func (c *Coordinator) MapJobUpdate(args *SignalMapDoneReq, reply *SignalMapDoneRes) error {
	delete(c.mapPhase, args.Filename)
	return nil
}

func (c *Coordinator) JobStatus(args *JobStatusReq, reply *JobStatusRes) error {
	if args.JobType == "Map" {
		if len(c.mapPhase) == 0 {
			reply.IsFinished = true
			return nil
		} else {
			reply.IsFinished = false
			return nil
		}
	}
	//Add for reduce, or remove reduce section if not required
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapPhase = make(map[string]string, 1)
	c.nReduce = nReduce
	fmt.Println("Coordinator spun up")
	// Your code here.
	i := 1
	for _, file := range files {
		c.mapPhase[file] = "0"
		if i == 4 {
			break
		}
	}

	// 0. metawork
	//completed: input files, count getting printed
	// 1. register workers
	// give worker IDs to every new worker

	//map task- worker + job info
	//to start map task, require >0 registered workers

	// 2. do map tasks

	// 2*. ensure map phase finished

	// 3. do reduce tasks (can optimize)
	c.server()
	return &c
}
