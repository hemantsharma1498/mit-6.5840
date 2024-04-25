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

	NReduce int

	// map worker table
	mapPhaseMutex sync.Mutex
	mapFileCount  int
	mapPhase      map[string]int // file : workerID

	// reduce jobs list <reduceID> : <filelist>
	intermediateMutex    sync.Mutex
	intermediateFilelist map[int][]string
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
	id := rand.Intn(20)
	for _, v := range c.mapPhase {
		if id == v {
			id = rand.Intn(20)
		}
	}
	reply.WorkerId = id
	reply.NReduce = c.NReduce
	return nil
}

func (c *Coordinator) AssignFile(args *AssignFileReq, reply *AssignFileRes) error {
	c.mapPhaseMutex.Lock()
	defer c.mapPhaseMutex.Unlock()
	for k, v := range c.mapPhase {
		if v == 0 {
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
	//hardcoding nReduce to be 1
	c.mapPhase = make(map[string]int, 1)
	c.NReduce = nReduce
	c.mapFileCount = nReduce
	fmt.Println("Coordinator spun up")
	// Your code here.
	i := 1
	for _, file := range files {
		c.mapPhase[file] = 0
		if i == 4 {
			break
		}
	}

	c.server()
	return &c
}
