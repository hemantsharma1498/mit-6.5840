package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

var id int = 0

type Coordinator struct {
	// Your definitions here.

	NReduce int

	// map worker table
	mapPhaseMutex sync.Mutex
	mapFileCount  int
	taskIds       []int
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
	c.mapPhaseMutex.Lock()
	defer c.mapPhaseMutex.Unlock()
	reply.WorkerId = id
	reply.NReduce = c.NReduce
	id++
	return nil
}

func (c *Coordinator) AssignFile(args *AssignFileReq, reply *AssignFileRes) error {
	c.mapPhaseMutex.Lock()
	defer c.mapPhaseMutex.Unlock()
	if len(c.taskIds) > 0 {
		for k, v := range c.mapPhase {
			if v == 0 {
				c.mapPhase[k] = args.WorkerId
				reply.Filename = k
				fmt.Println(c.taskIds, len(c.taskIds)-1)
				reply.TaskId = c.taskIds[len(c.taskIds)-1]
				if len(c.taskIds) > 0 {
					c.taskIds = c.taskIds[:len(c.taskIds)-1]
				}
				fmt.Println("File given: ", reply.Filename)
				break
			}
		}
	}
	return nil
}

func (c *Coordinator) AssignReduceTask(args *GetReduceTaskReq, reply *GetReduceTaskRes) error {
	for k, v := range c.intermediateFilelist {
		reply.IntermediateFiles = v
		reply.ReduceTaskId = k
	}
	delete(c.intermediateFilelist, reply.ReduceTaskId)
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

func (c *Coordinator) ReceiveIntermediateFiles(args *SendPartitionsReq, reply *SendPartitionsRes) error {
	for _, file := range args.IntermediateFiles {
		reduceTaskNumber, err := splitReduceIdAndFilename(file)
		if err != nil {
			return err
		}
		intermediateFiles := c.intermediateFilelist[reduceTaskNumber]
		if len(intermediateFiles) > 0 {
			found := false
			for _, v := range intermediateFiles {
				if v == file {
					found = true
				}
			}
			fmt.Println(found)
			if !found {
				c.intermediateFilelist[reduceTaskNumber] = append(c.intermediateFilelist[reduceTaskNumber], file)
			}
		} else {
			c.intermediateFilelist[reduceTaskNumber] = []string{file}
		}
	}
	fmt.Println(c.intermediateFilelist)
	return nil
}

func splitReduceIdAndFilename(filename string) (int, error) {
	splitFilename := strings.Split(filename, "-")
	reduceTaskNumber, err := strconv.Atoi(splitFilename[len(splitFilename)-1])
	if err != nil {
		return 0, err
	}

	return reduceTaskNumber, nil
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
	c.intermediateFilelist = make(map[int][]string, 1)
	c.NReduce = nReduce
	c.mapFileCount = nReduce
	fmt.Println("Coordinator spun up")
	// Your code here.
	for _, file := range files {
		c.mapPhase[file] = 0
	}

	c.taskIds = rand.Perm(len(c.mapPhase))
	c.server()
	return &c
}
