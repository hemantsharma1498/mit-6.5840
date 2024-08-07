package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var id int = 0

var mrtaskId int = 0

type File struct {
	FileName  string
	MapTaskId int
	Status    string
}

type Coordinator struct {
	// Your definitions here.

	NReduce int

	// map worker table
	mapPhaseMutex sync.Mutex
	mapPhase      map[File]int // file : workerID

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

func (c *Coordinator) WorkerTimeoutChecker(file *File) {
	time.Sleep(time.Second * 10)
	if file.Status != "COMPLETED" {
		file.Status = "IDLE"
	}
}

func (c *Coordinator) AssignFile(args *AssignFileReq, reply *AssignFileRes) error {
	c.mapPhaseMutex.Lock()
	defer c.mapPhaseMutex.Unlock()
	for k, v := range c.mapPhase {
		if v == -1 && k.Status == "IDLE" {
			c.mapPhase[k] = args.WorkerId
			reply.Filename = k.FileName
			reply.TaskId = mrtaskId
			go c.WorkerTimeoutChecker(&k)
			mrtaskId++
			break
		}
	}
	return nil
}

func (c *Coordinator) AssignReduceTask(args *GetReduceTaskReq, reply *GetReduceTaskRes) error {
	if len(c.intermediateFilelist) > 0 {
		for k, v := range c.intermediateFilelist {
			reply.IntermediateFiles = v
			reply.ReduceTaskId = k
			break
		}
		delete(c.intermediateFilelist, reply.ReduceTaskId)
	} else {
		reply.Message = 1
	}
	return nil
}

func (c *Coordinator) MapJobUpdate(args *SignalMapDoneReq, reply *SignalMapDoneRes) error {
	c.mapPhaseMutex.Lock()
	for file := range c.mapPhase {
		if file.FileName == args.Filename {
			delete(c.mapPhase, file)
			break
		}
	}
	c.mapPhaseMutex.Unlock()
	return nil
}

func (c *Coordinator) JobStatus(args *JobStatusReq, reply *JobStatusRes) error {
	if len(c.mapPhase) == 0 {
		reply.IsFinished = true
	}
	return nil
}

func (c *Coordinator) ReceiveIntermediateFiles(args *SendPartitionsReq, reply *SendPartitionsRes) error {
	c.intermediateMutex.Lock()
	defer c.intermediateMutex.Unlock()
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
			if !found {
				c.intermediateFilelist[reduceTaskNumber] = append(c.intermediateFilelist[reduceTaskNumber], file)
			}
		} else {
			c.intermediateFilelist[reduceTaskNumber] = []string{file}
		}
	}
	return nil
}

func splitReduceIdAndFilename(filename string) (int, error) {
	splitFilename := strings.Split(filename, "-")
	reduceTaskNumber, err := strconv.Atoi(strings.Split(filename, "-")[len(splitFilename)-1])
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
	fmt.Println(len(c.mapPhase), len(c.intermediateFilelist))
	if len(c.mapPhase) == 0 && len(c.intermediateFilelist) == 0 {
		ret = true
	}
	return ret
}

func (c *Coordinator) MapReduceDone() bool {
	ret := false
	if len(c.mapPhase) == 0 && len(c.intermediateFilelist) == 0 {
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapPhase = make(map[File]int, 1)
	c.intermediateFilelist = make(map[int][]string, 1)
	c.NReduce = nReduce
	// Your code here.
	for _, file := range files {
		newFile := &File{FileName: file, Status: "IDLE", MapTaskId: -1}
		c.mapPhase[*newFile] = -1
	}

	c.server()

	for {
		done := c.MapReduceDone()
		time.Sleep(2000 * time.Millisecond)
		if done {
			break
		}
	}
	os.Exit(0)
	return &c
}
