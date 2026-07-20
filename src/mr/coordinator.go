package mr

import (
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

	mapPhaseMutex sync.Mutex
	mapPhase      map[*File]int

	intermediateExpected int
	intermediateReceived int
	intermediateMutex    sync.Mutex
	intermediateFilelist map[int][]string

	reduceTasks        map[int]int // 0=idle, 1=in-progress, 2=completed
	reduceTaskDoneCount int
	reduceMutex         sync.Mutex
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
	for k, v := range c.mapPhase {
		if v == -1 && k.Status == "IDLE" {
			c.mapPhase[k] = args.WorkerId
			reply.Filename = k.FileName
			reply.TaskId = mrtaskId
			filePtr := k
			go func() {
				time.Sleep(time.Second * 10)
				c.mapPhaseMutex.Lock()
				if filePtr.Status != "COMPLETED" {
					filePtr.Status = "IDLE"
					c.mapPhase[filePtr] = -1
				}
				c.mapPhaseMutex.Unlock()
			}()
			mrtaskId++
			break
		}
	}
	return nil
}

func (c *Coordinator) AssignReduceTask(args *GetReduceTaskReq, reply *GetReduceTaskRes) error {
	c.intermediateMutex.Lock()
	defer c.intermediateMutex.Unlock()

	c.mapPhaseMutex.Lock()
	mapDone := len(c.mapPhase) == 0 && c.intermediateReceived >= c.intermediateExpected
	c.mapPhaseMutex.Unlock()

	if !mapDone {
		reply.Message = 2
		return nil
	}

	for k, status := range c.reduceTasks {
		if status == 0 {
			c.reduceTasks[k] = 1
			reply.IntermediateFiles = c.intermediateFilelist[k]
			reply.ReduceTaskId = k
			reduceId := k
			go func() {
				time.Sleep(time.Second * 10)
				c.intermediateMutex.Lock()
				if c.reduceTasks[reduceId] == 1 {
					c.reduceTasks[reduceId] = 0
				}
				c.intermediateMutex.Unlock()
			}()
			return nil
		}
	}

	c.reduceMutex.Lock()
	allDone := c.reduceTaskDoneCount >= c.NReduce
	c.reduceMutex.Unlock()
	if allDone {
		reply.Message = 1
	} else {
		reply.Message = 2
	}
	return nil
}

func (c *Coordinator) MapJobUpdate(args *SignalMapDoneReq, reply *SignalMapDoneRes) error {
	c.mapPhaseMutex.Lock()
	for f := range c.mapPhase {
		if f.FileName == args.Filename {
			f.Status = "COMPLETED"
			delete(c.mapPhase, f)
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
	count := 0
	for _, file := range args.IntermediateFiles {
		reduceTaskNumber, err := splitReduceIdAndFilename(file)
		if err != nil {
			return err
		}
		if _, ok := c.reduceTasks[reduceTaskNumber]; !ok {
			c.reduceTasks[reduceTaskNumber] = 0
		}
		intermediateFiles := c.intermediateFilelist[reduceTaskNumber]
		found := false
		for _, v := range intermediateFiles {
			if v == file {
				found = true
				break
			}
		}
		if !found {
			c.intermediateFilelist[reduceTaskNumber] = append(c.intermediateFilelist[reduceTaskNumber], file)
			count++
		}
	}
	c.intermediateReceived += count
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

func (c *Coordinator) ReduceTaskDone(args *ReduceTaskDoneReq, reply *ReduceTaskDoneRes) error {
	c.intermediateMutex.Lock()
	if status, ok := c.reduceTasks[args.ReduceTaskId]; ok && status == 1 {
		c.reduceTasks[args.ReduceTaskId] = 2
	}
	c.intermediateMutex.Unlock()

	c.reduceMutex.Lock()
	c.reduceTaskDoneCount++
	c.reduceMutex.Unlock()
	return nil
}

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

func (c *Coordinator) Done() bool {
	c.reduceMutex.Lock()
	defer c.reduceMutex.Unlock()
	return len(c.mapPhase) == 0 && c.reduceTaskDoneCount >= c.NReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapPhase = make(map[*File]int)
	c.intermediateFilelist = make(map[int][]string)
	c.reduceTasks = make(map[int]int)
	c.NReduce = nReduce
	c.intermediateExpected = len(files) * nReduce
	for _, file := range files {
		f := &File{FileName: file, Status: "IDLE", MapTaskId: -1}
		c.mapPhase[f] = -1
	}

	c.server()
	return &c
}
