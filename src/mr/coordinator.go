package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "math/rand"


type Coordinator struct {
	// Your definitions here.

	nReduce int

	// map worker table
	mapPhaseMutex sync.Mutex
	// files []string
	mapJobCount   int
	// workers []string
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

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	reply.WorkerID=RandStringBytes(5);
	return nil;
}

func (c *Coordinator)	 AssignFile(args *WorkerData, reply *WorkerData) error {
	for k, v := range c.mapPhase{
		if v == "0" {
			c.mapPhase[k]=args.WorkerId;
			args.fileName=k;
			break;
		}
	}

	return nil;
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce=nReduce;
	// Your code here.
	for _, file:= range files{
		c.mapPhase[file]="0";
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
