package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type MAPF func(string, string) []KeyValue
type REDF func(string, []string) string

type WorkerData struct {
	WorkerId int
	Filename string
	nReduce  int
	MapFunc  MAPF
	RedFunc  REDF
}

type Server interface {
	// Locates coordinator and gets workerID needed for tasks
	Register() (string, error)

	// Returns map filename when given worker ID
	GetMapTask(workerId string) (string, int, error)

	// Persist map jobs
	SignalMapDone(filename string)

	MapJobStatus(jobType string)

	// get reduce file list and reduce job ID
	GetReduceTask() (int, []string, error)
}

//add a register function
// store worker ID given to every new worker

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := WorkerData{}
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample();

	//Get worder ID from the coordinator
	workerId, nReduce, error := Register(&w)
	if error != nil {
		fmt.Println("Error encountered while getting worder ID: ", error)
	}
	nReduce = 10
	w.WorkerId = workerId
	w.nReduce = nReduce
	w.MapFunc = mapf
	w.RedFunc = reducef

	for {
		//Get file for map task
		fileName, errorInt, error := GetMapTask(w.WorkerId)
		if errorInt != 0 {
			fmt.Println("Error: ", errorInt, error)
		}
		if len(fileName) == 0 {
			break
		}
		w.Filename = fileName
		_, err := os.ReadFile(fileName)
		if err != nil {
			fmt.Println("File not found: ", fileName)
		}
		content, err := os.ReadFile(fileName)
		if err != nil {
			fmt.Println(err)
		}
		kv := w.MapFunc(w.Filename, string(content))
		files := make([][]KeyValue, w.nReduce)
		for _, pair := range kv {
			key := pair.Key
			partition := ihash(key) % w.nReduce
			files[partition] = append(files[partition], pair)
		}
		for partition, file := range files {
			filename := "mr-" + string(w.WorkerId) + "-" + string(partition)
			tempfile, err := os.Create(filename)
			if err != nil {
				fmt.Println(err)
			}
			defer tempfile.Close()
			enc := json.NewEncoder(tempfile)
			for _, kv := range file {
				err := enc.Encode(&kv)
				if err != nil {
					fmt.Println(err)
				}
			}
			tempfile.Close()
		}
		SignalMapDone(&w)
		jobStatus := JobStatus("Map")
		if jobStatus {
			break
		}
	}
	fmt.Println("Map finished")

}

// Register
func Register(w *WorkerData) (int, int, error) {
	args := RegisterWorkerReq{}
	reply := RegisterWorkerRes{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		w.WorkerId = reply.WorkerId
		fmt.Println(reply.nReduce)
		w.nReduce = reply.nReduce
		fmt.Printf("worker ID:  %v\n", w.WorkerId)
		return w.WorkerId, w.nReduce, nil
	} else {
		fmt.Printf("call failed!\n")
		return -1, 0, errors.New("could not get worker registered")
	}
}

func GetMapTask(workerId int) (string, int, error) {
	args := AssignFileReq{}
	args.WorkerId = workerId
	reply := AssignFileRes{}
	ok := call("Coordinator.AssignFile", &args, &reply)
	if ok {
		return reply.Filename, 0, nil
	}
	return "", 1, nil
}

func SignalMapDone(w *WorkerData) {
	args := SignalMapDoneReq{}
	args.Filename = w.Filename
	reply := SignalMapDoneRes{}
	ok := call("Coordinator.MapJobUpdate", &args, &reply)
	if ok {
		fmt.Println("map job closed for file ", w.Filename)
	}
}

func JobStatus(job string) bool {
	args := JobStatusReq{}
	args.JobType = job
	reply := JobStatusRes{}
	ok := call("Coordinator.JobStatus", &args, &reply)
	if !ok {
		fmt.Println("Error in checking ", job, " job status")
	}
	if args.JobType == "Map" && reply.IsFinished {
		fmt.Println("Map job finished")
		return true
	} else if args.JobType == "Reduce" && reply.IsFinished {
		fmt.Println("Reduce job finished")
		return true
	}
	return false
}

// example function to show how to make an RPC call to the coordinator.
// rm mr-out*
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
