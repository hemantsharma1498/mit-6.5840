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
	WorkerId string
	FileName string
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
	workerId, error := Register(&w)
	if error != nil {
		fmt.Println("Error encountered while getting worder ID: ", error)
	}
	w.WorkerId = workerId
	w.MapFunc = mapf
	w.RedFunc = reducef

	for {
		//Get file for map task
		fileName, errorInt, error := GetMapTask(&w, w.WorkerId)
		if errorInt != 0 {
			fmt.Println("Error: ", errorInt, error)
		}
		w.FileName = fileName
		fmt.Printf(w.FileName)
		content, err := os.ReadFile("../main/" + fileName)
		if err != nil {
			fmt.Println(err)
		}
		kv := w.MapFunc(w.FileName, string(content))
		var mapFileName = "mr-" + w.WorkerId + "-" + w.FileName
		file, err := os.Create(mapFileName)
		if err != nil {
			fmt.Println(err)
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range kv {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println(err)
			}
		}
		file.Close()
		SignalMapDone(&w, w.FileName)
		jobStatus := JobStatus("Map")
		if jobStatus {
			break
		}
	}

}

// Register
func Register(w *WorkerData) (string, error) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		// reply.Y should be 100.
		w.WorkerId = reply.WorkerID
		fmt.Printf("worker ID:  %v\n", w.WorkerId)
		return w.WorkerId, nil
	} else {
		fmt.Printf("call failed!\n")
		return "", errors.New("could not get worker registered")
	}
}

func GetMapTask(w *WorkerData, workerId string) (string, int, error) {

	ok := call("Coordinator.AssignFile", &w, &w)
	if ok {
		return w.FileName, 0, nil
	}
	return "", 1, nil
}

func SignalMapDone(w *WorkerData, fileName string) {

	ok := call("Coordinator.MapJobUpdate", &w, &w)
	if ok {
		fmt.Println("map job closed for file ", fileName)
	}
}

func JobStatus(job string) bool {
	args := MrJobStatus{}
	args.JobType = job
	ok := call("Coordinator.JobStatus", &args, &args)
	if !ok {
		fmt.Println("Error in checking ", job, " job status")
	}
	if args.JobType == "Map" && args.Map {
		fmt.Println("Map job finished")
		return true
	} else if args.JobType == "Reduce" && args.Reduce {
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
