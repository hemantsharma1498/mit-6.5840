package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var taskId int

type MAPF func(string, string) []KeyValue
type REDF func(string, []string) string

type WorkerData struct {
	WorkerId          int
	Filename          string
	NReduce           int
	IntermediateFiles []string
	MapFunc           MAPF
	RedFunc           REDF
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

/*
func newWorker() Server {
	return &WorkerData{}
}
*/

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := WorkerData{}

	//Get worder ID from the coordinator
	error := Register(&w)
	if error != nil {
		fmt.Println("Error encountered while getting worder ID: ", error)
	}
	w.MapFunc = mapf
	w.RedFunc = reducef

	for {
		//Get file for map task
		filename, errorInt, error := GetMapTask(w.WorkerId)
		if errorInt != 0 {
			fmt.Println("Error: ", errorInt, error)
		}
		if len(filename) == 0 {
			break
		}
		kv := MapTask(&w, filename)
		SaveIntermediateFiles(&w, kv, w.NReduce)
		SignalMapDone(&w)
		time.Sleep(300 * time.Millisecond)
	}
	err := SendIntermediateFiles(w.IntermediateFiles)
	if err != nil {
		fmt.Println(err)
	}

	for {
		reduceTaskId, intermediateFiles, reduceStatus, err := GetReduceTask(w.WorkerId)
		if err != nil {
			fmt.Println(err)
		}
		if reduceStatus == 1 {
			break
		}
		if len(intermediateFiles) == 0 && reduceStatus == 0 {
			fmt.Println("Failed getting intermediate files for reduce task: ", reduceTaskId)
		}

		intermediate := []KeyValue{}
		for _, v := range intermediateFiles {
			file, err := os.Open(v)
			if err != nil {
				fmt.Println(err)
			}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			sort.Sort(ByKey(intermediate))
		}

		err = runReduce(&w, intermediate, reduceTaskId)
		if err != nil {
			fmt.Println(err)
		}
	}
	os.Exit(0)
}

func runReduce(w *WorkerData, intermediate []KeyValue, reduceTaskId int) error {
	reduceTaskIdString := strconv.Itoa(reduceTaskId)
	outputFile, err := os.Create("mr-out-" + reduceTaskIdString)
	if err != nil {
		return err
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.RedFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	return nil
}

func GetReduceTask(workerId int) (int, []string, int, error) {
	args := GetReduceTaskReq{}
	args.WorkerId = workerId
	reply := GetReduceTaskRes{}

	ok := call("Coordinator.AssignReduceTask", &args, &reply)
	if ok {
		return reply.ReduceTaskId, reply.IntermediateFiles, reply.Message, nil
	}
	return -1, nil, reply.Message, errors.New("failed to get reduce task")
}

func Register(w *WorkerData) error {
	args := RegisterWorkerReq{}
	reply := RegisterWorkerRes{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		w.WorkerId = reply.WorkerId
		w.NReduce = reply.NReduce
		return nil
	} else {
		fmt.Printf("call failed!\n")
		return errors.New("could not get worker registered")
	}
}

func GetMapTask(workerId int) (string, int, error) {
	args := AssignFileReq{}
	args.WorkerId = workerId
	reply := AssignFileRes{}
	ok := call("Coordinator.AssignFile", &args, &reply)
	if ok {
		taskId = reply.TaskId
		return reply.Filename, 0, nil
	}
	return "", 1, nil
}

func SignalMapDone(w *WorkerData) {
	args := SignalMapDoneReq{}
	args.Filename = w.Filename
	args.WorkerId = w.WorkerId
	args.IntermediateFiles = w.IntermediateFiles
	reply := SignalMapDoneRes{}
	call("Coordinator.MapJobUpdate", &args, &reply)
}

func SaveIntermediateFiles(w *WorkerData, kv []KeyValue, nReduce int) error {
	files := make([][]KeyValue, nReduce)
	for _, pair := range kv {
		key := pair.Key
		partition := ihash(key) % nReduce
		files[partition] = append(files[partition], pair)
	}
	for partition, file := range files {
		filename := "mr-" + strconv.Itoa(taskId) + "-" + strconv.Itoa(partition)
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
		w.IntermediateFiles = append(w.IntermediateFiles, filename)
	}
	return nil
}

func SendIntermediateFiles(files []string) error {
	args := SendPartitionsReq{}
	res := SendPartitionsRes{}
	args.IntermediateFiles = files
	ok := call("Coordinator.ReceiveIntermediateFiles", &args, &res)
	if !ok {
		return errors.New("sending partitions failed")
	}
	return nil
}

func MapTask(w *WorkerData, filename string) []KeyValue {
	w.Filename = filename
	_, err := os.ReadFile(filename)
	if err != nil {
		fmt.Println("File not found: ", filename)
	}
	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Println(err)
	}
	kv := w.MapFunc(w.Filename, string(content))
	return kv
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
