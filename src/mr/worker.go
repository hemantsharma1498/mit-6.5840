package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "errors"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type WorkerData struct {
	WorkerId string
	fileName string
}

type Server interface {
	// Locates coordinator and gets workerID needed for tasks
	Register() (string, error)

	// Returns map filename when given worker ID
	GetMapTask(string) (string, int, error)

	// Persist map jobs
	SignalMapDone(filename string)

	// get reduce file list and reduce job ID
	GetReduceTask() (int, []string, error)
}

//add a register function
	// store worker ID given to every new worker

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}



//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	w := WorkerData{};
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample();

	//Get worder ID from the coordinator
	workerId, error:=Register(&w);
	if error != nil {
		fmt.Println("Error encountered while getting worder ID: ", error)
	}
	w.WorkerId=workerId;
	//Get file for map task
	fileName, errorInt, error:=GetMapTask(&w, w.WorkerId);
	if errorInt != 0 {
		fmt.Printf("Error: ", errorInt);
	}
	fmt.Printf("Worker with id ", w.WorkerId, " has file named ", fileName);


}

//Register
func Register(w *WorkerData) (string, error){
	args:=RegisterWorkerArgs{};
	reply:=RegisterWorkerReply{};

	ok := call("Coordinator.RegisterWorker", &args, &reply);
	if ok {
		// reply.Y should be 100.
		w.WorkerId=reply.WorkerID;
		fmt.Printf("worker ID:  %v\n", w.WorkerId);
		return w.WorkerId, nil;
	} else {
		fmt.Printf("call failed!\n")
		return "", errors.New("Could not get worker registered");
	}
}

func GetMapTask(w *WorkerData, workerId string) (string, int, error){

	ok := call("Coordinator.AssignFile", &w, &w);
	if ok {
		w.fileName=w.fileName;
		fmt.Println("Filename received: ", w.fileName);
		return w.fileName, 0, nil;
	}
	return "", 1, nil; 
}

//
// example function to show how to make an RPC call to the coordinator.
// rm mr-out*
// the RPC argument and reply types are defined in rpc.go.
//
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


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
