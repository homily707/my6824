package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
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
	finished := false

	// TODO  heartbeat

	var request WorkerRequest = WorkerRequest{MessageType: AskJob}
	// Your worker implementation here.
	for !finished {
		response := CallMaster(request)
		request, finished = handleOrder(response, mapf, reducef)
		if response.OrderType == Wait {
			time.Sleep(1 * time.Second)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func handleOrder(response MasterResponse, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (WorkerRequest, bool) {
	//log.Printf("receive order. type %s, file %v ,%v", response.OrderType, response.Order.Filename)
	switch response.OrderType {
	case Wait:
		return WorkerRequest{MessageType: AskJob}, false
	case End:
		return WorkerRequest{}, true
	case MapJob:
		return HandleMapOrder(response.Order, mapf, reducef)
	case ReduceJob:
		return HandleReduceOrder(response.Order, mapf, reducef)
	default:
		return WorkerRequest{}, false
	}
}

func HandleReduceOrder(order Job, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (WorkerRequest, bool) {

	//read
	dir, _ := os.Getwd()
	//log.Printf(dir + "| ")
	kva := []KeyValue{}
	for i := 0; i < order.NMap; i++ {
		fileName := dir + fmt.Sprintf("/mr-%v-%v", i, order.Index)
		//log.Print(fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannnot open %v", fileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// reduce
	sort.Sort(ByKey(kva))
	tempFile, err := ioutil.TempFile(dir, "reduce-tmp")
	if err != nil {
		log.Fatalf("create temp file error %v", err)
	}
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[i] == kva[j] {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	os.Rename(tempFile.Name(), fmt.Sprintf("mr-out-%v", order.Index))
	tempFile.Close()
	return WorkerRequest{Finished, order.Index}, false
}

func HandleMapOrder(order Job, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (WorkerRequest, bool) {

	// read and map
	file, err := os.Open(order.Filename)
	if err != nil {
		log.Fatalf("cannnot open %v", order.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", order.Filename)
	}
	file.Close()
	kva := mapf(order.Filename, string(content))

	//shuffle
	nReduce := order.NReduce
	kvaa := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		rindex := ihash(kv.Key) % nReduce
		kvaa[rindex] = append(kvaa[rindex], kv)
	}

	// write
	dir := getFilePath()
	for i, kva := range kvaa {
		tempFile, err := ioutil.TempFile(dir, "mr-temp-")
		if err != nil {
			log.Fatalf("create temp file error %v", err)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range kva {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("encode error %v", err)
			}
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), fmt.Sprintf("mr-%v-%v", order.Index, i))
	}
	//log.Printf("[worker %v]: finished map job %v", os.Getpid(), order.Index)
	return WorkerRequest{Finished, order.Index}, false
}

func getFilePath() string {
	pwd, _ := os.Getwd()
	return pwd
}

func CallMaster(request WorkerRequest) MasterResponse {
	response := MasterResponse{}

	call("Coordinator.Order", &request, &response)

	return response
}

//
// example function to show how to make an RPC call to the coordinator.
//
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
