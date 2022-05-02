package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce    int
	nMap       int
	files      []string
	stage      Stage
	mapChan    chan int
	mapMap     map[int]Info
	reduceChan chan int
	reduceMap  map[int]Info
}

type Info struct {
	Index    int
	NReduce  int
	FileName string
	State    Stage
	Success  bool
}

func (c *Coordinator) initMapperJob() {
	c.mapChan = make(chan int, len(c.files))
	c.mapMap = make(map[int]Info)
	for i, v := range c.files {
		var job Info = Info{
			Index: i, NReduce: c.nReduce, FileName: v,
			State: InMap, Success: false,
		}
		c.mapMap[i] = job
		c.mapChan <- i
	}
}

func (c *Coordinator) initReduceJob() {
	c.reduceChan = make(chan int, c.nReduce)
	c.reduceMap = make(map[int]Info)
	for i := 0; i < c.nReduce; i++ {
		var job Info = Info{
			Index: i, NReduce: c.nReduce, State: InReduce, Success: false,
		}
		c.reduceMap[i] = job
		c.reduceChan <- i
	}
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

func (c *Coordinator) SendJob(args *ExampleArgs, reply *Info) error {
	//if c.stage == InMap {
	//	index := <-c.mapChan
	//	reply := c.mapMap[index]
	//} else if c.stage
	return nil
}

func (c *Coordinator) GetFeedback(args *Info, reply *Info) error {
	return nil
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

	// check all job, update state

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//init
	c := Coordinator{}
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.stage = InMap

	c.initMapperJob()
	c.initReduceJob()

	c.server()
	for !c.Done() {
		time.Sleep(1)
	}
	return &c
}
