package mr

import (
	"log"
	"strconv"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type MasterStatus int

const (
	Init MasterStatus = iota
	Mapping
	Reducing
	Over
)

type Coordinator struct {
	status  MasterStatus
	nReduce int
	nMap    int
	files   []string
	jobs    chan *Job
	jobMap  RWMap
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Order(request *WorkerRequest, order *MasterResponse) error {
	switch c.status {
	case Init:
		order.OrderType = Wait
	case Over:
		order.OrderType = End
	case Mapping:
		switch request.MessageType {
		case AskJob:
			*order = *c.dispatchMapOrder()
		case Finished:
			*order = *c.recordMap(request.Index)
		}
	case Reducing:
		switch request.MessageType {
		case AskJob:
			*order = *c.dispatchReduceOrder()
		case Finished:
			*order = *c.recordReduce(request.Index)
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	switch c.status {
	case Mapping:
		go c.checkMapJob()
	case Reducing:
	case Over:
		ret = true
	}

	return ret
}

func (c *Coordinator) checkMapJob() {
	finished := 0
	for i := 0; i < c.nMap; i++ {
		val, ok := c.jobMap.get(strconv.Itoa(i))
		if ok {
			job := val.(Job)
			// if timeout
			if !job.AssignedTime.IsZero() {
				if job.FinishedTime.IsZero() && time.Now().Sub(job.AssignedTime) > time.Second*10 {
					// send into chan to dispatch again
					job.AssignedTime = time.Now()
					c.jobs <- &job
				} else if !job.FinishedTime.IsZero() {
					finished++
				}
			}
		}
	}
	if finished == c.nMap {
		c.initReduceJob()
		c.status = Reducing
	}
}

func (c *Coordinator) dispatchMapOrder() *MasterResponse {
	job := <-c.jobs
	// no job in chan, go back and wait
	if job == nil {
		return &MasterResponse{
			OrderType: Wait,
			Order:     Job{},
		}
	}
	// assign job and mark a start time
	job.AssignedTime = time.Now()
	rep := MasterResponse{
		OrderType: MapJob,
		Order:     *job,
	}
	log.Printf("[master]: dispatch map job %v, file %v", job.Index, job.Filename)
	return &rep
}

func (c *Coordinator) recordMap(index int) *MasterResponse {
	val, ok := c.jobMap.get(strconv.Itoa(index))
	if ok {
		job := val.(Job)
		job.FinishedTime = time.Now()
		c.jobMap.put(strconv.Itoa(index), job)
	}
	log.Printf("[master]: get map job %v finished", index)
	return c.dispatchMapOrder()
}

func (c *Coordinator) dispatchReduceOrder() *MasterResponse {
	return nil
}

func (c *Coordinator) recordReduce(index int) *MasterResponse {
	return nil
}

func (c *Coordinator) initMapJob() {
	jobs := make(chan *Job, c.nMap)
	jobMap := NewRWMap()
	for i, file := range c.files {
		job := Job{
			Index:    i,
			NMap:     c.nMap,
			NReduce:  c.nReduce,
			Filename: file,
		}
		jobs <- &job
		jobMap.put(strconv.Itoa(i), job)
	}
	c.jobs = jobs
	c.jobMap = jobMap
}

func (c *Coordinator) initReduceJob() {
	jobs := make(chan *Job, c.nReduce)
	jobMap := NewRWMap()
	for i := 0; i < c.nReduce; i++ {
		job := Job{
			Index:   i,
			NMap:    c.nMap,
			NReduce: c.nReduce,
		}
		jobs <- &job
		jobMap.put(strconv.Itoa(i), job)
	}
	c.jobs = jobs
	c.jobMap = jobMap
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.status = Init
	c.nMap = len(files)
	c.nReduce = nReduce
	c.files = files
	c.status = Mapping

	c.initMapJob()
	c.server()
	return &c
}
