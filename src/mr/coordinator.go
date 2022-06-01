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
	status     MasterStatus
	nReduce    int
	nMap       int
	files      []string
	mapChan    chan *Job
	reduceChan chan *Job
	jobMap     RWMap
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
		c.checkMapJob()
	case Reducing:
		c.checkReduceJob()
	case Over:
		ret = true
	}

	return ret
}

func (c *Coordinator) checkMapJob() {
	finished := 0
	for i := 0; i < c.nMap; i++ {
		job, ok := c.jobMap.get(strconv.Itoa(i))
		if ok {
			// if timeout
			if !job.AssignedTime.IsZero() {
				if job.FinishedTime.IsZero() && time.Now().Sub(job.AssignedTime) > time.Second*10 {
					// send into chan to dispatch again
					job.AssignedTime = time.Now()
					c.mapChan <- &job
				} else if !job.FinishedTime.IsZero() {
					finished++
				}
			}
		}
	}
	log.Printf("==== map job finished %v", finished)
	if c.status == Mapping && finished == c.nMap {
		close(c.mapChan)
		c.status = Reducing
		c.initReduceJob()
	}
}

func (c *Coordinator) checkReduceJob() {
	finished := 0
	for i := 0; i < c.nReduce; i++ {
		job, ok := c.jobMap.get(strconv.Itoa(i))
		if ok {
			// if timeout
			if !job.AssignedTime.IsZero() {
				if job.FinishedTime.IsZero() && time.Now().Sub(job.AssignedTime) > time.Second*10 {
					// send into chan to dispatch again
					job.AssignedTime = time.Now()
					c.reduceChan <- &job
				} else if !job.FinishedTime.IsZero() {
					finished++
				}
			}
		}
	}
	log.Printf("==== reduce job finished %v", finished)
	if c.status == Reducing && finished == c.nReduce {
		c.status = Over
	}
}

func (c *Coordinator) dispatchMapOrder() *MasterResponse {
	job := <-c.mapChan
	// no job in chan, go back and wait
	if job == nil {
		return &MasterResponse{
			OrderType: Wait,
			Order:     Job{},
		}
	}
	// assign job and mark a start time
	jobCopy := *job
	jobCopy.AssignedTime = time.Now()
	rep := MasterResponse{
		OrderType: MapJob,
		Order:     jobCopy,
	}
	c.jobMap.put(strconv.Itoa(jobCopy.Index), jobCopy)
	log.Printf("[master]: dispatch map job %v, file %v", jobCopy.Index, jobCopy.Filename)
	return &rep
}

func (c *Coordinator) recordMap(index int) *MasterResponse {
	job, ok := c.jobMap.get(strconv.Itoa(index))
	if ok {
		job.FinishedTime = time.Now()
		c.jobMap.put(strconv.Itoa(index), job)
		log.Printf("[master]: get map job %v finished", index)
	}
	return c.dispatchMapOrder()
}

func (c *Coordinator) dispatchReduceOrder() *MasterResponse {
	job := <-c.reduceChan
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
		OrderType: ReduceJob,
		Order:     *job,
	}
	c.jobMap.put(strconv.Itoa(job.Index), *job)
	log.Printf("[master]: dispatch reduce job %v, file %v", job.Index, job.Filename)
	return &rep
}

func (c *Coordinator) recordReduce(index int) *MasterResponse {
	job, ok := c.jobMap.get(strconv.Itoa(index))
	if ok {
		job.FinishedTime = time.Now()
		c.jobMap.put(strconv.Itoa(index), job)
		log.Printf("[master]: get reduce job %v finished", index)
	}
	return c.dispatchReduceOrder()
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
	c.mapChan = jobs
	c.jobMap = jobMap
}

func (c *Coordinator) initReduceJob() {
	reduceChan := make(chan *Job, c.nReduce)
	c.jobMap.clear()
	//jobMap := NewRWMap()
	for i := 0; i < c.nReduce; i++ {
		job := Job{
			Index:   i,
			NMap:    c.nMap,
			NReduce: c.nReduce,
		}
		reduceChan <- &job
		c.jobMap.put(strconv.Itoa(i), job)
	}
	c.reduceChan = reduceChan
	//c.jobMap = jobMap
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
