package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type OrderType int
type WorkerMessageType int

const (
	Wait OrderType = iota
	ReduceJob
	MapJob
	End
)

const (
	HeartBeat WorkerMessageType = iota
	AskJob
	Finished
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type Job struct {
	Index        int
	NMap         int
	NReduce      int
	Filename     string
	AssignedTime time.Time
	FinishedTime time.Time
}

type MasterResponse struct {
	OrderType OrderType
	Order     Job
}

type WorkerRequest struct {
	MessageType WorkerMessageType
	Index       int
}

//func JobCopy(a *Job, b *Job) {
//
//}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
