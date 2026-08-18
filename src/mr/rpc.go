package mr

import "time"

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RequestType string

const (
	Request RequestType = "request"
	Update  RequestType = "update"
)

type Status string

const (
	Pending  Status = "pending"
	Running  Status = "running"
	Complete Status = "complete"
)

type Action string

const (
	Map    Action = "mapf"
	Reduce Action = "reducef"
	Wait Action = "wait"
	Exit Action = "exit"
)

type Task struct {
	ID       string
	Filename string
	Action   Action
	Status   Status
	LastAssigned time.Time
	NReduce int
}

type MrArgs struct {
	Type RequestType
	Task Task
}
type MrReply struct {
	Task Task
}
