package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu             sync.Mutex
	Tasks          map[string]Task
	NReduce        int
	MapComplete    bool
	ReduceComplete bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequestTask(args *MrArgs, reply *MrReply) error {
	log.Printf("RequestTask...")
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.MapComplete {
		found := c.assignTask(Map, args, reply)
		if !found {
			c.MapComplete = c.allComplete(Map)
			if !c.MapComplete {
				t := Task{
					Action: Wait,
				}
				reply.Task = t
				return nil	
			}
		} else {
			return nil
		}
	}
	if !c.ReduceComplete {
		found := c.assignTask(Reduce, args, reply)
		if !found {
			c.ReduceComplete = c.allComplete(Reduce)
			if !c.ReduceComplete {
				t := Task{
					Action: Wait,
				}
				reply.Task = t
				return nil	
			}
		} else {
			return nil
		}
	}

	// no action to take
	reply.Task = Task{
		Action: Exit,
	}
	return nil
}

func (c *Coordinator) assignTask(action Action, args *MrArgs, reply *MrReply) bool {
			for i, task := range c.Tasks {
			if task.Action == action && task.Status == Pending {
				task.Status = Running
				task.LastAssigned = time.Now()
				reply.Task = task
				c.Tasks[i] = task
				return true 
			}
			if task.Action == action && task.Status == Running && time.Since(task.LastAssigned) > time.Second*10 {
				// reassign
				task.LastAssigned = time.Now()
				reply.Task = task
				c.Tasks[i] = task
				return true
			}
		}
		return false
}



func (c *Coordinator) allComplete(action Action) bool {

	for _, task := range c.Tasks {
		if task.Action == action && task.Status != Complete {
			return false
		}
	}
	return true

}
func (c *Coordinator) UpdateTask(args *MrArgs, reply *MrReply) error {
	log.Printf("UpdateTask... %v", args.Task)
	c.mu.Lock()
	defer c.mu.Unlock()
	id := args.Task.ID

	if task, ok := c.Tasks[id]; ok {
		task.Status = args.Task.Status
		c.Tasks[id] = task
		log.Printf("updated with %v", task)
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	if c.MapComplete && c.ReduceComplete {
		return true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	c.Tasks = make(map[string]Task)
	for i, f := range files {
		t := Task{
			ID:       strconv.Itoa(i),
			Filename: f,
			Action:   Map,
			Status:   Pending,
			NReduce:  nReduce,
		}
		c.Tasks[strconv.Itoa(i)] = t
	}

	for i := 0; i < nReduce; i++ {
		rt := Task{
			ID:       fmt.Sprintf("reduce-%s", strconv.Itoa(i)),
			Filename: "",
			Action:   Reduce,
			Status:   Pending,
			NReduce:  nReduce,
		}
		c.Tasks[rt.ID] = rt
	}

	c.server(sockname)
	return &c
}
