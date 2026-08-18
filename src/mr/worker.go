package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	for {

		args := MrArgs{Type: Request}
		reply := MrReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)

		if !ok {
			break
		}

		if reply.Task.Action == Wait {
			time.Sleep(time.Second)
			continue
		} else if reply.Task.Action == Exit {
			break
		}
		if reply.Task.Action == Map {
			executeMap(mapf, reply.Task)
		}
		if reply.Task.Action == Reduce {
			err := executeReduce(reducef, reply.Task)
			if err != nil {
				log.Fatalf("unable to executeReduce: %v", err)
			}
		}
	}
}

func executeReduce(reducef func(string, []string) string, task Task) error {
	log.Printf("executeReduce... ID %s", task.ID)
	bucket := strings.Split(task.ID, "-")[1]
	pattern := fmt.Sprintf("mr-map-*-%s", bucket)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}
	kvm := make(map[string][]string)

	for _, m := range matches {
		if strings.Contains(m, "-out-") {
			continue
		}
		file, err := os.Open(m)
		if err != nil {
			return err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			kvm[kv.Key] = append(kvm[kv.Key], kv.Value)
		}
	}
	outName := fmt.Sprintf("mr-out-%s", bucket)
	file, err := os.Create(outName)
	if err != nil {
		return err
	}
	for key, value := range kvm {
		results := reducef(key, value)
		fmt.Fprintf(file, "%v %v\n", key, results)
	}

	// update task
	task.Status = Complete
	reply := MrReply{}
	args := MrArgs{
		Type: Update,
		Task: task,
	}
	ok := call("Coordinator.UpdateTask", &args, &reply)
	if !ok {
		return fmt.Errorf("unable to update task ID: %s to complete", task.ID)
	}
	return nil

}

func executeMap(mapf func(string, string) []KeyValue, task Task) error {
	data, err := os.ReadFile(task.Filename)
	if err != nil {
		return err
	}
	kva := mapf(task.Filename, string(data))
	log.Print(len(kva))

	files := make(map[int]*os.File)

	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce

		f, ok := files[bucket]
		if !ok {
			name := fmt.Sprintf("mr-map-%s-%d", task.ID, bucket)
			f, err = os.Create(name)
			if err != nil {
				return err
			}
			files[bucket] = f
		}

		enc := json.NewEncoder(f)
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("encoding err: %v", err)
			return err
		}
	}

	for _, f := range files {
		f.Close()
	}

	// update task
	task.Status = Complete
	args := MrArgs{Type: Update, Task: task}
	log.Printf("updating with %v", args)
	ok := call("Coordinator.UpdateTask", &args, &MrReply{})
	if !ok {
		return fmt.Errorf("update task call failed")
	}
	return nil
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
