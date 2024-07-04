package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	sync.RWMutex
	nReduce      int
	nMap         int
	mapTask      []string //taskid filename
	mapStatus    []mrTask
	reduceStatus []mrTask
	mapDone      bool
	reduceDone   bool
}

// Your code here -- RPC handlers for the worker to call.

const outTime uint = 10

type mrTask uint8

func (t *mrTask) None() {
	*t = 0
}

func (t mrTask) IsNone() bool {
	return t == mrTask(0)
}

func (t *mrTask) Idle() {
	*t = 1
}

func (t mrTask) IsIdle() bool {
	return t == mrTask(1)
}

func (t *mrTask) Processing() {
	*t = 2
}

func (t mrTask) IsProcessing() bool {
	return t == mrTask(2)
}

func (t *mrTask) Complete() {
	*t = 3
}

func (t mrTask) IsComplete() bool {
	return t == mrTask(3)
}

// Y1 mapID
// Y2 mapTaskFileName
func (h *Coordinator) MapTask(args *FetchArgs, reply *MapFetchReply) error {
	reply.ID = -1
	h.Lock()
	if h.mapDone {
		h.Unlock()
		reply.ID = -2
		return nil
	}
	for i := 0; i < len(h.mapStatus); i++ {
		if !(h.mapStatus[i].IsIdle()) {
			continue
		}
		// fmt.Printf("assign MapTask:%d\n", i)
		reply.ID = i
		reply.Filename = h.mapTask[i]
		h.mapStatus[i].Processing()
		break
	}
	h.Unlock()
	if reply.ID == -1 {
		return nil
	}
	p := reply.ID
	go func() {
		time.Sleep(time.Duration(outTime) * time.Second)
		h.Lock()
		if !h.mapStatus[p].IsComplete() {
			// fmt.Printf("outtime MapTask:%d\n", p)
			h.mapStatus[p].Idle()
		}
		h.Unlock()
	}()
	return nil
}

// Y1 reduceID
// Y3 reduceTask
func (h *Coordinator) ReduceTask(args *FetchArgs, reply *ReduceFetchReply) error {
	reply.ID = -1
	h.Lock()
	if h.reduceDone {
		h.Unlock()
		reply.ID = -2
		return nil
	}
	for i := 0; i < len(h.reduceStatus); i++ {
		if !(h.reduceStatus[i].IsIdle()) {
			continue
		}
		// fmt.Printf("assign ReduceTask:%d\n", i)
		reply.ID = i
		h.reduceStatus[i].Processing()
		break
	}
	h.Unlock()
	if reply.ID == -1 {
		return nil
	}
	p := reply.ID
	go func() {
		time.Sleep(time.Duration(outTime) * time.Second)
		h.Lock()
		if !h.reduceStatus[p].IsComplete() {
			h.reduceStatus[p].Idle()
			// fmt.Printf("task %d not done\n", p)
		}
		h.Unlock()
	}()
	return nil
}

// X1 mapID
func (h *Coordinator) CompleteMap(args *CompleteArgs, reply *CompleteReply) error {
	h.Lock()
	defer h.Unlock()
	mapID := args.ID
	h.mapStatus[mapID].Complete()
	h.mapDone = true
	for i := range h.mapStatus {
		if h.mapStatus[i].IsComplete() {
			continue
		}
		h.mapDone = false
		break
	}
	return nil
}

func (h *Coordinator) CompleteReduce(args *CompleteArgs, reply *CompleteReply) error {
	h.Lock()
	defer h.Unlock()
	reduceID := args.ID
	h.reduceStatus[reduceID].Complete()
	h.reduceDone = true
	for i := range h.reduceStatus {
		if h.reduceStatus[i].IsComplete() {
			continue
		}
		h.reduceDone = false
		break
	}
	return nil
}

func (h *Coordinator) NReduce(args *FetchArgs, reply *NumFetchReply) error {
	h.RLock()
	reply.Num = h.nReduce
	h.RUnlock()
	return nil
}

func (h *Coordinator) NMap(args *FetchArgs, reply *NumFetchReply) error {
	h.RLock()
	reply.Num = h.nMap
	h.RUnlock()
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.RLock()
	ret := c.reduceDone
	c.RUnlock()
	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nMap = len(files)
	c.mapTask = make([]string, c.nMap)
	c.mapStatus = make([]mrTask, c.nMap)
	for i := range c.mapStatus {
		c.mapStatus[i].Idle()
	}
	c.mapDone = false
	copy(c.mapTask, files)

	c.nReduce = nReduce
	c.reduceStatus = make([]mrTask, c.nReduce)
	for i := range c.reduceStatus {
		c.reduceStatus[i].Idle()
	}
	c.reduceDone = false

	c.server()
	return &c
}
