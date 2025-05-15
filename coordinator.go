package mr

import (
	"container/heap"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type tasknode struct {
	task_id   int
	timestamp int64
}

type map_heap []tasknode

func (h *map_heap) Len() int { return len(*h) }
func (h *map_heap) Less(i int, j int) bool {
	if (*h)[i].timestamp == (*h)[j].timestamp {
		return (*h)[i].task_id < (*h)[j].task_id
	}
	return (*h)[i].timestamp < (*h)[j].timestamp
}
func (h *map_heap) Swap(i int, j int) { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }
func (h *map_heap) Push(x interface{}) {
	*h = append(*h, x.(tasknode))
}

func (h *map_heap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

const (
	STATUS_PENDING = iota
	STATUS_WORKING
	STATUS_DONE
	STATUS_ERR
	STATUS_TIMEOUT
)

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int
	nMap    int
	mutex   sync.Mutex

	reduce_is_done     bool
	reduce_done_num    int32
	reduce_heap        map_heap
	reduce_heap_lock   sync.Mutex
	reduce_status      []int
	reduce_status_lock []sync.Mutex

	map_is_done     bool
	map_done_num    int32
	map_heap        map_heap
	map_heap_lock   sync.Mutex
	map_status      []int
	map_status_lock []sync.Mutex
}

const MS2S = 1000

func (c *Coordinator) Handle(args ArgsType, reply *ReplyType) error {
	// c.mutex.Lock()
	// defer c.mutex.Unlock()

	//不需要锁，done只会从0变到1
	if c.reduce_is_done {
		reply.Reply_type = RPC_REPLY_DONE
		return nil
	}

	switch args.Send_type {
	case RPC_SEND_DONE_MAP:
		DoneMap(c, &args, reply)
	case RPC_SEND_DONE_REDUCE:
		DoneReduce(c, &args, reply)
	case RPC_SEND_ERROR:
		//do nothing
	case RPC_SEND_REQUEST:
		if !c.map_is_done {
			RequestMap(c, &args, reply)
		} else {
			RequestReduce(c, &args, reply)
		}
	}

	return nil
}

func DoneMap(c *Coordinator, args *ArgsType, reply *ReplyType) {
	// fmt.Printf("DoneMap %v\n", args.ID)
	id := args.ID
	c.map_status_lock[id].Lock()
	if c.map_status[id] != STATUS_DONE {
		atomic.AddInt32(&c.map_done_num, 1)
		c.map_status[id] = STATUS_DONE
		if c.map_done_num == int32(c.nMap) {
			c.map_is_done = true
		}
	}
	c.map_status_lock[id].Unlock()
}
func DoneReduce(c *Coordinator, args *ArgsType, reply *ReplyType) {
	// fmt.Printf("DoneReduce %v\n", args.ID)
	id := args.ID

	c.reduce_status_lock[id].Lock()
	if c.reduce_status[id] != STATUS_DONE {
		atomic.AddInt32(&c.reduce_done_num, 1)
		c.reduce_status[id] = STATUS_DONE
		if c.reduce_done_num == int32(c.nReduce) {
			c.reduce_is_done = true
		}
	}
	c.reduce_status_lock[id].Unlock()
}
func RequestMap(c *Coordinator, args *ArgsType, reply *ReplyType) {
	reply.Reply_type = RPC_REPLY_MAP
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	restart := true
	for restart {
		c.map_heap_lock.Lock()
		if c.map_heap.Len() == 0 {
			reply.Reply_type = RPC_REPLY_WAIT
			c.map_heap_lock.Unlock()
			return
		}
		task := heap.Pop(&c.map_heap).(tasknode)
		c.map_heap_lock.Unlock()

		c.map_status_lock[task.task_id].Lock()
		if c.map_status[task.task_id] == STATUS_WORKING && time.Now().UnixMilli()-task.timestamp > 10*MS2S {
			c.map_status[task.task_id] = STATUS_TIMEOUT
		}
		stat := c.map_status[task.task_id]
		switch stat {
		case STATUS_DONE:
			restart = true
		case STATUS_PENDING:
			fallthrough
		case STATUS_TIMEOUT:
			fallthrough
		case STATUS_ERR:
			restart = false
			reply.ID = task.task_id
			reply.File = c.files[task.task_id]

			c.map_status[task.task_id] = STATUS_WORKING
			c.map_heap_lock.Lock()
			heap.Push(&c.map_heap, tasknode{task.task_id, time.Now().UnixMilli()})
			c.map_heap_lock.Unlock()

		case STATUS_WORKING:
			restart = false
			reply.Reply_type = RPC_REPLY_WAIT
			c.map_heap_lock.Lock()
			heap.Push(&c.map_heap, tasknode{task.task_id, task.timestamp})
			c.map_heap_lock.Unlock()
		}
		c.map_status_lock[task.task_id].Unlock()
	}
}

func RequestReduce(c *Coordinator, args *ArgsType, reply *ReplyType) {
	reply.Reply_type = RPC_REPLY_REDUCE
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap

	restart := true
	for restart {
		restart = false
		c.reduce_heap_lock.Lock()
		if c.reduce_heap.Len() == 0 {
			reply.Reply_type = RPC_REPLY_DONE
			c.reduce_heap_lock.Unlock()
			break
		}
		task := heap.Pop(&c.reduce_heap).(tasknode)
		c.reduce_heap_lock.Unlock()

		c.reduce_status_lock[task.task_id].Lock()
		if c.reduce_status[task.task_id] == STATUS_WORKING && time.Now().UnixMilli()-task.timestamp > 5*MS2S {
			c.reduce_status[task.task_id] = STATUS_TIMEOUT
		}
		stat := c.reduce_status[task.task_id]
		switch stat {
		case STATUS_DONE:
			restart = true
		case STATUS_PENDING:
			fallthrough
		case STATUS_TIMEOUT:
			fallthrough
		case STATUS_ERR:
			restart = false
			reply.ID = task.task_id

			c.reduce_status[task.task_id] = STATUS_WORKING
			c.reduce_heap_lock.Lock()
			heap.Push(&c.reduce_heap, tasknode{task.task_id, time.Now().UnixMilli()})
			c.reduce_heap_lock.Unlock()

		case STATUS_WORKING:
			reply.Reply_type = RPC_REPLY_WAIT
			c.reduce_heap_lock.Lock()
			heap.Push(&c.reduce_heap, tasknode{task.task_id, task.timestamp})
			c.reduce_heap_lock.Unlock()
		}
		c.reduce_status_lock[task.task_id].Unlock()
	}
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
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.reduce_is_done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nMap: len(files), nReduce: nReduce}
	heap.Init(&c.map_heap)
	for i := 0; i < c.nMap; i++ {
		heap.Push(&c.map_heap, tasknode{task_id: i, timestamp: 0})
		c.map_status = append(c.map_status, STATUS_PENDING)
		c.map_status_lock = append(c.map_status_lock, sync.Mutex{})
	}

	heap.Init(&c.reduce_heap)
	for i := 0; i < c.nReduce; i++ {
		heap.Push(&c.reduce_heap, tasknode{task_id: i, timestamp: 0})
		c.reduce_status = append(c.reduce_status, STATUS_PENDING)
		c.reduce_status_lock = append(c.reduce_status_lock, sync.Mutex{})
	}

	c.server()

	// fmt.Println("Coordinator Running...")
	// fmt.Printf("MapFiles: %v nReduce: %v\n", len(files), nReduce)
	return &c
}
