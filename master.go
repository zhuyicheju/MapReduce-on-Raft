package master

import (
	"log"
	"mrrf/minheap"
	"mrrf/raft"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
)

const (
	STATUS_PENDING = iota
	STATUS_WORKING
	STATUS_DONE
	STATUS_ERR
	STATUS_TIMEOUT
)

type Op struct {
	
}

type Master struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	//commit后保存
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

func (m *Master) RPChandle(args *ArgsType, reply *ReplyType) error {
	_, isLeader := m.rf.GetState()
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		return nil
	}

	index, _, isLeader := m.rf.Start(cmd)
	if !isLeader {
		reply.Err = "ErrWrongLeader"
		return nil
	}

	res := m.waitForApply(index)
	*reply = res
	return nil
}



func MakeMaster(servers []string, me int, persister *raft.Persister, maxraftstate int, done chan bool
				,files []string, nReduce int) *KVServer {

	master := new(Master)
	master.me = me
	master.maxraftstate = maxraftstate

	master.applyCh = make(chan raft.ApplyMsg)
	master.rf = raft.Make(servers, me, persister, kv.applyCh)

	go func(me int) {
		server := rpc.NewServer()
		server.Register(rf)
		listener, err := net.Listen("tcp", cfg.addrs[me])
		if err != nil {
			log.Fatalf("Raft %d listen error: %v", me, err)
		}
		done <- true
		for {
			conn, err := listener.Accept()
			if err == nil {
				go server.ServeConn(conn)
			}
		}
	}(me)

	master.Init(files, nReduce)

	go func(me int, applyCh chan ApplyMsg) {
		for msg := range applyCh {
			log.Printf("[Raft %d] Apply: %+v", me, msg)
		}
	}(me, applyCh)

	return master
}

func (m *Master) Init(files []string, nReduce int) {
	m.files = files
	m.nReduce = nReduce
	m.nMap = len(files)

	heap.Init(&m.map_heap)
	for i := 0; i < m.nMap; i++ {
		heap.Push(&c.map_heap, tasknode{task_id: i, timestamp: 0})
		m.map_status = append(m.map_status, STATUS_PENDING)
		m.map_status_lock = append(m.map_status_lock, sync.Mutex{})
	}

	heap.Init(&m.reduce_heap)
	for i := 0; i < m.nReduce; i++ {
		heap.Push(&m.reduce_heap, tasknode{task_id: i, timestamp: 0})
		m.reduce_status = append(m.reduce_status, STATUS_PENDING)
		m.reduce_status_lock = append(m.reduce_status_lock, sync.Mutex{})
	}

}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
