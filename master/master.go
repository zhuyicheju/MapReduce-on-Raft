package master

import (
	"log"
	"mrrf/raft"
	"mrrf/rpcargs"
	"container/heap"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type request_t = int
type ReplyType = rpcargs.ReplyType
type ArgsType = rpcargs.ArgsType

const (
	STATUS_PENDING = iota
	STATUS_WORKING
	STATUS_DONE
	STATUS_ERR
	STATUS_TIMEOUT
)

type Op struct {
	Send_type  request_t
	ID         int
	Timestamp  int64
	Rand_Id    int64
}

type Master struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stop    chan bool
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	//commit后保存
	id2reply map[int64]*ReplyType
	id2chan  map[int64]chan *ReplyType
	lastApplied int

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
		reply.Reply_type = rpcargs.RPC_REPLY_WRONG_LEADER
		return nil
	}

	if rply, ok := m.id2reply[args.Rand_Id]; ok {
		*reply = *rply
	}

	cmd := Op{Send_type: args.Send_type, ID: args.ID, Timestamp: time.Now().UnixMilli(), Rand_Id: args.Rand_Id}

	_, _, isLeader = m.rf.Start(cmd)
	if !isLeader {
		reply.Reply_type = rpcargs.RPC_REPLY_WRONG_LEADER
		return nil
	}

	res, ok := m.waitForApply(args.Rand_Id)
	if !ok {
		//让其以相同请求id再次请求
		reply.Reply_type = rpcargs.RPC_REPLY_TIMEOUT
	}

	*reply = *res
	return nil
}

func (m *Master)waitForApply(index int64) (*ReplyType, bool){
	select {
	case <-time.After(2*time.Second):
		return nil,false
	case reply := <-m.id2chan[index]:
		return reply,true
	}
}

func (m *Master)handleApply() {
	for !m.killed() {
	select{
	case <-m.stop:
		return
	case apply := <- m.applyCh:
		cmd := apply.Command.(Op)
		reply := m.handleTask(&cmd)
		m.id2reply[cmd.Rand_Id] = reply
		m.id2chan[cmd.Rand_Id] <- reply
	}
	}
}

func MakeMaster(servers []string, me int, persister *raft.Persister, maxraftstate int, done chan bool,
				files []string, nReduce int) *Master {

	master := new(Master)
	master.me = me
	master.maxraftstate = maxraftstate

	master.applyCh = make(chan raft.ApplyMsg)
	master.rf = raft.Make(servers, me, persister, master.applyCh)
	master.stop = make(chan bool)

	go func(me int) {
		server := rpc.NewServer()
		server.Register(master.rf)
		listener, err := net.Listen("tcp", servers[me])
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

	return master
}

func (m *Master) Open(){
	m.rf.Open()
}

func (m *Master) Init(files []string, nReduce int) {
	m.files = files
	m.nReduce = nReduce
	m.nMap = len(files)

	heap.Init(&m.map_heap)
	for i := 0; i < m.nMap; i++ {
		heap.Push(&m.map_heap, tasknode{task_id: i, timestamp: 0})
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

func (m *Master) Kill() {
	atomic.StoreInt32(&m.dead, 1)
	m.rf.Kill()
	m.stop <- true
}

func (m *Master) killed() bool {
	z := atomic.LoadInt32(&m.dead)
	return z == 1
}
