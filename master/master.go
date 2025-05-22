package master

import (
	"container/heap"
	"encoding/gob"
	"mrrf/logging"
	"mrrf/raft"
	"mrrf/rpcargs"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
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
	Send_type request_t
	ID        int
	Timestamp int64
	Rand_Id   int64
}

type Master struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stop    chan bool
	dead    int32           // set by Kill()
	wg      *sync.WaitGroup //通知上层任务完成

	maxraftstate int // snapshot if log grows this big

	//commit后保存
	id2reply    map[int64]*ReplyType
	id2chan     map[int64]chan *ReplyType
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
	if m.killed() {
		reply.Reply_type = rpcargs.RPC_REPLY_DONE
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, isLeader := m.rf.GetState()
	if !isLeader {
		logging.Logger.Debug("Master 当前非leader", zap.Int("me", m.me), zap.Int("ArgsType", args.Send_type), zap.Int("id", args.ID))
		reply.Reply_type = rpcargs.RPC_REPLY_WRONG_LEADER
		return nil
	}

	logging.Logger.Debug("Master 收到RPC请求", zap.Int("me", m.me), zap.Int("ArgsType", args.Send_type), zap.Int("id", args.ID))
	if rply, ok := m.id2reply[args.Rand_Id]; ok {
		logging.Logger.Debug("Master 已存在此RPC请求", zap.Int("me", m.me), zap.Int("ArgsType", args.Send_type), zap.Int("id", args.ID))
		*reply = *rply
		return nil
	}

	cmd := Op{Send_type: args.Send_type, ID: args.ID, Timestamp: time.Now().UnixMilli(), Rand_Id: args.Rand_Id}
	m.id2chan[cmd.Rand_Id] = make(chan *ReplyType)

	_, _, isLeader = m.rf.Start(cmd)
	if !isLeader {
		logging.Logger.Debug("Master 当前非leader", zap.Int("me", m.me), zap.Int("ArgsType", args.Send_type), zap.Int("id", args.ID))
		reply.Reply_type = rpcargs.RPC_REPLY_WRONG_LEADER
		return nil
	}

	res, ok := m.waitForApply(args.Rand_Id)
	if !ok {
		//让其以相同请求id再次请求
		logging.Logger.Debug("Master 日志commit超时", zap.Int("me", m.me), zap.Int("ArgsType", args.Send_type), zap.Int("id", args.ID))
		reply.Reply_type = rpcargs.RPC_REPLY_TIMEOUT
		return nil
	}
	logging.Logger.Debug("Master 返回reply", zap.Int("me", m.me), zap.Int("ArgsType", args.Send_type), zap.Int("id", args.ID))
	*reply = *res
	return nil
}

func (m *Master) waitForApply(index int64) (*ReplyType, bool) {
	select {
	case <-time.After(5 * time.Second):
		return nil, false
	case reply := <-m.id2chan[index]:
		close(m.id2chan[index])
		return reply, true
	}
}

func (m *Master) handleApply() {
	for !m.killed() {
		select {
		case apply := <-m.applyCh:
			cmd := apply.Command.(Op)
			logging.Logger.Debug("Master 处理提交", zap.Int("me", m.me))
			reply := m.handleTask(&cmd)
			m.id2reply[cmd.Rand_Id] = reply
			if m.id2chan[cmd.Rand_Id] != nil {
				m.id2chan[cmd.Rand_Id] <- reply
			}
		}
	}
}

func MakeMaster(servers []string, me int, persister *raft.Persister, maxraftstate int, done chan bool,
	files []string, nReduce int, wg *sync.WaitGroup) *Master {

	m := new(Master)
	m.me = me
	m.maxraftstate = maxraftstate

	m.applyCh = make(chan raft.ApplyMsg)
	m.rf = raft.Make(servers, me, persister, m.applyCh)
	m.stop = make(chan bool)
	m.wg = wg

	go func(me int) {
		server := rpc.NewServer()
		server.Register(m)
		server.Register(m.rf)
		gob.Register(Op{})
		//同时注册master和raft，同时监听两种请求
		listener, err := net.Listen("tcp", servers[me])
		if err != nil {
			logging.Logger.Error("产生错误\n", zap.Error(err))
			panic(err)
		}
		done <- true
		logging.Logger.Info("Master 监听tcp", zap.Int("id", m.me))
		for !m.killed() {
			conn, err := listener.Accept()
			if err == nil {
				go server.ServeConn(conn)
			} else {
				logging.Logger.Error("产生错误\n", zap.Error(err))
				panic(err)
			}
		}
	}(me)

	m.Init(files, nReduce)

	m.id2chan = make(map[int64]chan *ReplyType)
	m.id2reply = make(map[int64]*ReplyType)
	go m.handleApply()

	logging.Logger.Info("Master 启动成功", zap.Int("id", m.me))
	return m
}

func (m *Master) Open() {
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
	time.Sleep(1 * time.Second)
	m.rf.Kill()
	m.wg.Done()
	logging.Logger.Info("Master 关闭", zap.Int("id", m.me))
}

func (m *Master) killed() bool {
	z := atomic.LoadInt32(&m.dead)
	return z == 1
}
