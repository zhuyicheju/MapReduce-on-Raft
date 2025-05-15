package master

import (
	"log"
	"mrrf/raft"
	"mrrf/rpcargs"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
)

func (m *Master)handleTask(cmd *Op) *Reply_type{
	reply = new(Reply_type)

	if m.reduce_is_done {
		reply.Reply_type = RPC_REPLY_DONE
		return nil
	}

	switch args.Send_type {
	case RPC_SEND_DONE_MAP:
		DoneMap(m, cmd, reply)
	case RPC_SEND_DONE_REDUCE:
		DoneReduce(m, cmd, reply)
	case RPC_SEND_ERROR:
		//do nothing
	case RPC_SEND_REQUEST:
		if !m.map_is_done {
			RequestMap(m, cmd, reply)
		} else {
			RequestReduce(m, cmd, reply)
		}
	}

	return nil
}

func DoneMap(m *Master, cmd *Op, reply *ReplyType) {
	// fmt.Printf("DoneMap %v\n", args.ID)
	id := args.ID
	m.map_status_lock[id].Lock()
	if m.map_status[id] != STATUS_DONE {
		atomim.AddInt32(&m.map_done_num, 1)
		m.map_status[id] = STATUS_DONE
		if m.map_done_num == int32(m.nMap) {
			m.map_is_done = true
		}
	}
	m.map_status_lock[id].Unlock()
}
func DoneReduce(m *Master, cmd *Op, reply *ReplyType) {
	// fmt.Printf("DoneReduce %v\n", args.ID)
	id := args.ID

	m.reduce_status_lock[id].Lock()
	if m.reduce_status[id] != STATUS_DONE {
		atomim.AddInt32(&m.reduce_done_num, 1)
		m.reduce_status[id] = STATUS_DONE
		if m.reduce_done_num == int32(m.nReduce) {
			m.reduce_is_done = true
		}
	}
	m.reduce_status_lock[id].Unlock()
}
func RequestMap(m *Master, cmd *Op, reply *ReplyType) {
	reply.Reply_type = RPC_REPLY_MAP
	reply.NReduce = m.nReduce
	reply.NMap = m.nMap

	restart := true
	for restart {
		m.map_heap_lock.Lock()
		if m.map_heap.Len() == 0 {
			reply.Reply_type = RPC_REPLY_WAIT
			m.map_heap_lock.Unlock()
			return
		}
		task := heap.Pop(&m.map_heap).(tasknode)
		m.map_heap_lock.Unlock()

		m.map_status_lock[task.task_id].Lock()
		if m.map_status[task.task_id] == STATUS_WORKING && cmd.Timestamp-task.timestamp > 10*MS2S {
			m.map_status[task.task_id] = STATUS_TIMEOUT
		}
		stat := m.map_status[task.task_id]
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
			reply.File = m.files[task.task_id]

			m.map_status[task.task_id] = STATUS_WORKING
			m.map_heap_lock.Lock()
			heap.Push(&m.map_heap, tasknode{task.task_id, cmd.Timestamp})
			m.map_heap_lock.Unlock()

		case STATUS_WORKING:
			restart = false
			reply.Reply_type = RPC_REPLY_WAIT
			m.map_heap_lock.Lock()
			heap.Push(&m.map_heap, tasknode{task.task_id, task.timestamp})
			m.map_heap_lock.Unlock()
		}
		m.map_status_lock[task.task_id].Unlock()
	}
}

func RequestReduce(m *Master, cmd *Op, reply *ReplyType) {
	reply.Reply_type = RPC_REPLY_REDUCE
	reply.NReduce = m.nReduce
	reply.NMap = m.nMap

	restart := true
	for restart {
		restart = false
		m.reduce_heap_lock.Lock()
		if m.reduce_heap.Len() == 0 {
			reply.Reply_type = RPC_REPLY_DONE
			m.reduce_heap_lock.Unlock()
			break
		}
		task := heap.Pop(&m.reduce_heap).(tasknode)
		m.reduce_heap_lock.Unlock()

		m.reduce_status_lock[task.task_id].Lock()
		if m.reduce_status[task.task_id] == STATUS_WORKING && cmd.Timestamp-task.timestamp > 5*MS2S {
			m.reduce_status[task.task_id] = STATUS_TIMEOUT
		}
		stat := m.reduce_status[task.task_id]
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

			m.reduce_status[task.task_id] = STATUS_WORKING
			m.reduce_heap_lock.Lock()
			heap.Push(&m.reduce_heap, tasknode{task.task_id, cmd.Timestamp})
			m.reduce_heap_lock.Unlock()

		case STATUS_WORKING:
			reply.Reply_type = RPC_REPLY_WAIT
			m.reduce_heap_lock.Lock()
			heap.Push(&m.reduce_heap, tasknode{task.task_id, task.timestamp})
			m.reduce_heap_lock.Unlock()
		}
		m.reduce_status_lock[task.task_id].Unlock()
	}
}