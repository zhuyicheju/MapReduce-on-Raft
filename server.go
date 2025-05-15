package server

import (
	"raft"
	"sync"
)

type Master struct {
	mu      sync.Mutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	tasks   map[int]Task
}

// 初始化 Master，并创建 Raft 节点
func MakeMaster(peers []*labrpc.ClientEnd, me int, persister *raft.Persister) *Master {
	applyCh := make(chan raft.ApplyMsg)
	rf := raft.Make(peers, me, persister, applyCh)

	m := &Master{
		rf:      rf,
		applyCh: applyCh,
		tasks:   make(map[int]Task),
	}

	go m.applyLoop()
	return m
}

// 提交任务变更请求
func (m *Master) assignTask(task Task) error {
	cmd := TaskCommand{
		Type:     "AssignTask",
		TaskInfo: task,
	}
	_, _, isLeader := m.rf.Start(cmd)
	if !isLeader {
		return ErrWrongLeader
	}
	return nil
}

// 从 applyCh 中读取 Raft 共识后的命令
func (m *Master) applyLoop() {
	for msg := range m.applyCh {
		if msg.CommandValid {
			cmd := msg.Command.(TaskCommand)
			switch cmd.Type {
			case "AssignTask":
				m.tasks[cmd.TaskInfo.ID] = cmd.TaskInfo
			case "CompleteTask":
				m.tasks[cmd.TaskInfo.ID].Done = true
			}
		}
	}
}
