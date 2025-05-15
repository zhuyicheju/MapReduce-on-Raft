package server

import (
	"raft"
	"sync"
)

// main/master.go
type Master struct {
	mu      sync.Mutex
	raft    *raft.Raft
	applyCh chan raft.ApplyMsg

	tasks map[int]Task // taskID -> Task 状态
}

func MakeMaster(peers []string, me int, persister *raft.Persister) *Master {
	applyCh := make(chan raft.ApplyMsg)
	rf := raft.Make(peers, me, persister, applyCh)

	m := &Master{
		raft:    rf,
		applyCh: applyCh,
		tasks:   make(map[int]Task),
	}

	go m.applyLoop()

	return m
}

func (m *Master) applyLoop() {
	for msg := range m.applyCh {
		if msg.CommandValid {
			cmd := msg.Command.(TaskCommand)
			m.mu.Lock()
			if cmd.Type == "AssignTask" {
				m.tasks[cmd.TaskInfo.ID] = cmd.TaskInfo
			} else if cmd.Type == "CompleteTask" {
				m.tasks[cmd.TaskInfo.ID].Done = true
			}
			m.mu.Unlock()
		}
	}
}
