package main

import (
	"fmt"
	"log"
	"mrrf/raft"
	"net"
	"net/rpc"
	"time"
)

type ApplyMsg = raft.ApplyMsg
type Raft = raft.Raft
type Persister = raft.Persister

var IPaddr = [4]string{
	"127.0.0.1:8000",
	"127.0.0.1:8001",
	"127.0.0.1:8002",
	"127.0.0.1:8003",
}

type Config struct {
	n        int
	rafts    []*Raft
	applyChs []chan ApplyMsg
	addrs    []string // TCP 地址
	saved    []*Persister
}

func MakeConfig(n int) *Config {
	cfg := &Config{}
	cfg.n = n
	cfg.rafts = make([]*Raft, n)
	cfg.applyChs = make([]chan ApplyMsg, n)
	cfg.addrs = make([]string, n)
	cfg.saved = make([]*Persister, n)

	for i := 0; i < n; i++ {
		cfg.addrs[i] = IPaddr[i]
	}

	done := make(chan bool, n)
	for i := 0; i < n; i++ {
		go cfg.makeRaft(i, done)
	}
	//先创建实例与监听

	for i := 0; i < n; i++ {
		<-done
	}
	close(done)

	//在连接和启动
	for i := 0; i < n; i++ {
		go cfg.rafts[i].Open()
	}

	return cfg
}

func (cfg *Config) makeRaft(me int, done chan bool) {
	applyCh := make(chan ApplyMsg)
	cfg.applyChs[me] = applyCh

	// if cfg.saved[me] == nil {
	// 	cfg.saved[me] = MakePersister()
	// } else {
	// 	cfg.saved[me] = cfg.saved[me].Copy()
	// }

	rf := raft.Make(cfg.addrs, me, cfg.saved[me], applyCh)
	cfg.rafts[me] = rf

	// 启动 RPC 服务端
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

	// 启动 applyLoop
	go func(me int, applyCh chan ApplyMsg) {
		for msg := range applyCh {
			log.Printf("[Raft %d] Apply: %+v", me, msg)
		}
	}(me, applyCh)
}

func main() {
	cfg := MakeConfig(3)

	time.Sleep(100 * time.Second)

	// 模拟提交一个命令到 Raft 0
	index, term, isLeader := cfg.rafts[0].Start("hello world")
	fmt.Printf("Index: %d, Term: %d, IsLeader: %v\n", index, term, isLeader)
	index, term, isLeader = cfg.rafts[1].Start("hello world")
	fmt.Printf("Index: %d, Term: %d, IsLeader: %v\n", index, term, isLeader)
	index, term, isLeader = cfg.rafts[2].Start("hello world")
	fmt.Printf("Index: %d, Term: %d, IsLeader: %v\n", index, term, isLeader)
}
