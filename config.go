package config

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
)

type Config struct {
	n        int
	rafts    []*Raft
	applyChs []chan ApplyMsg
	addrs    []string // TCP 地址
	// saved    []*Persister
}

func MakeConfig(n int, basePort int) *Config {
	cfg := &Config{}
	cfg.n = n
	cfg.rafts = make([]*Raft, n)
	cfg.applyChs = make([]chan ApplyMsg, n)
	cfg.addrs = make([]string, n)
	cfg.saved = make([]*Persister, n)

	for i := 0; i < n; i++ {
		port := basePort + i
		cfg.addrs[i] = fmt.Sprintf("127.0.0.1:%d", port)
	}

	for i := 0; i < n; i++ {
		cfg.startRaft(i)
	}

	return cfg
}

func (cfg *Config) startRaft(me int) {
	applyCh := make(chan ApplyMsg)
	cfg.applyChs[me] = applyCh

	// if cfg.saved[me] == nil {
	// 	cfg.saved[me] = MakePersister()
	// } else {
	// 	cfg.saved[me] = cfg.saved[me].Copy()
	// }

	rf := Raft.Make(cfg.addrs, me, nil, applyCh)
	cfg.rafts[me] = rf

	// 启动 RPC 服务端
	go func(me int) {
		rpc.Register(rf)
		listener, err := net.Listen("tcp", cfg.addrs[me])
		if err != nil {
			log.Fatalf("Raft %d listen error: %v", me, err)
		}
		for {
			conn, err := listener.Accept()
			if err == nil {
				go rpc.ServeConn(conn)
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
	cfg := MakeConfig(3, 8000)

	// 模拟提交一个命令到 Raft 0
	index, term, isLeader := cfg.rafts[0].Start("hello world")
	fmt.Printf("Index: %d, Term: %d, IsLeader: %v\n", index, term, isLeader)
}
