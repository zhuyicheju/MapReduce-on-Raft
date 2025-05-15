package main

import (
	"mrrf/raft"
	"mrrf/master"
	"sync"
)

var IPaddr = [4]string{
	"127.0.0.1:8000",
	"127.0.0.1:8001",
	"127.0.0.1:8002",
	"127.0.0.1:8003",
}

var files []string

type config struct {
	mu      sync.Mutex
	n       int
	addrs   []string
	masters []*Master
	saved   []*raft.Persister
	// endnames     [][]string // names of each server's sending ClientEnds
	// clerks       map[*Clerk][]string
	// nextClientId int
	// maxraftstate int
	// start        time.Time // time at which make_config() was called
	// // begin()/end() statistics
	// t0    time.Time // time at which test_test.go called cfg.begin()
	// rpcs0 int       // rpcTotal() at start of test
	// ops   int32     // number of clerk get/put/append method calls
}

func make_config(n int) *config {
	cfg := &Config{}
	cfg.n = n
	cfg.addrs = make([]string, n)
	cfg.saved = make([]*Persister, n)
	cfg.masters = make([]*Master, n)

	for i := 0; i < n; i++ {
		cfg.addrs[i] = IPaddr[i]
	}

	done := make(chan bool, n)
	for i := 0; i < n; i++ {
		go cfg.makeMaster(i, done)
	}
	//先创建实例与监听

	for i := 0; i < n; i++ {
		<-done
	}
	close(done)

	//在连接和启动
	for i := 0; i < n; i++ {
		go cfg.masters[i].Open()
	}

	return cfg
}

func makeMaster(me int, done chan bool) {
	master := master.MakeMaster(cfg.addrs, me, cfg.saved[me], 100, done, files, 10)
	cfg.masters[me] = master
}

func main() {
	make_config(3)
}
