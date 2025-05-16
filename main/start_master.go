package main

import (
	"flag"
	"fmt"
	"mrrf/config"
	"mrrf/logging"
	"mrrf/master"
	"mrrf/raft"
	"sync"
)

type Master = master.Master

// type Persister = raft.Persister

type RuntimeMaster struct {
	mu      sync.Mutex
	n       int
	addrs   []string
	masters []*Master
	saved   []*raft.Persister
}

func makeRuntimeMaster(n int, nReduce int, wg *sync.WaitGroup, files []string, IPaddr []string) *RuntimeMaster {
	cfg := &RuntimeMaster{}
	cfg.n = n
	cfg.addrs = make([]string, n)
	cfg.saved = make([]*Persister, n)
	cfg.masters = make([]*Master, n)

	for i := 0; i < n; i++ {
		cfg.addrs[i] = IPaddr[i]
	}

	done := make(chan bool, n)
	for i := 0; i < n; i++ {
		go cfg.makeMaster(i, nReduce, files, done, wg)
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

func (cfg *RuntimeMaster) makeMaster(me int, nReduce int, files []string, done chan bool, wg *sync.WaitGroup) {
	master := master.MakeMaster(cfg.addrs, me, cfg.saved[me], 100, done, files, nReduce, wg)
	cfg.masters[me] = master
}

func main() {
	configPath := flag.String("config", "config.yaml", "YAML 配置文件路径")
	flag.Parse()

	if err := config.LoadConfig(*configPath); err != nil {
		panic(fmt.Sprintf("配置加载失败: %v", err))
	}

	nMaster := config.GlobalConfig.Master.NMaster
	nReduce := config.GlobalConfig.Master.NReduce
	addrs := config.GlobalConfig.Raft.IPaddr
	files := config.GlobalConfig.Master.Files
	level := config.GlobalConfig.Log.Level
	logfile := config.GlobalConfig.Log.MasterFile

	logging.InitLogger(level, logfile)
	defer logging.Logger.Sync()

	logging.Logger.Info("服务启动")

	var wg sync.WaitGroup

	wg.Add(nMaster)
	makeRuntimeMaster(nMaster, nReduce, &wg, files, addrs)

	wg.Wait()
}
