package main

import (
	"flag"
	"log"
	"mrrf/config"
	"mrrf/logging"
	"mrrf/worker"
	"plugin"
	"sync"
)

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

func main() {
	configPath := flag.String("config", "config.yaml", "配置文件路径")
	flag.Parse()

	err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("读取配置失败: %v", err)
	}

	nWorker := config.GlobalConfig.Worker.NWorker
	pluginFile := config.GlobalConfig.Worker.Plugin
	addrs := config.GlobalConfig.Raft.IPaddr
	level := config.GlobalConfig.Log.Level
	logfile := config.GlobalConfig.Log.WorkerFile

	mapf, reducef := loadPlugin(pluginFile)

	logging.InitLogger(level, logfile)
	defer logging.Logger.Sync()

	logging.Logger.Info("服务启动")

	var wg sync.WaitGroup

	for i := 0; i < nWorker; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker.Worker(addrs, mapf, reducef)
		}(i)
	}

	wg.Wait()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []worker.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v %v", filename, err)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []worker.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
