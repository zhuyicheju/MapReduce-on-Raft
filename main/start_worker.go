package main

import (
	"flag"
	"log"
	"mrrf/config"
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

	mapf, reducef := loadPlugin(pluginFile)

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
		log.Fatalf("cannot load plugin %v", filename)
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
