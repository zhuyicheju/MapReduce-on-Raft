package worker

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"mrrf/logging"
	"mrrf/rpcargs"
	"net/rpc"
	"os"
	"sort"
	"time"

	_ "mrrf/master"

	"go.uber.org/zap"
)

type request_t = int
type ReplyType = rpcargs.ReplyType
type ArgsType = rpcargs.ArgsType

var client []*rpc.Client

func nrand() int64 {
	return rand.Int63()
}

// Map functions return a slice of KeyValue.
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func netinit(addr []string) {
	client = make([]*rpc.Client, len(addr))
	for i, ip := range addr {
		_client, err := rpc.Dial("tcp", ip)
		if err != nil {
			panic("服务器未启动")
		}
		client[i] = _client
	}
}

// main/mrworker.go calls this function.
func Worker(addr []string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// fmt.Println("Worker running...")
	// wd, _ := os.Getwd()
	// outPath := filepath.Join(wd, "out")
	// os.Chdir(outPath)

	rand.Seed(time.Now().UnixNano())
	netinit(addr)

	args := ArgsType{Send_type: rpcargs.RPC_SEND_REQUEST}
	is_done := false
	for !is_done {
		args.Rand_Id = nrand()
		reply := ReplyType{}
		logging.Logger.Info("Worker 请求任务")
		ok := CallMaster("Master.RPChandle", &args, &reply)
		if ok {
			switch reply.Reply_type {
			case rpcargs.RPC_REPLY_REDUCE:
				logging.Logger.Info("获得Reduce任务\n", zap.Int("ID", reply.ID))
				do_reduce(reply.ID, reply.NMap, reducef)
			case rpcargs.RPC_REPLY_MAP:
				logging.Logger.Info("获得Map任务\n", zap.Int("ID", reply.ID))
				do_map(reply.ID, reply.File, reply.NReduce, mapf)
			case rpcargs.RPC_REPLY_DONE:
				logging.Logger.Info("任务已完成\n", zap.Int("ID", reply.ID))
				is_done = true
			case rpcargs.RPC_REPLY_WAIT:
				time.Sleep(1 * time.Second / 2)
			}
		} else {
			is_done = true
		}
	}
}

func do_map(id int, filename string, nReduce int, mapf func(string, string) []KeyValue) {
	// fmt.Printf("map %v %v\n", id, os.Getpid())
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		call_done(id, rpcargs.RPC_SEND_ERROR)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read read %v", filename)
		call_done(id, rpcargs.RPC_SEND_ERROR)
	}
	file.Close()

	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	var files []*os.File
	var reducefiles []*json.Encoder

	for i := 0; i < nReduce; i++ {
		jsonname := fmt.Sprintf("mr-%v-%v", id, i)
		file, err := os.CreateTemp("", jsonname)
		if err != nil {
			log.Fatalf("cannot create file %v", jsonname)
			call_done(id, rpcargs.RPC_SEND_ERROR)
		}
		enc := json.NewEncoder(file)
		files = append(files, file)
		reducefiles = append(reducefiles, enc)
		defer file.Close()
	}

	//创建json

	for _, kv := range kva {
		partition := ihash(kv.Key) % nReduce
		err := reducefiles[partition].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode json %v", err)
			call_done(id, rpcargs.RPC_SEND_ERROR)
		}
	}
	//填入json

	for i, file := range files {
		jsonname := fmt.Sprintf("mr-%v-%v", id, i)
		os.Rename(file.Name(), jsonname)
	}

	logging.Logger.Info("Map任务完成\n", zap.Int("ID", id))
	call_done(id, rpcargs.RPC_SEND_DONE_MAP)

}

func do_reduce(id int, nMap int, reducef func(string, []string) string) {
	var kva []KeyValue
	h := &MinHeap{}
	heap.Init(h)
	var reducefiles []*json.Decoder
	for i := 0; i < nMap; i++ {
		jsonname := fmt.Sprintf("mr-%v-%v", i, id)
		file, err := os.Open(jsonname)
		if err != nil {
			log.Fatalf("cannot open file %v", jsonname)
			call_done(id, rpcargs.RPC_SEND_ERROR)
		}
		dec := json.NewDecoder(file)
		reducefiles = append(reducefiles, dec)

		//初始化堆
		var kv KeyValue
		err = dec.Decode(&kv)
		if err != io.EOF {
			heap.Push(h, Merge{
				kv:    kv,
				index: i,
			})
		}

		defer file.Close()
	}

	for h.Len() > 0 {
		// merge := h.Pop().(Merge)
		merge := heap.Pop(h).(Merge)
		kva = append(kva, merge.kv)

		var kv KeyValue
		err := reducefiles[merge.index].Decode(&kv)
		if err != io.EOF { //已读完
			heap.Push(h, Merge{
				kv:    kv,
				index: merge.index,
			})
		}
	}

	oname := fmt.Sprintf("mr-out-%v", id)
	ofile, _ := os.CreateTemp("", oname)
	defer ofile.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce indexeroutput.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	os.Rename(ofile.Name(), oname)
	logging.Logger.Info("Reduce任务完成\n", zap.Int("ID", id))
	call_done(id, rpcargs.RPC_SEND_DONE_REDUCE)
}

func call_done(id int, _type int) {
	args := ArgsType{ID: id, Send_type: _type, Rand_Id: nrand()}
	reply := ReplyType{}
	CallMaster("Master.RPChandle", &args, &reply)
}

func CallMaster(name string, args interface{}, reply interface{}) bool {
	restart_time := 0
	restart := true

	for restart {
		restart = false
		for i := range client {
			logging.Logger.Debug("Worker 发送请求", zap.Int("服务器", i))
			err := client[i].Call(name, args, reply)
			if err != nil {
				restart_time++
				if restart_time < 10 {
					restart = true
				} else {
					logging.Logger.Debug("Master 服务器关闭")
				}
				continue
			}
			if reply.(*ReplyType).Reply_type == rpcargs.RPC_REPLY_WRONG_LEADER {
				logging.Logger.Debug("Worker 不是leader")
				continue
			}
			if reply.(*ReplyType).Reply_type == rpcargs.RPC_REPLY_TIMEOUT {
				logging.Logger.Debug("Worker 超时")
				restart = true
				break
			}
			logging.Logger.Debug("Worker 请求成功")
			return true
		}
	}
	return false
}
