package worker

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
	"mrrf/rpcargs"
)


var IPaddr []string
var client []*rpc.Client

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// Map functions return a slice of KeyValue.
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func netinit(addr []string){
	IPaddr = addr
	client = make([]*rpc.Client, len(addr))
	for i,ip := addr {
		_client, err := rpc.Dial("tcp", ip)
		client[i] = _client
		if err != nil {
			panic("连接失败")
		}
	}
}

// main/mrworker.go calls this function.
func Worker(addr []string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// fmt.Println("Worker running...")
	
	netinit(addr)

	args := ArgsType{Send_type: RPC_SEND_REQUEST}
	is_done := false
	for {
		args.Rand_Id = nrand()
		reply := ReplyType{}
		ok := CallMaster("Master.RPChandle", args, &reply)
		if ok {
			switch reply.Reply_type {
			case RPC_REPLY_REDUCE:
				do_reduce(reply.ID, reply.NMap, reducef)
			case RPC_REPLY_MAP:
				do_map(reply.ID, reply.File, reply.NReduce, mapf)
			case RPC_REPLY_DONE:
				is_done = true
			case RPC_REPLY_WAIT:
				time.Sleep(1 * time.Second / 2)
			}
		} else {
			log.Fatalf("rpc send error")
		}
		if is_done {
			break
		}
	}

}

func do_map(id int, filename string, nReduce int, mapf func(string, string) []KeyValue) {
	// fmt.Printf("map %v %v\n", id, os.Getpid())
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		call_done(id, RPC_SEND_ERROR)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read read %v", filename)
		call_done(id, RPC_SEND_ERROR)
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
			call_done(id, RPC_SEND_ERROR)
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
			call_done(id, RPC_SEND_ERROR)
		}
	}
	//填入json

	for i, file := range files {
		jsonname := fmt.Sprintf("mr-%v-%v", id, i)
		os.Rename(file.Name(), jsonname)
	}

	call_done(id, RPC_SEND_DONE_MAP)

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
			call_done(id, RPC_SEND_ERROR)
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
	call_done(id, RPC_SEND_DONE_REDUCE)
}

func call_done(id int, _type int) {
	// fmt.Printf("done %v %v\n", id, os.Getpid())
	args := ArgsType{ID: id, Send_type: _type}
	reply := ReplyType{}
	ok := CallMaster("Master.RPChandle", args, &reply)
	if !ok {
		log.Fatalf("rpc send error")
	}
}

func CallMaster(name string, args interface{}, reply interface{}) bool {
	restart := true
	for restart{
		restart = false
		for i := range client {
			err := client[i].Call(name, args, reply)
			if err {
				restart = true
				continue
			}
			if reply.Reply_type == RPC_REPLY_WRONG_LEADER {
				continue
			}
			if reply.Reply_type == RPC_REPLY_TIMEOUT {
				restart = true
				break
			}
			return true
		}
	}
}