package mr

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
)

// Map functions return a slice of KeyValue.

type ByKey []KeyValue
type MinHeap []Merge
type KeyValue struct {
	Key   string
	Value string
}

type Merge struct {
	kv    KeyValue
	index int
}

func (h *MinHeap) Len() int               { return len(*h) }
func (h *MinHeap) Less(i int, j int) bool { return (*h)[i].kv.Key < (*h)[j].kv.Key }
func (h *MinHeap) Swap(i int, j int)      { (*h)[i], (*h)[j] = (*h)[j], (*h)[i] }

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(Merge))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// fmt.Println("Worker running...")
	args := ArgsType{Send_type: RPC_SEND_REQUEST}
	is_done := false
	for {
		reply := ReplyType{}
		ok := call("Coordinator.Handle", args, &reply)
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
	ok := call("Coordinator.Handle", args, &reply)
	if !ok {
		log.Fatalf("rpc send error")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
