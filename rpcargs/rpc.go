package rpcargs

const (
	RPC_REPLY_WAIT = iota
	RPC_REPLY_MAP
	RPC_REPLY_REDUCE
	RPC_REPLY_DONE
	RPC_REPLY_TIMEOUT //重试
	RPC_REPLY_WRONG_LEADER
)

const (
	RPC_SEND_REQUEST = iota
	RPC_SEND_DONE_MAP
	RPC_SEND_DONE_REDUCE
	RPC_SEND_ERROR
)

type request_t = int

type ArgsType struct {
	Rand_Id   int64
	Send_type int
	ID        int
}

type ReplyType struct {
	Err        string
	Reply_type request_t
	ID         int
	File       string
	NReduce    int
	NMap       int
}
