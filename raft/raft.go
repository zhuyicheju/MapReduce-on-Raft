package raft

import (
	"math/rand"
	"mrrf/logging"
	"net/rpc"
	"sync"
	"sync/atomic"

	"time"

	"go.uber.org/zap"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term int
	Log  interface{}
}

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type Raft struct {
	mu        sync.Mutex
	peers     []string
	client    []*rpc.Client
	persister *Persister
	me        int
	dead      int32
	applyCh   chan ApplyMsg

	currentTerm int
	votedFor    int

	log []LogEntry

	snapshot      []byte
	snapshotIndex int
	snapshotTerm  int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	heartbeat_timestamp int64

	state int

	median_tracker *MedianTracker

	done bool
	rw   sync.RWMutex
}

type RequestVoteArgs struct {
	Term         int
	CandidatedId int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int

	Entries []LogEntry

	LeaderCommit int
}

type AppendEntriesReply struct {
	Me            int
	Term          int
	Success       bool
	Append_num    int
	ConflictIndex int
	ConflictTerm  int
	PrevLogIndex  int
	LeaderTerm    int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Me         int
	Term       int
	Newindex   int
	LeaderTerm int
	Success    bool
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int(rf.currentTerm), rf.state == LEADER
}

func (rf *Raft) persist() {
	return
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.snapshotIndex)
	// e.Encode(rf.snapshotTerm)
	// e.Encode(rf.log)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	return
	// if data == nil || len(data) < 1 { // bootstrap without any state?
	// 	return
	// }
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var currentTerm int
	// var votedFor int
	// var snapshotIndex int
	// var snapshotTerm int
	// var log []LogEntry
	// if d.Decode(&currentTerm) != nil ||
	// 	d.Decode(&votedFor) != nil ||
	// 	d.Decode(&snapshotIndex) != nil ||
	// 	d.Decode(&snapshotTerm) != nil ||
	// 	d.Decode(&log) != nil {
	// 	panic("持久化状态解码错误")
	// }

	// rf.currentTerm = currentTerm
	// rf.votedFor = votedFor
	// rf.snapshotIndex = snapshotIndex
	// rf.commitIndex = snapshotIndex
	// rf.log = log
	// rf.snapshotTerm = snapshotTerm
}

// 日志总长度，不包括空索引
func (rf *Raft) LogLen() int {
	return len(rf.log) - 1 + rf.snapshotIndex
}

// 日志总索引转当前索引
func (rf *Raft) TrueIdx2FakeIdx(index int) int {
	return index - rf.snapshotIndex
}

// 当前索引转日志总索引
func (rf *Raft) FakeIdx2TrueIdx(index int) int {
	return index + rf.snapshotIndex
}

// 获得最后一个日志的Term
func (rf *Raft) LastLogTerm() int {
	if len(rf.log) <= 1 {
		return rf.snapshotTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) TermIndex(index int) int {
	if index > rf.snapshotIndex {
		return rf.log[rf.TrueIdx2FakeIdx(index)].Term
	}
	if index == rf.snapshotIndex {
		return rf.snapshotTerm
	}

	logging.Logger.Error("错误获取快照日志\n")
	panic("错误获取快照日志")
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	return
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotIndex {
		return
	}

	if rf.TrueIdx2FakeIdx(index) < len(rf.log) {
		//如果进入，则是被动调用snapshot
		//如果没有进入，则为follower接收日志后自己调用snapshot，所以缺少最后一个term
		rf.snapshotTerm = rf.log[rf.TrueIdx2FakeIdx(index)].Term
	}

	cut := rf.TrueIdx2FakeIdx(index + 1)
	if cut < len(rf.log) {
		newLog := make([]LogEntry, len(rf.log[cut:]))
		copy(newLog, rf.log[cut:])
		rf.log = append([]LogEntry{{Term: 0}}, newLog...)
	} else {
		rf.log = []LogEntry{{Term: 0}}
	}

	rf.snapshotIndex = index
	rf.snapshot = snapshot

	rf.commitIndex = max(rf.commitIndex, index)
	rf.persist()
}

func (rf *Raft) ParallelCommit(index int, log []LogEntry) {
	for i := range log {
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: log[i].Log, CommandIndex: i + index}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if rf.killed() {
		return nil
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	reply.Me = rf.me
	reply.PrevLogIndex = args.PrevLogIndex
	reply.LeaderTerm = args.Term

	if args.Term < currentTerm {
		//任期过低
		reply.Success = false
		return nil
	}
	rf.heartbeat_timestamp = time.Now().UnixMilli()
	if args.Term > rf.currentTerm {
		rf.ChangeState(args.Term, -1, FOLLOWER)
	}

	if args.PrevLogIndex > rf.FakeIdx2TrueIdx(len(rf.log)-1) || args.PrevLogTerm != rf.TermIndex(args.PrevLogIndex) {
		//日志不匹配
		//如果只是简单的对NextIndex逐步减1，则这该测试点很可能不通过。(leader backs up quickly over incorrect follower logs)
		//两种情况，prevlogindex可能在当前log右边，或者在左边和中间

		if len(rf.log) != 1 && args.PrevLogIndex <= rf.FakeIdx2TrueIdx(len(rf.log)-1) {
			rf.log = rf.log[:rf.TrueIdx2FakeIdx(args.PrevLogIndex)]
		}
		reply.ConflictIndex = min(args.PrevLogIndex, rf.FakeIdx2TrueIdx(len(rf.log)))
		if args.PrevLogIndex >= rf.FakeIdx2TrueIdx(len(rf.log)) {
			reply.ConflictIndex = rf.FakeIdx2TrueIdx(len(rf.log))
		} else {
			reply.ConflictTerm = rf.TermIndex(args.PrevLogIndex)
			l := 0
			r := rf.TrueIdx2FakeIdx(args.PrevLogIndex) + 1
			for l+1 < r {
				mid := (l + r) / 2
				if rf.log[mid].Term >= reply.ConflictTerm {
					r = mid
				} else {
					l = mid
				}
			}
			reply.ConflictIndex = rf.FakeIdx2TrueIdx(r) // 第一=term的下标
		}
		reply.Success = false
		return nil
	}

	for i := range args.Entries {
		if args.PrevLogIndex+i+1 > rf.FakeIdx2TrueIdx(len(rf.log)-1) {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
		if rf.TermIndex(args.PrevLogIndex+i+1) != args.Entries[i].Term {
			rf.log = rf.log[:rf.TrueIdx2FakeIdx(args.PrevLogIndex+i+1)]
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	reply.Append_num = len(args.Entries)
	reply.Success = true
	if args.LeaderCommit > rf.commitIndex {
		commitlog := rf.log[rf.TrueIdx2FakeIdx(rf.commitIndex+1) : min(rf.TrueIdx2FakeIdx(args.LeaderCommit), len(rf.log)-1)+1]
		go rf.ParallelCommit(rf.commitIndex+1, commitlog)
		rf.commitIndex = min(args.LeaderCommit, rf.FakeIdx2TrueIdx(len(rf.log)-1))
	}
	return nil
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	if rf.killed() {
		return nil
	}

	rf.mu.Lock()

	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.commitIndex {
		rf.mu.Unlock()
		reply.Success = false
		return nil
	}

	reply.Success = true
	reply.LeaderTerm = args.Term
	reply.Me = rf.me
	reply.Term = rf.currentTerm
	reply.Newindex = args.LastIncludedIndex

	rf.snapshotTerm = args.LastIncludedTerm

	rf.heartbeat_timestamp = time.Now().UnixMilli()

	rf.applyCh <- ApplyMsg{SnapshotValid: true, Snapshot: args.Data, SnapshotTerm: args.LastIncludedTerm, SnapshotIndex: args.LastIncludedIndex}

	rf.log = []LogEntry{{Term: 0}}

	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshot = args.Data

	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.persist()

	rf.mu.Unlock()
	//在无锁调用
	// rf.Snapshot(args.LastIncludedIndex, args.Data)
	return nil
}

func (rf *Raft) PreRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	if rf.killed() {
		return nil
	}

	rf.mu.Lock()

	defer rf.mu.Unlock()

	currentTerm := rf.currentTerm
	if args.Term < currentTerm || (args.Term == currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidatedId) ||
		args.LastLogTerm < rf.LastLogTerm() ||
		(args.LastLogTerm == rf.LastLogTerm() && rf.TrueIdx2FakeIdx(args.LastLogIndex) < len(rf.log)-1) {
		reply.VoteGranted = false
		return nil
	}

	rf.heartbeat_timestamp = time.Now().UnixMilli()
	reply.VoteGranted = true
	return nil
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	if rf.killed() {
		return nil
	}

	rf.mu.Lock()

	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term > rf.currentTerm {
		rf.ChangeState(args.Term, -1, FOLLOWER)
	}

	if args.Term < rf.currentTerm ||
		(rf.votedFor != -1 && rf.votedFor != args.CandidatedId) ||
		args.LastLogTerm < rf.LastLogTerm() ||
		(args.LastLogTerm == rf.LastLogTerm() && rf.TrueIdx2FakeIdx(args.LastLogIndex) < len(rf.log)-1) {

		reply.VoteGranted = false
		return nil
	}

	rf.heartbeat_timestamp = time.Now().UnixMilli()
	reply.VoteGranted = true
	rf.ChangeState(rf.currentTerm, args.CandidatedId, FOLLOWER)
	return nil
}

func (rf *Raft) ReceiveReply(stop chan struct{}, entries_replyCh chan *AppendEntriesReply, snapshot_replyCh chan *InstallSnapshotReply, done *bool) {
	for !rf.killed() {
		select {
		case <-stop:
			rf.rw.Lock()
			*done = true
			close(entries_replyCh)
			close(snapshot_replyCh)
			rf.rw.Unlock()
			return
		case reply := <-snapshot_replyCh:
			rf.mu.Lock()

			if reply.LeaderTerm != rf.currentTerm {
				rf.mu.Unlock()
				break
			}

			if reply.Term > rf.currentTerm {
				rf.ChangeState(reply.Term, -1, FOLLOWER)
				rf.mu.Unlock()
				break
			}

			if !reply.Success {
				rf.mu.Unlock()
				break
			}
			rf.nextIndex[reply.Me] = reply.Newindex + 1
			rf.matchIndex[reply.Me] = reply.Newindex
			rf.mu.Unlock()

		case reply := <-entries_replyCh:
			rf.mu.Lock()

			//reply类型 success term
			//正常成功   true    ==
			//任期过期   false   >
			//日志冲突   false   ==  term相同，日志不一致，prevLogIndex 或 prevLogTerm 处与 leader 不匹配。
			//日志太短   false   ==  leader 给出的 prevLogIndex 超过了跟随者日志的末尾。
			if reply.LeaderTerm != rf.currentTerm {
				rf.mu.Unlock()
				break
			}

			if rf.state == FOLLOWER {
				//因接到更高term的rpc
				rf.mu.Unlock()
				break
			}
			if rf.currentTerm < reply.Term {
				//任期过期
				rf.ChangeState(reply.Term, -1, FOLLOWER)
				//持有锁退出
				rf.mu.Unlock()
				break
			}

			if !reply.Success {
				//日志冲突&日志太短

				//寻找leader和follower一致日志
				if reply.ConflictTerm == 0 {
					rf.nextIndex[reply.Me] = reply.ConflictIndex
				} else {
					l := 0
					r := rf.TrueIdx2FakeIdx(reply.PrevLogIndex)
					for l+1 < r {
						mid := (l + r) / 2
						if rf.log[mid].Term <= reply.ConflictTerm {
							l = mid
						} else {
							r = mid
						}
					}
					if rf.log[l].Term == reply.ConflictTerm {
						rf.nextIndex[reply.Me] = rf.FakeIdx2TrueIdx(l + 1) // 最后一个<=term的下标
					} else {
						rf.nextIndex[reply.Me] = reply.ConflictIndex
					}
				}
			} else {
				//正常成功
				rf.nextIndex[reply.Me] = reply.PrevLogIndex + reply.Append_num + 1
				rf.matchIndex[reply.Me] = rf.nextIndex[reply.Me] - 1
				if reply.Append_num != 0 {
					rf.median_tracker.Add(reply.Me, rf.matchIndex[reply.Me])
					median := rf.median_tracker.GetMedian()

					if rf.TermIndex(median) == rf.currentTerm && median > rf.commitIndex {
						commitLog := rf.log[rf.TrueIdx2FakeIdx(rf.commitIndex+1):rf.TrueIdx2FakeIdx(median+1)]
						go rf.ParallelCommit(rf.commitIndex+1, commitLog)

						rf.commitIndex = median
					}
				}
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.FakeIdx2TrueIdx(len(rf.log))
	term := int(rf.currentTerm)
	isLeader := (rf.state == LEADER)

	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Log: command})
		rf.persist()
		rf.median_tracker.Add(rf.me, rf.FakeIdx2TrueIdx(len(rf.log)-1))
	}
	return index, term, isLeader
}

func Leader(rf *Raft) {
	//进入时持有锁

	logging.Logger.Debug("进入Leader\n", zap.Int("me", rf.me))

	rf.state = LEADER

	//no-op机制
	// rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Log: nil})
	// rf.persist()
	// rf.median_tracker.Add(rf.me, len(rf.log)-1)

	peers_num := len(rf.peers)

	rf.median_tracker.Add(rf.me, rf.FakeIdx2TrueIdx(len(rf.log)-1))

	for i := range rf.matchIndex {
		//初始化每个数组的匹配日志
		rf.matchIndex[i] = -1
		rf.nextIndex[i] = rf.FakeIdx2TrueIdx(len(rf.log))
	}

	done := false
	entries_replyCh := make(chan *AppendEntriesReply, 2*peers_num)
	snapshot_replyCh := make(chan *InstallSnapshotReply, 2*peers_num)
	stopCh := make(chan struct{})
	go rf.ReceiveReply(stopCh, entries_replyCh, snapshot_replyCh, &done)

	for !rf.killed() {

		for i := 0; i < peers_num; i++ {
			if i == rf.me {
				continue
			}

			if rf.nextIndex[i] > rf.snapshotIndex {
				prevlogindex := rf.nextIndex[i] - 1
				term := rf.currentTerm
				prevlogterm := rf.TermIndex(prevlogindex)
				leadercommit := rf.commitIndex
				entries := make([]LogEntry, 0)
				if rf.FakeIdx2TrueIdx(len(rf.log)-1) >= rf.nextIndex[i] {
					entries = append(entries, rf.log[rf.TrueIdx2FakeIdx(rf.nextIndex[i]):]...)
				}

				go func(server int) {
					args := AppendEntriesArgs{Term: term, LeaderId: rf.me,
						PrevLogIndex: prevlogindex, PrevLogTerm: prevlogterm,
						LeaderCommit: leadercommit,
						Entries:      entries}
					reply := AppendEntriesReply{}
					ok := RPCCall(rf, server, "Raft.AppendEntries", &args, &reply)
					if ok {
						// // // fmt.Printf("收到回复 %v\n", server)
						msg := &reply
						// // // fmt.Printf("获取读锁 %v\n", server)
						rf.rw.RLock()
						if !done {
							// // // fmt.Printf("加入管道 %v\n", server)
							entries_replyCh <- msg
						} else {
							// // // fmt.Printf("管道已关闭 %v\n", server)
						}
						rf.rw.RUnlock()
					}

				}(i)
			} else {
				go func(server int) {
					// // // fmt.Printf("Server %v: 向%v发送快照 prev %v last: %v Term %v\n", rf.me, server, rf.nextIndex[server]-1, rf.snapshotIndex, rf.snapshotTerm)
					args := InstallSnapshotArgs{Term: rf.currentTerm, LeaderId: rf.me,
						LastIncludedIndex: rf.snapshotIndex, Data: rf.snapshot, LastIncludedTerm: rf.snapshotTerm}
					reply := InstallSnapshotReply{}
					ok := RPCCall(rf, server, "Raft.InstallSnapshot", &args, &reply)

					if ok {
						// // // fmt.Printf("收到回复 %v", server)
						msg := &reply

						rf.rw.RLock()
						if !done {
							// // // fmt.Printf("加入管道 %v", server)
							snapshot_replyCh <- msg
						}
						rf.rw.RUnlock()
					}

				}(i)
			}
		}

		rf.mu.Unlock()

		ms := 50

		time.Sleep(time.Duration(ms) * time.Millisecond) //选举超时时间

		rf.mu.Lock()

		if rf.state == FOLLOWER {
			//因接到更高term的rpc
			//持有锁退出
			close(stopCh)
			return
		}

	}
}

func Prevote(rf *Raft) bool {
	peers_num := len(rf.peers)
	granted_cnt := 1
	args := RequestVoteArgs{Term: rf.currentTerm + 1, CandidatedId: rf.me,
		LastLogIndex: rf.FakeIdx2TrueIdx(len(rf.log) - 1), LastLogTerm: rf.LastLogTerm()}

	var rw sync.RWMutex
	done := false
	replyCh := make(chan *RequestVoteReply, peers_num-1)

	for i := 0; i < peers_num; i++ {
		if i == rf.me {
			continue
		}

		// // // fmt.Printf("Server %v: 向%v发送投票\n", rf.me, i)
		go func(server int) {
			reply := RequestVoteReply{}
			ok := RPCCall(rf, server, "Raft.PreRequestVote", &args, &reply)

			var msg *RequestVoteReply
			if ok {
				msg = &reply
			} else {
				msg = nil
			}

			rw.RLock()
			if !done {
				replyCh <- msg
			}
			rw.RUnlock()

		}(i)
	}

	rf.mu.Unlock()

	ms := 300 + (rand.Int63() % 200)
	time.Sleep(time.Duration(ms) * time.Millisecond) //选举超时时间

	rw.Lock()
	done = true
	close(replyCh)
	rw.Unlock()

	rf.mu.Lock()

	if rf.state == FOLLOWER {
		//因接到更高term的rpc
		return false
	}

	for i := 0; i < peers_num-1; i++ {
		reply, ok := <-replyCh
		if ok && reply != nil {
			if reply.VoteGranted {
				granted_cnt++
				if granted_cnt >= (peers_num)/2+1 {
					return true
				}
			} else {
				//还有对方任期小，但已投过, 情况

				if reply.Term > rf.currentTerm {
					return false
				}
			}
		}
	}
	return false
}

func Candidate(rf *Raft) {
	//进入时持有锁

	//PreVote机制
	rf.state = CANDIDATE
	if !Prevote(rf) {
		rf.state = FOLLOWER
		return
	}

	peers_num := len(rf.peers)
	for !rf.killed() {
		granted_cnt := 1
		rf.ChangeState(rf.currentTerm+1, rf.me, rf.state)
		args := RequestVoteArgs{Term: rf.currentTerm, CandidatedId: rf.me,
			LastLogIndex: rf.FakeIdx2TrueIdx(len(rf.log) - 1), LastLogTerm: rf.LastLogTerm()}

		var rw sync.RWMutex
		done := false
		replyCh := make(chan *RequestVoteReply, peers_num-1)

		for i := 0; i < peers_num; i++ {
			if i == rf.me {
				continue
			}

			go func(server int) {
				reply := RequestVoteReply{}
				ok := RPCCall(rf, server, "Raft.RequestVote", &args, &reply)
				var msg *RequestVoteReply
				if ok {
					msg = &reply
				} else {
					msg = nil
				}

				rw.RLock()
				if !done {
					replyCh <- msg
				}
				rw.RUnlock()

			}(i)
		}

		rf.mu.Unlock()

		ms := 300 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond) //选举超时时间

		rw.Lock()
		done = true
		close(replyCh)
		rw.Unlock()

		rf.mu.Lock()

		if rf.state == FOLLOWER {
			//因接到更高term的rpc
			return
		}

		for i := 0; i < peers_num-1; i++ {
			reply, ok := <-replyCh
			if ok && reply != nil {
				if reply.VoteGranted {
					granted_cnt++
					if granted_cnt >= (peers_num)/2+1 {
						Leader(rf)
						return //若是leader被打为follower直接return到tick
					}
				} else {
					//还有对方任期小，但已投过, 情况

					if reply.Term > rf.currentTerm {
						//对方任期大
						rf.ChangeState(reply.Term, -1, FOLLOWER)
						return //降为follower
					}
				}
			}
		}

	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		ms := 300 + (rand.Int63() % 200)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		rf.mu.Lock()
		if time.Since(time.UnixMilli(rf.heartbeat_timestamp)).Milliseconds() > ms {
			//超时
			//自下向上转换不需要手动添加状态转换
			Candidate(rf)
		}
		rf.mu.Unlock()
	}
}

func Make(peers []string, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.client = make([]*rpc.Client, len(rf.peers))
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.median_tracker = NewMedianTracker(make([]int, len(rf.peers)))

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// rf.readPersist(persister.ReadRaftState())
	// rf.snapshot = rf.persister.ReadSnapshot()

	logging.Logger.Debug("Raft实体构建\n", zap.Int("me", me))
	return rf
}

func (rf *Raft) Open() {
	for i, addr := range rf.peers {
		if i == rf.me {
			continue
		}
		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			logging.Logger.Error("产生错误\n", zap.Error(err))
			panic(err)
		}
		rf.client[i] = client
	}

	go rf.ticker()
}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

	rf.mu.Lock()
	for i, client := range rf.client {
		if i == rf.me {
			continue
		}
		client.Close()
	}
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ChangeState(term int, votefor int, state int) {
	rf.currentTerm = term
	rf.votedFor = votefor
	rf.persist()
	rf.state = state
}

func RPCCall(rf *Raft, server int, name string, args interface{}, reply interface{}) bool {
	if server == rf.me {
		panic("RPC自己")
	}
	err := rf.client[server].Call(name, args, reply)
	if err != nil {
		logging.Logger.Error("产生错误\n", zap.Error(err))
		panic(err)
	}
	return err == nil
}
