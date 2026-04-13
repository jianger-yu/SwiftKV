package raft

// 文件 raftapi/raft.go 定义了 Raft 必须向服务器（或测试器）
// 暴露的接口，但请查看下面每个函数的注释以获取详细说明。
//
// Make() 用于创建一个实现了 Raft 接口的新 Raft 节点。

import (
	//	"bytes"
	"bytes"
	"encoding/gob"
	"math/rand"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	//	"6.5840/labrpc"
	raftapi "kvraft/pkg/raftapi"
	//	tester "6.5840/tester1"
)

var persistBufferPool = sync.Pool{
	New: func() interface{} {
		return new(bytes.Buffer)
	},
}

type Persister interface {
	ReadRaftState() []byte
	ReadSnapshot() []byte
	Save(raftstate []byte, snapshot []byte)
	RaftStateSize() int
}

const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type persistentState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

// 实现单个 Raft 节点的 Go 对象。
type Raft struct {
	mu        sync.Mutex // 锁，用于保护该节点状态的并发访问
	peers     []string   // 所有节点的 RPC 端点
	persister Persister  // 用于保存该节点持久化状态的对象
	me        int        // 当前节点在 peers[] 中的索引
	dead      int32      // 由 Kill() 设置为已死亡

	Votes int
	state int

	electionTimeout time.Duration
	lastHeard       time.Time

	// 所有服务器上的持久化状态，在回复RPC之前更新持久化存储
	CurrentTerm int
	VotedFor    int // 保存的是 candidate 的 ID
	log         []LogEntry

	// 所有服务器上的易失性状态
	CommitIndex int
	LastApplied int

	//  leader上的易失性状态，在选举之后重新初始化
	nextIndex  []int
	matchIndex []int

	applyCh chan raftapi.ApplyMsg

	heartbeatInterval time.Duration
	replicateTrigger  chan struct{}
	leaseRatio        float64
	leaseUntil        time.Time

	lastIncludedIndex int
	lastIncludedTerm  int

	// RPC 客户端连接缓存
	rpcMu      sync.Mutex
	rpcClients map[string]*rpc.Client
}

// callPeer 通过 RPC 调用远程 Raft 节点
func (rf *Raft) callPeer(server int, method string, args interface{}, reply interface{}) bool {
	if server == rf.me {
		return false
	}

	if server < 0 || server >= len(rf.peers) {
		return false
	}

	address := rf.peers[server]

	rf.rpcMu.Lock()
	client := rf.rpcClients[address]
	rf.rpcMu.Unlock()

	if client == nil {
		newClient, err := rpc.Dial("tcp", address)
		if err != nil {
			return false
		}
		rf.rpcMu.Lock()
		if existing := rf.rpcClients[address]; existing != nil {
			_ = newClient.Close()
			client = existing
		} else {
			rf.rpcClients[address] = newClient
			client = newClient
		}
		rf.rpcMu.Unlock()
	}

	err := client.Call(method, args, reply)
	if err == nil {
		return true
	}

	// 连接失效时清理缓存，后续自动重建。
	rf.rpcMu.Lock()
	if cur := rf.rpcClients[address]; cur == client {
		delete(rf.rpcClients, address)
		_ = client.Close()
	}
	rf.rpcMu.Unlock()
	return false
}

// 返回当前任期以及该服务器是否认为自己是领导者。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.CurrentTerm, rf.state == Leader
}

// IsLeaderWithLease returns true only when this node is leader and the lease is still valid.
func (rf *Raft) IsLeaderWithLease() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader || rf.killed() {
		return false
	}
	return time.Now().Before(rf.leaseUntil)
}

func (rf *Raft) getLastLogIndex() int {
	// return len(rf.log) - 1
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	// return rf.log[len(rf.log)-1].Term
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getTerm(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[index-rf.lastIncludedIndex].Term
}

// 将 Raft 的持久状态保存到稳定存储中，
// 以便在崩溃并重启后能够恢复。
func (rf *Raft) persist() {
	data := encodePersistentState(rf.capturePersistentState())
	// Regular persist does not need to rewrite snapshot each time.
	rf.persister.Save(data, nil)
}

// 恢复先前持久化的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 没有任何状态时启动
		return
	}

	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int

	// 必须检查所有解码错误
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return // 解码失败，保持初始值
	}

	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

// 返回 Raft 持久化日志的字节数。
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	state := encodePersistentState(rf.capturePersistentState())
	rf.persister.Save(state, snapshot)
}

func (rf *Raft) capturePersistentState() persistentState {
	logs := make([]LogEntry, len(rf.log))
	copy(logs, rf.log)
	return persistentState{
		CurrentTerm:       rf.CurrentTerm,
		VotedFor:          rf.VotedFor,
		Log:               logs,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
}

func encodePersistentState(s persistentState) []byte {
	buf := persistBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	enc := gob.NewEncoder(buf)

	_ = enc.Encode(s.CurrentTerm)
	_ = enc.Encode(s.VotedFor)
	_ = enc.Encode(s.Log)
	_ = enc.Encode(s.LastIncludedIndex)
	_ = enc.Encode(s.LastIncludedTerm)

	data := append([]byte(nil), buf.Bytes()...)
	if buf.Cap() <= 1<<20 {
		persistBufferPool.Put(buf)
	} else {
		persistBufferPool.Put(new(bytes.Buffer))
	}
	return data
}

// 服务端通知 Raft：它已创建了一个包含
// index 及其之前所有信息的快照。
// 这意味着服务端不再需要该索引（及之前）的日志。
// Raft 此时应尽可能地截断日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 不能对已经被快照覆盖的 index 再做 snapshot
	if index <= rf.lastIncludedIndex {
		return
	}

	// 2. 不能 snapshot 未 commit 的日志
	if index > rf.CommitIndex {
		return
	}

	// 3. 找到对应 term
	// 数组下标 = 真实 index - lastIncludedIndex
	offset := index - rf.lastIncludedIndex
	lastIncludedTerm := rf.log[offset].Term

	// 4. 构造新的日志数组
	// 保留 index 之后的日志
	newLogs := make([]LogEntry, 0)
	newLogs = append(newLogs, LogEntry{
		Term: lastIncludedTerm,
	}) // dummy entry

	if offset+1 < len(rf.log) {
		newLogs = append(newLogs, rf.log[offset+1:]...)
	}

	rf.log = newLogs

	// 5. 更新 snapshot 元数据
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = lastIncludedTerm

	// 6. 更新 lastApplied
	if rf.LastApplied < index {
		rf.LastApplied = index
	}

	// 7. 持久化 (状态 + snapshot)
	rf.persistWithSnapshot(snapshot)
}

// RequestVote RPC 参数结构体
type RequestVoteArgs struct {
	Term         int // candidate的任期号
	CandidateId  int // 发起投票的candidate的ID
	LastLogIndex int // candidate的最高日志条目索引
	LastLogTerm  int // candidate的最高日志条目的任期号
}

// RequestVote RPC 回复结构体
type RequestVoteReply struct {
	Term        int  // 服务器的当前任期号，让candidate更新自己
	VoteGranted bool // 如果是true，意味着candidate收到了选票
}

// RequestVote RPC 处理函数
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.state = Follower
		rf.VotedFor = -1
		rf.persist()
	}

	if args.LastLogTerm < rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()) {
		reply.VoteGranted = false
		return nil
	}

	if rf.VotedFor == -1 || rf.VotedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateId
		rf.persist()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.CurrentTerm
	if reply.VoteGranted {
		rf.resetElectionTimer()
	}
	return nil
}

type AppendEntriesArgs struct {
	Term         int        // leader的任期号
	LeaderId     int        // 用来让follower把客户端请求定向到leader
	PrevLogIndex int        // 紧接新条目之前的日志条目索引(当前最大的日志条目索引)
	PrevLogTerm  int        // prevLogIndex的任期
	Entries      []LogEntry // 储存的日志条目
	LeaderCommit int        // leader的commitIndex
}

type AppendEntriesReply struct {
	Term          int
	Success       bool // 如果follower包含的日志匹配参数汇总的prevLogIndex和prevLogTerm，返回true
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) resetElectionTimer() {
	rf.lastHeard = time.Now()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.callPeer(server, "Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// ---（第 1 步）任期检查：拒绝过期 leader---
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return nil
	}
	// ---（第 2 步）如果 leader 的 term 更大，更新自己的 term 并转为 follower---
	if args.Term >= rf.CurrentTerm {
		if args.Term > rf.CurrentTerm {
			rf.CurrentTerm = args.Term
			rf.VotedFor = -1
			rf.persist()
		}
		rf.state = Follower
	}

	reply.Term = rf.CurrentTerm

	// ---（第 3 步）收到 AppendEntries 则重置选举计时器（心跳）---
	rf.resetElectionTimer()

	// ---（第 4 步）prevLogIndex 不存在：日志冲突---
	if args.PrevLogIndex > rf.lastIndex() {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIndex() + 1
		return nil
	}

	// ---（第 5 步）检查 prevLogTerm 是否匹配---
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		return nil
	}

	if rf.getTerm(args.PrevLogIndex) != args.PrevLogTerm {
		reply.Success = false
		reply.ConflictTerm = rf.getTerm(args.PrevLogIndex)

		// 找到该 term 的第一个 index
		index := args.PrevLogIndex
		for index > rf.lastIncludedIndex &&
			rf.getTerm(index-1) == reply.ConflictTerm {
			index--
		}

		reply.ConflictIndex = index
		return nil
	}

	// ---（第 6 步）删除冲突日志（同 index 不同 term）---
	// 从第一个冲突点开始删除
	i := 0
	for ; i < len(args.Entries); i++ {
		index := args.PrevLogIndex + 1 + i

		if index > rf.lastIndex() {
			break
		}

		if rf.getTerm(index) != args.Entries[i].Term {
			// 删除 index 及之后的日志
			cut := index - rf.lastIncludedIndex
			if cut < 1 {
				cut = 1
			}
			rf.log = rf.log[:cut]
			rf.persist()
			break
		}
	}

	// ---（第 7 步）追加 leader 发来的新日志---
	// 注意：只追加 follower 没有的部分
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
	}

	// // ---（第 8 步）更新 CommitIndex ---
	if args.LeaderCommit > rf.CommitIndex {
		// rf.CommitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.CommitIndex = min(args.LeaderCommit, rf.lastIndex())
		// 可选：应用日志到状态机 ApplyMsg
		// rf.applyLogEntries()
	}

	reply.Success = true
	return nil
}

// 向某个服务器发送 RequestVote RPC 的示例
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.callPeer(server, "Raft.RequestVote", args, reply)
	return ok
}

// 使用 Raft 的服务（例如 k/v 服务器）希望就下一个命令
// 达成一致并将其追加到 Raft 日志中。
// 如果该服务器不是领导者，则返回 false。
// 如果是领导者，则启动一致性流程并立即返回。
// 不保证该命令最终一定会被提交到日志中，
// 因为领导者可能崩溃或在选举中失败。
// 即使该 Raft 实例已被 Kill()，本函数也应安全返回。
//
// 第一个返回值是命令将出现的日志索引（若被提交）。
// 第二个返回值是当前任期。
// 第三个返回值表示该服务器是否认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 1. 如果不是 Leader，直接拒绝
	if rf.state != Leader {
		return -1, rf.CurrentTerm, false
	}

	// 2. 计算新日志的 index
	index := rf.getLastLogIndex() + 1
	term := rf.CurrentTerm

	// 3. 追加日志（这是 Start 的核心）
	rf.log = append(rf.log, LogEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()
	// 新日志到达时触发一次快速复制，避免每次 Start 都创建复制 goroutine。
	select {
	case rf.replicateTrigger <- struct{}{}:
	default:
	}
	// 4. 立即返回（不等提交）
	return index, term, true
}

// 测试器不会在每次测试结束后停止 Raft 创建的 goroutine，
// 但会调用 Kill() 方法。你的代码可以使用 killed()
// 来检查 Kill() 是否已被调用。使用 atomic 可避免使用锁。
//
// 问题在于：长时间运行的 goroutine 会占用内存和 CPU，
// 可能导致后续测试失败，并产生混乱的调试输出。
// 任何包含长循环的 goroutine 都应调用 killed() 检查是否应退出。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// 如有需要，可在此添加你的代码。
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	rf.mu.Lock()
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm {
		rf.mu.Unlock()
		return nil
	}

	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		rf.state = Follower
		rf.persist()
	}

	rf.resetElectionTimer()

	// 过期的快照，直接返回
	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return nil
	}

	// 关键修复：正确处理日志截断
	if args.LastIncludedIndex <= rf.lastIndex() &&
		rf.getTerm(args.LastIncludedIndex) == args.LastIncludedTerm {
		// 快照覆盖的日志与本地一致，保留后续日志
		newLog := make([]LogEntry, 0)
		newLog = append(newLog, LogEntry{Term: args.LastIncludedTerm})

		// 使用新的 LastIncludedIndex 计算 offset
		offset := args.LastIncludedIndex - rf.lastIncludedIndex
		if offset+1 < len(rf.log) {
			newLog = append(newLog, rf.log[offset+1:]...)
		}
		rf.log = newLog
	} else {
		// 快照覆盖的日志不存在或不一致，直接用快照
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}
	// 更新快照元数据
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.CommitIndex = max(rf.CommitIndex, rf.lastIncludedIndex)
	rf.LastApplied = max(rf.LastApplied, rf.lastIncludedIndex)

	rf.persistWithSnapshot(args.Data)

	// 保存 snapshot 数据
	snapshotData := args.Data
	snapshotTerm := rf.lastIncludedTerm
	snapshotIndex := rf.lastIncludedIndex

	rf.mu.Unlock() // 解锁

	// 发送 snapshot
	rf.applyCh <- raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshotData,
		SnapshotTerm:  snapshotTerm,
		SnapshotIndex: snapshotIndex,
	}
	return nil
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	var successCount int32 = 1 // leader itself

	// 向每个 Follower 发送心跳或日志同步
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(server int) {
			rf.mu.Lock()
			// 给 follower 安装快照
			if rf.nextIndex[server] <= rf.lastIncludedIndex {
				snapshotData := rf.persister.ReadSnapshot()

				// 检查快照是否存在
				if len(snapshotData) == 0 {
					rf.nextIndex[server] = rf.lastIncludedIndex + 1
					rf.mu.Unlock()
					return
				}

				args := &InstallSnapshotArgs{
					Term:              rf.CurrentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              snapshotData,
				}

				rf.mu.Unlock()

				reply := &InstallSnapshotReply{}
				ok := rf.callPeer(server, "Raft.InstallSnapshot", args, reply)

				rf.mu.Lock()
				if !ok {
					rf.mu.Unlock()
					return
				}

				if reply.Term > rf.CurrentTerm {
					rf.CurrentTerm = reply.Term
					rf.state = Follower
					rf.VotedFor = -1
					rf.persist()
					rf.mu.Unlock()
					return
				}

				rf.nextIndex[server] = rf.lastIncludedIndex + 1
				rf.matchIndex[server] = rf.lastIncludedIndex
				rf.mu.Unlock()
				return
			}

			// 获取 Follower 的上一个日志索引和任期
			prevIndex := rf.nextIndex[server] - 1
			if prevIndex < 0 {
				prevIndex = 0
			}

			prevTerm := 0
			if prevIndex == rf.lastIncludedIndex {
				prevTerm = rf.lastIncludedTerm
			} else {
				prevTerm = rf.log[prevIndex-rf.lastIncludedIndex].Term
			}

			term := rf.CurrentTerm

			// 构造日志条目，可能为空（即仅发送心跳）
			// entries := make([]LogEntry, len(rf.log[rf.nextIndex[server]:]))
			// copy(entries, rf.log[rf.nextIndex[server]:])
			start := rf.nextIndex[server] - rf.lastIncludedIndex
			if start < 0 {
				start = 0
			}
			entries := make([]LogEntry, len(rf.log[start:]))
			copy(entries, rf.log[start:])
			// 如果没有日志，就会是空的

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      entries, // 如果是心跳，entries 会为空
				LeaderCommit: rf.CommitIndex,
			}

			rf.mu.Unlock()

			reply := &AppendEntriesReply{}
			// 发送 AppendEntries RPC
			ok := rf.sendAppendEntries(server, args, reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.state != Leader || term != rf.CurrentTerm {
				return
			}

			// 如果收到的 Term 更大，说明需要转换为 Follower
			if reply.Term > rf.CurrentTerm {
				rf.CurrentTerm = reply.Term
				rf.state = Follower
				rf.VotedFor = -1
				rf.persist()
				return
			}

			if rf.CurrentTerm != args.Term || rf.state != Leader {
				return
			}

			// 更新 nextIndex 和 matchIndex
			if reply.Success {
				rf.matchIndex[server] = prevIndex + len(entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
				n := atomic.AddInt32(&successCount, 1)
				if n > int32(len(rf.peers)/2) {
					d := time.Duration(float64(rf.heartbeatInterval) * rf.leaseRatio)
					rf.leaseUntil = time.Now().Add(d)
				}
			} else {
				if reply.ConflictTerm == -1 {
					// follower 太短
					rf.nextIndex[server] = reply.ConflictIndex
				} else {
					// follower 有冲突 term
					lastIndex := -1

					// 查找 leader 中是否存在该 term
					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Term == reply.ConflictTerm {
							lastIndex = rf.lastIncludedIndex + i
							break
						}
					}

					if lastIndex != -1 {
						// leader 也有该 term
						rf.nextIndex[server] = lastIndex + 1
					} else {
						// leader 没有该 term
						rf.nextIndex[server] = reply.ConflictIndex
					}
				}
				if rf.nextIndex[server] < 1 {
					rf.nextIndex[server] = 1
				}
			}

			// fmt.Printf("[log]:later AppendEntries %d peer, matchIndex[%d] = %d, nextIndex[%d] = %d\n", server, server, rf.matchIndex[server], server, rf.nextIndex[server])

			// 推进 commitIndex
			for N := rf.getLastLogIndex(); N > rf.CommitIndex && N >= 1; N-- {

				// 只提交当前任期的日志
				// if rf.log[N].Term != rf.CurrentTerm {
				// 	continue
				// }
				if N == rf.lastIncludedIndex {
					continue
				}
				if rf.log[N-rf.lastIncludedIndex].Term != rf.CurrentTerm {
					continue
				}

				count := 1 // leader自己

				for i := range rf.peers {
					if i != rf.me && rf.matchIndex[i] >= N {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.CommitIndex = N
					// fmt.Printf("count > len(rf.peers)/2, rf.CommitIndex = N = %d", N);
					break
				}
			}

		}(i)
	}
}

func (rf *Raft) leaderLoop() {
	go func() {
		ticker := time.NewTicker(rf.heartbeatInterval)
		defer ticker.Stop()
		for rf.killed() == false {
			select {
			case <-ticker.C:
			case <-rf.replicateTrigger:
			}
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			rf.sendHeartbeats() // 发送一次心跳
		}
	}()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	d := time.Duration(float64(rf.heartbeatInterval) * rf.leaseRatio)
	rf.leaseUntil = time.Now().Add(d)
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
	// fmt.Printf("[leader]:%d become leader,term:%d\n", rf.me, rf.CurrentTerm)
	rf.leaderLoop()
}

func (rf *Raft) handleVoteResponse(peer int, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 收到比自己大的 term，变回 follower
	if reply.Term > rf.CurrentTerm {
		rf.CurrentTerm = reply.Term
		rf.state = Follower
		rf.VotedFor = -1
		rf.persist()
		return
	}

	if reply.Term < rf.CurrentTerm {
		return
	}

	// 如果不是 Candidate 了（可能已经变成 follower 或 leader），忽略
	if rf.state != Candidate {
		return
	}

	// 计算投票
	if reply.VoteGranted {
		rf.Votes++
		// 是否超过一半？
		if rf.Votes > len(rf.peers)/2 {
			rf.becomeLeader()
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	rf.Votes = 1
	rf.resetElectionTimer()

	args := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				rf.handleVoteResponse(server, args, reply)
			}
		}(i, args)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// 检查是否需要发起领导者选举。
		needStart := false
		rf.mu.Lock()
		if rf.state != Leader && time.Since(rf.lastHeard) > rf.electionTimeout {
			// 超时未收到心跳，发起选举
			needStart = true
		}
		rf.mu.Unlock()
		if needStart {
			rf.startElection()
		}

		// 随机暂停 50~350 毫秒。
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		var msgs []raftapi.ApplyMsg

		rf.mu.Lock()

		// 只应用已提交且未被应用的日志
		for rf.LastApplied < rf.CommitIndex {
			rf.LastApplied++
			index := rf.LastApplied

			// 跳过快照已覆盖的日志
			if index <= rf.lastIncludedIndex {
				continue
			}

			// 正确的索引转换
			pos := index - rf.lastIncludedIndex

			// 边界检查
			if pos < 1 || pos >= len(rf.log) {
				rf.LastApplied--
				break
			}

			command := rf.log[pos].Command
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      command,
				CommandIndex: index,
			})
		}

		rf.mu.Unlock()

		// 释放锁后批量发送
		for _, msg := range msgs {
			rf.applyCh <- msg
		}

		time.Sleep(1 * time.Millisecond) // 降低提交到状态机的额外等待
	}
}

// 服务或测试器希望创建一个 Raft 服务器。
// 所有 Raft 服务器的端口都存放在 peers[] 中，
// 当前服务器的端口为 peers[me]。
// 所有服务器的 peers[] 数组顺序一致。
// persister 用于保存该服务器的持久化状态，
// 同时也保存了崩溃前的最近一次状态（若有）。
// applyCh 是一个通道，Raft 会通过它向测试器或服务发送 ApplyMsg。
// Make() 必须快速返回，因此应为任何长期运行的任务启动 goroutine。
func Make(peers []string, me int,
	persister Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.electionTimeout = time.Duration(300+rand.Intn(200)) * time.Millisecond
	rf.lastHeard = time.Now()

	rf.state = Follower
	rf.VotedFor = -1
	rf.log = []LogEntry{{Term: 0}}
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.lastIncludedIndex = 0 // 显式初始化
	rf.lastIncludedTerm = 0  // 显式初始化

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.rpcClients = make(map[string]*rpc.Client, len(peers))
	rf.heartbeatInterval = 80 * time.Millisecond
	rf.replicateTrigger = make(chan struct{}, 1)
	rf.leaseRatio = 1.5
	rf.leaseUntil = time.Now()

	// 从崩溃前的持久化状态中恢复
	rf.readPersist(persister.ReadRaftState())
	rf.CommitIndex = rf.lastIncludedIndex
	rf.LastApplied = rf.lastIncludedIndex

	// 快照由 InstallSnapshot 和 applier 自动处理

	// 注册 Raft 为 RPC 服务
	// 注意：TCP 监听由上层 KVServer 负责，不在这里创建
	rpc.Register(rf)

	go rf.ticker()
	go rf.applier()

	return rf
}
