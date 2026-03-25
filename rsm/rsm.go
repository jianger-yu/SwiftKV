package rsm

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"kvraft/raft"
	raftapi "kvraft/raftapi"
	kvraftapi "kvraft/raftkv/rpc"
	"kvraft/watch"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Persister interface {
	ReadRaftState() []byte
	ReadSnapshot() []byte
	Save(raftstate []byte, snapshot []byte)
	RaftStateSize() int
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int   // 发起请求的服务器 id
	Id  int64 // 每次为一个请求生成一个唯一的 id
	Req any   // 请求内容
}

func opEquals(a *Op, b *Op) bool {
	return a.Me == b.Me && a.Id == b.Id
}

// OpCompleteListener 操作完成监听器接口
// 用于在 Raft 日志提交后回调，例如触发 Watch 事件
type OpCompleteListener interface {
	// OnOpComplete 在操作被 Raft 提交和应用后调用
	// req: 原始请求
	// result: 操作结果
	// index: Raft 日志索引
	OnOpComplete(req any, result any, index int64)
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type waitingOp struct {
	oper   Op        // 操作
	result any       // 操作结果
	done   chan bool // 操作完成的信号通道
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	persister    raft.Persister
	// Your definitions here.
	idCounter  int64
	waitingOps map[int]*waitingOp // 正在等待的操作，key 是日志索引
	shutdown   atomic.Bool
	watchMgr   *watch.Manager     // Watch 管理器
	opListener OpCompleteListener // 操作完成监听器（用于 Watch 回调）
}

// Close 优雅关闭 RSM 相关后台组件。
func (rsm *RSM) Close() {
	if !rsm.shutdown.CompareAndSwap(false, true) {
		return
	}

	if rsm.rf != nil {
		rsm.rf.Kill()
	}
	if rsm.watchMgr != nil {
		rsm.watchMgr.Close()
	}

	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	for _, wop := range rsm.waitingOps {
		select {
		case wop.done <- false:
		default:
		}
	}
	rsm.waitingOps = make(map[int]*waitingOp)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(
	peers []string,
	me int,
	persister Persister,
	maxraftstate int,
	sm StateMachine,
) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		persister:    persister,
		idCounter:    0,
		waitingOps:   make(map[int]*waitingOp),
		watchMgr:     watch.NewManager(watch.DefaultConfig()),
	}
	rsm.shutdown.Store(false)
	rsm.rf = raft.Make(peers, me, persister, rsm.applyCh)
	if maxraftstate != -1 {
		snapshot := persister.ReadSnapshot()
		if len(snapshot) > 0 {
			r := bytes.NewBuffer(snapshot)
			d := gob.NewDecoder(r)
			var idctr int64
			var smSnapshot []byte
			if d.Decode(&idctr) != nil ||
				d.Decode(&smSnapshot) != nil {
				panic("RSM unable to read snapshot")
			}
			atomic.StoreInt64(&rsm.idCounter, idctr)
			rsm.sm.Restore(smSnapshot)
		}
	}
	go rsm.applyLoop()
	return rsm
}

func (rsm *RSM) genID() int64 {
	return atomic.AddInt64(&rsm.idCounter, 1)
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// GetWatchManager 返回 Watch 管理器
func (rsm *RSM) GetWatchManager() *watch.Manager {
	return rsm.watchMgr
}

// RegisterOpCompleteListener 注册操作完成监听器
// 监听器会在每个操作被 Raft 提交和应用后被调用
func (rsm *RSM) RegisterOpCompleteListener(listener OpCompleteListener) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	rsm.opListener = listener
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (kvraftapi.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	if rsm.shutdown.Load() {
		return kvraftapi.ErrWrongLeader, nil
	}

	opID := rsm.genID()
	oper := Op{Me: rsm.me, Id: opID, Req: req}
	index, term, isLeader := rsm.rf.Start(oper)
	if !isLeader {
		return kvraftapi.ErrWrongLeader, nil
	}
	waitingOp := &waitingOp{
		oper: oper,
		done: make(chan bool, 1),
	}
	rsm.mu.Lock()
	if wop, exists := rsm.waitingOps[index]; exists {
		select {
		case wop.done <- false:
		default:
		}
	}
	rsm.waitingOps[index] = waitingOp
	rsm.mu.Unlock()

	err, result := func() (kvraftapi.Err, any) {
		timer := time.NewTimer(1500 * time.Millisecond)
		leaderCheck := time.NewTicker(50 * time.Millisecond)
		defer timer.Stop()
		defer leaderCheck.Stop()
		for {
			if rsm.shutdown.Load() {
				return kvraftapi.ErrWrongLeader, nil
			}
			select {
			case <-timer.C:
				// 超时，返回错误
				return kvraftapi.ErrWrongLeader, nil
			case <-leaderCheck.C:
				currentTerm, stillLeader := rsm.rf.GetState()
				if !stillLeader || currentTerm != term {
					// 领导者已经变更
					return kvraftapi.ErrWrongLeader, nil
				}
			case res := <-waitingOp.done:
				if res {
					return kvraftapi.OK, waitingOp.result
				} else {
					return kvraftapi.ErrWrongLeader, nil
				}
			}
		}
	}()

	rsm.mu.Lock()
	delete(rsm.waitingOps, index)
	rsm.mu.Unlock()
	return err, result
}

func (rsm *RSM) kill() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	rsm.shutdown.Store(true)
	for _, wop := range rsm.waitingOps {
		select {
		case wop.done <- false:
		default:
		}
	}
	rsm.waitingOps = make(map[int]*waitingOp)
}

func (rsm *RSM) applyLoop() {
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			rsm.kill()
			return
		}
		if rsm.shutdown.Load() {
			return
		}
		if msg.CommandValid {
			rsm.applyCommand(msg)
		} else {
			rsm.applySnapshot(msg)
		}
	}
}

func (rsm *RSM) applyCommand(msg raftapi.ApplyMsg) {
	oper, ok := msg.Command.(Op)
	if !ok {
		// 非法的操作类型，忽略
		return
	}

	result := rsm.sm.DoOp(oper.Req)

	// ============================================================
	// 关键：在状态机应用操作后，立即调用监听器
	// 这确保 Watch 事件的线性一致性（在 CommandValid 之后）
	// ============================================================
	rsm.mu.Lock()
	listener := rsm.opListener
	rsm.mu.Unlock()

	if listener != nil {
		// 异步调用监听器，避免阻塞 apply loop
		go listener.OnOpComplete(oper.Req, result, int64(msg.CommandIndex))
	}

	rsm.mu.Lock()
	if wop, exists := rsm.waitingOps[msg.CommandIndex]; exists {
		if opEquals(&wop.oper, &oper) {
			wop.result = result
			select {
			case wop.done <- true:
			default:
			}
		} else {
			// 操作被覆盖
			select {
			case wop.done <- false:
			default:
			}
		}
	}
	rsm.mu.Unlock()

	if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > (rsm.maxraftstate*19)/20 {
		go rsm.createSnapshot(msg.CommandIndex)
	}
}

func (rsm *RSM) applySnapshot(msg raftapi.ApplyMsg) {
	if rsm.shutdown.Load() {
		return
	}
	r := bytes.NewBuffer(msg.Snapshot)
	d := gob.NewDecoder(r)
	var idctr int64
	var smSnapshot []byte
	if d.Decode(&idctr) != nil ||
		d.Decode(&smSnapshot) != nil {
		panic("RSM unable to read snapshot")
	}
	if idctr > atomic.LoadInt64(&rsm.idCounter) {
		atomic.StoreInt64(&rsm.idCounter, idctr)
	}
	rsm.sm.Restore(smSnapshot)
}

func (rsm *RSM) createSnapshot(lastIncludedIndex int) {
	if rsm.shutdown.Load() {
		return
	}
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	smSnapshot := rsm.sm.Snapshot()
	idctr := atomic.LoadInt64(&rsm.idCounter)
	e.Encode(idctr)
	e.Encode(smSnapshot)
	rsm.rf.Snapshot(lastIncludedIndex, w.Bytes())
}
