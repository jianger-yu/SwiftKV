package rsm

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"kvraft/pkg/raft"
	kvraftapi "kvraft/pkg/raftapi"
	"kvraft/pkg/wal"
	"kvraft/pkg/watch"
)

var useRaftStateMachine bool // 用于选择另一个 Raft 实现（未使用）

type Persister interface {
	ReadRaftState() []byte
	ReadHardState() []byte
	ReadSnapshot() []byte
	Save(raftstate []byte, snapshot []byte)
	SaveHardState(hardstate []byte)
	RaftStateSize() int
}

type Op struct {
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

// StateMachine 是状态机接口。需要复制自身的服务器应调用 MakeRSM 并实现本接口。
// 该接口允许 rsm 包与应用层交互。应用层必须实现 DoOp 以执行操作（如 Get/Put 请求），
// 并通过 Snapshot/Restore 实现快照功能以持久化和恢复服态。
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
	mu            sync.Mutex
	me            int
	rf            kvraftapi.Raft
	applyCh       chan kvraftapi.ApplyMsg
	maxraftstate  int // 当 Raft 日志大小超过此值时触发快照
	sm            StateMachine
	persister     raft.Persister
	idCounter     int64
	waitingOps    map[int]*waitingOp // 正在等待的操作，key 是日志索引
	shutdown      atomic.Bool
	watchMgr      *watch.Manager     // Watch 管理器
	opListener    OpCompleteListener // 操作完成监听器（用于 Watch 回调）
	walLogger     *wal.Logger
	leaseRead     atomic.Bool
	snapshotInFly atomic.Bool
	walGCInFly    atomic.Bool
	lastSnapIndex int
	lastApplied   int
	lastSnapAt    time.Time
	snapMinDelta  int
	snapMinGap    time.Duration
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
	if rsm.walLogger != nil {
		if err := rsm.walLogger.Close(); err != nil {
			log.Printf("[RSM-%d] close WAL failed: %v", rsm.me, err)
		}
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

// MakeRSM 创建复制状态机实例。
// MakeRSM 应快速返回，由后台 goroutine 进行长期运行的工作。
func MakeRSM(
	peers []string, // Raft 集群中各节点的网络地址
	me int, // 当前节点在 peers 中的下标
	persister Persister, // 用于持久化 Raft 状态和快照的存储器
	maxraftstate int, // 当 Raft 日志大小达到此值时触发快照（-1 表示不启用快照）
	sm StateMachine, // 应用层实现的状态机
) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan kvraftapi.ApplyMsg),
		sm:           sm,
		persister:    persister,
		idCounter:    0,
		waitingOps:   make(map[int]*waitingOp),
		watchMgr:     watch.NewManager(watch.DefaultConfig()),
		snapMinDelta: 64,
		snapMinGap:   200 * time.Millisecond,
	}
	rsm.shutdown.Store(false)
	rsm.leaseRead.Store(true)
	// 根据当前节点 ID 生成预写日志（WAL）的绝对路径
	walPath := filepath.Join(runtimeDataRoot(), "wal", fmt.Sprintf("rsm-node-%d.log", me))
	walLogger, err := wal.NewLogger(walPath, true)
	if err != nil {
		log.Printf("[RSM-%d] WAL disabled due to init error: %v", me, err)
	} else {
		rsm.walLogger = walLogger
	}
	rsm.rf = raft.Make(peers, me, persister, rsm.applyCh)
	rsm.lastSnapAt = time.Now()
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
		rsm.lastSnapAt = time.Now()
	}

	rsm.lastSnapIndex = rsm.rf.GetLastIncludedIndex()
	rsm.lastApplied = rsm.lastSnapIndex

	if rsm.walLogger != nil && rsm.walLogger.Enabled() {
		recoveredIndex, err := rsm.recoverFromWAL(rsm.lastSnapIndex)
		if err != nil {
			panic(fmt.Sprintf("RSM WAL replay failed: %v", err))
		}
		rsm.rf.SyncAppliedIndex(recoveredIndex)
		rsm.lastApplied = recoveredIndex
	}
	go rsm.applyLoop()
	return rsm
}

func (rsm *RSM) genID() int64 {
	return atomic.AddInt64(&rsm.idCounter, 1)
}

func (rsm *RSM) Raft() kvraftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) EnableLeaseRead(enable bool) {
	rsm.leaseRead.Store(enable)
}

// SubmitLeaseRead tries local lease read first, then falls back to consensus path.
func (rsm *RSM) SubmitLeaseRead(req any) (kvraftapi.Err, any) {
	err, ret, _ := rsm.SubmitLeaseReadWithMode(req)
	return err, ret
}

// SubmitLeaseReadWithMode returns whether the request was served via local lease read.
func (rsm *RSM) SubmitLeaseReadWithMode(req any) (kvraftapi.Err, any, bool) {
	// 检查是否开启租约功能
	if !rsm.leaseRead.Load() {
		err, ret := rsm.Submit(req)
		return err, ret, false
	}
	// 转换为 Raft 实现类
	rfImpl, ok := rsm.rf.(*raft.Raft)
	if !ok {
		err, ret := rsm.Submit(req)
		return err, ret, false
	}
	// 在租约期内直接读取本地数据
	if rfImpl.IsLeaderWithLease() {
		result := rsm.sm.DoOp(req)
		return kvraftapi.OK, result, true
	}
	err, ret := rsm.Submit(req)
	return err, ret, false
}

func walEntryFromOp(me int, commandIndex int, term int, oper Op) wal.Entry {
	entry := wal.Entry{
		RaftIndex: int64(commandIndex),
		Term:      term,
		NodeID:    me,
		ReqID:     oper.Id,
		Timestamp: time.Now().UnixNano(),
	}
	switch t := oper.Req.(type) {
	case *kvraftapi.GetArgs:
		entry.OpType = "GET"
		entry.Key = t.Key
	case kvraftapi.GetArgs:
		entry.OpType = "GET"
		entry.Key = t.Key
	case *kvraftapi.ScanArgs:
		entry.OpType = "SCAN"
	case kvraftapi.ScanArgs:
		entry.OpType = "SCAN"
	case *kvraftapi.PutArgs:
		entry.OpType = "PUT"
		entry.Key = t.Key
		entry.Value = t.Value
		entry.Version = int64(t.Version)
		entry.TTL = t.TTL
	case kvraftapi.PutArgs:
		entry.OpType = "PUT"
		entry.Key = t.Key
		entry.Value = t.Value
		entry.Version = int64(t.Version)
		entry.TTL = t.TTL
	case *kvraftapi.DeleteArgs:
		entry.OpType = "DELETE"
		entry.Key = t.Key
	case kvraftapi.DeleteArgs:
		entry.OpType = "DELETE"
		entry.Key = t.Key
	case *kvraftapi.ExpireArgs:
		entry.OpType = "EXPIRE"
		entry.Keys = append([]string(nil), t.Keys...)
		entry.Cutoff = t.Cutoff
	case kvraftapi.ExpireArgs:
		entry.OpType = "EXPIRE"
		entry.Keys = append([]string(nil), t.Keys...)
		entry.Cutoff = t.Cutoff
	default:
		entry.OpType = fmt.Sprintf("%T", oper.Req)
	}
	return entry
}

func walEntryToRequest(entry wal.Entry) (any, bool, error) {
	opType := strings.ToUpper(strings.TrimSpace(entry.OpType))
	switch opType {
	case "GET":
		return nil, false, nil
	case "SCAN":
		return nil, false, nil
	case "PUT":
		return &kvraftapi.PutArgs{
			Key:     entry.Key,
			Value:   entry.Value,
			Version: kvraftapi.Tversion(entry.Version),
			TTL:     entry.TTL,
		}, true, nil
	case "DELETE":
		return &kvraftapi.DeleteArgs{Key: entry.Key}, true, nil
	case "EXPIRE":
		return &kvraftapi.ExpireArgs{
			Keys:   append([]string(nil), entry.Keys...),
			Cutoff: entry.Cutoff,
		}, true, nil
	default:
		// 兼容历史 WAL 中 default fmt(%T) 记录的只读操作类型。
		if strings.Contains(entry.OpType, "GetArgs") || strings.Contains(entry.OpType, "ScanArgs") {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("unsupported WAL op type: %s", entry.OpType)
	}
}

func (rsm *RSM) recoverFromWAL(snapshotIndex int) (int, error) {
	recoveredIndex := snapshotIndex
	err := rsm.walLogger.Replay(func(entry wal.Entry) error {
		entryIndex := int(entry.RaftIndex)
		if entryIndex <= snapshotIndex {
			return nil
		}
		if entryIndex <= recoveredIndex {
			return nil
		}

		req, mutates, err := walEntryToRequest(entry)
		if err != nil {
			return err
		}
		if mutates {
			rsm.sm.DoOp(req)
		}
		recoveredIndex = entryIndex
		return nil
	})
	if err != nil {
		return snapshotIndex, err
	}
	return recoveredIndex, nil
}

func (rsm *RSM) truncateWALAsync(upToIndex int) {
	if upToIndex <= 0 || rsm.walLogger == nil || !rsm.walLogger.Enabled() {
		return
	}
	if !rsm.walGCInFly.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer rsm.walGCInFly.Store(false)
		if err := rsm.walLogger.TruncateUpTo(int64(upToIndex)); err != nil {
			log.Printf("[RSM-%d] WAL truncate failed upTo=%d: %v", rsm.me, upToIndex, err)
		}
	}()
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

// Submit 向 Raft 提交一条命令并等待其被提交。
// 如果当前节点不是 Leader，返回 ErrWrongLeader，客户端应重新查找 Leader 后重试。
func (rsm *RSM) Submit(req any) (kvraftapi.Err, any) {
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

func (rsm *RSM) applyCommand(msg kvraftapi.ApplyMsg) {
	oper, ok := msg.Command.(Op)
	if !ok {
		// 非法的操作类型，忽略
		return
	}

	term, _ := rsm.rf.GetState()
	if rsm.walLogger != nil {
		if err := rsm.walLogger.Append(walEntryFromOp(rsm.me, msg.CommandIndex, term, oper)); err != nil {
			log.Printf("[RSM-%d] WAL append failed at index=%d: %v", rsm.me, msg.CommandIndex, err)
		}
	}

	result := rsm.sm.DoOp(oper.Req)
	rsm.mu.Lock()
	if msg.CommandIndex > rsm.lastApplied {
		rsm.lastApplied = msg.CommandIndex
	}
	rsm.mu.Unlock()

	// 关键：在状态机应用操作后，立即调用监听器
	// 这确保 Watch 事件的线性一致性（在 CommandValid 之后）
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

	if rsm.shouldCreateSnapshot(msg.CommandIndex) {
		if rsm.snapshotInFly.CompareAndSwap(false, true) {
			go rsm.createSnapshot(msg.CommandIndex)
		}
	}
}

func (rsm *RSM) shouldCreateSnapshot(commandIndex int) bool {
	if rsm.maxraftstate == -1 {
		return false
	}
	if rsm.rf.PersistBytes() <= (rsm.maxraftstate*3)/4 {
		return false
	}
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if commandIndex-rsm.lastSnapIndex < rsm.snapMinDelta {
		return false
	}
	if time.Since(rsm.lastSnapAt) < rsm.snapMinGap {
		return false
	}
	return true
}

func (rsm *RSM) applySnapshot(msg kvraftapi.ApplyMsg) {
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
	rsm.mu.Lock()
	if msg.SnapshotIndex > rsm.lastSnapIndex {
		rsm.lastSnapIndex = msg.SnapshotIndex
	}
	if msg.SnapshotIndex > rsm.lastApplied {
		rsm.lastApplied = msg.SnapshotIndex
	}
	rsm.lastSnapAt = time.Now()
	rsm.mu.Unlock()
	rsm.truncateWALAsync(msg.SnapshotIndex)
}

func (rsm *RSM) createSnapshot(lastIncludedIndex int) {
	defer rsm.snapshotInFly.Store(false)
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
	rsm.truncateWALAsync(lastIncludedIndex)
	rsm.mu.Lock()
	rsm.lastSnapIndex = lastIncludedIndex
	if lastIncludedIndex > rsm.lastApplied {
		rsm.lastApplied = lastIncludedIndex
	}
	rsm.lastSnapAt = time.Now()
	rsm.mu.Unlock()
}
