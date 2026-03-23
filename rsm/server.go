package rsm

import (
	"encoding/gob"
	"kvraft/cache"
	kvraftapi "kvraft/raftkv/rpc"
	"kvraft/storage"
	"kvraft/watch"
	"log"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// KVServer - 高性能分布式键值存储服务器
// 集成特性：LRU 缓存、Watch 实时推送、性能统计
// ============================================================

type OperationInfo struct {
	Type       string
	Key        string
	OldValue   string
	NewValue   string
	OldVersion int64
	NewVersion int64
	Timestamp  time.Time
	Success    bool
	Error      kvraftapi.Err
}

type KVServer struct {
	me       int
	dead     int32
	address  string
	rsm      *RSM
	mu       sync.RWMutex
	store    *storage.Store
	lruCache *cache.LRUCache
	stats    *ServerStats
}

type ServerStats struct {
	TotalRequests  int64
	TotalWrites    int64
	TotalReads     int64
	FailedRequests int64
	CacheHits      int64
	CacheMisses    int64
	WatchNotifies  int64
}

// ============================================================
// 核心业务逻辑 - Do Op (执行状态机操作)
// ============================================================

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch t := req.(type) {
	case *kvraftapi.GetArgs:
		return kv.doGet(t)
	case kvraftapi.GetArgs:
		return kv.doGet(&t)
	case *kvraftapi.PutArgs:
		return kv.doPut(t)
	case kvraftapi.PutArgs:
		return kv.doPut(&t)
	case *kvraftapi.DeleteArgs:
		return kv.doDelete(t)
	case kvraftapi.DeleteArgs:
		return kv.doDelete(&t)
	default:
		log.Printf("[KVServer-%d] Unknown request type: %T", kv.me, t)
		return kvraftapi.GetReply{Err: kvraftapi.ErrWrongLeader}
	}
}

// doGet - 读优化：缓存优先级最高
func (kv *KVServer) doGet(args *kvraftapi.GetArgs) kvraftapi.GetReply {
	if kv.killed() {
		return kvraftapi.GetReply{Err: kvraftapi.ErrWrongLeader}
	}

	// 缓存查询（最快路径）
	if val, version, ok := kv.lruCache.GetWithVersion(args.Key); ok {
		kv.stats.RecordCacheHit()
		return kvraftapi.GetReply{
			Value:   val,
			Version: kvraftapi.Tversion(version),
			Err:     kvraftapi.OK,
		}
	}

	// 存储查询
	value, version, exists, err := kv.store.Get(args.Key)
	if err != nil {
		log.Printf("[KVServer-%d] Get error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.GetReply{Err: kvraftapi.ErrWrongLeader}
	}

	if exists {
		kv.lruCache.PutWithVersion(args.Key, value, version)
		kv.stats.RecordCacheMiss()
		return kvraftapi.GetReply{
			Value:   value,
			Version: kvraftapi.Tversion(version),
			Err:     kvraftapi.OK,
		}
	}

	kv.stats.RecordCacheMiss()
	return kvraftapi.GetReply{Err: kvraftapi.ErrNoKey}
}

// doPut - 写操作：更新缓存和存储
func (kv *KVServer) doPut(args *kvraftapi.PutArgs) kvraftapi.PutReply {
	if kv.killed() {
		return kvraftapi.PutReply{Err: kvraftapi.ErrWrongLeader}
	}

	_, version, exists, err := kv.store.Get(args.Key)
	if err != nil {
		log.Printf("[KVServer-%d] Get error during Put: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.PutReply{Err: kvraftapi.ErrWrongLeader}
	}

	if exists {
		if kvraftapi.Tversion(version) == args.Version {
			newVersion := version + 1
			if err := kv.store.Put(args.Key, args.Value, newVersion); err != nil {
				log.Printf("[KVServer-%d] Put error: %v", kv.me, err)
				kv.stats.RecordFailure()
				return kvraftapi.PutReply{Err: kvraftapi.ErrWrongLeader}
			}
			kv.lruCache.PutWithVersion(args.Key, args.Value, newVersion)
			kv.stats.RecordWrite()
			return kvraftapi.PutReply{Err: kvraftapi.OK}
		}
		kv.stats.RecordFailure()
		return kvraftapi.PutReply{Err: kvraftapi.ErrVersion}
	}

	if args.Version == 0 {
		if err := kv.store.Put(args.Key, args.Value, 1); err != nil {
			log.Printf("[KVServer-%d] Put error: %v", kv.me, err)
			kv.stats.RecordFailure()
			return kvraftapi.PutReply{Err: kvraftapi.ErrWrongLeader}
		}
		kv.lruCache.PutWithVersion(args.Key, args.Value, 1)
		kv.stats.RecordWrite()
		return kvraftapi.PutReply{Err: kvraftapi.OK}
	}

	kv.stats.RecordFailure()
	return kvraftapi.PutReply{Err: kvraftapi.ErrNoKey}
}

func (kv *KVServer) doDelete(args *kvraftapi.DeleteArgs) kvraftapi.DeleteReply {
	if kv.killed() {
		return kvraftapi.DeleteReply{Err: kvraftapi.ErrWrongLeader}
	}

	_, _, exists, err := kv.store.Get(args.Key)
	if err != nil {
		log.Printf("[KVServer-%d] Get error during Delete: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.DeleteReply{Err: kvraftapi.ErrWrongLeader}
	}
	if !exists {
		return kvraftapi.DeleteReply{Err: kvraftapi.ErrNoKey}
	}

	if err := kv.store.Delete(args.Key); err != nil {
		log.Printf("[KVServer-%d] Delete error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.DeleteReply{Err: kvraftapi.ErrWrongLeader}
	}

	kv.lruCache.Delete(args.Key)
	kv.stats.RecordWrite()
	return kvraftapi.DeleteReply{Err: kvraftapi.OK}
}

// ============================================================
// 快照管理
// ============================================================

func (kv *KVServer) Snapshot() []byte {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if kv.killed() {
		return nil
	}

	data, err := kv.store.SaveSnapshot()
	if err != nil {
		log.Printf("[KVServer-%d] Snapshot error: %v", kv.me, err)
		return nil
	}
	return data
}

func (kv *KVServer) Restore(data []byte) {
	kv.lruCache.Clear()

	if len(data) == 0 {
		if err := kv.store.Clear(); err != nil {
			log.Printf("[KVServer-%d] Clear store error: %v", kv.me, err)
		}
		return
	}

	if err := kv.store.LoadSnapshot(data); err != nil {
		log.Printf("[KVServer-%d] Restore snapshot error: %v", kv.me, err)
	}
}

// ============================================================
// RPC Handlers - 兼容旧 RPC 接口
// ============================================================

func (kv *KVServer) Get(args *kvraftapi.GetArgs, reply *kvraftapi.GetReply) error {
	if kv.killed() {
		reply.Err = kvraftapi.ErrWrongLeader
		return nil
	}
	err, ret := kv.rsm.Submit(args)
	if err != kvraftapi.OK {
		reply.Err = err
		return nil
	}
	getReply := ret.(kvraftapi.GetReply)
	reply.Value = getReply.Value
	reply.Version = getReply.Version
	reply.Err = getReply.Err
	kv.stats.RecordRead()
	return nil
}

func (kv *KVServer) Put(args *kvraftapi.PutArgs, reply *kvraftapi.PutReply) error {
	if kv.killed() {
		reply.Err = kvraftapi.ErrWrongLeader
		return nil
	}
	err, ret := kv.rsm.Submit(args)
	if err != kvraftapi.OK {
		reply.Err = err
		return nil
	}
	putReply := ret.(kvraftapi.PutReply)
	reply.Err = putReply.Err
	kv.stats.RecordWrite()
	return nil
}

func (kv *KVServer) Delete(args *kvraftapi.DeleteArgs, reply *kvraftapi.DeleteReply) error {
	if kv.killed() {
		reply.Err = kvraftapi.ErrWrongLeader
		return nil
	}
	err, ret := kv.rsm.Submit(args)
	if err != kvraftapi.OK {
		reply.Err = err
		return nil
	}
	deleteReply := ret.(kvraftapi.DeleteReply)
	reply.Err = deleteReply.Err
	return nil
}

// ============================================================
// Watch 集成 - 关键的事件推送链路
// ============================================================

func (kv *KVServer) OnOpComplete(req any, result any, index int64) {
	if kv.killed() {
		return
	}

	watchMgr := kv.rsm.GetWatchManager()
	if watchMgr == nil {
		return
	}

	switch t := req.(type) {
	case *kvraftapi.PutArgs:
		kv.notifyPutEvent(t, result, watchMgr)
	case kvraftapi.PutArgs:
		kv.notifyPutEvent(&t, result, watchMgr)
	}
}

func (kv *KVServer) notifyPutEvent(putArgs *kvraftapi.PutArgs, result any, watchMgr *watch.Manager) {
	putReply, ok := result.(kvraftapi.PutReply)
	if !ok {
		return
	}

	if putReply.Err == kvraftapi.OK {
		kv.mu.RLock()
		oldValue, _, exists, _ := kv.store.Get(putArgs.Key)
		kv.mu.RUnlock()

		oldValueStr := ""
		if exists {
			oldValueStr = oldValue
		}

		eventType := "PUT"
		if !exists {
			eventType = "SET"
		}

		err := watchMgr.Notify(
			putArgs.Key,
			oldValueStr,
			putArgs.Value,
			int64(putArgs.Version),
			eventType,
		)

		if err == nil {
			kv.stats.RecordWatchNotify()
		}
	}
}

// ============================================================
// 生命周期管理
// ============================================================

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

// ============================================================
// 统计方法
// ============================================================

func (s *ServerStats) RecordRead() {
	atomic.AddInt64(&s.TotalRequests, 1)
	atomic.AddInt64(&s.TotalReads, 1)
}

func (s *ServerStats) RecordWrite() {
	atomic.AddInt64(&s.TotalRequests, 1)
	atomic.AddInt64(&s.TotalWrites, 1)
}

func (s *ServerStats) RecordCacheHit() {
	atomic.AddInt64(&s.CacheHits, 1)
}

func (s *ServerStats) RecordCacheMiss() {
	atomic.AddInt64(&s.CacheMisses, 1)
}

func (s *ServerStats) RecordFailure() {
	atomic.AddInt64(&s.FailedRequests, 1)
}

func (s *ServerStats) RecordWatchNotify() {
	atomic.AddInt64(&s.WatchNotifies, 1)
}

func (s *ServerStats) GetStats() (requests, writes, reads, failures, hits, misses int64) {
	return atomic.LoadInt64(&s.TotalRequests),
		atomic.LoadInt64(&s.TotalWrites),
		atomic.LoadInt64(&s.TotalReads),
		atomic.LoadInt64(&s.FailedRequests),
		atomic.LoadInt64(&s.CacheHits),
		atomic.LoadInt64(&s.CacheMisses)
}

// ============================================================
// 服务器启动
// ============================================================

func StartKVServer(servers []string, gid int, me int, persister Persister, maxraftstate int, address string) *KVServer {
	gob.Register(Op{})
	gob.Register(kvraftapi.PutArgs{})
	gob.Register(kvraftapi.GetArgs{})
	gob.Register(kvraftapi.DeleteArgs{})

	store, err := storage.NewStore("badger-" + address)
	if err != nil {
		log.Fatal(err)
	}

	kv := &KVServer{
		me:       me,
		address:  address,
		store:    store,
		lruCache: cache.NewLRUCache(10000),
		stats:    &ServerStats{},
	}

	kv.rsm = MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.rsm.RegisterOpCompleteListener(kv)

	rpc.Register(kv)
	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	// 注册 Raft RPC 服务，供节点间选举与日志复制调用。
	if err := rpcs.RegisterName("Raft", kv.rsm.Raft()); err != nil {
		log.Fatal(err)
	}
	l, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal(e)
	}

	go func() {
		for !kv.killed() {
			conn, err := l.Accept()
			if err == nil && !kv.killed() {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
		}
		l.Close()
	}()

	// 对外业务接口迁移到 gRPC（Raft 复制仍使用 RPC）。
	startGRPCServer(kv, address)

	return kv
}
