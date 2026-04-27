package rsm

import (
	"encoding/gob"
	"kvraft/pkg/raft"
	kvraftapi "kvraft/pkg/raftapi"
	"kvraft/pkg/storage"
	"kvraft/pkg/watch"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

func isExpired(expires int64, now int64) bool {
	return expires > 0 && expires <= now
}

func absoluteExpiryFromTTL(ttlSeconds int64, now int64) int64 {
	if ttlSeconds <= 0 {
		return 0
	}
	return now + ttlSeconds*int64(time.Second)
}

// KVServer - 高性能分布式键值存储服务器

type OperationInfo struct {
	Type       string        // 操作类型：GET、PUT、DELETE、SCAN、EXPIRE 等
	Key        string        // 操作的键名
	OldValue   string        // 操作执行前的值
	NewValue   string        // 操作执行后的值
	OldVersion int64         // 操作执行前的版本号
	NewVersion int64         // 操作执行后的版本号
	Timestamp  time.Time     // 操作的时间戳
	Success    bool          // 操作是否执行成功
	Error      kvraftapi.Err // 操作的错误码
}

type KVServer struct {
	me               int            // 当前节点在集群中的索引 ID
	dead             int32          // 用于关闭服务
	address          string         // 本节点的原始 RPC 地址
	rsm              *RSM           // 复制状态机指针
	mu               sync.RWMutex   // 读写锁
	store            *storage.Store // 存储引擎指针
	stats            *ServerStats   // 统计器指针
	leaseStatEnabled bool           // 控制是否开启昂贵的租约统计逻辑
	ttlEvery         time.Duration  // 定义后台清理过期键的频率
	ttlBatch         int            // 定义每次清理任务最多删除多少个键，防止长时间占用 CPU
	rpcLn            net.Listener   // 网络监听器,用于节点间 Raft 通信
	grpcLn           net.Listener   // 网络监听器,用于外部客户端 gRPC 接入
	grpcSrv          *grpc.Server   // gRPC 服务实例
}

// 决定是否开启“租约统计”
func leaseStatsEnabledFromEnv() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("KV_LEASE_STATS")))
	return v == "1" || v == "true" || v == "yes" || v == "on"
}

func runtimeDataRoot() string {
	v := strings.TrimSpace(os.Getenv("KV_DATA_DIR"))
	if v != "" {
		return v
	}
	if info, err := os.Stat("/data"); err == nil && info.IsDir() {
		return "/data"
	}
	return "data"
}

type ServerStats struct {
	TotalRequests  int64 // 总请求数（读写请求总和）
	TotalWrites    int64 // 总写操作数
	TotalReads     int64 // 总读操作数
	FailedRequests int64 // 失败请求数
	WatchNotifies  int64 // 生成的监听通知数
	LeaseHits      int64 // 租约命中次数
	LeaseFallbacks int64 // 租约回退（降级）次数
	TTLExpiredOps  int64 // TTL过期而删除的键数
}

type RuntimePerfStats struct {
	Persister PersisterMetrics
	Raft      raft.RaftPerfStats
	ApplyLoop ApplyLoopPerfStats
}

// 核心业务逻辑 - Do Op (执行状态机操作)

func (kv *KVServer) DoOp(req any) any {
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
	case *kvraftapi.ScanArgs:
		return kv.doScan(t)
	case kvraftapi.ScanArgs:
		return kv.doScan(&t)
	case *kvraftapi.ExpireArgs:
		return kv.doExpire(t)
	case kvraftapi.ExpireArgs:
		return kv.doExpire(&t)
	default:
		log.Printf("[KVServer-%d] Unknown request type: %T", kv.me, t)
		return kvraftapi.GetReply{Err: kvraftapi.ErrWrongLeader}
	}
}

// doGet - 直接从 BadgerDB 读取（其 BlockCache 提供缓存功能）
func (kv *KVServer) doGet(args *kvraftapi.GetArgs) kvraftapi.GetReply {
	if kv.killed() {
		return kvraftapi.GetReply{Err: kvraftapi.ErrWrongLeader}
	}

	// 存储查询（BadgerDB 的 BlockCache 会处理热数据缓存）
	now := time.Now().UnixNano()
	value, version, expires, exists, err := kv.store.Get(args.Key)
	if err != nil {
		log.Printf("[KVServer-%d] Get error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.GetReply{Err: kvraftapi.ErrWrongLeader}
	}

	if exists && !isExpired(expires, now) {
		return kvraftapi.GetReply{
			Value:   value,
			Version: kvraftapi.Tversion(version),
			Expires: expires,
			Err:     kvraftapi.OK,
		}
	}

	return kvraftapi.GetReply{Err: kvraftapi.ErrNoKey}
}

// doPut - 写操作：更新缓存和存储
func (kv *KVServer) doPut(args *kvraftapi.PutArgs) kvraftapi.PutReply {
	if kv.killed() {
		return kvraftapi.PutReply{Err: kvraftapi.ErrWrongLeader}
	}

	now := time.Now().UnixNano()
	newExpires := absoluteExpiryFromTTL(args.TTL, now)

	oldValue, status, err := kv.store.PutCASWithTTL(args.Key, args.Value, uint64(args.Version), newExpires)
	if err != nil {
		log.Printf("[KVServer-%d] Put error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.PutReply{Err: kvraftapi.ErrWrongLeader}
	}

	switch status {
	case storage.PutCASOK:
		kv.stats.RecordWrite()
		return kvraftapi.PutReply{Err: kvraftapi.OK, OldValue: oldValue}
	case storage.PutCASVersionMismatch:
		kv.stats.RecordFailure()
		return kvraftapi.PutReply{Err: kvraftapi.ErrVersion}
	case storage.PutCASNoKey:
		kv.stats.RecordFailure()
		return kvraftapi.PutReply{Err: kvraftapi.ErrNoKey}
	default:
		kv.stats.RecordFailure()
		return kvraftapi.PutReply{Err: kvraftapi.ErrWrongLeader}
	}
}

func (kv *KVServer) doDelete(args *kvraftapi.DeleteArgs) kvraftapi.DeleteReply {
	if kv.killed() {
		return kvraftapi.DeleteReply{Err: kvraftapi.ErrWrongLeader}
	}

	now := time.Now().UnixNano()
	oldValue, _, expires, exists, err := kv.store.Get(args.Key)
	if err != nil {
		log.Printf("[KVServer-%d] Get error during Delete: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.DeleteReply{Err: kvraftapi.ErrWrongLeader}
	}
	if exists && isExpired(expires, now) {
		exists = false
	}
	if !exists {
		return kvraftapi.DeleteReply{Err: kvraftapi.ErrNoKey}
	}

	if err := kv.store.Delete(args.Key); err != nil {
		log.Printf("[KVServer-%d] Delete error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.DeleteReply{Err: kvraftapi.ErrWrongLeader}
	}

	kv.stats.RecordWrite()
	return kvraftapi.DeleteReply{Err: kvraftapi.OK, OldValue: oldValue}
}

func (kv *KVServer) doScan(args *kvraftapi.ScanArgs) kvraftapi.ScanReply {
	if kv.killed() {
		return kvraftapi.ScanReply{Err: kvraftapi.ErrWrongLeader}
	}

	all, err := kv.store.GetAll()
	if err != nil {
		log.Printf("[KVServer-%d] Scan GetAll error: %v", kv.me, err)
		kv.stats.RecordFailure()
		return kvraftapi.ScanReply{Err: kvraftapi.ErrWrongLeader}
	}

	keys := make([]string, 0, len(all))
	for k := range all {
		if args.Prefix == "" || strings.HasPrefix(k, args.Prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	limit := int(args.Limit)
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}

	items := make([]kvraftapi.ScanItem, 0, limit)
	for i := 0; i < limit; i++ {
		k := keys[i]
		v := all[k]
		now := time.Now().UnixNano()
		if isExpired(v.Expires, now) {
			continue
		}
		items = append(items, kvraftapi.ScanItem{
			Key:     k,
			Value:   v.Value,
			Version: kvraftapi.Tversion(v.Version),
			Expires: v.Expires,
		})
		if len(items) >= limit {
			break
		}
	}

	return kvraftapi.ScanReply{Items: items, Err: kvraftapi.OK}
}

func (kv *KVServer) doExpire(args *kvraftapi.ExpireArgs) kvraftapi.ExpireReply {
	if kv.killed() {
		return kvraftapi.ExpireReply{Err: kvraftapi.ErrWrongLeader}
	}
	if len(args.Keys) == 0 {
		return kvraftapi.ExpireReply{Err: kvraftapi.OK}
	}

	expired := make([]string, 0, len(args.Keys))
	expiredOldValues := make(map[string]string, len(args.Keys))
	for _, key := range args.Keys {
		oldValue, _, expires, exists, err := kv.store.Get(key)
		if err != nil {
			continue
		}
		if !exists || !isExpired(expires, args.Cutoff) {
			continue
		}
		if err := kv.store.Delete(key); err != nil {
			continue
		}
		expired = append(expired, key)
		expiredOldValues[key] = oldValue
	}
	if len(expired) > 0 {
		kv.stats.RecordTTLExpired(int64(len(expired)))
	}
	return kvraftapi.ExpireReply{ExpiredKeys: expired, ExpiredOldValues: expiredOldValues, Err: kvraftapi.OK}
}

// 快照管理

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
	if len(data) == 0 {
		// 空快照表示没有可恢复内容，不应清空已有持久化数据。
		return
	}

	if err := kv.store.LoadSnapshot(data); err != nil {
		log.Printf("[KVServer-%d] Restore snapshot error: %v", kv.me, err)
	}
}

// Watch 集成 - 关键的事件推送链路

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
	case *kvraftapi.DeleteArgs:
		kv.notifyDeleteEvent(t, result, watchMgr)
	case kvraftapi.DeleteArgs:
		kv.notifyDeleteEvent(&t, result, watchMgr)
	case *kvraftapi.ExpireArgs:
		kv.notifyExpireEvent(t, result, watchMgr)
	case kvraftapi.ExpireArgs:
		kv.notifyExpireEvent(&t, result, watchMgr)
	}
}

func (kv *KVServer) notifyPutEvent(putArgs *kvraftapi.PutArgs, result any, watchMgr *watch.Manager) {
	putReply, ok := result.(kvraftapi.PutReply)
	if !ok {
		return
	}

	if putReply.Err == kvraftapi.OK {
		// 从PutReply中直接获取OldValue，避免在Put后再次读取导致获取到新值的问题
		oldValueStr := putReply.OldValue

		eventType := "PUT"
		if oldValueStr == "" {
			// 如果OldValue为空，说明这是新key（version 0的Put）
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

func (kv *KVServer) notifyDeleteEvent(delArgs *kvraftapi.DeleteArgs, result any, watchMgr *watch.Manager) {
	delReply, ok := result.(kvraftapi.DeleteReply)
	if !ok {
		return
	}
	if delReply.Err != kvraftapi.OK {
		return
	}

	err := watchMgr.Notify(
		delArgs.Key,
		delReply.OldValue,
		"",
		0,
		"DELETE",
	)
	if err == nil {
		kv.stats.RecordWatchNotify()
	}
}

func (kv *KVServer) notifyExpireEvent(_ *kvraftapi.ExpireArgs, result any, watchMgr *watch.Manager) {
	expReply, ok := result.(kvraftapi.ExpireReply)
	if !ok || expReply.Err != kvraftapi.OK {
		return
	}
	for _, key := range expReply.ExpiredKeys {
		oldValue := ""
		if expReply.ExpiredOldValues != nil {
			oldValue = expReply.ExpiredOldValues[key]
		}
		err := watchMgr.Notify(key, oldValue, "", 0, "EXPIRE")
		if err == nil {
			kv.stats.RecordWatchNotify()
		}
	}
}

// 生命周期管理

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	if kv.grpcSrv != nil {
		kv.grpcSrv.Stop()
	}
	if kv.grpcLn != nil {
		_ = kv.grpcLn.Close()
	}
	if kv.rpcLn != nil {
		_ = kv.rpcLn.Close()
	}
	if kv.rsm != nil {
		kv.rsm.Close()
	}
	if kv.store != nil {
		if err := kv.store.Close(); err != nil {
			log.Printf("[KVServer-%d] close store error: %v", kv.me, err)
		}
	}
}

func (kv *KVServer) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *KVServer) IsAlive() bool {
	return !kv.killed()
}

func (kv *KVServer) StatsSnapshot() ServerStats {
	return ServerStats{
		TotalRequests:  atomic.LoadInt64(&kv.stats.TotalRequests),
		TotalWrites:    atomic.LoadInt64(&kv.stats.TotalWrites),
		TotalReads:     atomic.LoadInt64(&kv.stats.TotalReads),
		FailedRequests: atomic.LoadInt64(&kv.stats.FailedRequests),
		WatchNotifies:  atomic.LoadInt64(&kv.stats.WatchNotifies),
		LeaseHits:      atomic.LoadInt64(&kv.stats.LeaseHits),
		LeaseFallbacks: atomic.LoadInt64(&kv.stats.LeaseFallbacks),
		TTLExpiredOps:  atomic.LoadInt64(&kv.stats.TTLExpiredOps),
	}
}

func (kv *KVServer) PerfSnapshot() RuntimePerfStats {
	perf := RuntimePerfStats{}
	if kv == nil || kv.rsm == nil {
		return perf
	}

	perf.ApplyLoop = kv.rsm.ApplyLoopPerfStatsSnapshot()
	if rfImpl, ok := kv.rsm.rf.(*raft.Raft); ok {
		perf.Raft = rfImpl.PerfStatsSnapshot()
	}
	if fp, ok := kv.rsm.persister.(*FilePersister); ok {
		perf.Persister = fp.MetricsSnapshot()
	}
	return perf
}

// 统计方法

func (s *ServerStats) RecordRead() {
	atomic.AddInt64(&s.TotalRequests, 1)
	atomic.AddInt64(&s.TotalReads, 1)
}

func (s *ServerStats) RecordWrite() {
	atomic.AddInt64(&s.TotalRequests, 1)
	atomic.AddInt64(&s.TotalWrites, 1)
}

func (s *ServerStats) RecordFailure() {
	atomic.AddInt64(&s.FailedRequests, 1)
}

func (s *ServerStats) RecordWatchNotify() {
	atomic.AddInt64(&s.WatchNotifies, 1)
}

func (s *ServerStats) RecordLeaseHit() {
	atomic.AddInt64(&s.LeaseHits, 1)
}

func (s *ServerStats) RecordLeaseFallback() {
	atomic.AddInt64(&s.LeaseFallbacks, 1)
}

func (s *ServerStats) RecordTTLExpired(n int64) {
	atomic.AddInt64(&s.TTLExpiredOps, n)
}

func (s *ServerStats) GetStats() (requests, writes, reads, failures int64) {
	return atomic.LoadInt64(&s.TotalRequests),
		atomic.LoadInt64(&s.TotalWrites),
		atomic.LoadInt64(&s.TotalReads),
		atomic.LoadInt64(&s.FailedRequests)
}

// 服务器启动
func StartKVServer(servers []string, gid int, me int, persister Persister, maxraftstate int, address string) *KVServer {
	gob.Register(Op{})
	gob.Register(kvraftapi.PutArgs{})
	gob.Register(kvraftapi.GetArgs{})
	gob.Register(kvraftapi.DeleteArgs{})
	gob.Register(kvraftapi.ScanArgs{})
	gob.Register(kvraftapi.ExpireArgs{})

	storePath := filepath.Join(runtimeDataRoot(), "badger-"+address)
	store, err := storage.NewStore(storePath)
	if err != nil {
		log.Fatal(err)
	}

	kv := &KVServer{
		me:               me,
		address:          address,
		store:            store,
		stats:            &ServerStats{},
		leaseStatEnabled: leaseStatsEnabledFromEnv(),
		ttlEvery:         2 * time.Second,
		ttlBatch:         128,
	}

	kv.rsm = MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.rsm.RegisterOpCompleteListener(kv)

	rpcs := rpc.NewServer()
	// 注册 Raft RPC 服务，供节点间选举与日志复制调用。
	if err := rpcs.RegisterName("Raft", kv.rsm.Raft()); err != nil {
		log.Fatal(err)
	}
	l, e := net.Listen("tcp", address)
	if e != nil {
		log.Fatal(e)
	}
	kv.rpcLn = l

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
	kv.grpcSrv, kv.grpcLn = startGRPCServer(kv, address)
	go kv.ttlCleanupLoop()

	return kv
}

func (kv *KVServer) ttlCleanupLoop() {
	ticker := time.NewTicker(kv.ttlEvery)
	defer ticker.Stop()
	for !kv.killed() {
		<-ticker.C
		if kv.killed() {
			return
		}
		_, isLeader := kv.rsm.Raft().GetState()
		if !isLeader {
			continue
		}
		now := time.Now().UnixNano()
		keys, err := kv.store.GetExpiredKeys(now, kv.ttlBatch)
		if err != nil || len(keys) == 0 {
			continue
		}
		args := &kvraftapi.ExpireArgs{Keys: keys, Cutoff: now}
		_, _ = kv.rsm.Submit(args)
	}
}
