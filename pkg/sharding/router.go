package sharding

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RaftGroupConfig 描述一个分组的副本信息。
type RaftGroupConfig struct {
	GroupID   int
	Replicas  []string
	LeaderIdx int
}

// ShardingConfig 描述路由层配置。
type ShardingConfig struct {
	Groups            []RaftGroupConfig
	VirtualNodeCount  int
	PreferredReplicas int
	ConnectTimeout    time.Duration
	RequestTimeout    time.Duration
}

// ShardRouter 基于一致性哈希将请求路由到对应分组。
type ShardRouter struct {
	mu           sync.RWMutex
	config       ShardingConfig
	hashRing     *ConsistentHash
	groupClients map[int]map[string]pb.KVServiceClient
	groupConns   map[int]map[string]*grpc.ClientConn
	groupsByID   map[int]RaftGroupConfig
	leaderCache  map[int]string
}

// WatchEventHandler 处理来自分片路由层的 Watch 事件。
type WatchEventHandler func(groupID int, event *pb.WatchEvent)

// NewShardRouter 创建并初始化分片路由器。
func NewShardRouter(cfg ShardingConfig) (*ShardRouter, error) {
	// 每个物理分片在哈希环上生成的“影子”数量
	if cfg.VirtualNodeCount <= 0 {
		cfg.VirtualNodeCount = 150
	}
	// 限制底层 TCP/gRPC 建立连接的最长时间
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 2 * time.Second
	}
	// 规定单次业务请求（如 Get/Put）的等待上限
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 1200 * time.Millisecond
	}
	// 每个物理分片在哈希环上生成的“影子”数量
	if cfg.PreferredReplicas <= 0 {
		cfg.PreferredReplicas = 3
	}
	if len(cfg.Groups) == 0 {
		return nil, fmt.Errorf("sharding config has no groups")
	}

	r := &ShardRouter{
		config:       cfg,
		hashRing:     NewConsistentHash(cfg.VirtualNodeCount),
		groupConns:   make(map[int]map[string]*grpc.ClientConn),      // 物理连接池
		groupClients: make(map[int]map[string]pb.KVServiceClient),    // 业务逻辑接口池
		groupsByID:   make(map[int]RaftGroupConfig, len(cfg.Groups)), // 配置查找表
		leaderCache:  make(map[int]string),                           // leader状态缓存表
	}

	// 构建分片映射
	for _, g := range cfg.Groups {
		if g.GroupID <= 0 {
			return nil, fmt.Errorf("invalid group id %d", g.GroupID)
		}
		if len(g.Replicas) == 0 {
			return nil, fmt.Errorf("group %d has no replicas", g.GroupID)
		}
		if _, exists := r.groupsByID[g.GroupID]; exists {
			return nil, fmt.Errorf("duplicate group id %d", g.GroupID)
		}
		r.groupsByID[g.GroupID] = g
		r.hashRing.AddNode(fmt.Sprintf("group-%d", g.GroupID))
	}

	// 连接初始化
	if err := r.initConnections(); err != nil {
		r.Close()
		return nil, err
	}

	return r, nil
}

func (r *ShardRouter) initConnections() error {
	for _, g := range r.config.Groups {
		replicas := append([]string(nil), g.Replicas...)
		// 为 g.GroupID 初始化专属的连接池ID
		r.groupConns[g.GroupID] = make(map[string]*grpc.ClientConn, len(replicas))
		r.groupClients[g.GroupID] = make(map[string]pb.KVServiceClient, len(replicas))

		for _, replica := range replicas {
			dialCtx, cancel := context.WithTimeout(context.Background(), r.config.ConnectTimeout)
			conn, err := grpc.DialContext(
				dialCtx,
				replica,
				grpc.WithTransportCredentials(insecure.NewCredentials()), // 不使用 SSL/TLS 加密
				grpc.WithBlock(), // 阻塞等待直到连接真正 Ready
			)
			cancel()
			if err != nil {
				// 允许部分副本不可用，只要该组至少有一个可连通。
				continue
			}
			r.groupConns[g.GroupID][replica] = conn
			r.groupClients[g.GroupID][replica] = pb.NewKVServiceClient(conn)
		}

		leader := replicas[g.LeaderIdx%len(replicas)]
		if _, ok := r.groupClients[g.GroupID][leader]; ok {
			r.leaderCache[g.GroupID] = leader
		}
		if len(r.groupClients[g.GroupID]) == 0 {
			return fmt.Errorf("group %d has no reachable replicas", g.GroupID)
		}
	}
	return nil
}

// Resolve 返回 key 对应的分组 ID。
func (r *ShardRouter) Resolve(key string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	node := r.hashRing.GetNode(key)
	if node == "" {
		return -1
	}

	var gid int
	_, _ = fmt.Sscanf(node, "group-%d", &gid)
	return gid
}

// 返回 key 对应的分组 ID。
func (r *ShardRouter) groupForKey(key string) (int, error) {
	gid := r.Resolve(key)
	if gid < 0 {
		return -1, fmt.Errorf("no group for key %q", key)
	}
	return gid, nil
}

// GroupIDs 返回当前路由器管理的分组 ID 列表（升序）。
func (r *ShardRouter) GroupIDs() []int {
	r.mu.RLock()
	ids := make([]int, 0, len(r.groupsByID))
	for gid := range r.groupsByID {
		ids = append(ids, gid)
	}
	r.mu.RUnlock()
	sort.Ints(ids)
	return ids
}

func (r *ShardRouter) isWrongLeader(errText string) bool {
	if errText == "" {
		return false
	}
	return errText == "ErrWrongLeader"
}

func (r *ShardRouter) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, r.config.RequestTimeout)
}

func (r *ShardRouter) groupReplicaCandidates(gid int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	g, ok := r.groupsByID[gid]
	if !ok {
		return nil
	}

	seen := make(map[string]bool, len(g.Replicas))   // 地址去重表
	candidates := make([]string, 0, len(g.Replicas)) // 候选地址切片(作为函数输出)

	if leader, ok := r.leaderCache[gid]; ok && leader != "" {
		if _, exists := r.groupClients[gid][leader]; exists {
			candidates = append(candidates, leader)
			seen[leader] = true
		}
	}

	// 选取可用副本
	for _, addr := range g.Replicas {
		if seen[addr] {
			continue
		}
		if _, exists := r.groupClients[gid][addr]; exists {
			candidates = append(candidates, addr)
			seen[addr] = true
		}
	}

	maxCandidates := r.config.PreferredReplicas
	if maxCandidates <= 0 || maxCandidates > len(candidates) {
		maxCandidates = len(candidates)
	}
	if maxCandidates < len(candidates) {
		candidates = candidates[:maxCandidates]
	}

	return candidates
}

func (r *ShardRouter) setLeader(gid int, leaderAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.groupClients[gid][leaderAddr]; ok {
		r.leaderCache[gid] = leaderAddr
	}
}

// GetFromGroup 在指定分组内执行 Get。
func (r *ShardRouter) GetFromGroup(ctx context.Context, gid int, key string) (*pb.GetResponse, error) {
	var lastErr error
	for _, replica := range r.groupReplicaCandidates(gid) {
		r.mu.RLock()
		client := r.groupClients[gid][replica]
		r.mu.RUnlock()
		if client == nil {
			continue
		}

		reqCtx, cancel := r.withRequestTimeout(ctx)
		resp, rpcErr := client.Get(reqCtx, &pb.GetRequest{Key: key})
		cancel()

		if rpcErr != nil {
			lastErr = rpcErr
			continue
		}
		if resp == nil {
			lastErr = fmt.Errorf("empty get response from %s", replica)
			continue
		}
		if r.isWrongLeader(resp.GetError()) {
			lastErr = fmt.Errorf("group %d wrong leader via %s", gid, replica)
			continue
		}

		r.setLeader(gid, replica)
		return resp, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no available replica for group %d", gid)
	}
	return nil, lastErr
}

// PutToGroup 在指定分组内执行 Put（不带 TTL）。
func (r *ShardRouter) PutToGroup(ctx context.Context, gid int, key string, value string, version int64) (*pb.PutResponse, error) {
	return r.PutToGroupWithTTL(ctx, gid, key, value, version, 0)
}

// PutToGroupWithTTL 在指定分组内执行 Put（带 TTL，单位秒；<=0 表示不过期）。
func (r *ShardRouter) PutToGroupWithTTL(ctx context.Context, gid int, key string, value string, version int64, ttlSeconds int64) (*pb.PutResponse, error) {
	var lastErr error
	for _, replica := range r.groupReplicaCandidates(gid) {
		r.mu.RLock()
		client := r.groupClients[gid][replica]
		r.mu.RUnlock()
		if client == nil {
			continue
		}

		reqCtx, cancel := r.withRequestTimeout(ctx)
		resp, rpcErr := client.Put(reqCtx, &pb.PutRequest{Key: key, Value: value, Version: version, TtlSeconds: ttlSeconds})
		cancel()

		if rpcErr != nil {
			lastErr = rpcErr
			continue
		}
		if resp == nil {
			lastErr = fmt.Errorf("empty put response from %s", replica)
			continue
		}
		if r.isWrongLeader(resp.GetError()) {
			lastErr = fmt.Errorf("group %d wrong leader via %s", gid, replica)
			continue
		}

		r.setLeader(gid, replica)
		return resp, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no available replica for group %d", gid)
	}
	return nil, lastErr
}

// DeleteFromGroup 在指定分组内执行 Delete。
func (r *ShardRouter) DeleteFromGroup(ctx context.Context, gid int, key string) (*pb.DeleteResponse, error) {
	var lastErr error
	for _, replica := range r.groupReplicaCandidates(gid) {
		r.mu.RLock()
		client := r.groupClients[gid][replica]
		r.mu.RUnlock()
		if client == nil {
			continue
		}

		reqCtx, cancel := r.withRequestTimeout(ctx)
		resp, rpcErr := client.Delete(reqCtx, &pb.DeleteRequest{Key: key})
		cancel()

		if rpcErr != nil {
			lastErr = rpcErr
			continue
		}
		if resp == nil {
			lastErr = fmt.Errorf("empty delete response from %s", replica)
			continue
		}
		if r.isWrongLeader(resp.GetError()) {
			lastErr = fmt.Errorf("group %d wrong leader via %s", gid, replica)
			continue
		}

		r.setLeader(gid, replica)
		return resp, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no available replica for group %d", gid)
	}
	return nil, lastErr
}

// ScanGroup 在指定分组内执行 Scan。
func (r *ShardRouter) ScanGroup(ctx context.Context, gid int, prefix string, limit int32) ([]*pb.KeyValue, error) {
	var lastErr error
	for _, replica := range r.groupReplicaCandidates(gid) {
		r.mu.RLock()
		client := r.groupClients[gid][replica]
		r.mu.RUnlock()
		if client == nil {
			continue
		}

		reqCtx, cancel := r.withRequestTimeout(ctx)
		resp, rpcErr := client.Scan(reqCtx, &pb.ScanRequest{Prefix: prefix, Limit: limit})
		cancel()

		if rpcErr != nil {
			lastErr = rpcErr
			continue
		}
		if resp == nil {
			lastErr = fmt.Errorf("empty scan response from %s", replica)
			continue
		}
		if r.isWrongLeader(resp.GetError()) {
			lastErr = fmt.Errorf("group %d wrong leader via %s", gid, replica)
			continue
		}

		r.setLeader(gid, replica)
		return resp.GetItems(), nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no available replica for group %d", gid)
	}
	return nil, lastErr
}

// GetRoute 路由 Get 请求。
func (r *ShardRouter) GetRoute(ctx context.Context, key string) (*pb.GetResponse, error) {
	gid, err := r.groupForKey(key)
	if err != nil {
		return nil, err
	}
	return r.GetFromGroup(ctx, gid, key)
}

// PutRoute 路由 Put 请求（不带 TTL）。
func (r *ShardRouter) PutRoute(ctx context.Context, key string, value string, version int64) (*pb.PutResponse, error) {
	return r.PutRouteWithTTL(ctx, key, value, version, 0)
}

// PutRouteWithTTL 路由 Put 请求（带 TTL，单位秒；<=0 表示不过期）。
func (r *ShardRouter) PutRouteWithTTL(ctx context.Context, key string, value string, version int64, ttlSeconds int64) (*pb.PutResponse, error) {
	gid, err := r.groupForKey(key)
	if err != nil {
		return nil, err
	}
	return r.PutToGroupWithTTL(ctx, gid, key, value, version, ttlSeconds)
}

// DeleteRoute 路由 Delete 请求。
func (r *ShardRouter) DeleteRoute(ctx context.Context, key string) (*pb.DeleteResponse, error) {
	gid, err := r.groupForKey(key)
	if err != nil {
		return nil, err
	}
	return r.DeleteFromGroup(ctx, gid, key)
}

// ScanRoute 跨分片执行 Scan 并合并结果（按 key 排序）。
func (r *ShardRouter) ScanRoute(ctx context.Context, prefix string, limit int32) ([]*pb.KeyValue, error) {
	gids := r.GroupIDs()
	if len(gids) == 0 {
		return nil, fmt.Errorf("no groups configured")
	}
	if len(gids) == 1 {
		return r.ScanGroup(ctx, gids[0], prefix, limit)
	}

	all := make([]*pb.KeyValue, 0)
	for _, gid := range gids {
		items, err := r.ScanGroup(ctx, gid, prefix, 0)
		if err != nil {
			return nil, err
		}
		all = append(all, items...)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].GetKey() == all[j].GetKey() {
			return all[i].GetVersion() < all[j].GetVersion()
		}
		return all[i].GetKey() < all[j].GetKey()
	})

	if limit > 0 && len(all) > int(limit) {
		all = all[:limit]
	}
	return all, nil
}

func (r *ShardRouter) watchGroupsFor(key string, prefix bool) ([]int, error) {
	if !prefix {
		gid, err := r.groupForKey(key)
		if err != nil {
			return nil, err
		}
		return []int{gid}, nil
	}
	gids := r.GroupIDs()
	if len(gids) == 0 {
		return nil, fmt.Errorf("no groups configured")
	}
	return gids, nil
}

func (r *ShardRouter) watchSingleGroup(ctx context.Context, gid int, key string, prefix bool, handler WatchEventHandler) error {
	var lastErr error

	for {
		if ctx.Err() != nil {
			return nil
		}

		candidates := r.groupReplicaCandidates(gid)
		if len(candidates) == 0 {
			lastErr = fmt.Errorf("group %d has no available replica", gid)
			select {
			case <-time.After(200 * time.Millisecond):
				continue
			case <-ctx.Done():
				return nil
			}
		}

		for _, replica := range candidates {
			r.mu.RLock()
			client := r.groupClients[gid][replica]
			r.mu.RUnlock()
			if client == nil {
				continue
			}

			stream, err := client.Watch(ctx)
			if err != nil {
				lastErr = err
				continue
			}

			if err := stream.Send(&pb.WatchRequest{RequestType: &pb.WatchRequest_Create{Create: &pb.WatchCreateRequest{Key: key, Prefix: prefix}}}); err != nil {
				lastErr = err
				continue
			}

			r.setLeader(gid, replica)

			for {
				ev, recvErr := stream.Recv()
				if recvErr == nil {
					if handler != nil && ev != nil {
						handler(gid, ev)
					}
					continue
				}

				if recvErr == io.EOF || ctx.Err() != nil {
					return nil
				}

				lastErr = recvErr
				break
			}
		}

		if lastErr != nil {
			select {
			case <-time.After(150 * time.Millisecond):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// WatchRoute 按分片隔离监听。
// prefix=false 时仅监听 key 所在分片；prefix=true 时监听所有分片并聚合事件。
func (r *ShardRouter) WatchRoute(ctx context.Context, key string, prefix bool, handler WatchEventHandler) error {
	gids, err := r.watchGroupsFor(key, prefix)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, len(gids))

	for _, gid := range gids {
		groupID := gid
		wg.Add(1)
		go func() {
			defer wg.Done()
			if werr := r.watchSingleGroup(ctx, groupID, key, prefix, handler); werr != nil {
				errCh <- werr
			}
		}()
	}

	wg.Wait()
	close(errCh)

	if ctx.Err() != nil {
		return nil
	}
	for e := range errCh {
		if e != nil {
			return e
		}
	}
	return nil
}

// BatchGet 按分组并发拉取多个 key。
func (r *ShardRouter) BatchGet(ctx context.Context, keys []string) map[string]*pb.GetResponse {
	grouped := make(map[int][]string)
	for _, key := range keys {
		gid := r.Resolve(key)
		if gid >= 0 {
			grouped[gid] = append(grouped[gid], key)
		}
	}

	results := make(map[string]*pb.GetResponse)
	var resultsMu sync.Mutex
	var wg sync.WaitGroup

	for gid, groupKeys := range grouped {
		wg.Add(1)
		go func(groupID int, ks []string) {
			defer wg.Done()

			for _, key := range ks {
				resp, err := r.GetRoute(ctx, key)
				if err == nil && resp != nil {
					resultsMu.Lock()
					results[key] = resp
					resultsMu.Unlock()
				}
			}
		}(gid, groupKeys)
	}

	wg.Wait()
	return results
}

// Close 释放路由器持有的连接资源。
func (r *ShardRouter) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for gid, conn := range r.groupConns {
		for addr, c := range conn {
			if c != nil {
				_ = c.Close()
			}
			delete(r.groupConns[gid], addr)
			delete(r.groupClients[gid], addr)
		}
		delete(r.groupConns, gid)
		delete(r.groupClients, gid)
		delete(r.leaderCache, gid)
		delete(r.groupsByID, gid)
	}
}

// HealthChecker 定期检查各分组服务可用性。
type HealthChecker struct {
	router   *ShardRouter
	interval time.Duration
	ticker   *time.Ticker
	done     chan struct{}
	wg       sync.WaitGroup
}

// NewHealthChecker 创建健康检查器。
func NewHealthChecker(router *ShardRouter, interval time.Duration) *HealthChecker {
	return &HealthChecker{
		router:   router,
		interval: interval,
		done:     make(chan struct{}),
	}
}

// Start 启动后台健康检查协程。
func (hc *HealthChecker) Start() {
	hc.ticker = time.NewTicker(hc.interval)
	hc.wg.Add(1)

	go func() {
		defer hc.wg.Done()
		for {
			select {
			case <-hc.ticker.C:
				hc.checkGroupHealth()
			case <-hc.done:
				return
			}
		}
	}()
}

// Stop 停止后台健康检查。
func (hc *HealthChecker) Stop() {
	close(hc.done)
	if hc.ticker != nil {
		hc.ticker.Stop()
	}
	hc.wg.Wait()
}

func (hc *HealthChecker) checkGroupHealth() {
	hc.router.mu.RLock()
	groups := append([]RaftGroupConfig(nil), hc.router.config.Groups...)
	clients := make(map[int][]pb.KVServiceClient, len(hc.router.groupClients))
	for gid, replicas := range hc.router.groupClients {
		arr := make([]pb.KVServiceClient, 0, len(replicas))
		for _, c := range replicas {
			arr = append(arr, c)
		}
		clients[gid] = arr
	}
	hc.router.mu.RUnlock()

	for _, group := range groups {
		replicaClients := clients[group.GroupID]
		if len(replicaClients) == 0 {
			continue
		}

		healthy := false
		for _, client := range replicaClients {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_, err := client.GetClusterStatus(ctx, &pb.ClusterStatusRequest{})
			cancel()
			if err == nil {
				healthy = true
				break
			}
		}
		if !healthy {
			log.Printf("[sharding] group %d health check failed: no reachable replica", group.GroupID)
		}
	}
}
