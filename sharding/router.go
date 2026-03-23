package sharding

import (
	"context"
	"fmt"
	"log"
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

// NewShardRouter 创建并初始化分片路由器。
func NewShardRouter(cfg ShardingConfig) (*ShardRouter, error) {
	if cfg.VirtualNodeCount <= 0 {
		cfg.VirtualNodeCount = 150
	}
	if cfg.ConnectTimeout <= 0 {
		cfg.ConnectTimeout = 2 * time.Second
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 1200 * time.Millisecond
	}
	if len(cfg.Groups) == 0 {
		return nil, fmt.Errorf("sharding config has no groups")
	}

	r := &ShardRouter{
		config:       cfg,
		hashRing:     NewConsistentHash(cfg.VirtualNodeCount),
		groupConns:   make(map[int]map[string]*grpc.ClientConn),
		groupClients: make(map[int]map[string]pb.KVServiceClient),
		groupsByID:   make(map[int]RaftGroupConfig, len(cfg.Groups)),
		leaderCache:  make(map[int]string),
	}

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

	if err := r.initConnections(); err != nil {
		r.Close()
		return nil, err
	}

	return r, nil
}

func (r *ShardRouter) initConnections() error {
	for _, g := range r.config.Groups {
		replicas := append([]string(nil), g.Replicas...)
		r.groupConns[g.GroupID] = make(map[string]*grpc.ClientConn, len(replicas))
		r.groupClients[g.GroupID] = make(map[string]pb.KVServiceClient, len(replicas))

		for _, replica := range replicas {
			dialCtx, cancel := context.WithTimeout(context.Background(), r.config.ConnectTimeout)
			conn, err := grpc.DialContext(
				dialCtx,
				replica,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
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

func (r *ShardRouter) groupForKey(key string) (int, error) {
	gid := r.Resolve(key)
	if gid < 0 {
		return -1, fmt.Errorf("no group for key %q", key)
	}
	return gid, nil
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

	seen := make(map[string]bool, len(g.Replicas))
	candidates := make([]string, 0, len(g.Replicas))

	if leader, ok := r.leaderCache[gid]; ok && leader != "" {
		if _, exists := r.groupClients[gid][leader]; exists {
			candidates = append(candidates, leader)
			seen[leader] = true
		}
	}

	for _, addr := range g.Replicas {
		if seen[addr] {
			continue
		}
		if _, exists := r.groupClients[gid][addr]; exists {
			candidates = append(candidates, addr)
			seen[addr] = true
		}
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

// GetRoute 路由 Get 请求。
func (r *ShardRouter) GetRoute(ctx context.Context, key string) (*pb.GetResponse, error) {
	gid, err := r.groupForKey(key)
	if err != nil {
		return nil, err
	}

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

// PutRoute 路由 Put 请求。
func (r *ShardRouter) PutRoute(ctx context.Context, key string, value string, version int64) (*pb.PutResponse, error) {
	gid, err := r.groupForKey(key)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for _, replica := range r.groupReplicaCandidates(gid) {
		r.mu.RLock()
		client := r.groupClients[gid][replica]
		r.mu.RUnlock()
		if client == nil {
			continue
		}

		reqCtx, cancel := r.withRequestTimeout(ctx)
		resp, rpcErr := client.Put(reqCtx, &pb.PutRequest{Key: key, Value: value, Version: version})
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

// DeleteRoute 路由 Delete 请求。
func (r *ShardRouter) DeleteRoute(ctx context.Context, key string) (*pb.DeleteResponse, error) {
	gid, err := r.groupForKey(key)
	if err != nil {
		return nil, err
	}

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
