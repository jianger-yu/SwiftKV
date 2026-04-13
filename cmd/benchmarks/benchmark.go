package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	kvraftapi "kvraft/pkg/raftapi"
	"kvraft/pkg/rsm"
	"kvraft/pkg/sharding"
)

// 注意：SimplePersister 已移至 rsm/persister.go (FilePersister)
// 使用 FilePersister 实现真正的磁盘持久化

// BenchmarkConfig 描述压测配置。
type BenchmarkConfig struct {
	Servers          int           // 集群节点数
	ServerAddrs      []string      // 外部集群地址列表（非空时不在进程内拉起集群）
	ShardingConfig   string        // 分片配置文件路径（sharded=true 时优先生效）
	Clients          int           // 并发客户端数
	Requests         int           // 每个客户端的请求数
	ReadRatio        float64       // 读请求比例 (0.0 - 1.0)
	Keys             int           // key 空间大小
	InitKeys         int           // 预热 key 数量，0 表示跳过预热
	SkipInitIfSeeded bool          // 外部集群下，若 key 已存在则跳过预热
	Duration         time.Duration // 压测阶段最长时长
	MaxRaftState     int           // 快照阈值，-1 表示关闭
	Sharded          bool          // 是否使用 MakeShardedClerk
}

// BenchmarkResult 汇总压测结果。
type BenchmarkResult struct {
	TotalRequests   int64
	ReadRequests    int64
	WriteRequests   int64
	WriteOKRequests int64
	WriteConflicts  int64
	SuccessRequests int64
	FailedRequests  int64
	Duration        time.Duration
	MinLatency      time.Duration
	MaxLatency      time.Duration
	AvgLatency      time.Duration
	P99Latency      time.Duration
}

func cleanBenchmarkDataDirs(servers []string) {
	for _, addr := range servers {
		_ = os.RemoveAll(benchmarkDataDir(addr))
	}
}

func benchmarkDataDir(addr string) string {
	return filepath.Join("data", "badger-"+addr)
}

func raftToGRPCAddr(raftAddr string) string {
	host, port, err := net.SplitHostPort(strings.TrimSpace(raftAddr))
	if err != nil {
		return raftAddr
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return raftAddr
	}
	return net.JoinHostPort(host, strconv.Itoa(p+1000))
}

func loadShardingConfig(path string) (*sharding.ShardingConfig, error) {
	type groupJSON struct {
		GroupID   int      `json:"group_id"`
		Replicas  []string `json:"replicas"`
		LeaderIdx int      `json:"leader_idx"`
	}
	type cfgJSON struct {
		Groups            []groupJSON `json:"groups"`
		VirtualNodeCount  int         `json:"virtual_node_count"`
		ConnectTimeoutMS  int         `json:"connect_timeout_ms"`
		RequestTimeoutMS  int         `json:"request_timeout_ms"`
		PreferredReplicas int         `json:"preferred_replicas"`
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read sharding config failed: %w", err)
	}

	var parsed cfgJSON
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("parse sharding config failed: %w", err)
	}

	groups := make([]sharding.RaftGroupConfig, 0, len(parsed.Groups))
	for _, g := range parsed.Groups {
		groups = append(groups, sharding.RaftGroupConfig{
			GroupID:   g.GroupID,
			Replicas:  append([]string(nil), g.Replicas...),
			LeaderIdx: g.LeaderIdx,
		})
	}

	cfg := &sharding.ShardingConfig{
		Groups:            groups,
		VirtualNodeCount:  parsed.VirtualNodeCount,
		PreferredReplicas: parsed.PreferredReplicas,
		ConnectTimeout:    time.Duration(parsed.ConnectTimeoutMS) * time.Millisecond,
		RequestTimeout:    time.Duration(parsed.RequestTimeoutMS) * time.Millisecond,
	}
	return cfg, nil
}

func makeBenchmarkClerk(servers []string, shardedMode bool, shardingCfg *sharding.ShardingConfig) (*rsm.Clerk, error) {
	if !shardedMode {
		return rsm.MakeClerk(servers), nil
	}
	if shardingCfg != nil {
		return rsm.MakeShardedClerk(*shardingCfg)
	}

	replicas := make([]string, 0, len(servers))
	for _, s := range servers {
		replicas = append(replicas, raftToGRPCAddr(s))
	}
	cfg := sharding.ShardingConfig{
		Groups: []sharding.RaftGroupConfig{{
			GroupID:   1,
			Replicas:  replicas,
			LeaderIdx: 0,
		}},
		VirtualNodeCount:  150,
		PreferredReplicas: 3,
		ConnectTimeout:    2 * time.Second,
		RequestTimeout:    1200 * time.Millisecond,
	}

	return rsm.MakeShardedClerk(cfg)
}

// RunRealBenchmark 启动一个实际的 KVraft 集群进行压测
func RunRealBenchmark(ctx context.Context, cfg BenchmarkConfig) (BenchmarkResult, error) {
	if cfg.Servers <= 0 {
		cfg.Servers = 3
	}
	if cfg.Clients <= 0 {
		cfg.Clients = 10
	}
	if cfg.Requests <= 0 {
		cfg.Requests = 1000
	}
	if cfg.Keys <= 0 {
		cfg.Keys = 10000
	}
	if cfg.InitKeys < 0 {
		cfg.InitKeys = 0
	}
	if cfg.InitKeys > cfg.Keys {
		cfg.InitKeys = cfg.Keys
	}
	if cfg.ReadRatio < 0 || cfg.ReadRatio > 1 {
		cfg.ReadRatio = 0.7
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 30 * time.Second
	}
	if cfg.MaxRaftState == 0 {
		cfg.MaxRaftState = -1
	}

	// 1. 构建服务器地址列表
	servers := make([]string, 0, cfg.Servers)
	if len(cfg.ServerAddrs) > 0 {
		servers = append(servers, cfg.ServerAddrs...)
		cfg.Servers = len(servers)
	} else {
		for i := 0; i < cfg.Servers; i++ {
			servers = append(servers, fmt.Sprintf("127.0.0.1:%d", 15000+i))
		}
	}

	manageCluster := len(cfg.ServerAddrs) == 0
	kvServers := make([]*rsm.KVServer, cfg.Servers)
	var shardingCfg *sharding.ShardingConfig
	if cfg.Sharded && strings.TrimSpace(cfg.ShardingConfig) != "" {
		loadedCfg, err := loadShardingConfig(strings.TrimSpace(cfg.ShardingConfig))
		if err != nil {
			return BenchmarkResult{}, err
		}
		shardingCfg = loadedCfg
	}

	// 2. 启动集群（仅在内置模式）
	if manageCluster {
		fmt.Print("启动 KVraft 集群...")
		// 每次压测前清理旧数据，避免历史版本导致初始化阶段大量 ErrVersion。
		cleanBenchmarkDataDirs(servers)
		persisters := make([]rsm.Persister, cfg.Servers)

		for i := 0; i < cfg.Servers; i++ {
			p, err := rsm.NewFilePersister(benchmarkDataDir(servers[i]))
			if err != nil {
				return BenchmarkResult{}, fmt.Errorf("create persister for server %d: %w", i, err)
			}
			persisters[i] = p
			kvServers[i] = rsm.StartKVServer(servers, 1, i, persisters[i], cfg.MaxRaftState, servers[i])
		}
		fmt.Printf(" OK (%d 个节点)\n", cfg.Servers)
	} else {
		fmt.Printf("使用外部集群: %s\n", strings.Join(servers, ","))
	}

	// 3. 等待集群选举完成（等待 leader 产生）
	fmt.Print("等待集群选举完成...")
	time.Sleep(1000 * time.Millisecond) // 给 raft 时间选举 leader
	fmt.Println(" OK")

	// 4. 初始化 key 空间（并发预热）
	fmt.Print("初始化 key 空间...")
	initStart := time.Now()
	initDuration := time.Duration(0)
	initTarget := cfg.InitKeys
	if initTarget == 0 {
		fmt.Println(" SKIP (init-keys=0)")
	} else if !manageCluster && cfg.SkipInitIfSeeded {
		seedCheckClerk, err := makeBenchmarkClerk(servers, cfg.Sharded, shardingCfg)
		if err == nil {
			allSeeded := true
			probe := initTarget
			if probe > 16 {
				probe = 16
			}
			for i := 0; i < probe; i++ {
				_, _, _, e := seedCheckClerk.Get(fmt.Sprintf("key-%d", i))
				if e != kvraftapi.OK {
					allSeeded = false
					break
				}
			}
			seedCheckClerk.Close()
			if allSeeded {
				fmt.Printf(" SKIP (external cluster already seeded, checked %d keys)\n", probe)
				initTarget = 0
			}
		}
	}

	if initTarget > 0 {
		initWorkers := cfg.Clients
		if initWorkers < 1 {
			initWorkers = 1
		}
		if initWorkers > 32 {
			initWorkers = 32
		}
		if initTarget > 0 && initWorkers > initTarget {
			initWorkers = initTarget
		}

		initKeyCh := make(chan int, initWorkers*2)
		var initWg sync.WaitGroup
		var initMu sync.Mutex
		var initOK int64
		var initFail int64
		var initVersionConflict int64

		for w := 0; w < initWorkers; w++ {
			initWg.Add(1)
			go func() {
				defer initWg.Done()
				c, clerkErr := makeBenchmarkClerk(servers, cfg.Sharded, shardingCfg)
				if clerkErr != nil {
					initMu.Lock()
					initFail += int64(initTarget / initWorkers)
					initMu.Unlock()
					return
				}
				defer c.Close()

				var localOK int64
				var localFail int64
				var localVersionConflict int64
				for i := range initKeyCh {
					key := fmt.Sprintf("key-%d", i)
					value := fmt.Sprintf("value-%d", i)
					errCode := c.Put(key, value, 0)
					if errCode == kvraftapi.OK {
						localOK++
					} else if errCode == kvraftapi.ErrVersion {
						// 历史数据已存在时视作预热成功。
						localOK++
						localVersionConflict++
					} else {
						localFail++
					}
				}

				initMu.Lock()
				initOK += localOK
				initFail += localFail
				initVersionConflict += localVersionConflict
				initMu.Unlock()
			}()
		}

		for i := 0; i < initTarget; i++ {
			initKeyCh <- i
		}
		close(initKeyCh)
		initWg.Wait()
		initDuration = time.Since(initStart)

		initOpsPerSec := 0.0
		if initDuration.Seconds() > 0 {
			initOpsPerSec = float64(initOK) / initDuration.Seconds()
		}
		fmt.Printf(" OK (%d 个 key, workers=%d, init fail=%d, version-conflict=%d, init throughput=%.2f ops/s)\n", initTarget, initWorkers, initFail, initVersionConflict, initOpsPerSec)
	}

	// 5. 运行压测
	fmt.Printf("运行压测 (%d 个客户端, 每个 %d 个请求)...\n", cfg.Clients, cfg.Requests)

	res := BenchmarkResult{
		MinLatency: time.Duration(1<<63 - 1),
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var totalLatency time.Duration
	var latencySamples int64
	latencyValues := make([]int64, 0, cfg.Clients*cfg.Requests)
	workerClerks := make([]*rsm.Clerk, cfg.Clients)
	for i := 0; i < cfg.Clients; i++ {
		c, err := makeBenchmarkClerk(servers, cfg.Sharded, shardingCfg)
		if err != nil {
			return BenchmarkResult{}, fmt.Errorf("create worker clerk %d: %w", i, err)
		}
		workerClerks[i] = c
	}

	// 压测阶段的时间预算应只受 cfg.Duration 控制，避免外层 deadline 在预热阶段耗尽。
	benchCtx, benchCancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer benchCancel()
	go func() {
		select {
		case <-ctx.Done():
			benchCancel()
		case <-benchCtx.Done():
		}
	}()

	start := time.Now()

	for clientIdx := 0; clientIdx < cfg.Clients; clientIdx++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			workerClerk := workerClerks[id]
			r := rand.New(rand.NewSource(int64(id)*1000 + time.Now().UnixNano()))

			var localRead int64
			var localWrite int64
			var localWriteOK int64
			var localWriteConflicts int64
			var localSuccess int64
			var localFailed int64
			var localLatencySum time.Duration
			var localSamples int64
			localMinLatency := time.Duration(1<<63 - 1)
			localMaxLatency := time.Duration(0)
			localLatencyValues := make([]int64, 0, cfg.Requests)

		loop:
			for i := 0; i < cfg.Requests; i++ {
				select {
				case <-benchCtx.Done():
					break loop
				default:
				}

				key := fmt.Sprintf("key-%d", r.Intn(cfg.Keys))
				opStart := time.Now()
				var errCode kvraftapi.Err

				if r.Float64() < cfg.ReadRatio {
					_, _, _, errCode = workerClerk.Get(key)
					localRead++
				} else {
					value := fmt.Sprintf("value-%d-%d", id, i)
					_, version, _, getErr := workerClerk.Get(key)
					if getErr == kvraftapi.OK {
						errCode = workerClerk.Put(key, value, version)
					} else if getErr == kvraftapi.ErrNoKey {
						errCode = workerClerk.Put(key, value, 0)
					} else {
						errCode = getErr
					}
					localWrite++
					if errCode == kvraftapi.OK {
						localWriteOK++
					} else if errCode == kvraftapi.ErrVersion {
						localWriteConflicts++
					}
				}

				latency := time.Since(opStart)
				localLatencySum += latency
				localSamples++
				if latency < localMinLatency {
					localMinLatency = latency
				}
				if latency > localMaxLatency {
					localMaxLatency = latency
				}
				localLatencyValues = append(localLatencyValues, latency.Nanoseconds())

				if errCode == kvraftapi.OK || errCode == kvraftapi.ErrNoKey || errCode == kvraftapi.ErrVersion {
					localSuccess++
				} else {
					localFailed++
				}
			}

			mu.Lock()
			res.ReadRequests += localRead
			res.WriteRequests += localWrite
			res.WriteOKRequests += localWriteOK
			res.WriteConflicts += localWriteConflicts
			res.SuccessRequests += localSuccess
			res.FailedRequests += localFailed
			totalLatency += localLatencySum
			latencySamples += localSamples
			if localMinLatency < res.MinLatency {
				res.MinLatency = localMinLatency
			}
			if localMaxLatency > res.MaxLatency {
				res.MaxLatency = localMaxLatency
			}
			latencyValues = append(latencyValues, localLatencyValues...)
			mu.Unlock()
		}(clientIdx)
	}

	wg.Wait()
	elapsed := time.Since(start)
	res.Duration = elapsed
	res.TotalRequests = res.ReadRequests + res.WriteRequests
	if res.TotalRequests == 0 {
		fmt.Printf("警告: 压测阶段未执行任何请求 (bench-duration=%v, init-duration=%v, parent-ctx-err=%v)\n", cfg.Duration, initDuration, ctx.Err())
	}

	// 6. 计算平均延迟
	if latencySamples > 0 {
		res.AvgLatency = totalLatency / time.Duration(latencySamples)
		sort.Slice(latencyValues, func(i, j int) bool {
			return latencyValues[i] < latencyValues[j]
		})
		idx := int(float64(len(latencyValues))*0.99) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(latencyValues) {
			idx = len(latencyValues) - 1
		}
		res.P99Latency = time.Duration(latencyValues[idx])
	} else {
		res.MinLatency = 0
		res.MaxLatency = 0
		res.P99Latency = 0
	}

	// 7. 清理资源
	for i, kv := range kvServers {
		if kv == nil {
			continue
		}
		s := kv.StatsSnapshot()
		fmt.Printf("server[%d] stats: req=%d read=%d write=%d lease_hit=%d lease_fallback=%d fail=%d\n",
			i, s.TotalRequests, s.TotalReads, s.TotalWrites, s.LeaseHits, s.LeaseFallbacks, s.FailedRequests)
		leaseStatsEnabled := strings.EqualFold(strings.TrimSpace(os.Getenv("KV_LEASE_STATS")), "1") ||
			strings.EqualFold(strings.TrimSpace(os.Getenv("KV_LEASE_STATS")), "true") ||
			strings.EqualFold(strings.TrimSpace(os.Getenv("KV_LEASE_STATS")), "yes") ||
			strings.EqualFold(strings.TrimSpace(os.Getenv("KV_LEASE_STATS")), "on")
		if leaseStatsEnabled && s.TotalReads > 0 && s.LeaseHits+s.LeaseFallbacks == 0 {
			fmt.Printf("server[%d] warning: read>0 but lease counters are 0; please rebuild benchmark/server binaries or use go run to ensure latest code.\n", i)
		}
	}

	fmt.Print("清理资源...")
	for _, c := range workerClerks {
		if c != nil {
			c.Close()
		}
	}
	if manageCluster {
		for _, kv := range kvServers {
			if kv != nil {
				kv.Kill()
			}
		}
	} else {
		fmt.Print(" (保留外部集群运行)")
	}
	fmt.Println(" OK")

	return res, nil
}

func main() {
	servers := flag.Int("servers", 3, "KVraft 集群节点数")
	serverAddrs := flag.String("server-addrs", "", "外部集群 rpc 地址列表，逗号分隔（设置后不会自动启动/关闭集群）")
	shardingConfig := flag.String("sharding-config", "", "分片配置文件路径（sharded=true 时建议传入 data/cluster/sharding.json）")
	clients := flag.Int("clients", 10, "并发客户端数")
	requests := flag.Int("requests", 1000, "每个客户端的请求数")
	readRatio := flag.Float64("read-ratio", 0.7, "读请求比例 (0.0-1.0)")
	keys := flag.Int("keys", 10000, "key 空间大小")
	initKeys := flag.Int("init-keys", 10000, "预热 key 数量，0 表示跳过预热")
	skipInitIfSeeded := flag.Bool("skip-init-if-seeded", true, "外部集群下若已存在预热 key 则跳过预热")
	duration := flag.Duration("duration", 30*time.Second, "压测最长时长")
	maxRaftState := flag.Int("maxraftstate", -1, "快照阈值字节数，-1 表示关闭快照")
	sharded := flag.Bool("sharded", false, "是否使用 MakeShardedClerk 路由模式（单 Raft 组建议 false，多 group 分片建议 true）")
	flag.Parse()

	parsedAddrs := make([]string, 0)
	if strings.TrimSpace(*serverAddrs) != "" {
		for _, raw := range strings.Split(*serverAddrs, ",") {
			a := strings.TrimSpace(raw)
			if a != "" {
				parsedAddrs = append(parsedAddrs, a)
			}
		}
	}

	cfg := BenchmarkConfig{
		Servers:          *servers,
		ServerAddrs:      parsedAddrs,
		ShardingConfig:   *shardingConfig,
		Clients:          *clients,
		Requests:         *requests,
		ReadRatio:        *readRatio,
		Keys:             *keys,
		InitKeys:         *initKeys,
		SkipInitIfSeeded: *skipInitIfSeeded,
		Duration:         *duration,
		MaxRaftState:     *maxRaftState,
		Sharded:          *sharded,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("========================================")
	fmt.Println("     KVraft 性能基准测试 (Real Mode)")
	fmt.Println("========================================")
	fmt.Println()
	fmt.Printf("配置: %d 个节点, %d 个客户端, 每个客户端 %d 个请求\n", cfg.Servers, cfg.Clients, cfg.Requests)
	if len(cfg.ServerAddrs) > 0 {
		fmt.Printf("集群模式: external (%s)\n", strings.Join(cfg.ServerAddrs, ","))
	} else {
		fmt.Printf("集群模式: embedded (压测结束自动停止)\n")
	}
	fmt.Printf("读写比: %.1f%% 读 / %.1f%% 写\n", cfg.ReadRatio*100, (1-cfg.ReadRatio)*100)
	fmt.Printf("key 空间: %d\n", cfg.Keys)
	fmt.Printf("预热 key: %d (skip-init-if-seeded=%t)\n", cfg.InitKeys, cfg.SkipInitIfSeeded)
	fmt.Printf("maxraftstate: %d\n", cfg.MaxRaftState)
	fmt.Printf("clerk mode: %s\n", map[bool]string{true: "sharded", false: "classic"}[cfg.Sharded])
	if strings.TrimSpace(cfg.ShardingConfig) != "" {
		fmt.Printf("sharding config: %s\n", cfg.ShardingConfig)
	}
	fmt.Println()

	startTime := time.Now()
	res, err := RunRealBenchmark(ctx, cfg)
	if err != nil && err != context.DeadlineExceeded {
		fmt.Printf("错误: %v\n", err)
		return
	}

	fmt.Println()
	fmt.Println("========== 基准测试结果 ==========")
	fmt.Printf("总请求数: %d\n", res.TotalRequests)
	if res.TotalRequests > 0 {
		fmt.Printf("成功请求: %d (%.1f%%)\n", res.SuccessRequests, float64(res.SuccessRequests)*100/float64(res.TotalRequests))
		fmt.Printf("失败请求: %d (%.1f%%)\n", res.FailedRequests, float64(res.FailedRequests)*100/float64(res.TotalRequests))
		fmt.Printf("读请求: %d (%.1f%%)\n", res.ReadRequests, float64(res.ReadRequests)*100/float64(res.TotalRequests))
		fmt.Printf("写请求: %d (%.1f%%)\n", res.WriteRequests, float64(res.WriteRequests)*100/float64(res.TotalRequests))
	}
	fmt.Println()
	fmt.Printf("完整时间: %v\n", res.Duration)
	if res.Duration.Seconds() > 0 {
		fmt.Printf("吞吐: %.2f ops/s\n", float64(res.SuccessRequests)/res.Duration.Seconds())
		fmt.Printf("OK 写入吞吐: %.2f ops/s\n", float64(res.WriteOKRequests)/res.Duration.Seconds())
	}
	if res.WriteRequests > 0 {
		fmt.Printf("写入冲突率: %.2f%%\n", float64(res.WriteConflicts)*100/float64(res.WriteRequests))
	}
	fmt.Println()
	fmt.Printf("延迟统计:\n")
	fmt.Printf("  最小: %.2f ms\n", res.MinLatency.Seconds()*1000)
	fmt.Printf("  最大: %.2f ms\n", res.MaxLatency.Seconds()*1000)
	fmt.Printf("  平均: %.2f ms\n", res.AvgLatency.Seconds()*1000)
	fmt.Printf("  P99: %.2f ms\n", res.P99Latency.Seconds()*1000)
	fmt.Println()
	fmt.Printf("总耗时: %v\n", time.Since(startTime))
	fmt.Println("========================================")
}
