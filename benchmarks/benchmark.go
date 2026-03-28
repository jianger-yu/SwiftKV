package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	kvraftapi "kvraft/raftkv/rpc"
	"kvraft/rsm"
)

// 注意：SimplePersister 已移至 rsm/persister.go (FilePersister)
// 使用 FilePersister 实现真正的磁盘持久化

// BenchmarkConfig 描述压测配置。
type BenchmarkConfig struct {
	Servers   int           // 集群节点数
	Clients   int           // 并发客户端数
	Requests  int           // 每个客户端的请求数
	ReadRatio float64       // 读请求比例 (0.0 - 1.0)
	Keys      int           // key 空间大小
	Duration  time.Duration // 压测阶段最长时长
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
}

func cleanBenchmarkDataDirs(servers []string) {
	for _, addr := range servers {
		_ = os.RemoveAll("badger-" + addr)
	}
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
	if cfg.ReadRatio < 0 || cfg.ReadRatio > 1 {
		cfg.ReadRatio = 0.7
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 30 * time.Second
	}

	// 1. 构建服务器地址列表
	servers := make([]string, cfg.Servers)
	for i := 0; i < cfg.Servers; i++ {
		servers[i] = fmt.Sprintf("127.0.0.1:%d", 15000+i)
	}

	// 2. 启动集群
	fmt.Print("启动 KVraft 集群...")
	// 每次压测前清理旧数据，避免历史版本导致初始化阶段大量 ErrVersion。
	cleanBenchmarkDataDirs(servers)
	kvServers := make([]*rsm.KVServer, cfg.Servers)
	persisters := make([]rsm.Persister, cfg.Servers)

	for i := 0; i < cfg.Servers; i++ {
		p, err := rsm.NewFilePersister("badger-" + servers[i])
		if err != nil {
			return BenchmarkResult{}, fmt.Errorf("create persister for server %d: %w", i, err)
		}
		persisters[i] = p
		kvServers[i] = rsm.StartKVServer(servers, 1, i, persisters[i], -1, servers[i])
	}
	fmt.Printf(" OK (%d 个节点)\n", cfg.Servers)

	// 3. 等待集群选举完成（等待 leader 产生）
	fmt.Print("等待集群选举完成...")
	time.Sleep(1000 * time.Millisecond) // 给 raft 时间选举 leader
	fmt.Println(" OK")

	// 4. 初始化 key 空间（并发预热）
	fmt.Print("初始化 key 空间...")
	initStart := time.Now()
	initWorkers := cfg.Clients
	if initWorkers < 1 {
		initWorkers = 1
	}
	if initWorkers > 32 {
		initWorkers = 32
	}
	if cfg.Keys > 0 && initWorkers > cfg.Keys {
		initWorkers = cfg.Keys
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
			c := rsm.MakeClerk(servers)
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

	for i := 0; i < cfg.Keys; i++ {
		initKeyCh <- i
	}
	close(initKeyCh)
	initWg.Wait()
	initDuration := time.Since(initStart)

	initOpsPerSec := 0.0
	if initDuration.Seconds() > 0 {
		initOpsPerSec = float64(initOK) / initDuration.Seconds()
	}
	fmt.Printf(" OK (%d 个 key, workers=%d, init fail=%d, version-conflict=%d, init throughput=%.2f ops/s)\n", cfg.Keys, initWorkers, initFail, initVersionConflict, initOpsPerSec)

	// 5. 运行压测
	fmt.Printf("运行压测 (%d 个客户端, 每个 %d 个请求)...\n", cfg.Clients, cfg.Requests)

	res := BenchmarkResult{
		MinLatency: time.Duration(1<<63 - 1),
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	var totalLatency time.Duration
	var latencySamples int64
	workerClerks := make([]*rsm.Clerk, cfg.Clients)
	for i := 0; i < cfg.Clients; i++ {
		workerClerks[i] = rsm.MakeClerk(servers)
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
					_, _, errCode = workerClerk.Get(key)
					localRead++
				} else {
					value := fmt.Sprintf("value-%d-%d", id, i)
					_, version, getErr := workerClerk.Get(key)
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
	} else {
		res.MinLatency = 0
		res.MaxLatency = 0
	}

	// 7. 清理资源
	fmt.Print("清理资源...")
	for _, c := range workerClerks {
		if c != nil {
			c.Close()
		}
	}
	for _, kv := range kvServers {
		kv.Kill()
	}
	fmt.Println(" OK")

	return res, nil
}

func main() {
	servers := flag.Int("servers", 3, "KVraft 集群节点数")
	clients := flag.Int("clients", 10, "并发客户端数")
	requests := flag.Int("requests", 1000, "每个客户端的请求数")
	readRatio := flag.Float64("read-ratio", 0.7, "读请求比例 (0.0-1.0)")
	keys := flag.Int("keys", 10000, "key 空间大小")
	duration := flag.Duration("duration", 30*time.Second, "压测最长时长")
	flag.Parse()

	cfg := BenchmarkConfig{
		Servers:   *servers,
		Clients:   *clients,
		Requests:  *requests,
		ReadRatio: *readRatio,
		Keys:      *keys,
		Duration:  *duration,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("========================================")
	fmt.Println("     KVraft 性能基准测试 (Real Mode)")
	fmt.Println("========================================")
	fmt.Println()
	fmt.Printf("配置: %d 个节点, %d 个客户端, 每个客户端 %d 个请求\n", cfg.Servers, cfg.Clients, cfg.Requests)
	fmt.Printf("读写比: %.1f%% 读 / %.1f%% 写\n", cfg.ReadRatio*100, (1-cfg.ReadRatio)*100)
	fmt.Printf("key 空间: %d\n", cfg.Keys)
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
	fmt.Println()
	fmt.Printf("总耗时: %v\n", time.Since(startTime))
	fmt.Println("========================================")
}
