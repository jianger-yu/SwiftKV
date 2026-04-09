package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	kvraftapi "kvraft/pkg/raftapi"
	"kvraft/pkg/rsm"
)

// ============================================================
// 参数组合测试框架
// ============================================================

// BenchmarkParams 定义一个性能测试的参数组合
type BenchmarkParams struct {
	Name                     string
	RaftElectionTimeoutMs    int
	RaftHeartbeatIntervalMs  int
	RaftLeaseRatio           string
	RsmSubmitTimeoutMs       int
	RsmLeaderCheckIntervalMs int
	Description              string
}

// TestScenario 定义测试场景
type TestScenario struct {
	Name      string
	Servers   int
	Clients   int
	Requests  int
	ReadRatio float64
	Duration  time.Duration
}

// BenchResult 记录单个测试结果
type BenchResult struct {
	Params       BenchmarkParams
	Scenario     TestScenario
	OpsPerSec    float64
	AvgLatencyMs float64
	MinLatencyMs float64
	MaxLatencyMs float64
	P99LatencyMs float64
	ReadOps      int64
	WriteOps     int64
	SuccessOps   int64
	FailedOps    int64
	Duration     time.Duration
}

// ============================================================
// 预定义的参数组合
// ============================================================

var parameterSets = []BenchmarkParams{
	{
		Name:                     "Current",
		RaftElectionTimeoutMs:    300,
		RaftHeartbeatIntervalMs:  80,
		RaftLeaseRatio:           "1.5",
		RsmSubmitTimeoutMs:       1500,
		RsmLeaderCheckIntervalMs: 50,
		Description:              "当前配置（基准）",
	},
	{
		Name:                     "Optimized-Short-Timeout",
		RaftElectionTimeoutMs:    300,
		RaftHeartbeatIntervalMs:  80,
		RaftLeaseRatio:           "1.5",
		RsmSubmitTimeoutMs:       500, // 减少 3x
		RsmLeaderCheckIntervalMs: 50,
		Description:              "缩短 RSM Submit 超时到 500ms",
	},
	{
		Name:                     "Optimized-Fast-Check",
		RaftElectionTimeoutMs:    300,
		RaftHeartbeatIntervalMs:  80,
		RaftLeaseRatio:           "1.5",
		RsmSubmitTimeoutMs:       1000,
		RsmLeaderCheckIntervalMs: 25, // 减少 2x
		Description:              "加快 Leader 检查间隔到 25ms",
	},
	{
		Name:                     "Optimized-Both",
		RaftElectionTimeoutMs:    300,
		RaftHeartbeatIntervalMs:  80,
		RaftLeaseRatio:           "1.5",
		RsmSubmitTimeoutMs:       800,
		RsmLeaderCheckIntervalMs: 25,
		Description:              "同时优化超时和检查间隔",
	},
	{
		Name:                     "Aggressive-Timeout",
		RaftElectionTimeoutMs:    300,
		RaftHeartbeatIntervalMs:  80,
		RaftLeaseRatio:           "1.5",
		RsmSubmitTimeoutMs:       300, // 激进配置
		RsmLeaderCheckIntervalMs: 10,
		Description:              "激进的快速失败配置",
	},
	{
		Name:                     "Raft-Fast-Heartbeat",
		RaftElectionTimeoutMs:    300,
		RaftHeartbeatIntervalMs:  50, // 加快心跳
		RaftLeaseRatio:           "1.5",
		RsmSubmitTimeoutMs:       800,
		RsmLeaderCheckIntervalMs: 25,
		Description:              "加快 Raft 心跳到 50ms",
	},
	{
		Name:                     "Raft-Slower-Heartbeat",
		RaftElectionTimeoutMs:    300,
		RaftHeartbeatIntervalMs:  150, // 降低心跳
		RaftLeaseRatio:           "1.5",
		RsmSubmitTimeoutMs:       1500,
		RsmLeaderCheckIntervalMs: 50,
		Description:              "降低 Raft 心跳到 150ms（降低 CPU）",
	},
	{
		Name:                     "Conservative-Timeout",
		RaftElectionTimeoutMs:    400, // 更宽松的选举超时
		RaftHeartbeatIntervalMs:  100,
		RaftLeaseRatio:           "1.5",
		RsmSubmitTimeoutMs:       2000,
		RsmLeaderCheckIntervalMs: 100,
		Description:              "保守的配置（更稳定但可能更慢）",
	},
	{
		Name:                     "Extreme-Fast",
		RaftElectionTimeoutMs:    300,
		RaftHeartbeatIntervalMs:  40,    // 很快的心跳
		RaftLeaseRatio:           "2.0", // 更长的 lease
		RsmSubmitTimeoutMs:       200,   // 快速超时
		RsmLeaderCheckIntervalMs: 10,
		Description:              "极端快速配置（可能不稳定）",
	},
}

// ============================================================
// 测试场景
// ============================================================

var testScenarios = []TestScenario{
	{
		Name:      "write-heavy",
		Servers:   3,
		Clients:   10,
		Requests:  1000,
		ReadRatio: 0.1,
		Duration:  30 * time.Second,
	},
	{
		Name:      "balanced",
		Servers:   3,
		Clients:   10,
		Requests:  1000,
		ReadRatio: 0.5,
		Duration:  30 * time.Second,
	},
	{
		Name:      "read-heavy",
		Servers:   3,
		Clients:   10,
		Requests:  1000,
		ReadRatio: 0.8,
		Duration:  30 * time.Second,
	},
}

func benchmarkDataDir(addr string) string {
	return filepath.Join("data", "badger-"+addr)
}

// ============================================================
// 执行基准测试
// ============================================================

func runBenchmark(params BenchmarkParams, scenario TestScenario) (*BenchResult, error) {
	fmt.Printf("\n[测试] %s + %s\n", params.Name, scenario.Name)
	fmt.Printf("  描述: %s (ReadRatio=%.1f%%)\n", params.Description, scenario.ReadRatio*100)

	// 设置环境变量
	env := os.Environ()
	env = append(env,
		fmt.Sprintf("RAFT_ELECTION_TIMEOUT_MS=%d", params.RaftElectionTimeoutMs),
		fmt.Sprintf("RAFT_HEARTBEAT_INTERVAL_MS=%d", params.RaftHeartbeatIntervalMs),
		fmt.Sprintf("RAFT_LEASE_RATIO=%s", params.RaftLeaseRatio),
		fmt.Sprintf("RSM_SUBMIT_TIMEOUT_MS=%d", params.RsmSubmitTimeoutMs),
		fmt.Sprintf("RSM_LEADER_CHECK_INTERVAL_MS=%d", params.RsmLeaderCheckIntervalMs),
	)

	// 准备数据目录（自动清理）
	for i := 0; i < scenario.Servers; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 15000+i)
		_ = os.RemoveAll(benchmarkDataDir(addr))
	}

	// 启动集群
	fmt.Print("  启动集群...")
	servers := make([]string, scenario.Servers)
	for i := 0; i < scenario.Servers; i++ {
		servers[i] = fmt.Sprintf("127.0.0.1:%d", 15000+i)
	}

	kvServers := make([]*rsm.KVServer, scenario.Servers)
	persisters := make([]rsm.Persister, scenario.Servers)

	for i := 0; i < scenario.Servers; i++ {
		p, err := rsm.NewFilePersister(benchmarkDataDir(servers[i]))
		if err != nil {
			return nil, fmt.Errorf("创建 persister %d 失败: %w", i, err)
		}
		persisters[i] = p
		kvServers[i] = rsm.StartKVServer(servers, 1, i, persisters[i], -1, servers[i])
	}
	time.Sleep(500 * time.Millisecond)
	fmt.Println(" OK")

	// 初始化 key 空间
	fmt.Print("  初始化 key 空间...")
	c := rsm.MakeClerk(servers)
	for j := 0; j < 100; j++ {
		key := fmt.Sprintf("key-%d", j)
		value := fmt.Sprintf("value-%d", j)
		c.Put(key, value, 0)
	}
	c.Close()
	fmt.Println(" OK")

	// 等待集群稳定
	time.Sleep(1000 * time.Millisecond)

	// 执行基准测试
	fmt.Print("  执行压测...")
	benchResult := runBenchmarkInternal(scenario, servers)

	// 清理资源
	for _, kv := range kvServers {
		if kv != nil {
			kv.Kill()
		}
	}
	time.Sleep(100 * time.Millisecond)

	// 清理数据
	for i := 0; i < scenario.Servers; i++ {
		addr := fmt.Sprintf("127.0.0.1:%d", 15000+i)
		_ = os.RemoveAll(benchmarkDataDir(addr))
	}

	fmt.Println(" OK")

	result := &BenchResult{
		Params:       params,
		Scenario:     scenario,
		OpsPerSec:    benchResult.OpsPerSec,
		AvgLatencyMs: benchResult.AvgLatencyMs,
		MinLatencyMs: benchResult.MinLatencyMs,
		MaxLatencyMs: benchResult.MaxLatencyMs,
		P99LatencyMs: benchResult.P99LatencyMs,
		ReadOps:      benchResult.ReadOps,
		WriteOps:     benchResult.WriteOps,
		SuccessOps:   benchResult.SuccessOps,
		FailedOps:    benchResult.FailedOps,
		Duration:     benchResult.Duration,
	}

	return result, nil
}

// BenchmarkInternalResult 内部基准测试结果
type BenchmarkInternalResult struct {
	OpsPerSec    float64
	AvgLatencyMs float64
	MinLatencyMs float64
	MaxLatencyMs float64
	P99LatencyMs float64
	ReadOps      int64
	WriteOps     int64
	SuccessOps   int64
	FailedOps    int64
	Duration     time.Duration
}

func runBenchmarkInternal(scenario TestScenario, servers []string) *BenchmarkInternalResult {
	ctx, cancel := context.WithTimeout(context.Background(), scenario.Duration+10*time.Second)
	defer cancel()

	benchStart := time.Now()

	var totalOps int64
	var readOps int64
	var writeOps int64
	var successOps int64
	var failedOps int64
	var totalLatency time.Duration
	minLatency := time.Duration(1<<63 - 1)
	var maxLatency time.Duration
	var latencies []time.Duration

	// 创建工作 goroutine
	for clientIdx := 0; clientIdx < scenario.Clients; clientIdx++ {
		go func(id int) {
			r := getRandom()
			c := rsm.MakeClerk(servers)
			defer c.Close()

			benchCtx, benchCancel := context.WithTimeout(ctx, scenario.Duration)
			defer benchCancel()

			for i := 0; i < scenario.Requests; i++ {
				select {
				case <-benchCtx.Done():
					return
				default:
				}

				key := fmt.Sprintf("key-%d", r.Intn(100))
				opStart := time.Now()

				var errCode kvraftapi.Err
				if r.Float64() < scenario.ReadRatio {
					_, _, _, errCode = c.Get(key)
					readOps++
				} else {
					value := fmt.Sprintf("value-%d-%d", id, i)
					_, version, _, getErr := c.Get(key)
					if getErr == kvraftapi.OK {
						errCode = c.Put(key, value, version)
					} else {
						errCode = c.Put(key, value, 0)
					}
					writeOps++
				}

				latency := time.Since(opStart)
				totalLatency += latency
				totalOps++
				latencies = append(latencies, latency)

				if latency < minLatency {
					minLatency = latency
				}
				if latency > maxLatency {
					maxLatency = latency
				}

				if errCode == kvraftapi.OK || errCode == kvraftapi.ErrNoKey || errCode == kvraftapi.ErrVersion {
					successOps++
				} else {
					failedOps++
				}
			}
		}(clientIdx)
	}

	// 等待测试完成
	time.Sleep(scenario.Duration)

	elapsed := time.Since(benchStart)

	// 计算统计数据
	avgLatency := time.Duration(0)
	if totalOps > 0 {
		avgLatency = totalLatency / time.Duration(totalOps)
	}

	// 计算 P99
	var p99Latency time.Duration
	if len(latencies) > 0 {
		// 简化版 P99 计算
		p99Idx := (len(latencies) * 99) / 100
		if p99Idx >= len(latencies) {
			p99Idx = len(latencies) - 1
		}
		p99Latency = latencies[p99Idx]
	}

	opsPerSec := float64(totalOps) / elapsed.Seconds()

	return &BenchmarkInternalResult{
		OpsPerSec:    opsPerSec,
		AvgLatencyMs: float64(avgLatency.Milliseconds()),
		MinLatencyMs: float64(minLatency.Milliseconds()),
		MaxLatencyMs: float64(maxLatency.Milliseconds()),
		P99LatencyMs: float64(p99Latency.Milliseconds()),
		ReadOps:      readOps,
		WriteOps:     writeOps,
		SuccessOps:   successOps,
		FailedOps:    failedOps,
		Duration:     elapsed,
	}
}

func getRandom() interface {
	Intn(int) int
	Float64() float64
} {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}

// ============================================================
// 报告生成
// ============================================================

func printResults(results []*BenchResult) {
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("性能测试结果汇总")
	fmt.Println(strings.Repeat("=", 120))

	// 按场景分组显示
	scenarioMap := make(map[string][]*BenchResult)
	for _, r := range results {
		key := r.Scenario.Name
		scenarioMap[key] = append(scenarioMap[key], r)
	}

	for _, scenario := range testScenarios {
		scenarioResults := scenarioMap[scenario.Name]
		if len(scenarioResults) == 0 {
			continue
		}

		fmt.Printf("\n【%s】ReadRatio=%.0f%%\n", strings.ToUpper(scenario.Name), scenario.ReadRatio*100)
		fmt.Println(strings.Repeat("-", 120))
		fmt.Printf("%-30s | %12s | %12s | %12s | %12s | %12s\n",
			"配置", "QPS", "平均延迟(ms)", "最小延迟(ms)", "最大延迟(ms)", "P99延迟(ms)")
		fmt.Println(strings.Repeat("-", 120))

		for _, r := range scenarioResults {
			fmt.Printf("%-30s | %12.0f | %12.3f | %12.3f | %12.3f | %12.3f\n",
				r.Params.Name,
				r.OpsPerSec,
				r.AvgLatencyMs,
				r.MinLatencyMs,
				r.MaxLatencyMs,
				r.P99LatencyMs)
		}
	}

	// 性能对比（相对于 Current）
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("性能对比（基准 = Current）")
	fmt.Println(strings.Repeat("=", 120))

	for _, scenario := range testScenarios {
		scenarioResults := scenarioMap[scenario.Name]
		if len(scenarioResults) == 0 {
			continue
		}

		// 找 Current 配置
		var currentResult *BenchResult
		var otherResults []*BenchResult
		for _, r := range scenarioResults {
			if r.Params.Name == "Current" {
				currentResult = r
			} else {
				otherResults = append(otherResults, r)
			}
		}

		if currentResult == nil {
			continue
		}

		fmt.Printf("\n【%s】\n", strings.ToUpper(scenario.Name))
		fmt.Println(strings.Repeat("-", 120))
		fmt.Printf("%-30s | %12s | %12s | %12s\n",
			"配置 vs Current", "QPS 改进", "延迟改进", "综合评分")
		fmt.Println(strings.Repeat("-", 120))

		for _, r := range otherResults {
			qpsImprove := ((r.OpsPerSec - currentResult.OpsPerSec) / currentResult.OpsPerSec) * 100
			latencyImprove := ((currentResult.AvgLatencyMs - r.AvgLatencyMs) / currentResult.AvgLatencyMs) * 100
			score := qpsImprove + latencyImprove // 简单的综合评分

			mark := " "
			if score > 20 {
				mark = "⭐"
			} else if score > 10 {
				mark = "✓ "
			} else if score < -10 {
				mark = "✗ "
			}

			fmt.Printf("%-30s | %+10.1f%% | %+10.1f%% | %+10.1f%% %s\n",
				r.Params.Name,
				qpsImprove,
				latencyImprove,
				score,
				mark)
		}
	}
}

// ============================================================
// Main
// ============================================================

func main() {
	fmt.Println("KVraft 性能参数优化基准测试")
	fmt.Println("=" + strings.Repeat("=", 119))
	fmt.Println("将使用多个参数组合和测试场景来找到最优配置")
	fmt.Println("测试参数:")
	fmt.Printf("  - 参数组合数: %d\n", len(parameterSets))
	fmt.Printf("  - 测试场景数: %d\n", len(testScenarios))
	fmt.Printf("  - 总测试组数: %d\n", len(parameterSets)*len(testScenarios))
	fmt.Println()

	var results []*BenchResult
	var failedTests []string

	// 执行所有组合测试
	for _, params := range parameterSets {
		for _, scenario := range testScenarios {
			result, err := runBenchmark(params, scenario)
			if err != nil {
				fmt.Printf("  ✗ 测试失败: %v\n", err)
				failedTests = append(failedTests, fmt.Sprintf("%s + %s", params.Name, scenario.Name))
			} else {
				results = append(results, result)
				fmt.Printf("  ✓ QPS=%.0f, Avg Latency=%.2fms\n", result.OpsPerSec, result.AvgLatencyMs)
			}
		}
	}

	// 打印结果汇总
	printResults(results)

	// 打印推荐配置
	fmt.Println("\n" + strings.Repeat("=", 120))
	fmt.Println("推荐配置")
	fmt.Println(strings.Repeat("=", 120))

	if len(results) > 0 {
		// 找最高 QPS
		bestQPS := results[0]
		for _, r := range results {
			if r.OpsPerSec > bestQPS.OpsPerSec {
				bestQPS = r
			}
		}

		// 找最低延迟
		bestLatency := results[0]
		for _, r := range results {
			if r.AvgLatencyMs < bestLatency.AvgLatencyMs {
				bestLatency = r
			}
		}

		fmt.Printf("\n最高吞吐量: %s (%s, %.0f QPS)\n", bestQPS.Params.Name, bestQPS.Scenario.Name, bestQPS.OpsPerSec)
		fmt.Printf("最低延迟:   %s (%s, %.3fms)\n", bestLatency.Params.Name, bestLatency.Scenario.Name, bestLatency.AvgLatencyMs)
	}

	if len(failedTests) > 0 {
		fmt.Printf("\n⚠️  %d 个测试失败:\n", len(failedTests))
		for _, t := range failedTests {
			fmt.Printf("  - %s\n", t)
		}
	}
}
