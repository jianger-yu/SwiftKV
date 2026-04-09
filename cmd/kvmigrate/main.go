package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"kvraft/pkg/sharding"
)

// kvmigrate 使用方法（中文速览）:
//
// 1) 最常用（显式指定源/目标配置）
//    go run ./cmd/kvmigrate \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json
//
// 2) 只看迁移计划，不执行写入（推荐先 dry-run）
//    go run ./cmd/kvmigrate \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json \
//      -dry-run
//
// 3) 仅迁移某个前缀，并限制条数
//    go run ./cmd/kvmigrate \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json \
//      -prefix user: -limit 1000
//
// 4) 迁移完成后删除源数据（谨慎使用）
//    go run ./cmd/kvmigrate \
//      -source-config ./data/cluster/sharding.json \
//      -target-config ./data/cluster/sharding-next.json \
//      -delete-source
//
// 5) 自动读取运行时配置（无需参数）
//    直接执行: go run ./cmd/kvmigrate
//    将优先读取 data/cluster/runtime.env 里的:
//      SHARDING_CONFIG      (source-config)
//      SHARDING_NEXT_CONFIG (target-config)

type jsonGroup struct {
	GroupID   int      `json:"group_id"`
	Replicas  []string `json:"replicas"`
	LeaderIdx int      `json:"leader_idx"`
}

type jsonShardingConfig struct {
	Groups            []jsonGroup `json:"groups"`
	VirtualNodeCount  int         `json:"virtual_node_count"`
	ConnectTimeoutMS  int         `json:"connect_timeout_ms"`
	RequestTimeoutMS  int         `json:"request_timeout_ms"`
	PreferredReplicas int         `json:"preferred_replicas"`
}

func loadRuntimeMetadata() map[string]string {
	meta := map[string]string{}
	path := filepath.Join("data", "cluster", "runtime.env")
	raw, err := os.ReadFile(path)
	if err != nil {
		return meta
	}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		meta[strings.TrimSpace(line[:idx])] = strings.TrimSpace(line[idx+1:])
	}
	return meta
}

func loadConfig(path string) (sharding.ShardingConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return sharding.ShardingConfig{}, err
	}

	var jc jsonShardingConfig
	if err := json.Unmarshal(content, &jc); err != nil {
		return sharding.ShardingConfig{}, err
	}

	cfg := sharding.ShardingConfig{
		VirtualNodeCount:  jc.VirtualNodeCount,
		PreferredReplicas: jc.PreferredReplicas,
	}
	if jc.ConnectTimeoutMS > 0 {
		cfg.ConnectTimeout = time.Duration(jc.ConnectTimeoutMS) * time.Millisecond
	}
	if jc.RequestTimeoutMS > 0 {
		cfg.RequestTimeout = time.Duration(jc.RequestTimeoutMS) * time.Millisecond
	}

	cfg.Groups = make([]sharding.RaftGroupConfig, 0, len(jc.Groups))
	for _, g := range jc.Groups {
		cfg.Groups = append(cfg.Groups, sharding.RaftGroupConfig{
			GroupID:   g.GroupID,
			Replicas:  append([]string(nil), g.Replicas...),
			LeaderIdx: g.LeaderIdx,
		})
	}
	return cfg, nil
}

func main() {
	sourceCfgPath := flag.String("source-config", "", "source sharding config json path")
	targetCfgPath := flag.String("target-config", "", "target sharding config json path")
	prefix := flag.String("prefix", "", "migrate keys with this prefix only")
	limit := flag.Int("limit", 0, "max number of keys in migration plan (0 means no limit)")
	dryRun := flag.Bool("dry-run", false, "only build and print migration plan")
	deleteSource := flag.Bool("delete-source", false, "delete source keys after successful copy")
	timeoutSec := flag.Int("timeout-sec", 30, "migration timeout in seconds")

	flag.Parse()
	meta := loadRuntimeMetadata()

	if strings.TrimSpace(*sourceCfgPath) == "" {
		if p := strings.TrimSpace(meta["SHARDING_CONFIG"]); p != "" {
			*sourceCfgPath = p
		}
	}
	if strings.TrimSpace(*targetCfgPath) == "" {
		if p := strings.TrimSpace(meta["SHARDING_NEXT_CONFIG"]); p != "" {
			*targetCfgPath = p
		}
	}
	if strings.TrimSpace(*targetCfgPath) == "" && strings.TrimSpace(*sourceCfgPath) != "" {
		*targetCfgPath = *sourceCfgPath
	}

	if *sourceCfgPath == "" || *targetCfgPath == "" {
		fmt.Println("usage:")
		fmt.Println("  go run ./cmd/kvmigrate -source-config ./source.json -target-config ./target.json [-prefix p] [-limit n] [--dry-run] [--delete-source]")
		fmt.Println("hint:")
		fmt.Println("  直接执行时会优先读取 data/cluster/runtime.env 中的 SHARDING_CONFIG / SHARDING_NEXT_CONFIG")
		os.Exit(2)
	}

	sourceCfg, err := loadConfig(*sourceCfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load source config failed: %v\n", err)
		os.Exit(1)
	}
	targetCfg, err := loadConfig(*targetCfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load target config failed: %v\n", err)
		os.Exit(1)
	}

	sourceRouter, err := sharding.NewShardRouter(sourceCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create source router failed: %v\n", err)
		os.Exit(1)
	}
	defer sourceRouter.Close()

	targetRouter, err := sharding.NewShardRouter(targetCfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create target router failed: %v\n", err)
		os.Exit(1)
	}
	defer targetRouter.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	migrator := sharding.NewMigrator(sourceRouter, targetRouter)
	plan, err := migrator.BuildPlan(ctx, *prefix, *limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "build migration plan failed: %v\n", err)
		os.Exit(1)
	}

	if *sourceCfgPath == *targetCfgPath {
		fmt.Println("提示: source 和 target 配置相同，本次迁移计划通常为空（用于连通性与流程验证）。")
	}

	fmt.Printf("plan size: %d\n", len(plan))
	for i, item := range plan {
		if i >= 20 {
			fmt.Printf("... (%d more)\n", len(plan)-20)
			break
		}
		fmt.Printf("[%03d] key=%s src=%d dst=%d version=%d\n", i+1, item.Key, item.SourceGroup, item.TargetGroup, item.Version)
	}

	if *dryRun {
		fmt.Println("dry-run mode: no data changed")
		return
	}

	stats, execErr := migrator.ExecutePlan(ctx, plan, *deleteSource)
	fmt.Printf("migration stats: %+v\n", stats)
	if execErr != nil {
		fmt.Fprintf(os.Stderr, "migration finished with error: %v\n", execErr)
		os.Exit(1)
	}

	fmt.Println("migration done")
}
