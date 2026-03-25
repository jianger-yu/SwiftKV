package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"kvraft/sharding"
)

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

	if *sourceCfgPath == "" || *targetCfgPath == "" {
		fmt.Println("usage:")
		fmt.Println("  go run ./cmd/kvmigrate -source-config ./source.json -target-config ./target.json [-prefix p] [-limit n] [--dry-run] [--delete-source]")
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
