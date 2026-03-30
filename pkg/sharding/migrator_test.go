package sharding

import (
	"context"
	"testing"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
)

func TestMigratorExecutePlanMoveAndDelete(t *testing.T) {
	sourceSvc := &testKVService{kv: map[string]*pb.KeyValue{}}
	sourceAddr, stopSource := startTestKVServer(t, sourceSvc)
	defer stopSource()

	targetSvc := &testKVService{kv: map[string]*pb.KeyValue{}}
	targetAddr, stopTarget := startTestKVServer(t, targetSvc)
	defer stopTarget()

	sourceRouter, err := NewShardRouter(ShardingConfig{
		Groups:           []RaftGroupConfig{{GroupID: 1, Replicas: []string{sourceAddr}, LeaderIdx: 0}},
		VirtualNodeCount: 32,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new source router failed: %v", err)
	}
	defer sourceRouter.Close()

	targetRouter, err := NewShardRouter(ShardingConfig{
		Groups:           []RaftGroupConfig{{GroupID: 2, Replicas: []string{targetAddr}, LeaderIdx: 0}},
		VirtualNodeCount: 32,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new target router failed: %v", err)
	}
	defer targetRouter.Close()

	ctx := context.Background()
	if _, err := sourceRouter.PutToGroup(ctx, 1, "migrate-key", "migrate-value", 0); err != nil {
		t.Fatalf("seed source key failed: %v", err)
	}

	migrator := NewMigrator(sourceRouter, targetRouter)
	plan, err := migrator.BuildPlan(ctx, "migrate-", 0)
	if err != nil {
		t.Fatalf("build plan failed: %v", err)
	}
	if len(plan) != 1 {
		t.Fatalf("plan size mismatch, want=1 got=%d", len(plan))
	}

	stats, err := migrator.ExecutePlan(ctx, plan, true)
	if err != nil {
		t.Fatalf("execute plan failed: %v (stats=%+v)", err, stats)
	}
	if stats.Migrated != 1 || stats.DeletedFromSrc != 1 || stats.Failed != 0 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	srcGet, err := sourceRouter.GetFromGroup(ctx, 1, "migrate-key")
	if err != nil {
		t.Fatalf("verify source failed: %v", err)
	}
	if srcGet.GetError() != "ErrNoKey" {
		t.Fatalf("source still has key after migration: %+v", srcGet)
	}

	dstGet, err := targetRouter.GetFromGroup(ctx, 2, "migrate-key")
	if err != nil {
		t.Fatalf("verify target failed: %v", err)
	}
	if dstGet.GetError() != "OK" || dstGet.GetValue() != "migrate-value" {
		t.Fatalf("target key mismatch after migration: %+v", dstGet)
	}
}

func TestMigratorRollbackOnSourceDeleteFailure(t *testing.T) {
	sourceSvc := &testKVService{kv: map[string]*pb.KeyValue{}, failDelete: true}
	sourceAddr, stopSource := startTestKVServer(t, sourceSvc)
	defer stopSource()

	targetSvc := &testKVService{kv: map[string]*pb.KeyValue{}}
	targetAddr, stopTarget := startTestKVServer(t, targetSvc)
	defer stopTarget()

	sourceRouter, err := NewShardRouter(ShardingConfig{
		Groups:           []RaftGroupConfig{{GroupID: 1, Replicas: []string{sourceAddr}, LeaderIdx: 0}},
		VirtualNodeCount: 32,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new source router failed: %v", err)
	}
	defer sourceRouter.Close()

	targetRouter, err := NewShardRouter(ShardingConfig{
		Groups:           []RaftGroupConfig{{GroupID: 2, Replicas: []string{targetAddr}, LeaderIdx: 0}},
		VirtualNodeCount: 32,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new target router failed: %v", err)
	}
	defer targetRouter.Close()

	ctx := context.Background()
	if _, err := sourceRouter.PutToGroup(ctx, 1, "rollback-key", "rollback-value", 0); err != nil {
		t.Fatalf("seed source key failed: %v", err)
	}

	migrator := NewMigrator(sourceRouter, targetRouter)
	plan, err := migrator.BuildPlan(ctx, "rollback-", 0)
	if err != nil {
		t.Fatalf("build plan failed: %v", err)
	}
	if len(plan) != 1 {
		t.Fatalf("plan size mismatch, want=1 got=%d", len(plan))
	}

	stats, err := migrator.ExecutePlan(ctx, plan, true)
	if err == nil {
		t.Fatalf("expected migration error on delete failure, got nil (stats=%+v)", stats)
	}
	if stats.Failed == 0 {
		t.Fatalf("expected failed count > 0, stats=%+v", stats)
	}

	srcGet, err := sourceRouter.GetFromGroup(ctx, 1, "rollback-key")
	if err != nil {
		t.Fatalf("verify source failed: %v", err)
	}
	if srcGet.GetError() != "OK" || srcGet.GetValue() != "rollback-value" {
		t.Fatalf("source should remain intact after rollback: %+v", srcGet)
	}

	dstGet, err := targetRouter.GetFromGroup(ctx, 2, "rollback-key")
	if err != nil {
		t.Fatalf("verify target failed: %v", err)
	}
	if dstGet.GetError() != "ErrNoKey" {
		t.Fatalf("target should be rolled back to empty, got: %+v", dstGet)
	}
}
