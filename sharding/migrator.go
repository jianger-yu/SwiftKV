package sharding

import (
	"context"
	"fmt"
	"sort"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
)

// MigrationPlanItem 描述单个 key 的迁移计划。
type MigrationPlanItem struct {
	Key         string
	Value       string
	Version     int64
	SourceGroup int
	TargetGroup int
}

// MigrationStats 描述一次迁移执行结果。
type MigrationStats struct {
	Planned          int
	Migrated         int
	Skipped          int
	DeletedFromSrc   int
	Failed           int
	LastErrorMessage string
}

// Migrator 用于在不同分片拓扑之间迁移数据。
type Migrator struct {
	source *ShardRouter
	target *ShardRouter
}

// NewMigrator 创建迁移器。
func NewMigrator(source *ShardRouter, target *ShardRouter) *Migrator {
	return &Migrator{source: source, target: target}
}

// BuildPlan 扫描 source 中的数据，并计算目标拓扑下需要迁移的 key。
func (m *Migrator) BuildPlan(ctx context.Context, prefix string, limit int) ([]MigrationPlanItem, error) {
	if m == nil || m.source == nil || m.target == nil {
		return nil, fmt.Errorf("migrator/source/target is nil")
	}

	plan := make([]MigrationPlanItem, 0)
	for _, sourceGID := range m.source.GroupIDs() {
		items, err := m.source.ScanGroup(ctx, sourceGID, prefix, 0)
		if err != nil {
			return nil, err
		}

		for _, item := range items {
			if item == nil {
				continue
			}

			targetGID := m.target.Resolve(item.GetKey())
			if targetGID < 0 {
				return nil, fmt.Errorf("target router has no group for key %q", item.GetKey())
			}

			if targetGID == sourceGID {
				continue
			}

			plan = append(plan, MigrationPlanItem{
				Key:         item.GetKey(),
				Value:       item.GetValue(),
				Version:     item.GetVersion(),
				SourceGroup: sourceGID,
				TargetGroup: targetGID,
			})
			if limit > 0 && len(plan) >= limit {
				sort.Slice(plan, func(i, j int) bool {
					if plan[i].SourceGroup == plan[j].SourceGroup {
						return plan[i].Key < plan[j].Key
					}
					return plan[i].SourceGroup < plan[j].SourceGroup
				})
				return plan, nil
			}
		}
	}

	sort.Slice(plan, func(i, j int) bool {
		if plan[i].SourceGroup == plan[j].SourceGroup {
			return plan[i].Key < plan[j].Key
		}
		return plan[i].SourceGroup < plan[j].SourceGroup
	})
	return plan, nil
}

func migrationPutVersion(getResp *pb.GetResponse) int64 {
	if getResp == nil {
		return 0
	}
	if getResp.GetError() == "ErrNoKey" {
		return 0
	}
	if getResp.GetError() == "OK" {
		return getResp.GetVersion()
	}
	return 0
}

func migrationPutAccepted(resp *pb.PutResponse) bool {
	if resp == nil {
		return false
	}
	return resp.GetError() == "OK"
}

func migrationDeleteAccepted(resp *pb.DeleteResponse) bool {
	if resp == nil {
		return false
	}
	return resp.GetError() == "OK" || resp.GetError() == "ErrNoKey"
}

// ExecutePlan 执行迁移计划。
// deleteSource=true 时，目标写入成功后删除源数据。
func (m *Migrator) ExecutePlan(ctx context.Context, plan []MigrationPlanItem, deleteSource bool) (MigrationStats, error) {
	stats := MigrationStats{Planned: len(plan)}
	if len(plan) == 0 {
		return stats, nil
	}

	if m == nil || m.source == nil || m.target == nil {
		return stats, fmt.Errorf("migrator/source/target is nil")
	}

	for _, item := range plan {
		if ctx.Err() != nil {
			stats.LastErrorMessage = ctx.Err().Error()
			return stats, ctx.Err()
		}

		getCtx, cancelGet := context.WithTimeout(ctx, 1500*time.Millisecond)
		targetCurrent, getErr := m.target.GetFromGroup(getCtx, item.TargetGroup, item.Key)
		cancelGet()
		if getErr != nil {
			stats.Failed++
			stats.LastErrorMessage = getErr.Error()
			continue
		}

		if targetCurrent != nil && targetCurrent.GetError() == "OK" && targetCurrent.GetValue() == item.Value {
			stats.Skipped++
			continue
		}

		putVersion := migrationPutVersion(targetCurrent)
		putCtx, cancelPut := context.WithTimeout(ctx, 1500*time.Millisecond)
		putResp, putErr := m.target.PutToGroup(putCtx, item.TargetGroup, item.Key, item.Value, putVersion)
		cancelPut()
		if putErr != nil || !migrationPutAccepted(putResp) {
			stats.Failed++
			if putErr != nil {
				stats.LastErrorMessage = putErr.Error()
			} else if putResp != nil {
				stats.LastErrorMessage = putResp.GetError()
			}
			continue
		}

		stats.Migrated++
		if !deleteSource {
			continue
		}

		delCtx, cancelDel := context.WithTimeout(ctx, 1500*time.Millisecond)
		delResp, delErr := m.source.DeleteFromGroup(delCtx, item.SourceGroup, item.Key)
		cancelDel()
		if delErr != nil || !migrationDeleteAccepted(delResp) {
			stats.Failed++
			if delErr != nil {
				stats.LastErrorMessage = delErr.Error()
			} else if delResp != nil {
				stats.LastErrorMessage = delResp.GetError()
			}
			continue
		}
		stats.DeletedFromSrc++
	}

	if stats.Failed > 0 {
		return stats, fmt.Errorf("migration finished with %d failures", stats.Failed)
	}
	return stats, nil
}
