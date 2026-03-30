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

func migrationGetAccepted(resp *pb.GetResponse) bool {
	if resp == nil {
		return false
	}
	errText := resp.GetError()
	return errText == "OK" || errText == "ErrNoKey"
}

func migrationValueEquals(resp *pb.GetResponse, item MigrationPlanItem) bool {
	if resp == nil || resp.GetError() != "OK" {
		return false
	}
	return resp.GetValue() == item.Value
}

func (m *Migrator) fetchGroupValue(ctx context.Context, router *ShardRouter, gid int, key string) (*pb.GetResponse, error) {
	getCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	resp, err := router.GetFromGroup(getCtx, gid, key)
	if err != nil {
		return nil, err
	}
	if !migrationGetAccepted(resp) {
		return nil, fmt.Errorf("unexpected get response: %v", resp.GetError())
	}
	return resp, nil
}

func (m *Migrator) rollbackTarget(ctx context.Context, item MigrationPlanItem, before *pb.GetResponse) error {
	if before == nil {
		return nil
	}

	if before.GetError() == "ErrNoKey" {
		delCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer cancel()
		delResp, delErr := m.target.DeleteFromGroup(delCtx, item.TargetGroup, item.Key)
		if delErr != nil {
			return delErr
		}
		if !migrationDeleteAccepted(delResp) {
			return fmt.Errorf("rollback delete target failed: %s", delResp.GetError())
		}
		return nil
	}

	current, err := m.fetchGroupValue(ctx, m.target, item.TargetGroup, item.Key)
	if err != nil {
		return err
	}
	if current.GetError() != "OK" {
		return fmt.Errorf("rollback get current target failed: %s", current.GetError())
	}

	putCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()
	putResp, putErr := m.target.PutToGroup(putCtx, item.TargetGroup, item.Key, before.GetValue(), current.GetVersion())
	if putErr != nil {
		return putErr
	}
	if !migrationPutAccepted(putResp) {
		return fmt.Errorf("rollback restore target failed: %s", putResp.GetError())
	}
	return nil
}

func (m *Migrator) deleteSourceWithVerify(ctx context.Context, item MigrationPlanItem) error {
	delCtx, cancelDel := context.WithTimeout(ctx, 1500*time.Millisecond)
	delResp, delErr := m.source.DeleteFromGroup(delCtx, item.SourceGroup, item.Key)
	cancelDel()
	if delErr != nil {
		return delErr
	}
	if !migrationDeleteAccepted(delResp) {
		return fmt.Errorf("delete source failed: %s", delResp.GetError())
	}

	srcAfter, err := m.fetchGroupValue(ctx, m.source, item.SourceGroup, item.Key)
	if err != nil {
		return err
	}
	if srcAfter.GetError() != "ErrNoKey" {
		return fmt.Errorf("source key still exists after delete")
	}
	return nil
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

		sourceBefore, srcErr := m.fetchGroupValue(ctx, m.source, item.SourceGroup, item.Key)
		if srcErr != nil {
			stats.Failed++
			stats.LastErrorMessage = srcErr.Error()
			continue
		}
		if sourceBefore.GetError() == "ErrNoKey" {
			stats.Skipped++
			continue
		}
		if sourceBefore.GetValue() != item.Value {
			stats.Failed++
			stats.LastErrorMessage = fmt.Sprintf("source value changed for key %s, plan stale", item.Key)
			continue
		}

		targetBefore, getErr := m.fetchGroupValue(ctx, m.target, item.TargetGroup, item.Key)
		if getErr != nil {
			stats.Failed++
			stats.LastErrorMessage = getErr.Error()
			continue
		}

		if migrationValueEquals(targetBefore, item) {
			if deleteSource {
				if err := m.deleteSourceWithVerify(ctx, item); err != nil {
					stats.Failed++
					stats.LastErrorMessage = err.Error()
					continue
				}
				stats.DeletedFromSrc++
				stats.Migrated++
				continue
			}
			stats.Skipped++
			continue
		}

		putVersion := migrationPutVersion(targetBefore)
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

		targetAfter, verifyErr := m.fetchGroupValue(ctx, m.target, item.TargetGroup, item.Key)
		if verifyErr != nil || !migrationValueEquals(targetAfter, item) {
			_ = m.rollbackTarget(ctx, item, targetBefore)
			stats.Failed++
			if verifyErr != nil {
				stats.LastErrorMessage = verifyErr.Error()
			} else {
				stats.LastErrorMessage = "target verify failed after put"
			}
			continue
		}

		stats.Migrated++
		if !deleteSource {
			continue
		}

		if err := m.deleteSourceWithVerify(ctx, item); err != nil {
			rollbackErr := m.rollbackTarget(ctx, item, targetBefore)
			stats.Failed++
			if rollbackErr != nil {
				stats.LastErrorMessage = fmt.Sprintf("%v; rollback failed: %v", err, rollbackErr)
			} else {
				stats.LastErrorMessage = fmt.Sprintf("%v; rollback applied", err)
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
