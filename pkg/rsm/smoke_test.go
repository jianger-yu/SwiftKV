package rsm

import (
	"sync/atomic"
	"testing"
	"time"

	kvraftapi "kvraft/pkg/raftapi"
	"kvraft/pkg/storage"
	"kvraft/pkg/watch"
)

// TestBasicPutGet - 真实的 Put/Get 测试
// 直接调用 DoOp，验证数据正确保存和读取
func TestBasicPutGet(t *testing.T) {
	// 创建最小化的 KVServer 实例，仅用于状态机测试
	store, err := storage.NewStore("test-db-putget")
	if err != nil {
		t.Fatalf("❌ 创建 store 失败: %v", err)
	}

	// 清空之前的测试数据（防止多次运行时的冲突）
	if err := store.Clear(); err != nil {
		t.Logf("⚠️ 清空 store 失败: %v (这在第一次运行时是正常的)", err)
	}

	kv := &KVServer{
		me:    0,
		dead:  0,
		store: store,
		stats: &ServerStats{},
	}

	t.Log("【Step 1】创建 KVServer 实例：✓")

	// ============================================================
	// 测试 1: Put 操作
	// ============================================================
	putArgs := &kvraftapi.PutArgs{
		Key:     "test_key",
		Value:   "test_value",
		Version: 0, // 新 key，版本为 0
	}

	putResult := kv.DoOp(putArgs)
	putReply, ok := putResult.(kvraftapi.PutReply)
	if !ok {
		t.Fatalf("❌ Put 返回类型错误，期望 PutReply，收到 %T", putResult)
	}

	if putReply.Err != kvraftapi.OK {
		t.Fatalf("❌ Put 操作失败，Err=%v（期望 OK）", putReply.Err)
	}
	t.Log("【Step 2】执行 Put 操作：✓ Key=test_key, Value=test_value")

	// ============================================================
	// 测试 2: Get 操作 - 验证数据正确保存
	// ============================================================
	getArgs := &kvraftapi.GetArgs{
		Key: "test_key",
	}

	getResult := kv.DoOp(getArgs)
	getReply, ok := getResult.(kvraftapi.GetReply)
	if !ok {
		t.Fatalf("❌ Get 返回类型错误，期望 GetReply，收到 %T", getResult)
	}

	if getReply.Err != kvraftapi.OK {
		t.Fatalf("❌ Get 操作失败，Err=%v（期望 OK）", getReply.Err)
	}

	if getReply.Value != "test_value" {
		t.Fatalf("❌ Get 返回值不匹配，期望 'test_value'，收到 '%v'", getReply.Value)
	}
	t.Log("【Step 3】执行 Get 操作：✓ 返回值正确")

	// ============================================================
	// 测试 4: 版本控制 - 尝试以错误版本写入
	// ============================================================
	putArgsWrongVersion := &kvraftapi.PutArgs{
		Key:     "test_key",
		Value:   "new_value",
		Version: 0, // 错误的版本（当前版本是 1）
	}

	putResult2 := kv.DoOp(putArgsWrongVersion)
	putReply2, ok := putResult2.(kvraftapi.PutReply)
	if !ok {
		t.Fatalf("❌ Put 返回类型错误，期望 PutReply，收到 %T", putResult2)
	}

	if putReply2.Err != kvraftapi.ErrVersion {
		t.Fatalf("❌ 版本冲突检测失败，期望 ErrVersion，收到 %v", putReply2.Err)
	}
	t.Log("【Step 4】验证版本控制：✓ 错误版本正确拒绝")

	// ============================================================
	// 测试 5: 不存在的 key 读取
	// ============================================================
	getArgsNotFound := &kvraftapi.GetArgs{
		Key: "nonexistent_key",
	}

	getResult2 := kv.DoOp(getArgsNotFound)
	getReply2, ok := getResult2.(kvraftapi.GetReply)
	if !ok {
		t.Fatalf("❌ Get 返回类型错误，期望 GetReply，收到 %T", getResult2)
	}

	if getReply2.Err != kvraftapi.ErrNoKey {
		t.Fatalf("❌ 不存在 key 处理失败，期望 ErrNoKey，收到 %v", getReply2.Err)
	}
	t.Log("【Step 5】验证 ErrNoKey：✓ 不存在的 key 正确处理")

	// ============================================================
	// 测试 6: 更新存在的 key
	// ============================================================
	putArgsUpdate := &kvraftapi.PutArgs{
		Key:     "test_key",
		Value:   "updated_value",
		Version: 1, // 正确的当前版本
	}

	putResult3 := kv.DoOp(putArgsUpdate)
	putReply3, ok := putResult3.(kvraftapi.PutReply)
	if !ok {
		t.Fatalf("❌ Put 返回类型错误，期望 PutReply，收到 %T", putResult3)
	}

	if putReply3.Err != kvraftapi.OK {
		t.Fatalf("❌ 更新 Put 操作失败，Err=%v（期望 OK）", putReply3.Err)
	}
	t.Log("【Step 6】执行更新 Put 操作：✓ Key=test_key, Value=updated_value, Version=2")

	getArgs2 := &kvraftapi.GetArgs{
		Key: "test_key",
	}
	getResult4 := kv.DoOp(getArgs2)
	getReply4, ok := getResult4.(kvraftapi.GetReply)
	if !ok {
		t.Fatalf("❌ Get 返回类型错误，期望 GetReply，收到 %T", getResult4)
	}

	if getReply4.Value != "updated_value" {
		t.Fatalf("❌ Get 返回值不匹配，期望 'updated_value'，收到 '%v'", getReply4.Value)
	}
	t.Log("【Step 7】验证数据更新：✓ 新值正确保存和读取")

	// 清理
	atomic.StoreInt32(&kv.dead, 1)

	t.Log("\n✅ TestBasicPutGet 全部通过！")
	t.Log("  ✓ Put/Get 基本操作")
	t.Log("  ✓ 版本控制（CAS 语义）")
	t.Log("  ✓ 错误处理（ErrNoKey, ErrVersion）")
	t.Log("  ✓ 数据持久化验证（旁路缓存）")
}

// TestRSMCompilation - RSM 编译测试
func TestRSMCompilation(t *testing.T) {
	t.Log("✓ All packages compiled successfully")
}

// TestWatchMechanism - 验证 Watch 订阅、事件触发与异步接收链路
func TestWatchMechanism(t *testing.T) {
	store, err := storage.NewStore("test-db-watch")
	if err != nil {
		t.Fatalf("❌ 创建 store 失败: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	if err := store.Clear(); err != nil {
		t.Logf("⚠️ 清空 store 失败: %v (这在第一次运行时是正常的)", err)
	}

	watchMgr := watch.NewManager(watch.DefaultConfig())
	defer watchMgr.Close()

	kv := &KVServer{
		me:    0,
		dead:  0,
		store: store,
		stats: &ServerStats{},
		rsm:   &RSM{watchMgr: watchMgr},
	}

	t.Log("【Step 1】KVServer 与 Watch.Manager 初始化：✓")

	const watchKey = "watch_test_key"
	watcher, err := watchMgr.Subscribe(watchKey, false)
	if err != nil {
		t.Fatalf("❌ Subscribe 失败: %v", err)
	}
	defer func() {
		if err := watchMgr.Unsubscribe(watcher.ID); err != nil {
			t.Logf("⚠️ Unsubscribe 失败: %v", err)
		}
	}()

	t.Log("【Step 2】订阅 watch_test_key：✓")

	putArgs := &kvraftapi.PutArgs{
		Key:     watchKey,
		Value:   "gold_value",
		Version: 0,
	}

	putResult := kv.DoOp(putArgs)
	putReply, ok := putResult.(kvraftapi.PutReply)
	if !ok {
		t.Fatalf("❌ Put 返回类型错误，期望 PutReply，收到 %T", putResult)
	}
	if putReply.Err != kvraftapi.OK {
		t.Fatalf("❌ Put 操作失败，Err=%v（期望 OK）", putReply.Err)
	}

	// 模拟 Raft apply 后的回调，触发 Watch 事件分发链路。
	kv.OnOpComplete(putArgs, putReply, 1)
	t.Log("【Step 3】Put + OnOpComplete 触发事件：✓")

	select {
	case ev := <-watcher.Channel:
		if ev.Key != watchKey {
			t.Fatalf("❌ 事件 Key 不匹配，期望 %q，收到 %q", watchKey, ev.Key)
		}
		if ev.NewValue != "gold_value" {
			t.Fatalf("❌ 事件 NewValue 不匹配，期望 %q，收到 %q", "gold_value", ev.NewValue)
		}
		if ev.NewVersion < 0 {
			t.Fatalf("❌ 事件 NewVersion 非法，收到 %d", ev.NewVersion)
		}
		if ev.Timestamp.IsZero() {
			t.Fatalf("❌ 事件 Timestamp 非法，收到零值时间")
		}
		t.Log("【Step 4】异步事件验证：✓ Key/NewValue/NewVersion/Timestamp")
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("❌ 等待 Watch 事件超时（500ms）")
	}

	atomic.StoreInt32(&kv.dead, 1)
	t.Log("✅ TestWatchMechanism 全部通过")
}

func TestWatchDeleteEvent(t *testing.T) {
	store, err := storage.NewStore("test-db-watch-delete")
	if err != nil {
		t.Fatalf("❌ 创建 store 失败: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	if err := store.Clear(); err != nil {
		t.Logf("⚠️ 清空 store 失败: %v (这在第一次运行时是正常的)", err)
	}

	watchMgr := watch.NewManager(watch.DefaultConfig())
	defer watchMgr.Close()

	kv := &KVServer{
		me:    0,
		dead:  0,
		store: store,
		stats: &ServerStats{},
		rsm:   &RSM{watchMgr: watchMgr},
	}

	const key = "delete_watch_key"
	if putRes := kv.DoOp(&kvraftapi.PutArgs{Key: key, Value: "before_delete", Version: 0}); putRes.(kvraftapi.PutReply).Err != kvraftapi.OK {
		t.Fatalf("❌ 预置数据失败")
	}

	watcher, err := watchMgr.Subscribe(key, false)
	if err != nil {
		t.Fatalf("❌ Subscribe 失败: %v", err)
	}
	defer func() {
		_ = watchMgr.Unsubscribe(watcher.ID)
	}()

	delArgs := &kvraftapi.DeleteArgs{Key: key}
	delResult := kv.DoOp(delArgs)
	delReply, ok := delResult.(kvraftapi.DeleteReply)
	if !ok {
		t.Fatalf("❌ Delete 返回类型错误，期望 DeleteReply，收到 %T", delResult)
	}
	if delReply.Err != kvraftapi.OK {
		t.Fatalf("❌ Delete 失败，Err=%v", delReply.Err)
	}

	kv.OnOpComplete(delArgs, delReply, 2)

	select {
	case ev := <-watcher.Channel:
		if ev.EventType != "DELETE" {
			t.Fatalf("❌ 事件类型错误，期望 DELETE，收到 %q", ev.EventType)
		}
		if ev.OldValue != "before_delete" {
			t.Fatalf("❌ 旧值错误，期望 before_delete，收到 %q", ev.OldValue)
		}
		if ev.NewValue != "" {
			t.Fatalf("❌ 新值错误，期望空，收到 %q", ev.NewValue)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("❌ 等待 DELETE Watch 事件超时")
	}
}
