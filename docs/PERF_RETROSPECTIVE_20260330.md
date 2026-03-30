# SwiftKV/KVraft 性能复盘（2026-03-30）

## 1. 本次改动里，什么让性能明显提升？

基于你的两组结果：

- `maxraftstate=1048576` 时：`576.41 ops/s`，平均 `17.28 ms`
- `maxraftstate=-1` 时：`436.89 ops/s`，平均 `22.55 ms`

核心收益来源（按贡献排序）：

1. 开启快照阈值，抑制 Raft 日志无限增长
- 改动点：`benchmarks/benchmark.go` 增加 `--maxraftstate` 并传给 `StartKVServer(...)`。
- 原因：当 `maxraftstate=-1` 时，日志持续增长；Raft 每次 persist 都需要编码更大的 log，成本随时间上升。
- 这次 A/B 数据里这是最显著收益来源（约 +31.9% 吞吐）。

2. Raft 常规 persist 不再每次附带 snapshot
- 改动点：`raft/raft.go` 的 `persist()` 从 `Save(state, ReadSnapshot())` 改为 `Save(state, nil)`。
- 原因：避免每条日志追加都触碰 snapshot 文件，减少额外 I/O。

3. 状态机写路径从“双事务”改为“单事务 CAS”
- 改动点：`storage/store.go` 新增 `PutCASWithTTL`，`rsm/server.go` 的 `doPut` 改为调用单事务 CAS。
- 原因：原流程是 `GetWithMeta + PutWithTTL` 两次 Badger 事务；现在合并为一次事务，减少 I/O 与锁争。

4. 去掉 `DoOp` 全局锁串行化
- 改动点：`rsm/server.go` 的 `DoOp` 去掉 `kv.mu.Lock()`。
- 原因：之前把请求处理人为串行化，拉高排队时延；去掉后只依赖存储层并发控制。

5. 客户端单次 RPC 超时调短（可配置）
- 改动点：`rsm/client.go` 增加 `KV_RPC_TIMEOUT_MS`，默认 250~300ms。
- 原因：错误路由/短抖动下，避免单次 1200ms 的长等待拖尾延迟。

## 2. 这些改动对架构有什么影响？“高顺序性”等亮点是否被破坏？

结论：核心架构没被改坏，写路径顺序性仍由 Raft 保证。

未改变的关键架构约束：

1. 写入仍走 Raft 共识
- Put/Delete/Expire 仍通过 `rsm.Submit -> raft.Start -> applyLoop -> DoOp`。
- 因此写入的全序与多数派提交语义仍在。

2. 状态机应用顺序仍由 `applyLoop` 串行驱动
- Raft 提交后的命令按日志索引应用，顺序一致性机制未被删除。

3. 快照机制仍存在
- 只是通过 `maxraftstate` 控制是否触发，架构上未移除。

需要明确的边界：

- 去掉 `DoOp` 全局锁后，并不是“无序写入”，因为 Raft apply 本身是串行。
- 但 Lease Read 是绕过共识的本地读路径，线性化依赖 lease 正确性（这本来就存在，不是这次新引入）。

## 3. 现在客户端一次请求的流程是什么？

### 3.1 Get（优先走 Lease Read）

1. `Clerk.Get` 选择候选节点并发起 gRPC `Get`。
2. 服务端 `grpc_service.Get` 调用 `rsm.SubmitLeaseRead(GetArgs)`。
3. 若 leader 且 lease 有效：直接 `sm.DoOp(Get)` 本地读返回。
4. 若 lease 无效：回退到 `Submit`（即 Raft 路径）。

### 3.2 Put

1. `Clerk.Put` 发起 gRPC `Put`。
2. 服务端 `grpc_service.Put` 调用 `rsm.Submit(PutArgs)`。
3. Raft 提交后 `applyLoop` 执行 `KVServer.doPut`。
4. `doPut` 调用 `store.PutCASWithTTL`，在一个事务内完成版本校验+写入。
5. 返回 OK/ErrVersion/ErrNoKey。

### 3.3 TTL 清理

1. leader 周期性扫描过期键（`ttlCleanupLoop`）。
2. 生成 `ExpireArgs` 走 `rsm.Submit`（经 Raft 复制）。
3. 各节点按日志一致应用删除。

## 4. TTL 相关改动是否带来隐患？

有两个要重点关注：

1. 时间相关逻辑的确定性风险（理论风险）
- 状态机执行里使用了 `time.Now()` 判断过期与计算过期时间。
- 在严格意义上，Raft 状态机最好避免依赖本地时钟，以免节点间边界时刻判断差异。
- 当前实现在工程上通常可跑，但这是中长期一致性风险点。

2. Lease Read 与 TTL 边界
- Lease Read 是本地读，若临界时刻 lease/时钟边界不理想，可能出现短暂读值不一致窗口。
- 建议后续将 TTL 判定尽量基于写入时固定的绝对时间字段，并减少运行时本地时钟分支。

结论：本次优化没有新增 TTL 架构模块，但暴露了原本就存在的“时钟依赖”风险，建议后续专项治理。

## 5. 哈希分片路由是否有较大性能开销？要不要去掉？

结论：在当前单 Raft 组下，哈希路由不是主瓶颈，不建议直接删架构。

原因：

1. 你当前性能瓶颈主要在共识与持久化路径
- 证据是 `maxraftstate` 开关带来 30%+ 变化；而哈希计算本身是微秒级。

2. 现有路由逻辑已优先最近 leader
- `preferredCandidates` 会把最近成功 leader 放前面，已降低 WrongLeader 往返。

3. 删除分片架构会损失未来扩展能力
- 当前虽然是单组，但分片是未来横向扩展基础。

建议：

- 不删分片架构。
- 可做“单组快速路径开关”：单组时直接 leader-first，少做一层候选构造；多组时仍走哈希。

## 6. 指标解读注意事项（你这次输出中的一个现象）

你的输出里 `server[x].read=0` 并不代表没有读请求，主要是统计口径问题：

- gRPC `Get` 路径没有像旧 RPC handler 那样调用 `RecordRead/RecordLeaseHit`。
- 所以这个统计当前不能用于判断真实读流量，应先补齐 gRPC 侧统计埋点。

## 7. pull 后“最小改动重做”清单（建议顺序）

1. `raft/raft.go`
- `persist()` 改为 `Save(state, nil)`。

2. `benchmarks/benchmark.go`
- 增加 `--maxraftstate` 参数，并传入 `StartKVServer(...)`。
- 保留 server stats 输出（便于快速定位）。

3. `storage/store.go` + `rsm/server.go`
- 增加 `PutCASWithTTL`。
- `doPut` 改单事务 CAS。
- 去掉 `DoOp` 全局锁。

4. `rsm/client.go`
- 增加 `KV_RPC_TIMEOUT_MS`。
- 单次 RPC 超时默认下调到 250~300ms。

这是当前收益/改动比最高的一组补丁。

## 8. 当前阶段结论

- 这次性能上升不是“某个参数玄学”，而是把几处关键热路径成本降下来了。
- 核心架构（Raft 写入全序、复制状态机、快照）还在，没有被优化过程破坏。
- 后续若继续冲吞吐，优先做两件事：
  1) 修复 gRPC 统计口径（先看清真实读路径）；
  2) 处理 TTL/时间依赖确定性风险（提升一致性鲁棒性）。
