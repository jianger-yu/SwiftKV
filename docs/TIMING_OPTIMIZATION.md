# KVraft 等待时间参数优化指南

## 概述

本指南帮助你找到最优的 RSM 和 Raft 时间参数配置，以提高系统性能。

## 关键参数说明

### Raft 参数

| 参数 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| 选举超时 | `RAFT_ELECTION_TIMEOUT_MS` | 300 | Leader 未收到心跳后多少毫秒触发新选举。实际值 = 基础值 + 0-200ms 随机。值越小，故障检测越快，但易误判；值越大，更稳定但滞后。|
| 心跳间隔 | `RAFT_HEARTBEAT_INTERVAL_MS` | 80 | Leader 向 Follower 发送心跳的间隔。值越小，日志同步越快但 CPU 开销更大；值越大，CPU 开销小但延迟高。|
| Lease 比率 | `RAFT_LEASE_RATIO` | 1.5 | Lease 有效期 = 心跳间隔 × 比率。用于优化 read-only 操作（Lease Read）。值越小，读操作本地化越多但故障风险高；值越大，更安全但读性能受限。|

### RSM 参数

| 参数 | 环境变量 | 默认值 | 说明 |
|------|---------|--------|------|
| Submit 超时 | `RSM_SUBMIT_TIMEOUT_MS` | 1500 | 等待写操作提交的最长时间。值越小，快速失败但可能产生更多重试；值越大，容错性越好但响应延迟高。|
| Leader 检查间隔 | `RSM_LEADER_CHECK_INTERVAL_MS` | 50 | 轮询检查自身是否仍是 Leader 的时间间隔。值越小，发现 Leader 变更越快但 CPU 开销大；值越大，CPU 开销小但反应迟钝。|

---

## 快速测试方法

### 方法 1: 逐个参数测试（推荐新手）

#### 步骤 1: 基准测试（当前配置）

```bash
cd /home/jianger/codes/KVraft

# 编译基准测试程序
go build -o ./benchmarks/benchmark ./benchmarks/benchmark.go

# 运行基准测试（3 节点，10 个客户端，30 秒）
./benchmarks/benchmark --servers=3 --clients=10 --read-ratio=0.5 --duration=30s

# 记录输出中的:
# - TotalRequests (或换算为 QPS = TotalRequests / 30)
# - AvgLatency
# - MaxLatency
```

**期望输出格式**:
```
启动 KVraft 集群... OK (3 个节点)
等待集群选举完成... OK
初始化 key 空间... OK (初始化耗时: XX)
开始压测... OK
  总请求数: 12345
  读请求: 6172
  写请求: 6173
  成功: 12000
  失败: 345
  总耗时: 30.05s
  平均延迟: 2.43ms
  最小延迟: 0.12ms
  最大延迟: 145.67ms
QPS: 410
```

**记录当前基准**:
- QPS: ______
- Avg Latency: ______ ms
- Max Latency: ______ ms

#### 步骤 2: 测试参数组合

使用下面预定义的测试组合逐一测试。对于每个配置，运行：

```bash
export RAFT_ELECTION_TIMEOUT_MS=300
export RAFT_HEARTBEAT_INTERVAL_MS=80
export RAFT_LEASE_RATIO=1.5
export RSM_SUBMIT_TIMEOUT_MS=800  # 关键：从 1500 改为 800
export RSM_LEADER_CHECK_INTERVAL_MS=25  # 关键：从 50 改为 25

./benchmarks/benchmark --servers=3 --clients=10 --read-ratio=0.5 --duration=30s
```

**记录结果**：
| 配置名 | 参数值 | QPS | Avg(ms) | 改进% |
|--------|--------|-----|---------|-------|
| Current (基准) | 800/25 | ____ | ____ | 0% |
| Test-1 | __/__  | ____ | ____ | ? |

---

### 预定义参数组合

**写入密集场景** (ReadRatio=0.1):

1. **当前配置** (基准)
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=300 \
          RAFT_HEARTBEAT_INTERVAL_MS=80 \
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=1500 \
          RSM_LEADER_CHECK_INTERVAL_MS=50
   ```

2. **短超时** (减少等待时间)
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=300 \
          RAFT_HEARTBEAT_INTERVAL_MS=80 \
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=500 \    # ↓ 减少 3x
          RSM_LEADER_CHECK_INTERVAL_MS=50
   ```

3. **快速检查** (加快 Leader 故障检测)
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=300 \
          RAFT_HEARTBEAT_INTERVAL_MS=80 \
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=1000 \
          RSM_LEADER_CHECK_INTERVAL_MS=25  # ↓ 减少 2x
   ```

4. **同时优化** (推荐)
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=300 \
          RAFT_HEARTBEAT_INTERVAL_MS=80 \
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=800 \     # ↓
          RSM_LEADER_CHECK_INTERVAL_MS=25  # ↓
   ```

5. **激进配置** (快速但可能不稳定)
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=300 \
          RAFT_HEARTBEAT_INTERVAL_MS=80 \
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=300 \     # ↓↓
          RSM_LEADER_CHECK_INTERVAL_MS=10  # ↓↓
   ```

**读侧重场景** (ReadRatio=0.8):

6. **启用 Lease Read**
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=300 \
          RAFT_HEARTBEAT_INTERVAL_MS=80 \
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=1500 \
          RSM_LEADER_CHECK_INTERVAL_MS=50
   # 修改客户端代码使用 SubmitLeaseRead 而非 Submit
   ```

**其他变体**:

7. **快速心跳** (降低日志复制延迟)
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=300 \
          RAFT_HEARTBEAT_INTERVAL_MS=50 \  # ↓
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=800 \
          RSM_LEADER_CHECK_INTERVAL_MS=25
   ```

8. **慢速心跳** (降低 CPU 开销)
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=300 \
          RAFT_HEARTBEAT_INTERVAL_MS=150 \  # ↑
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=1500 \
          RSM_LEADER_CHECK_INTERVAL_MS=50
   ```

9. **保守配置** (更稳定的故障检测)
   ```bash
   export RAFT_ELECTION_TIMEOUT_MS=400 \    # ↑ 更宽松的选举超时
          RAFT_HEARTBEAT_INTERVAL_MS=100 \
          RAFT_LEASE_RATIO=1.5 \
          RSM_SUBMIT_TIMEOUT_MS=2000 \      # ↑
          RSM_LEADER_CHECK_INTERVAL_MS=100  # ↑
   ```

---

## 方法 2: 批量测试（推荐有 shell 经验的用户）

```bash
# 使实验脚本可执行
chmod +x scripts/benchmark-params.sh

# 运行所有参数组合（需要 ~30-40 分钟）
scripts/benchmark-params.sh
```

脚本会：
1. 自动运行 8 个参数组合 × 3 个测试场景 = 24 个基准测试
2. 生成对比表格
3. 推荐最优配置

---

## 性能分析指标

### 关键指标解读

| 指标 | 含义 | 优化方向 |
|------|------|---------|
| **QPS (Ops/sec)** | 吞吐量。越高越好。 | 减少超时、加快检查间隔、提高心跳频率 |
| **平均延迟** | 平均响应时间。越低越好。 | 同 QPS |
| **最大延迟** | 最坏情况延迟。越低越好。 | 增加 Raft 心跳，改善网络稳定性 |
| **P99 延迟** | 99% 请求的延迟。反映尾部情况。 | 同最大延迟 |
| **失败率** | 请求失败的百分比。越低越好。 | 增加超时时间，改善网络 |

### 典型性能曲线

```
当前状态: 300-400 ops/s, ~3ms 延迟

短超时 (500ms):
  - QPS: ↑ 可能 +20-30%
  - 延迟: ↓ 可能 -10-20%
  - 失败率: ↑ 可能 +5-10%（因为快速超时）

同时优化 (800ms + 25ms 检查):
  - QPS: ↑ 可能 +50-100%
  - 延迟: ↓ 可能 -30-50%
  - 失败率: ≈ 无显著变化

激进配置 (300ms + 10ms 检查):
  - QPS: ↑↑ 可能 +100-200%（或更多！）
  - 延迟: ↓↓ 可能 -60-80%
  - 失败率: ↑↑ 可能 +20-40%（权衡考虑）
```

---

## 推荐方案

### 方案 A: 平衡（推荐大多数场景）

```bash
export RAFT_ELECTION_TIMEOUT_MS=300 \
       RAFT_HEARTBEAT_INTERVAL_MS=80 \
       RAFT_LEASE_RATIO=1.5 \
       RSM_SUBMIT_TIMEOUT_MS=800 \
       RSM_LEADER_CHECK_INTERVAL_MS=25
```

**期望效果**:
- QPS: 1000-1500 ops/s (+2-4倍)
- 延迟: 1-2ms (-50% 相对当前)
- 稳定性: 保持

### 方案 B: 吞吐优先（大批量写入场景）

```bash
export RAFT_ELECTION_TIMEOUT_MS=300 \
       RAFT_HEARTBEAT_INTERVAL_MS=50 \   # 更快的日志同步
       RAFT_LEASE_RATIO=2.0 \            # 更长的 lease
       RSM_SUBMIT_TIMEOUT_MS=600 \
       RSM_LEADER_CHECK_INTERVAL_MS=15
```

**期望效果**:
- QPS: 2000-3000 ops/s (+5-8倍)
- 延迟: 0.5-1ms (-80%)
- 稳定性: 可能下降 5-10%

### 方案 C: 稳定优先（金融等关键场景）

```bash
export RAFT_ELECTION_TIMEOUT_MS=400 \    # 更宽松的选举
       RAFT_HEARTBEAT_INTERVAL_MS=100 \
       RAFT_LEASE_RATIO=1.5 \
       RSM_SUBMIT_TIMEOUT_MS=1200 \
       RSM_LEADER_CHECK_INTERVAL_MS=40
```

**期望效果**:
- QPS: 500-700 ops/s (+不明显或略低)
- 延迟: 2-3ms (可能略高)
- 稳定性: ↑↑ 更好的故障检测

---

## 故障排除

### 问题 1: 测试运行后 QPS 反而下降

**可能原因**:
1. Leader 选举频繁（超时太短）
2. 网络拥塞（心跳太频繁）
3. 容器资源不足（CPU/内存瓶颈）

**解决方案**:
- 逐步减小改动幅度（先改一个参数）
- 增加超时值 +50ms 并重新测试
- 检查系统资源使用：`top`, `iostat`

### 问题 2: 测试失败率高

**可能原因**:
1. 超时时间过短，正常操作被判断为超时
2. 激进的 Leader 检查导致误判

**解决方案**:
```bash
# 临时增加超时以验证
export RSM_SUBMIT_TIMEOUT_MS=3000  # 加倍到 3 秒
# 重新运行测试
```

### 问题 3: 一致性问题（写入数据丢失或不一致）

**可能原因**:
- 心跳间隔过长，Follower 掉队
- Lease 比率过小，Lease 频繁失效

**解决方案**:
```bash
# 回到保守配置
export RAFT_HEARTBEAT_INTERVAL_MS=100
export RAFT_LEASE_RATIO=2.0  # 增加 lease 有效期
```

---

## 总结

| 优化目标 | 建议改动 | 风险 |
|---------|---------|------|
| **降低延迟** | ↓ RSM_SUBMIT_TIMEOUT, ↓ RSM_LEADER_CHECK_INTERVAL_MS | 失败率可能 ↑ |
| **提高吞吐** | ↓ RAFT_HEARTBEAT_INTERVAL_MS | CPU ↑, 网络 ↑ |
| **改善稳定性** | ↑ RAFT_ELECTION_TIMEOUT_MS, ↑ RSM_SUBMIT_TIMEOUT_MS | 延迟可能 ↑ |
| **Lease Read** | ↑ RAFT_LEASE_RATIO, ↑ RAFT_HEARTBEAT_INTERVAL_MS | 读延迟 ↑ 但 CPU ↓ |

**最后的话**: 没有一个配置适合所有场景。根据你的具体需求（吞吐 vs 延迟 vs 稳定性），选择合适的权衡点。建议从「方案 A: 平衡」开始，然后根据实际性能表现进行微调。
