# SwiftKV

**SwiftKV** 是一款基于 Go 语言开发的高性能分布式 Key-Value 存储系统。系统以 **Raft 共识协议** 为核心实现强一致性保障，并支持基于**一致性哈希分片**的多 Raft 组横向扩展架构。

## 核心特性

1. **分布式一致性与架构**

- **强一致状态机 (RSM)**：基于 Raft 协议实现线性一致性读写，确保分布式环境下数据绝对可靠。
- **弹性分片路由 (Sharding)**：采用一致性哈希结合虚拟节点，支持动态扩容并实现请求的精准分发。
- **工业级容灾恢复**：支持分段 **WAL** 与 **Snapshot** 机制，大幅提升冷启动恢复速度并优化日志压缩。

2. **高级存储语义**
- **多维订阅 (Watch)**：基于 gRPC Stream 实现 Key/Prefix 级别的变更订阅，支持 Leader 感知的自动重连。
- **并发控制 (CAS)**：提供基于版本的原子 Compare-and-Swap 操作，通过乐观锁机制规避分布式环境下的“丢失更新”问题。
- **租约与生命周期 (TTL/Lease)**：内置 Lease 机制提供分布式锁基础能力，结合“被动失效+主动扫描”实现精准的 TTL 治理。

3. **工程化与可观测性**
- **高性能通信**：全链路采用 **gRPC** 协议，提供统一的读写、配置管理及集群状态监控接口。
- **全链路可观测性**：内置 Prometheus 标准的 /metrics 接口，实时暴露 QPS、延迟分布及 Raft 运行状态。

## 快速开始

### 1. 环境要求

- Linux 或 macOS
- Go 1.21+
- Git

### 2. 拉取与编译

```bash
git clone git@github.com:jianger-yu/KVraft.git
cd KVraft
go mod tidy
go build ./...
```

### 3. 启动 3 节点集群

```bash
bash scripts/run_cluster.sh
```

默认节点：

- 127.0.0.1:15000
- 127.0.0.1:15001
- 127.0.0.1:15002

如需清理历史数据后重启：

```bash
bash scripts/run_cluster.sh --clean
```

可选运行模式（脚本参数映射）：

```bash
# 模式 1：单 Raft 组（默认开发模式）
# 参数映射：--arch node-ring
bash scripts/run_cluster.sh --arch node-ring --servers 3 --base-port 15000 --clean

# 模式 2：多 group 分片（按 group 做一致性哈希）
# 参数映射：--arch group-ring
bash scripts/run_cluster.sh --arch group-ring --groups 2 --replicas 3 --base-port 15000 --clean
```

脚本会自动生成运行元数据与分片配置：

- `data/cluster/runtime.env`
- `data/cluster/sharding.json`

### 4. 运行示例与 CLI

```bash
go run cmd/kvcli/main.go
go run cmd/kvmigrate/main.go --dry-run
```

说明：`kvcli` 与 `kvmigrate` 在未传参时会优先读取 `data/cluster/runtime.env` 自动适配脚本启动的集群。

## 推荐使用方式

日常开发建议按这个顺序：

1. 启动集群
	- `bash scripts/run_cluster.sh --clean`
2. 功能验证
	- `go run cmd/kvcli/main.go`
	- `go run cmd/kvmigrate/main.go --dry-run`
3. 全量测试（精简输出）
	- `bash scripts/test-all.sh`
4. 性能压测
	- `go build -o ./benchmarks/benchmark ./benchmarks/benchmark.go`
	- `./benchmarks/benchmark --servers=3 --clients=10 --duration=30s --maxraftstate=1048576 --sharded=false`
	- 多 group 分片模式建议：`--sharded=true --sharding-config=data/cluster/sharding.json`
	- 连续压测若要避免历史数据干扰：`bash scripts/test_perf.sh --fresh-run true`
5. 提交推送
	- `bash push-to-github.sh -m "your message"`

说明：`push-to-github.sh` 已接入 `scripts/test-all.sh`，测试输出只保留关键包状态和关键报错行。

## 架构概览

- `raft/`：Raft 共识（选主、日志复制、提交）
- `rsm/`：复制状态机（请求编排、提交后应用）
- `storage/`：BadgerDB 持久化封装
- `watch/`：订阅与事件分发
- `sharding/`：一致性哈希与路由
- `api/pb/`：Proto 契约与生成代码
- `raftkv/rpc/`：RPC 类型定义

### 架构形态

- 单 Raft 组：一个共识组内多副本（常规 Raft 集群访问模式，客户端可使用 MakeClerk）
- 多 group 分片：多个 Raft group 组成分片层，按 key 路由到 group（客户端可使用 MakeShardedClerk）


## 当前能力边界

### 已实现

- Get / Put / Delete / Scan
- CAS 版本控制
- gRPC KVService（Get/Put/Delete/Scan/Watch/GetClusterStatus）
- Watch key / Watch prefix
- 单集群 + 分片路由访问
- 分片迁移工具（kvmigrate）与路由重分配
- 基础健康检查与指标导出（/health, /ready, /metrics）
- 快照与持久化恢复主链路
- WAL 分段化（提升恢复速度与日志管理能力）
- TTL 到期治理（被动过期 + 主动清理）
- Lease 租约（会话与分布式锁基础能力）
- 配置连接池提供连接复用与并发压测优化

## 数据目录说明

运行节点会在仓库根目录产生 `badger-127.0.0.1:PORT/` 数据目录，用于持久化恢复。

- 开发期保留：便于重启回放和问题复现
- 演示前清理：使用 `examples/start_cluster.sh --clean`

## 测试与验证

推荐执行全量测试（精简输出）：

```bash
bash scripts/test-all.sh
```

如果你需要原始详细输出：

```bash
go test ./...
```

## Docker 说明

仓库包含 `Dockerfile` 与 `docker-compose.yml`，当前主要用于本地演示与后续扩展。若用于生产部署，请先校验入口文件、健康检查与配置目录映射是否与当前代码一致。

## 简历展示建议

若用于实习/校招展示，建议重点突出：

- 一致性设计：Raft + RSM 分层职责
- 扩展性设计：Sharding 路由与迁移策略
- 工程化能力：可重复启动、核心测试、脚本化交付
- 后续路线：在现有能力上继续推进性能优化、运维自动化与跨集群治理