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
- Go 1.25+
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
# 模式 1：单 Raft 组
# 参数映射：--arch node-ring
bash scripts/run_cluster.sh --arch node-ring --servers 3 --base-port 15000 --clean

# 模式 2：多 group 分片（按 group 做一致性哈希，脚本默认模式）
# 参数映射：--arch group-ring
bash scripts/run_cluster.sh --arch group-ring --groups 2 --replicas 3 --base-port 15000 --clean
```

说明：当前 `scripts/run_cluster.sh` 默认启动 `group-ring`（`--groups 1 --replicas 3`）。

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
	- `go build -o ./cmd/benchmarks/benchmark ./cmd/benchmarks/benchmark.go`
	- `./cmd/benchmarks/benchmark --servers=3 --clients=10 --duration=30s --maxraftstate=1048576 --sharded=false`
	- 多 group 分片模式建议：`--sharded=true --sharding-config=data/cluster/sharding.json`
	- 连续压测若要避免历史数据干扰：`bash scripts/test_perf.sh --fresh-run true`
5. 提交推送
	- `bash push-to-github.sh -m "your message"`

说明：`push-to-github.sh` 已接入 `scripts/test-all.sh`，测试输出只保留关键包状态和关键报错行。

## 架构概览

- `pkg/raft/`：Raft 共识（选主、日志复制、提交）
- `pkg/rsm/`：复制状态机（请求编排、提交后应用）
- `pkg/storage/`：BadgerDB 持久化封装
- `pkg/watch/`：订阅与事件分发
- `pkg/sharding/`：一致性哈希与路由
- `pkg/raftapi/`：Raft 与 RSM 接口抽象
- `pkg/wal/`：预写日志（WAL）分段管理
- `api/pb/`：Proto 契约与生成代码
- `cmd/`：可执行入口（server、kvcli、kvmigrate、benchmarks）

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

默认情况下，运行数据写入 `data/`（或环境变量 `KV_DATA_DIR` 指定目录），典型结构如下：

- `data/arch-node-ring/node-*/badger-127.0.0.1:PORT/`
- `data/arch-group-ring/g*-n*/badger-127.0.0.1:PORT/`
- `data/cluster/runtime.env`
- `data/cluster/sharding.json`

- 开发期保留：便于重启回放和问题复现
- 演示前清理：使用 `bash scripts/run_cluster.sh --clean`

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

容器相关文件位于 `deployments/`：

- `deployments/Dockerfile`
- `deployments/Dockerfile.benchmark`
- `deployments/docker-compose.yml`
- `deployments/prometheus.yml`

当前配置已经对齐当前代码结构，可直接在仓库根目录执行：

```bash
docker-compose -f deployments/docker-compose.yml up --build
```

- 如果你的环境还没有 Docker，请先安装 Docker Engine 和 Docker Compose 插件，再执行上面的命令。
- 若用于生产部署，建议先检查端口映射、数据卷、资源限制和监控策略。