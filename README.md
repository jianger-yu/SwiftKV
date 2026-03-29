# KVraft

KVraft 是一个基于 Go 的高性能分布式键值存储系统，采用 Raft 保证强一致，结合 BadgerDB 持久化、Watch 事件通知和一致性哈希分片，实现从单集群到多分片扩展的完整路径。

## 核心亮点

- 强一致写入路径：Client → RSM (KVServer) → Raft (Start) → Raft → ApplyCh → RSM (Apply Loop) → Storage (Badger)
- 版本化 CAS 语义：避免并发覆盖写
- gRPC 服务接口：统一对外读写与集群状态访问
- Watch 机制：支持按 key/prefix 订阅变更
- Sharding 路由：一致性哈希 + 虚拟节点，支持横向扩展
- WAL 分段化（提升恢复速度与日志管理能力）
- TTL 到期治理（被动过期 + 主动清理）
- Lease 租约（会话与分布式锁基础能力）
- 可观测性：内置 /metrics 暴露请求与失败等关键指标
- 可恢复：本地持久化 + 重启恢复

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
bash examples/start_cluster.sh
```

默认节点：

- 127.0.0.1:5001
- 127.0.0.1:5002
- 127.0.0.1:5003

如需清理历史数据后重启：

```bash
bash examples/start_cluster.sh --clean
```

### 4. 运行示例与 CLI

```bash
go run examples/basic/main.go
go run examples/scenarios/main.go
go run cmd/kvcli/main.go -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003
```

## 架构概览

- `raft/`：Raft 共识（选主、日志复制、提交）
- `rsm/`：复制状态机（请求编排、提交后应用）
- `storage/`：BadgerDB 持久化封装
- `watch/`：订阅与事件分发
- `sharding/`：一致性哈希与路由
- `api/pb/`：Proto 契约与生成代码
- `raftkv/rpc/`：RPC 类型定义

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

建议至少执行核心链路测试：

```bash
go test ./rsm ./watch ./sharding -v
```

## Docker 说明

仓库包含 `Dockerfile` 与 `docker-compose.yml`，当前主要用于本地演示与后续扩展。若用于生产部署，请先校验入口文件、健康检查与配置目录映射是否与当前代码一致。

## 简历展示建议

若用于实习/校招展示，建议重点突出：

- 一致性设计：Raft + RSM 分层职责
- 扩展性设计：Sharding 路由与迁移策略
- 工程化能力：可重复启动、核心测试、脚本化交付
- 后续路线：在现有能力上继续推进性能优化、运维自动化与跨集群治理