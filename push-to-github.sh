#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

COMMIT_MSG="refactor: 优化持久化链路，提升数据可靠性与并发性能

针对存储链路进行深度重构，主要改进如下：
- [P0] 引入代际 Manifest 机制，确保 raft-state 与 snapshot 的原子一致性。
- [P0] 升级 atomicWrite 为 durable-write，补全 fsync 语义以应对掉电场景。
- [P1] 将 gob 序列化移出 Raft 主锁路径，大幅降低心跳延迟与选举抖动。
- [P1] 实现 Save 操作的异步批处理（Group Commit），提升 IO 吞吐量。
- [P2] 引入 sync.Pool 复用编码缓冲区，减少高频持久化带来的 GC 压力。

此改动通过了 smoke_test 基础验证，显著提升了高负载下的系统稳定性。"
RUN_TEST=1
DRY_RUN=0

usage() {
    cat <<'EOF'
Usage:
  bash push-to-github.sh [-m "commit message"] [--no-test] [--dry-run]

Options:
  -m, --message   Commit message
  --no-test       Skip test step
  --dry-run       Show staged changes without commit/push
  -h, --help      Show this help
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -m|--message)
            COMMIT_MSG="${2:-}"
            shift 2
            ;;
        --no-test)
            RUN_TEST=0
            shift
            ;;
        --dry-run)
            DRY_RUN=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1"
            usage
            exit 1
            ;;
    esac
done

if [[ ! -d .git ]]; then
    echo "Error: not a git repository: ${SCRIPT_DIR}"
    exit 1
fi

if [[ -z "${COMMIT_MSG}" ]]; then
    echo "Error: commit message cannot be empty"
    exit 1
fi

if ! git remote get-url origin >/dev/null 2>&1; then
    echo "Error: remote 'origin' is not configured"
    echo "Hint: git remote add origin <your-repo-url>"
    exit 1
fi

if [[ ${RUN_TEST} -eq 1 ]]; then
    echo "=== 1/4 Run tests ==="
    bash scripts/test-all.sh
else
    echo "=== 1/4 Skip tests (--no-test) ==="
fi

echo ""
echo "=== 2/4 Stage source files ==="

stage_path() {
    local path="$1"
    if [[ -e "${path}" ]]; then
        git add -A -- "${path}"
        return
    fi

    # Stage deletions for tracked files/dirs that were removed locally.
    if git ls-files -- "${path}" "${path}/**" | grep -q .; then
        git add -A -- "${path}"
    fi
}

PATHS=(
    .gitignore
    README.md
    push-to-github.sh
    go.mod
    go.sum
    go.work
    go.work.sum
    Dockerfile
    Dockerfile.benchmark
    docker-compose.yml
    prometheus.yml
    api
    cmd
    deployments
    examples
    pkg
    scripts
    benchmarks
    raft
    raftapi
    raftkv
    rsm
    sharding
    storage
    wal
    watch
)

for path in "${PATHS[@]}"; do
    stage_path "${path}"
done

stage_path "benchmarks/benchmark.go"
stage_path "benchmarks/run.sh"
stage_path "cmd/benchmarks/benchmark.go"

# Avoid committing local benchmark/runtime artifacts.
git reset -q -- benchmarks/benchmark cmd/benchmarks/benchmark data 2>/dev/null || true

if git diff --cached --quiet; then
    echo "No staged source changes. Nothing to commit."
    exit 0
fi

echo ""
echo "=== 3/4 Review staged changes ==="
git status --short

if [[ ${DRY_RUN} -eq 1 ]]; then
    echo ""
    echo "Dry run enabled. Skipping commit and push."
    exit 0
fi

echo ""
echo "=== 4/4 Commit and push ==="
CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "${CURRENT_BRANCH}" == "HEAD" ]]; then
    echo "Error: detached HEAD. Please checkout a branch first."
    exit 1
fi

git commit -m "${COMMIT_MSG}"
git push -u origin "${CURRENT_BRANCH}"

echo ""
echo "Push completed on branch: ${CURRENT_BRANCH}"