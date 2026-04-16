#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

COMMIT_MSG="feat(wal): implement log truncation and replay mechanism"
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