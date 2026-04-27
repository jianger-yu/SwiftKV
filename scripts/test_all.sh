#!/usr/bin/env bash

# 执行仓库测试（含关键构建、RSM专项、线性一致性、全量包测试）并输出精简结果。
# 示例：
#   ./scripts/test_all.sh
#   ./scripts/test_all.sh -run TestWatch

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PACKAGES=(./api/... ./cmd/... ./pkg/...)

echo "=== Stage 1/4: 编译关键命令 ==="
go build ./cmd/kvcli
go build ./cmd/kvmigrate
go build ./cmd/server

echo
echo "=== Stage 2/4: RSM 基础/Watch/TTL 测试 ==="
go test -count=1 -v -timeout 150s ./pkg/rsm -run 'TestBasicPutGet|TestWatchMechanism|TestWatchDeleteEvent|TestWatchExpireEventOldValue|TestTTLVisibleAndExpireDelete'

echo
echo "=== Stage 3/4: 线性一致性测试 ==="
go test -count=1 -v -timeout 240s ./pkg/rsm -run 'TestLinearizability|TestLinearizabilityWithNetworkFaults'

TMP_OUT="$(mktemp)"
trap 'rm -f "${TMP_OUT}"' EXIT

echo
echo "=== Stage 4/4: 全量包测试摘要 ==="

set +e
if [[ $# -gt 0 ]]; then
    go test "${PACKAGES[@]}" "$@" >"${TMP_OUT}" 2>&1
else
    go test "${PACKAGES[@]}" >"${TMP_OUT}" 2>&1
fi
STATUS=$?
set -e

grep -E '^(ok|FAIL|\?)\s' "${TMP_OUT}" || true

if [[ ${STATUS} -ne 0 ]]; then
    echo
    echo "=== 关键错误 ==="
    grep -E '^(#\s|[A-Za-z0-9_./-]+\.go:[0-9]+:[0-9]+:)' "${TMP_OUT}" | head -n 50 || true
    echo
    echo "结果: FAIL"
    exit "${STATUS}"
fi

if [[ -x "./scripts/cleanup_artifacts.sh" ]]; then
    ./scripts/cleanup_artifacts.sh >/dev/null 2>&1 || true
fi

echo
echo "结果: PASS"