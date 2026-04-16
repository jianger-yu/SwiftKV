#!/usr/bin/env bash

# 清理 KVraft 运行/测试冗余产物，避免 data 目录无界增长。
# 默认会：
# 1) 清理与当前 runtime.env 不匹配的 arch-group-ring / arch-node-ring 目录
# 2) 对 data/rsm-tests 执行“保留最近 N 目录 + 超龄删除”
#
# 示例：
#   ./scripts/cleanup_artifacts.sh
#   ./scripts/cleanup_artifacts.sh --rsm-max-dirs 32 --rsm-max-age-hours 24
#   ./scripts/cleanup_artifacts.sh --dry-run

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
RUNTIME_ENV="${DATA_ROOT}/cluster/runtime.env"

RSM_MAX_DIRS="${RSM_TEST_MAX_DIRS:-64}"
RSM_MAX_AGE_HOURS="${RSM_TEST_MAX_AGE_HOURS:-72}"
DRY_RUN=0

runtime_value() {
  local key="$1"
  if [[ ! -f "${RUNTIME_ENV}" ]]; then
    echo ""
    return
  fi
  grep -E "^${key}=" "${RUNTIME_ENV}" | head -n1 | cut -d'=' -f2-
}

run_rm_rf() {
  local path="$1"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "[dry-run] rm -rf ${path}"
  else
    rm -rf "${path}"
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rsm-max-dirs)
      RSM_MAX_DIRS="$2"
      shift 2
      ;;
    --rsm-max-age-hours)
      RSM_MAX_AGE_HOURS="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    *)
      echo "未知参数: $1"
      exit 1
      ;;
  esac
done

if [[ ! "${RSM_MAX_DIRS}" =~ ^[0-9]+$ ]] || [[ "${RSM_MAX_DIRS}" -le 0 ]]; then
  echo "--rsm-max-dirs 必须是正整数"
  exit 1
fi
if [[ ! "${RSM_MAX_AGE_HOURS}" =~ ^[0-9]+$ ]] || [[ "${RSM_MAX_AGE_HOURS}" -le 0 ]]; then
  echo "--rsm-max-age-hours 必须是正整数"
  exit 1
fi

ARCH="$(runtime_value ARCH)"
GROUPS="$(runtime_value GROUPS)"
REPLICAS="$(runtime_value REPLICAS)"
SERVERS="$(runtime_value SERVERS)"

if [[ -z "${ARCH}" ]]; then
  ARCH="group-ring"
fi
if [[ -z "${GROUPS}" ]]; then
  GROUPS=1
fi
if [[ -z "${REPLICAS}" ]]; then
  REPLICAS=3
fi
if [[ -z "${SERVERS}" ]]; then
  SERVERS=3
fi

echo "清理策略: arch=${ARCH}, groups=${GROUPS}, replicas=${REPLICAS}, servers=${SERVERS}, rsm_max_dirs=${RSM_MAX_DIRS}, rsm_max_age_hours=${RSM_MAX_AGE_HOURS}, dry_run=${DRY_RUN}"

# 1) 清理 arch-node-ring 冗余目录
if [[ -d "${DATA_ROOT}/arch-node-ring" ]]; then
  shopt -s nullglob
  for d in "${DATA_ROOT}/arch-node-ring"/node-*; do
    [[ -d "${d}" ]] || continue
    name="${d##*/}"
    idx="${name#node-}"
    if [[ ! "${idx}" =~ ^[0-9]+$ ]]; then
      continue
    fi
    if (( idx >= SERVERS )); then
      echo "清理冗余 node-ring 目录: ${d}"
      run_rm_rf "${d}"
    fi
  done
  shopt -u nullglob
fi

# 2) 清理 arch-group-ring 冗余目录
if [[ -d "${DATA_ROOT}/arch-group-ring" ]]; then
  shopt -s nullglob
  for d in "${DATA_ROOT}/arch-group-ring"/g*-n*; do
    [[ -d "${d}" ]] || continue
    name="${d##*/}"
    if [[ ! "${name}" =~ ^g([0-9]+)-n([0-9]+)$ ]]; then
      continue
    fi
    gid="${BASH_REMATCH[1]}"
    rid="${BASH_REMATCH[2]}"
    if (( gid >= GROUPS || rid >= REPLICAS )); then
      echo "清理冗余 group-ring 目录: ${d}"
      run_rm_rf "${d}"
    fi
  done
  shopt -u nullglob
fi

# 3) 清理 rsm-tests 历史目录（数量 + 时间）
RSM_DIR="${DATA_ROOT}/rsm-tests"
if [[ -d "${RSM_DIR}" ]]; then
  now_epoch="$(date +%s)"
  max_age_seconds=$((RSM_MAX_AGE_HOURS * 3600))

  # 按修改时间倒序，超过保留数量的候选删除
  mapfile -t all_dirs < <(find "${RSM_DIR}" -mindepth 1 -maxdepth 1 -type d -printf '%T@ %p\n' | sort -nr)

  i=0
  for line in "${all_dirs[@]}"; do
    ts="${line%% *}"
    dir="${line#* }"
    i=$((i + 1))

    # 仅清理 test-db-* 模式，避免误删其他目录
    base="${dir##*/}"
    if [[ "${base}" != test-db-* ]]; then
      continue
    fi

    sec="${ts%.*}"
    age=$((now_epoch - sec))
    remove_by_count=0
    remove_by_age=0
    if (( i > RSM_MAX_DIRS )); then
      remove_by_count=1
    fi
    if (( age > max_age_seconds )); then
      remove_by_age=1
    fi

    if (( remove_by_count == 1 || remove_by_age == 1 )); then
      echo "清理 rsm-tests 目录: ${dir} (rank=${i}, age=${age}s)"
      run_rm_rf "${dir}"
    fi
  done
fi

echo "清理完成"
