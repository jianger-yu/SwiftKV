#!/usr/bin/env bash

# Capture CPU profile from KVraft pprof endpoint and render SVG flame graph.
# Examples:
#   ./scripts/flamegraph.sh
#   ./scripts/flamegraph.sh --target 127.0.0.1:17060 --seconds 45
#   ./scripts/flamegraph.sh --target 127.0.0.1:17060 --seconds 30 --heap

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
RUNTIME_ENV="${DATA_ROOT}/cluster/runtime.env"
OUT_DIR="${DATA_ROOT}/perf/flamegraphs"
TARGET=""
SECONDS=30
NAME=""
WITH_HEAP=false

usage() {
  cat <<EOF
Usage: scripts/flamegraph.sh [options]

Options:
  --target <host:port>   pprof endpoint target (e.g. 127.0.0.1:17060)
  --seconds <n>          CPU profiling seconds (default: 30)
  --out-dir <path>       output directory (default: data/perf/flamegraphs)
  --name <label>         output file label suffix
  --heap                 capture heap profile and render svg too
  -h, --help             show this help
EOF
}

runtime_value() {
  local key="$1"
  if [[ ! -f "${RUNTIME_ENV}" ]]; then
    echo ""
    return
  fi
  grep -E "^${key}=" "${RUNTIME_ENV}" | head -n1 | cut -d'=' -f2-
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET="$2"
      shift 2
      ;;
    --seconds)
      SECONDS="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --name)
      NAME="$2"
      shift 2
      ;;
    --heap)
      WITH_HEAP=true
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

if ! [[ "${SECONDS}" =~ ^[0-9]+$ ]] || [[ "${SECONDS}" -le 0 ]]; then
  echo "--seconds must be a positive integer"
  exit 1
fi

if [[ -z "${TARGET}" ]]; then
  pprof_servers="$(runtime_value PPROF_SERVERS)"
  if [[ -n "${pprof_servers}" ]]; then
    TARGET="${pprof_servers%%,*}"
  fi
fi

if [[ -z "${TARGET}" ]]; then
  echo "No --target provided and no PPROF_SERVERS found in ${RUNTIME_ENV}."
  echo "Please start cluster via scripts/run_cluster.sh or pass --target explicitly."
  exit 1
fi

if ! command -v go >/dev/null 2>&1; then
  echo "go command not found; go tool pprof is required"
  exit 1
fi

if ! command -v curl >/dev/null 2>&1; then
  echo "curl command not found"
  exit 1
fi

mkdir -p "${OUT_DIR}"

ts="$(date +%Y%m%d_%H%M%S)"
label="${TARGET//:/_}"
if [[ -n "${NAME}" ]]; then
  label="${label}_${NAME}"
fi

cpu_url="http://${TARGET}/debug/pprof/profile?seconds=${SECONDS}"
cpu_pb="${OUT_DIR}/cpu_${label}_${ts}.pb.gz"
cpu_svg="${OUT_DIR}/cpu_${label}_${ts}.svg"

echo "[flamegraph] target=${TARGET} seconds=${SECONDS}"
echo "[flamegraph] fetching CPU profile..."
curl -fsS "${cpu_url}" -o "${cpu_pb}"

echo "[flamegraph] rendering CPU flamegraph..."
go tool pprof -svg "${cpu_pb}" > "${cpu_svg}"

echo "[flamegraph] CPU profile: ${cpu_pb}"
echo "[flamegraph] CPU svg:     ${cpu_svg}"

if [[ "${WITH_HEAP}" == "true" ]]; then
  heap_url="http://${TARGET}/debug/pprof/heap"
  heap_pb="${OUT_DIR}/heap_${label}_${ts}.pb.gz"
  heap_svg="${OUT_DIR}/heap_${label}_${ts}.svg"

  echo "[flamegraph] fetching heap profile..."
  curl -fsS "${heap_url}" -o "${heap_pb}"

  echo "[flamegraph] rendering heap flamegraph..."
  go tool pprof -svg "${heap_pb}" > "${heap_svg}"

  echo "[flamegraph] Heap profile: ${heap_pb}"
  echo "[flamegraph] Heap svg:     ${heap_svg}"
fi

echo "[flamegraph] done"
