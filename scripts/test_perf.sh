#!/usr/bin/env bash

# Run automated performance benchmark against an external local cluster and save report to data/perf.
# Example:
#   ./scripts/test_perf.sh
#   ./scripts/test_perf.sh --clients 20 --requests 2000 --read-ratio 0.7 --duration 45s
#   ./scripts/test_perf.sh --maxraftstate 1048576 --sharded true
#   ./scripts/test_perf.sh --restart-cluster true --clean true
#   ./scripts/test_perf.sh --init-keys 1000 --skip-init-if-seeded true

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
PERF_DIR="${DATA_ROOT}/perf"
mkdir -p "${PERF_DIR}"

SERVERS=3
CLIENTS=10
REQUESTS=1000
READ_RATIO=0.7
DURATION="30s"
KEYS=10000
MAXRAFTSTATE=1048576
SHARDED=true
INIT_KEYS=1000
SKIP_INIT_IF_SEEDED=true
BASE_PORT=15000
RESTART_CLUSTER=false
CLEAN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --servers)
      SERVERS="$2"
      shift 2
      ;;
    --clients)
      CLIENTS="$2"
      shift 2
      ;;
    --requests)
      REQUESTS="$2"
      shift 2
      ;;
    --read-ratio)
      READ_RATIO="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --keys)
      KEYS="$2"
      shift 2
      ;;
    --maxraftstate)
      MAXRAFTSTATE="$2"
      shift 2
      ;;
    --sharded)
      SHARDED="$2"
      shift 2
      ;;
    --init-keys)
      INIT_KEYS="$2"
      shift 2
      ;;
    --skip-init-if-seeded)
      SKIP_INIT_IF_SEEDED="$2"
      shift 2
      ;;
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    --restart-cluster)
      RESTART_CLUSTER="$2"
      shift 2
      ;;
    --clean)
      CLEAN="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

cd "${ROOT_DIR}"

RPC_SERVERS=""
for ((i=0; i<SERVERS; i++)); do
  addr="127.0.0.1:$((BASE_PORT + i))"
  if [[ -z "${RPC_SERVERS}" ]]; then
    RPC_SERVERS="${addr}"
  else
    RPC_SERVERS="${RPC_SERVERS},${addr}"
  fi
done

if [[ "${RESTART_CLUSTER}" == "true" ]]; then
  ./scripts/stop_cluster.sh --servers "${SERVERS}" --base-port "${BASE_PORT}" >/dev/null 2>&1 || true
  if [[ "${CLEAN}" == "true" ]]; then
    ./scripts/run_cluster.sh --servers "${SERVERS}" --base-port "${BASE_PORT}" --clean >/dev/null
  else
    ./scripts/run_cluster.sh --servers "${SERVERS}" --base-port "${BASE_PORT}" >/dev/null
  fi
else
  unreachable_count=$(./scripts/check_status.sh --servers "127.0.0.1:$((BASE_PORT+1000)),127.0.0.1:$((BASE_PORT+1001)),127.0.0.1:$((BASE_PORT+1002))" | grep -c "UNREACH" || true)
  if [[ "${unreachable_count}" -ge 3 ]]; then
    if [[ "${CLEAN}" == "true" ]]; then
      ./scripts/run_cluster.sh --servers "${SERVERS}" --base-port "${BASE_PORT}" --clean >/dev/null
    else
      ./scripts/run_cluster.sh --servers "${SERVERS}" --base-port "${BASE_PORT}" >/dev/null
    fi
  fi
fi

stamp="$(date +%Y%m%d_%H%M%S)"
report_file="${PERF_DIR}/perf_${stamp}.log"

echo "Running benchmark..."
echo "Report file: ${report_file}"

go run ./cmd/benchmarks/benchmark.go \
  --servers="${SERVERS}" \
  --server-addrs="${RPC_SERVERS}" \
  --clients="${CLIENTS}" \
  --requests="${REQUESTS}" \
  --read-ratio="${READ_RATIO}" \
  --duration="${DURATION}" \
  --keys="${KEYS}" \
  --init-keys="${INIT_KEYS}" \
  --skip-init-if-seeded="${SKIP_INIT_IF_SEEDED}" \
  --maxraftstate="${MAXRAFTSTATE}" \
  --sharded="${SHARDED}" | tee "${report_file}"

echo "Performance test done. Cluster remains running."
