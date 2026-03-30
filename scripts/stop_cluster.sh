#!/usr/bin/env bash

# Stop local KVraft cluster processes and optionally clean local runtime data.
# Example:
#   ./scripts/stop_cluster.sh
#   ./scripts/stop_cluster.sh --clean
#   ./scripts/stop_cluster.sh --base-port 15000 --servers 3 --clean

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
PID_DIR="${DATA_ROOT}/cluster/pids"

SERVERS=3
BASE_PORT=15000
CLEAN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --servers)
      SERVERS="$2"
      shift 2
      ;;
    --base-port)
      BASE_PORT="$2"
      shift 2
      ;;
    --clean)
      CLEAN=1
      shift
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

stop_pid() {
  local pid="$1"
  if ! kill -0 "${pid}" 2>/dev/null; then
    return 0
  fi
  kill "${pid}" 2>/dev/null || true
  for _ in {1..20}; do
    if ! kill -0 "${pid}" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  kill -9 "${pid}" 2>/dev/null || true
}

if [[ -d "${PID_DIR}" ]]; then
  for pid_file in "${PID_DIR}"/node-*.pid; do
    [[ -f "${pid_file}" ]] || continue
    pid="$(cat "${pid_file}")"
    echo "Stopping pid=${pid} from ${pid_file}"
    stop_pid "${pid}"
    rm -f "${pid_file}"
  done
fi

for ((i=0; i<SERVERS; i++)); do
  for port in $((BASE_PORT + i)) $((BASE_PORT + i + 1000)) $((18080 + i)) $((19100 + i)); do
    pids="$(ss -ltnp 2>/dev/null | awk -v p=":${port}" '$4 ~ p {print $0}' | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | sort -u)"
    for pid in ${pids}; do
      echo "Stopping residual pid=${pid} on port ${port}"
      stop_pid "${pid}"
    done
  done
done

if [[ "${CLEAN}" -eq 1 ]]; then
  echo "Cleaning runtime data under ${DATA_ROOT}"
  rm -rf "${DATA_ROOT}/cluster" "${DATA_ROOT}/wal"
  for ((i=0; i<SERVERS; i++)); do
    rpc_port=$((BASE_PORT + i))
    rm -rf "${DATA_ROOT}/node-${i}" "${DATA_ROOT}/badger-127.0.0.1:${rpc_port}"
  done
fi

echo "Cluster stop/cleanup completed."
