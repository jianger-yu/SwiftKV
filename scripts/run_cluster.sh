#!/usr/bin/env bash

# Start a local 3-node KVraft cluster in background mode.
# Example:
#   ./scripts/run_cluster.sh
#   ./scripts/run_cluster.sh --clean
#   ./scripts/run_cluster.sh --servers 3 --base-port 15000

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_ROOT="${KV_DATA_DIR:-${ROOT_DIR}/data}"
CLUSTER_DIR="${DATA_ROOT}/cluster"
PID_DIR="${CLUSTER_DIR}/pids"
LOG_DIR="${CLUSTER_DIR}/logs"
BIN_DIR="${DATA_ROOT}/bin"
SERVER_BIN="${BIN_DIR}/kvserver"

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

if [[ "${SERVERS}" -ne 3 ]]; then
    echo "This script is intended for a 3-node local cluster."
    exit 1
fi

mkdir -p "${PID_DIR}" "${LOG_DIR}" "${BIN_DIR}"

for pid_file in "${PID_DIR}"/node-*.pid; do
    [[ -f "${pid_file}" ]] || continue
    pid="$(cat "${pid_file}")"
    if kill -0 "${pid}" 2>/dev/null; then
        echo "Cluster appears running (pid=${pid}). Run ./scripts/stop_cluster.sh first."
        exit 1
    fi
done

RAFT_PEERS=""
for ((i=0; i<SERVERS; i++)); do
    addr="127.0.0.1:$((BASE_PORT + i))"
    if [[ -z "${RAFT_PEERS}" ]]; then
        RAFT_PEERS="${addr}"
    else
        RAFT_PEERS="${RAFT_PEERS},${addr}"
    fi
done

echo "Building server binary..."
cd "${ROOT_DIR}"
go build -o "${SERVER_BIN}" ./cmd/server

for ((i=0; i<SERVERS; i++)); do
    rpc_port=$((BASE_PORT + i))
    grpc_port=$((rpc_port + 1000))
    rest_port=$((18080 + i))
    metrics_port=$((19100 + i))

    if [[ "${CLEAN}" -eq 1 ]]; then
        rm -rf "${DATA_ROOT}/node-${i}" "${DATA_ROOT}/badger-127.0.0.1:${rpc_port}"
    fi

    log_file="${LOG_DIR}/node-${i}.log"
    pid_file="${PID_DIR}/node-${i}.pid"

    echo "Starting node ${i} (rpc=${rpc_port}, grpc=${grpc_port}, rest=${rest_port}, metrics=${metrics_port})"
    NODE_ID="${i}" \
    RAFT_PEERS="${RAFT_PEERS}" \
    REST_LISTEN="127.0.0.1:${rest_port}" \
    METRICS_LISTEN="127.0.0.1:${metrics_port}" \
    KV_DATA_DIR="${DATA_ROOT}" \
    "${SERVER_BIN}" >"${log_file}" 2>&1 &

    echo "$!" > "${pid_file}"
done

sleep 1

for ((i=0; i<SERVERS; i++)); do
    pid_file="${PID_DIR}/node-${i}.pid"
    pid="$(cat "${pid_file}")"
    if ! kill -0 "${pid}" 2>/dev/null; then
        echo "Node ${i} failed to start. Check ${LOG_DIR}/node-${i}.log"
        exit 1
    fi
done

echo "Cluster started."
echo "PIDs: ${PID_DIR}"
echo "Logs: ${LOG_DIR}"
echo "Check roles with: ./scripts/check_status.sh"
