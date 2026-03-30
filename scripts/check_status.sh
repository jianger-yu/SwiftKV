#!/usr/bin/env bash

# Query each node status and print Leader/Follower via GetClusterStatus API.
# Example:
#   ./scripts/check_status.sh
#   ./scripts/check_status.sh --servers 127.0.0.1:16000,127.0.0.1:16001,127.0.0.1:16002

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVERS="127.0.0.1:16000,127.0.0.1:16001,127.0.0.1:16002"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --servers)
      SERVERS="$2"
      shift 2
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

TMP_GO="$(mktemp /tmp/kvraft-check-status-XXXXXX.go)"
trap 'rm -f "${TMP_GO}"' EXIT

cat > "${TMP_GO}" <<'EOF'
package main

import (
  "context"
  "fmt"
  "os"
  "strings"
  "time"

  pb "kvraft/api/pb/kvraft/api/pb"
  "google.golang.org/grpc"
  "google.golang.org/grpc/credentials/insecure"
)

func main() {
  raw := strings.TrimSpace(os.Getenv("KV_STATUS_SERVERS"))
  if raw == "" {
    fmt.Println("KV_STATUS_SERVERS is empty")
    os.Exit(1)
  }
  addrs := strings.Split(raw, ",")

  fmt.Printf("%-22s %-10s %-8s %-10s\n", "Address", "Role", "Term", "Alive")
  fmt.Println("--------------------------------------------------------")

  for _, a := range addrs {
    addr := strings.TrimSpace(a)
    if addr == "" {
      continue
    }

    dialCtx, dialCancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
    conn, err := grpc.DialContext(dialCtx, addr,
      grpc.WithTransportCredentials(insecure.NewCredentials()),
      grpc.WithBlock(),
    )
    dialCancel()
    if err != nil {
      fmt.Printf("%-22s %-10s %-8s %-10s\n", addr, "UNREACH", "-", "false")
      continue
    }

    cli := pb.NewKVServiceClient(conn)
    reqCtx, reqCancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
    resp, reqErr := cli.GetClusterStatus(reqCtx, &pb.ClusterStatusRequest{})
    reqCancel()
    _ = conn.Close()

    if reqErr != nil {
      fmt.Printf("%-22s %-10s %-8s %-10s\n", addr, "ERROR", "-", "false")
      continue
    }

    role := "Follower"
    alive := "false"
    if len(resp.GetNodes()) > 0 {
      if resp.GetNodes()[0].GetIsLeader() {
        role = "Leader"
      }
      if resp.GetNodes()[0].GetIsAlive() {
        alive = "true"
      }
    }
    fmt.Printf("%-22s %-10s %-8d %-10s\n", addr, role, resp.GetCurrentTerm(), alive)
  }
}
EOF

cd "${ROOT_DIR}"
KV_STATUS_SERVERS="${SERVERS}" go run "${TMP_GO}"
