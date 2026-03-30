#!/usr/bin/env bash

# One-shot environment bootstrap: dependencies, proto generation, and optional tests.
# Example:
#   ./scripts/setup_env.sh
#   ./scripts/setup_env.sh --skip-proto
#   ./scripts/setup_env.sh --with-test

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SKIP_PROTO=0
WITH_TEST=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-proto)
      SKIP_PROTO=1
      shift
      ;;
    --with-test)
      WITH_TEST=1
      shift
      ;;
    *)
      echo "Unknown arg: $1"
      exit 1
      ;;
  esac
done

cd "${ROOT_DIR}"

echo "Downloading Go dependencies..."
go mod download

echo "Installing protoc Go plugins..."
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

if [[ "${SKIP_PROTO}" -eq 0 ]]; then
  if command -v protoc >/dev/null 2>&1; then
    echo "Generating protobuf/grpc code..."
    bash ./api/pb/compile.sh
  else
    echo "protoc not found. Install it first, then rerun: ./api/pb/compile.sh"
  fi
fi

mkdir -p data
if [[ ! -f data/.gitkeep ]]; then
  : > data/.gitkeep
fi

chmod +x ./scripts/*.sh

if [[ "${WITH_TEST}" -eq 1 ]]; then
  echo "Running full tests..."
  ./scripts/test-all.sh
fi

echo "Environment setup completed."
