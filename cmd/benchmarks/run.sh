#!/bin/bash

# ============================================================
# Benchmark 运行脚本
# 用于执行不同类型的性能测试
# ============================================================

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

# 配置
TARGET_NODES="${TARGET_NODES:-localhost:6000,localhost:6001,localhost:6002}"
BENCHMARK_TYPE="${BENCHMARK_TYPE:-write}"
NUM_CLIENTS="${NUM_CLIENTS:-10}"
NUM_REQUESTS="${NUM_REQUESTS:-10000}"
DURATION="${DURATION:-30s}"
READ_RATIO="${READ_RATIO:-0.5}"
NUM_KEYS="${NUM_KEYS:-10000}"

# 由节点列表推导集群节点数（例如 a,b,c -> 3）
SERVER_COUNT=$(awk -F',' '{print NF}' <<< "$TARGET_NODES")
if [[ -z "$SERVER_COUNT" || "$SERVER_COUNT" -le 0 ]]; then
    SERVER_COUNT=3
fi

# 兼容旧的 workload 配置，自动覆盖 read-ratio
case "$BENCHMARK_TYPE" in
    read)
        EFFECTIVE_READ_RATIO="1.0"
        ;;
    write)
        EFFECTIVE_READ_RATIO="0.0"
        ;;
    mixed)
        EFFECTIVE_READ_RATIO="$READ_RATIO"
        ;;
    *)
        EFFECTIVE_READ_RATIO="$READ_RATIO"
        ;;
esac

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}================================================${NC}"
echo -e "${GREEN}       KVraft Benchmark 工具${NC}"
echo -e "${GREEN}================================================${NC}"
echo ""

# 检查 benchmark 工具是否存在
if [ ! -f "$PROJECT_DIR/benchmarks/benchmark.go" ]; then
    echo -e "${RED}Error: benchmark.go 不存在${NC}"
    exit 1
fi

# 编译 benchmark 工具
echo -e "${YELLOW}[1/3] 编译 benchmark 工具...${NC}"
cd "$PROJECT_DIR"
go build -o benchmarks/benchmark ./benchmarks
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ 编译成功${NC}"
else
    echo -e "${RED}✗ 编译失败${NC}"
    exit 1
fi

echo ""

# 运行 benchmark
echo -e "${YELLOW}[2/3] 运行 Benchmark...${NC}"
echo "  工作负载类型: $BENCHMARK_TYPE"
echo "  目标节点: $TARGET_NODES"
echo "  集群节点数: $SERVER_COUNT"
echo "  并发客户端: $NUM_CLIENTS"
echo "  每客户端请求数: $NUM_REQUESTS"
echo "  运行时间: $DURATION"

"$PROJECT_DIR/benchmarks/benchmark" \
    -servers "$SERVER_COUNT" \
    -clients "$NUM_CLIENTS" \
    -requests "$NUM_REQUESTS" \
    -duration "$DURATION" \
    -keys "$NUM_KEYS" \
    -read-ratio "$EFFECTIVE_READ_RATIO"

BENCHMARK_EXIT=$?

echo ""

# 输出总结
if [ $BENCHMARK_EXIT -eq 0 ]; then
    echo -e "${GREEN}[3/3] Benchmark 完成${NC}"
    echo ""
    echo -e "${GREEN}================================================${NC}"
    echo -e "${GREEN}                Benchmark 总结${NC}"
    echo -e "${GREEN}================================================${NC}"
    echo ""
    echo -e "建议:"
    echo -e "  1. 多次运行以获得平均值"
    echo -e "  2. 比较不同的工作负载类型（read/write/mixed）"
    echo -e "  3. 调整客户端数以找到最优吞吐量"
    echo -e "  4. 查看 Prometheus 仪表板进行更详细的分析"
    echo ""
    exit 0
else
    echo -e "${RED}[3/3] Benchmark 失败${NC}"
    exit 1
fi
