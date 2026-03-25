#!/bin/bash

# KVraft 快速启动脚本
# 用于快速启动 3 个服务器的 KVraft 集群

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 配置
SERVERS_COUNT=3
BASE_PORT=5001
SERVERS=""
CLEAN_DATA=0

for arg in "$@"; do
    case "$arg" in
        --clean)
            CLEAN_DATA=1
            ;;
    esac
done

# 构建服务器列表
for ((i=0; i<SERVERS_COUNT; i++)); do
    PORT=$((BASE_PORT + i))
    if [ $i -eq 0 ]; then
        SERVERS="127.0.0.1:$PORT"
    else
        SERVERS="$SERVERS,127.0.0.1:$PORT"
    fi
done

echo -e "${GREEN}=== KVraft 集群启动脚本 ===${NC}"
echo "将启动 $SERVERS_COUNT 个服务器"
echo "集群配置: $SERVERS"
echo ""

# 预清理：处理上次异常退出残留的进程和数据库目录锁
for ((i=0; i<SERVERS_COUNT; i++)); do
    PORT=$((BASE_PORT + i))

    # 清理监听端口的旧进程（如果存在）
    PIDS=$(ss -ltnp 2>/dev/null | awk -v p=":$PORT" '$4 ~ p {print $0}' | sed -n 's/.*pid=\([0-9]\+\).*/\1/p' | sort -u)
    if [ -n "$PIDS" ]; then
        echo -e "${YELLOW}检测到端口 $PORT 已被占用，正在清理旧进程: $PIDS${NC}"
        for pid in $PIDS; do
            kill "$pid" 2>/dev/null || true
        done
        sleep 1
    fi

    # 持久化数据默认保留；仅在 --clean 时删除数据目录。
    DB_DIR="badger-127.0.0.1:$PORT"
    if [ "$CLEAN_DATA" -eq 1 ] && [ -d "$DB_DIR" ]; then
        rm -rf "$DB_DIR"
    fi
done

if [ "$CLEAN_DATA" -eq 1 ]; then
    echo -e "${YELLOW}已启用 --clean：启动前会清理 badger-* 数据目录${NC}"
else
    echo -e "${GREEN}默认保留 badger-* 数据目录（支持重启后恢复）${NC}"
fi

# 创建临时目录用于日志和 PID 文件
WORK_DIR="/tmp/kvraft_$$"
mkdir -p "$WORK_DIR"

cleanup() {
    echo -e "${YELLOW}清理资源...${NC}"
    # 杀死所有启动的服务器进程
    for pid_file in "$WORK_DIR"/server_*.pid; do
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  关闭服务器 $pid..."
                kill "$pid" 2>/dev/null || true
            fi
        fi
    done
    rm -rf "$WORK_DIR"
    echo -e "${GREEN}清理完成${NC}"
}

# 设置信号处理
trap cleanup EXIT INT TERM

# 启动每个服务器
for ((i=0; i<SERVERS_COUNT; i++)); do
    PORT=$((BASE_PORT + i))
    LOG_FILE="$WORK_DIR/server_${i}.log"
    PID_FILE="$WORK_DIR/server_${i}.pid"
    
    echo -n "启动服务器 $i (端口 $PORT)... "
    
    go run examples/server/main.go \
        -me $i \
        -servers "$SERVERS" \
        -port $PORT > "$LOG_FILE" 2>&1 &
    
    SERVER_PID=$!
    echo $SERVER_PID > "$PID_FILE"
    
    # 等待服务器启动
    sleep 1
    
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        echo -e "${GREEN}[OK]${NC} (PID: $SERVER_PID)"
    else
        echo -e "${RED}[失败]${NC}"
        echo "查看日志: cat $LOG_FILE"
        exit 1
    fi
done

echo ""
echo -e "${GREEN}所有服务器已启动！${NC}"
echo ""
echo "服务器日志:"
for ((i=0; i<SERVERS_COUNT; i++)); do
    LOG_FILE="$WORK_DIR/server_${i}.log"
    echo "  服务器 $i: $LOG_FILE"
done
echo ""

echo "在另一个终端运行客户端示例:"
echo -e "  ${YELLOW}cd $(pwd) && go run examples/basic/main.go${NC}"
echo -e "  ${YELLOW}cd $(pwd) && go run examples/scenarios/main.go${NC}"
echo ""
echo "按 Ctrl+C 停止集群"
echo ""

# 保持运行
wait
