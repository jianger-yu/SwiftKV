# ============================================================
# Dockerfile for KVraft Server
# 基于 Go 1.21 构建多阶段镜像
# ============================================================

# 构建阶段
FROM golang:1.21-alpine AS builder

WORKDIR /app
RUN apk add --no-cache git gcc musl-dev

# 复制 go.mod 和 go.sum
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 生成 Proto 文件的 Go 代码（如果需要）
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
    go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest && \
    go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest

# 编译主程序
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w" \
    -o /app/kvraft-server \
    ./cmd/server/main.go

# 运行阶段
FROM alpine:3.18

WORKDIR /app

# 安装运行时依赖
RUN apk add --no-cache ca-certificates curl bash

# 从构建阶段复制二进制文件
COPY --from=builder /app/kvraft-server /app/kvraft-server

# 创建数据目录
RUN mkdir -p /data && chmod 777 /data

# 暴露端口
EXPOSE 50000 6000 8001 9100

# 健康检查
HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8001/health || exit 1

# 启动命令
CMD ["/app/kvraft-server"]
