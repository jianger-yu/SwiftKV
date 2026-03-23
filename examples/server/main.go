package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"kvraft/rsm"
)

func main() {
	// 定义命令行参数
	mePtr := flag.Int("me", -1, "当前服务器在集群中的索引（0, 1, 2, ...）")
	serversPtr := flag.String("servers", "", "逗号分隔的服务器地址列表（例如: 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003）")
	portPtr := flag.String("port", "", "当前服务器的监听端口（例如: 5001）")
	maxRaftStatePtr := flag.Int("maxraftstate", -1, "Raft 日志的最大大小（-1 表示无限制）")

	flag.Parse()

	// 验证参数
	if *mePtr < 0 || *serversPtr == "" || *portPtr == "" {
		fmt.Println("使用方法:")
		fmt.Println("  go run server.go -me <index> -servers <server_list> -port <port>")
		fmt.Println("")
		fmt.Println("参数说明:")
		fmt.Println("  -me           : 当前服务器在集群中的索引（0 表示第一个服务器）")
		fmt.Println("  -servers      : 逗号分隔的服务器地址列表")
		fmt.Println("  -port         : 当前服务器的监听端口")
		fmt.Println("  -maxraftstate : Raft 日志的最大大小，超过此值将触发快照（默认: -1，无限制）")
		fmt.Println("")
		fmt.Println("示例 1：启动 3 个服务器集群")
		fmt.Println("  终端 1: go run server.go -me 0 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5001")
		fmt.Println("  终端 2: go run server.go -me 1 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5002")
		fmt.Println("  终端 3: go run server.go -me 2 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5003")
		fmt.Println("")
		fmt.Println("示例 2：启动 5 个服务器集群")
		fmt.Println("  go run server.go -me 0 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003,127.0.0.1:5004,127.0.0.1:5005 -port 5001")
		os.Exit(1)
	}

	// 解析服务器地址列表
	servers := strings.Split(*serversPtr, ",")

	// 验证 me 的有效性
	if *mePtr >= len(servers) {
		log.Fatalf("错误: -me %d 超出服务器列表范围 (有 %d 个服务器)", *mePtr, len(servers))
	}

	// 构建完整的监听地址
	address := fmt.Sprintf("127.0.0.1:%s", *portPtr)

	fmt.Printf("=== KVraft 服务器 ===\n")
	fmt.Printf("服务器索引: %d\n", *mePtr)
	fmt.Printf("监听地址: %s\n", address)
	fmt.Printf("集群配置: %v\n", servers)
	fmt.Printf("Raft 最大日志大小: %d\n", *maxRaftStatePtr)
	fmt.Println()

	// 创建持久化器（基于文件系统，支持重启后恢复）
	raftDataDir := "badger-" + address
	persister, err := rsm.NewFilePersister(raftDataDir)
	if err != nil {
		log.Fatalf("创建持久化器失败: %v", err)
	}
	defer persister.Close()

	// 启动 KV 服务器
	// 参数说明：
	//   servers      - 所有服务器的地址列表
	//   gid          - 分组 ID（这里设为 1，可用于多分组场景）
	//   me           - 当前服务器在集群中的索引
	//   persister    - 持久化接口实现
	//   maxraftstate - Raft 日志最大大小
	//   address      - 当前服务器的监听地址
	server := rsm.StartKVServer(servers, 1, *mePtr, persister, *maxRaftStatePtr, address)

	fmt.Printf("服务器已启动，等待连接...\n")
	fmt.Printf("提示: 按 Ctrl+C 优雅关闭服务器\n")
	fmt.Println()

	// 设置信号处理，用于优雅关闭
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// 等待信号或阻塞
	<-sigChan

	fmt.Println()
	fmt.Println("正在关闭服务器...")
	server.Kill()
	fmt.Println("服务器已关闭")
}
