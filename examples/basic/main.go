package main

import (
	"fmt"
	"log"
	"net/rpc"
	"time"

	kvapi "kvraft/pkg/raftapi"
	"kvraft/pkg/rsm"
)

var demoKeyPrefix = fmt.Sprintf("demo:%d:", time.Now().UnixNano())

func demoKey(name string) string {
	return demoKeyPrefix + name
}

// checkServerAvailable 检查服务器是否可用
func checkServerAvailable(servers []string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, server := range servers {
			client, err := rpc.Dial("tcp", server)
			if err == nil {
				client.Close()
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// SimpleExample 展示最基础的使用场景
func SimpleExample() {
	fmt.Println("=== 简单示例 ===")
	fmt.Println()

	// 1. 创建客户端，连接到服务器集群
	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}
	clerk := rsm.MakeClerk(servers)
	fmt.Println("[步骤 1] 创建客户端")
	fmt.Printf("  连接到服务器: %v\n\n", servers)

	// 2. 写入数据
	fmt.Println("[步骤 2] 写入初始数据")
	key := demoKey("username")
	value := "alice"
	err := clerk.Put(key, value, 0) // 版本号 0 表示新建
	if err != kvapi.OK {
		log.Fatalf("写入失败: %v", err)
	}
	fmt.Printf("  Put('%s', '%s', v0) 成功\n\n", key, value)

	// 3. 读取数据
	fmt.Println("[步骤 3] 读取数据")
	readValue, version, err := clerk.Get(key)
	if err != kvapi.OK {
		log.Fatalf("读取失败: %v", err)
	}
	fmt.Printf("  Get('%s') => value='%s', version=%d\n\n", key, readValue, version)

	// 4. 更新数据
	fmt.Println("[步骤 4] 更新数据")
	newValue := "bob"
	err = clerk.Put(key, newValue, version) // 使用读取到的版本号
	if err != kvapi.OK {
		log.Fatalf("更新失败: %v", err)
	}
	fmt.Printf("  Put('%s', '%s', v%d) 成功\n\n", key, newValue, version)

	// 5. 验证更新
	fmt.Println("[步骤 5] 验证更新")
	readValue, newVersion, _ := clerk.Get(key)
	fmt.Printf("  Get('%s') => value='%s', version=%d\n\n", key, readValue, newVersion)

	fmt.Println("✓ 示例完成")
}

// ConcurrencyExample 展示并发访问
func ConcurrencyExample() {
	fmt.Println("\n=== 并发访问示例 ===")
	fmt.Println()

	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}

	// 初始化数据
	clerk := rsm.MakeClerk(servers)
	counterKey := demoKey("counter")
	clerk.Put(counterKey, "0", 0)

	fmt.Println("[场景] 两个客户端并发写同一个键")
	fmt.Println("")

	// 客户端 1
	fmt.Println("客户端 1:")
	c1 := rsm.MakeClerk(servers)
	val1, ver1, _ := c1.Get(counterKey)
	fmt.Printf("  1. Get('%s') => '%s' (v%d)\n", counterKey, val1, ver1)

	// 客户端 2
	fmt.Println("客户端 2:")
	c2 := rsm.MakeClerk(servers)
	val2, ver2, _ := c2.Get(counterKey)
	fmt.Printf("  1. Get('%s') => '%s' (v%d)\n", counterKey, val2, ver2)

	// 客户端 1 尝试更新
	fmt.Println("客户端 1:")
	err := c1.Put(counterKey, "1", ver1)
	fmt.Printf("  2. Put('%s', '1', v%d) => %v\n", counterKey, ver1, err)

	// 客户端 2 尝试用旧版本更新（应该失败）
	fmt.Println("客户端 2:")
	err = c2.Put(counterKey, "2", ver2)
	fmt.Printf("  2. Put('%s', '2', v%d) => %v (期望 ErrVersion)\n", counterKey, ver2, err)

	// 客户端 2 重新读取并更新
	fmt.Println("客户端 2:")
	val2New, ver2New, _ := c2.Get(counterKey)
	fmt.Printf("  3. Get('%s') => '%s' (v%d)\n", counterKey, val2New, ver2New)
	err = c2.Put(counterKey, "2", ver2New)
	fmt.Printf("  4. Put('%s', '2', v%d) => %v\n", counterKey, ver2New, err)

	fmt.Println("\n✓ 演示完成")
}

// ErrorHandlingExample 展示错误处理
func ErrorHandlingExample() {
	fmt.Println("\n=== 错误处理示例 ===")
	fmt.Println()

	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}

	clerk := rsm.MakeClerk(servers)

	// 示例 1：ErrNoKey
	fmt.Println("[错误 1] ErrNoKey - 读取不存在的键")
	missingKey := demoKey("nonexistent")
	_, _, err := clerk.Get(missingKey)
	fmt.Printf("  Get('%s') => %v\n", missingKey, err)
	if err == kvapi.ErrNoKey {
		fmt.Println("  ✓ 正确捕获 ErrNoKey")
	}
	fmt.Println("")

	// 示例 2：ErrVersion
	fmt.Println("[错误 2] ErrVersion - 版本号不匹配（首次重试）")
	testKey := demoKey("test")
	clerk.Put(testKey, "value", 0)
	_, version, _ := clerk.Get(testKey)
	clerk.Put(testKey, "changed", version) // 更新使版本号变化

	err = clerk.Put(testKey, "wrong", version) // 使用过时版本
	fmt.Printf("  Put('%s', 'wrong', v%d) => %v\n", testKey, version, err)
	if err == kvapi.ErrVersion {
		fmt.Println("  ✓ 正确捕获 ErrVersion")
	}
	fmt.Println("")

	// 示例 3：ErrMaybe
	fmt.Println("[错误 3] ErrMaybe - 不确定状态（重试后版本不匹配）")
	_, version, _ = clerk.Get(testKey)
	err = clerk.Put(testKey, "attempt1", version)
	if err == kvapi.OK {
		_, version, _ = clerk.Get(testKey)
		// 使用更新前的版本重试
		err = clerk.Put(testKey, "attempt2", version)
		if err == kvapi.ErrVersion {
			// 再次尝试（视为重试）
			err = clerk.Put(testKey, "attempt3", version)
			fmt.Printf("  Put('%s', 'attempt3', v%d) => %v\n", testKey, version, err)
			if err == kvapi.ErrMaybe {
				fmt.Println("  ✓ 正确捕获 ErrMaybe")
			}
		}
	}

	fmt.Println("\n✓ 错误处理演示完成")
}

// PersistenceExample 展示数据持久性
func PersistenceExample() {
	fmt.Println("\n=== 数据持久性示例 ===")
	fmt.Println()

	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}

	// 第一个客户端写数据
	fmt.Println("[步骤 1] 第一个客户端写入数据")
	clerk1 := rsm.MakeClerk(servers)
	persistentKey := demoKey("persistent_key")
	clerk1.Put(persistentKey, "important_data", 0)
	fmt.Printf("  Put('%s', 'important_data', v0) 成功\n", persistentKey)
	fmt.Println("")

	// 第二个客户端读取数据
	fmt.Println("[步骤 2] 第二个客户端读取数据")
	clerk2 := rsm.MakeClerk(servers)
	value, _, _ := clerk2.Get(persistentKey)
	fmt.Printf("  Get('%s') => '%s'\n", persistentKey, value)
	fmt.Println("")

	// 验证数据一致性
	if value == "important_data" {
		fmt.Println("✓ 数据已正确持久化（在 Raft 日志中）")
	} else {
		fmt.Println("✗ 数据不一致")
	}
}

func main() {
	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}

	fmt.Println("========================================")
	fmt.Println("    KVraft 客户端使用示例")
	fmt.Println("========================================")
	fmt.Println("")
	fmt.Println("请确保以下 3 个服务器正在运行:")
	fmt.Println("  - 127.0.0.1:5001")
	fmt.Println("  - 127.0.0.1:5002")
	fmt.Println("  - 127.0.0.1:5003")
	fmt.Println("")
	fmt.Println("如果未启动，请在另一个终端运行:")
	fmt.Println("  bash examples/start_cluster.sh")
	fmt.Println("")
	fmt.Println("或手动启动（需要 3 个终端）:")
	fmt.Println("  go run examples/server.go -me 0 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5001")
	fmt.Println("  go run examples/server.go -me 1 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5002")
	fmt.Println("  go run examples/server.go -me 2 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5003")
	fmt.Println("")

	// 检查服务器可用性
	fmt.Println("\n正在检查服务器连接...")
	if !checkServerAvailable(servers, 5*time.Second) {
		fmt.Println("\n❌ 错误：无法连接到任何服务器！")
		fmt.Println("\n请确保已启动 KVraft 集群:")
		fmt.Println("  bash examples/start_cluster.sh")
		fmt.Println("\n或在 3 个不同的终端中手动启动:")
		fmt.Println("  go run examples/server.go -me 0 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5001")
		fmt.Println("  go run examples/server.go -me 1 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5002")
		fmt.Println("  go run examples/server.go -me 2 -servers 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 -port 5003")
		log.Fatal("启动失败")
	}
	fmt.Println("✅ 服务器连接成功!")
	fmt.Println()

	SimpleExample()
	ConcurrencyExample()
	ErrorHandlingExample()
	PersistenceExample()

	fmt.Println("\n========================================")
	fmt.Println("所有示例执行完成！")
	fmt.Println("========================================")
}
