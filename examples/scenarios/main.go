package main

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"time"

	kvapi "kvraft/pkg/raftapi"
	"kvraft/pkg/rsm"
)

var scenarioKeyPrefix = fmt.Sprintf("scenario:%d:", time.Now().UnixNano())

func scenarioKey(name string) string {
	return scenarioKeyPrefix + name
}

// checkServerAvailableForRealWorld 检查服务器是否可用（避免无限循环）
func checkServerAvailableForRealWorld(servers []string, timeout time.Duration) bool {
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

// ===== 实际应用场景 1：分布式计数器 =====

type DistributedCounter struct {
	clerk *rsm.Clerk
	key   string
}

func NewDistributedCounter(servers []string, key string) *DistributedCounter {
	return &DistributedCounter{
		clerk: rsm.MakeClerk(servers),
		key:   scenarioKey(key),
	}
}

func (dc *DistributedCounter) Increment() error {
	// 重试直到成功
	for attempts := 0; attempts < 10; attempts++ {
		// 读取当前值
		valStr, version, err := dc.clerk.Get(dc.key)
		if err == kvapi.ErrNoKey {
			// 键不存在，从 0 开始
			err = dc.clerk.Put(dc.key, "1", 0)
			if err == kvapi.OK {
				return nil
			}
		} else if err == kvapi.OK {
			// 解析当前值（简化起见，假设都是整数）
			var val int
			fmt.Sscanf(valStr, "%d", &val)

			// 递增
			newVal := fmt.Sprintf("%d", val+1)
			err = dc.clerk.Put(dc.key, newVal, version)
			if err == kvapi.OK {
				return nil
			}
			// 如果 err == ErrVersion，重试
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("计数器递增失败")
}

func (dc *DistributedCounter) Get() (int, error) {
	valStr, _, err := dc.clerk.Get(dc.key)
	if err == kvapi.ErrNoKey {
		return 0, nil
	}
	if err != kvapi.OK {
		return 0, fmt.Errorf("获取值失败: %v", err)
	}

	var val int
	fmt.Sscanf(valStr, "%d", &val)
	return val, nil
}

// ===== 实际应用场景 2：分布式缓存 =====

type CacheEntry struct {
	Value     string
	ExpiresAt time.Time
}

type DistributedCache struct {
	clerk *rsm.Clerk
}

func NewDistributedCache(servers []string) *DistributedCache {
	return &DistributedCache{
		clerk: rsm.MakeClerk(servers),
	}
}

func (dc *DistributedCache) Set(key string, value string, ttl time.Duration) error {
	// 简化实现：忽略过期时间，直接存储
	err := dc.clerk.Put(key, value, 0)
	if err == kvapi.OK {
		return nil
	}
	return fmt.Errorf("缓存设置失败: %v", err)
}

func (dc *DistributedCache) Get(key string) (string, bool) {
	val, _, err := dc.clerk.Get(key)
	return val, err == kvapi.OK
}

func (dc *DistributedCache) Increment(key string, delta int64) (int64, error) {
	for attempts := 0; attempts < 10; attempts++ {
		valStr, version, err := dc.clerk.Get(key)

		var currentVal int64 = 0
		if err == kvapi.OK {
			fmt.Sscanf(valStr, "%d", &currentVal)
		} else if err != kvapi.ErrNoKey {
			return 0, fmt.Errorf("缓存读取失败: %v", err)
		}

		newVal := currentVal + delta
		newValStr := fmt.Sprintf("%d", newVal)

		err = dc.clerk.Put(key, newValStr, version)
		if err == kvapi.OK {
			return newVal, nil
		}

		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("缓存操作失败")
}

// ===== 实际应用场景 3：分布式配置管理 =====

type ConfigManager struct {
	clerk *rsm.Clerk
	mu    sync.RWMutex
	cache map[string]string
}

func NewConfigManager(servers []string) *ConfigManager {
	return &ConfigManager{
		clerk: rsm.MakeClerk(servers),
		cache: make(map[string]string),
	}
}

func (cm *ConfigManager) GetConfig(key string) string {
	cm.mu.RLock()
	if val, exists := cm.cache[key]; exists {
		cm.mu.RUnlock()
		return val
	}
	cm.mu.RUnlock()

	val, _, err := cm.clerk.Get(key)
	if err == kvapi.OK {
		cm.mu.Lock()
		cm.cache[key] = val
		cm.mu.Unlock()
	}
	return val
}

func (cm *ConfigManager) SetConfig(key string, value string) error {
	// 简化：总是尝试初始化为版本 0
	err := cm.clerk.Put(key, value, 0)
	if err == kvapi.OK {
		cm.mu.Lock()
		cm.cache[key] = value
		cm.mu.Unlock()
		return nil
	}

	// 如果版本冲突，重试一次
	if err == kvapi.ErrVersion {
		_, version, _ := cm.clerk.Get(key)
		err = cm.clerk.Put(key, value, version)
		if err == kvapi.OK {
			cm.mu.Lock()
			cm.cache[key] = value
			cm.mu.Unlock()
			return nil
		}
	}

	return fmt.Errorf("配置设置失败: %v", err)
}

// ===== 测试函数 =====

func TestDistributedCounter() {
	fmt.Println("=== 分布式计数器测试 ===")
	fmt.Println()

	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}

	counter := NewDistributedCounter(servers, "global_counter")

	fmt.Println("单线程递增测试:")
	for i := 0; i < 5; i++ {
		err := counter.Increment()
		val, _ := counter.Get()
		fmt.Printf("  第 %d 次递增: 当前值 = %d, 错误 = %v\n", i+1, val, err)
	}

	fmt.Println("\n并发递增测试:")
	counter2 := NewDistributedCounter(servers, "concurrent_counter")
	counter2.clerk.Put(counter2.key, "0", 0)

	var wg sync.WaitGroup
	numGoroutines := 5

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				err := counter2.Increment()
				fmt.Printf("  Goroutine %d: 递增 %d => %v\n", id, j+1, err)
				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()

	val, _ := counter2.Get()
	fmt.Printf("\n最终计数器值: %d (期望: %d)\n", val, numGoroutines*3)
}

func TestDistributedCache() {
	fmt.Println("\n=== 分布式缓存测试 ===")
	fmt.Println()

	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}

	cache := NewDistributedCache(servers)

	// 基本操作
	fmt.Println("基本缓存操作:")
	nameKey := scenarioKey("name")
	cache.Set(nameKey, "Alice", 0)
	if val, ok := cache.Get(nameKey); ok {
		fmt.Printf("  cache.Get('%s') => '%s'\n", nameKey, val)
	}

	// 缓存递增
	fmt.Println("\n缓存计数器:")
	pageViewsKey := scenarioKey("page_views")
	cache.Increment(pageViewsKey, 1)
	val, _ := cache.Increment(pageViewsKey, 1)
	fmt.Printf("  第一次访问后: page_views = %d\n", val)

	val, _ = cache.Increment(pageViewsKey, 5)
	fmt.Printf("  第二次访问后: page_views = %d\n", val)

	// 并发缓存操作
	fmt.Println("\n并发缓存操作:")
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cache.Set(scenarioKey(fmt.Sprintf("user_%d", id)), fmt.Sprintf("User%d", id), 0)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 3; i++ {
		if val, ok := cache.Get(scenarioKey(fmt.Sprintf("user_%d", i))); ok {
			fmt.Printf("  user_%d = %s\n", i, val)
		}
	}
}

func TestConfigManager() {
	fmt.Println("\n=== 分布式配置管理测试 ===")
	fmt.Println()

	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}

	config := NewConfigManager(servers)

	fmt.Println("设置和读取配置:")
	databaseURLKey := scenarioKey("database_url")
	apiKeyKey := scenarioKey("api_key")
	debugModeKey := scenarioKey("debug_mode")

	config.SetConfig(databaseURLKey, "postgresql://localhost/mydb")
	config.SetConfig(apiKeyKey, "secret123")
	config.SetConfig(debugModeKey, "true")

	fmt.Printf("  database_url = %s\n", config.GetConfig(databaseURLKey))
	fmt.Printf("  api_key = %s\n", config.GetConfig(apiKeyKey))
	fmt.Printf("  debug_mode = %s\n", config.GetConfig(debugModeKey))

	fmt.Println("\n缓存验证（第二次读取应该从缓存获取）:")
	fmt.Printf("  database_url = %s\n", config.GetConfig(databaseURLKey))

	fmt.Println("\n配置更新:")
	config.SetConfig(databaseURLKey, "postgresql://newhost/mydb")
	fmt.Printf("  更新后 database_url = %s\n", config.GetConfig(databaseURLKey))
}

func main() {
	servers := []string{
		"127.0.0.1:5001",
		"127.0.0.1:5002",
		"127.0.0.1:5003",
	}

	fmt.Println("========================================")
	fmt.Println("  KVraft 实际应用场景演示")
	fmt.Println("========================================")
	fmt.Println("")
	fmt.Println("场景 1: 分布式计数器")
	fmt.Println("场景 2: 分布式缓存")
	fmt.Println("场景 3: 分布式配置管理")
	fmt.Println("")

	// 检查服务器可用性
	fmt.Println("\n正在检查服务器连接...")
	if !checkServerAvailableForRealWorld(servers, 5*time.Second) {
		fmt.Println("\n❌ 错误：无法连接到任何服务器！")
		fmt.Println("\n请确保已启动 KVraft 集群:")
		fmt.Println("  bash examples/start_cluster.sh")
		log.Fatal("启动失败")
	}
	fmt.Println("✅ 服务器连接成功!")
	fmt.Println()

	TestDistributedCounter()
	TestDistributedCache()
	TestConfigManager()

	fmt.Println("\n========================================")
	fmt.Println("所有场景测试完成！")
	fmt.Println("========================================")
}
