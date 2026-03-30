package sharding

import (
	"crypto/md5"
	"fmt"
	"sort"
	"sync"
)

// ConsistentHash 一致性哈希实现
type ConsistentHash struct {
	mu           sync.RWMutex
	virtualNodes int
	ring         map[uint32]string
	sortedHashes []uint32
	nodeReplicas map[string]int
}

// NewConsistentHash 创建一致性哈希
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes < 1 {
		virtualNodes = 3
	}
	return &ConsistentHash{
		virtualNodes: virtualNodes,
		ring:         make(map[uint32]string),
		nodeReplicas: make(map[string]int),
	}
}

// AddNode 添加节点
func (ch *ConsistentHash) AddNode(node string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	for i := 0; i < ch.virtualNodes; i++ {
		hash := ch.hash(fmt.Sprintf("%s:%d", node, i))
		ch.ring[hash] = node
	}

	ch.nodeReplicas[node]++
	ch.sortedHashes = ch.getSortedHashes()
}

// RemoveNode 移除节点
func (ch *ConsistentHash) RemoveNode(node string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	for i := 0; i < ch.virtualNodes; i++ {
		hash := ch.hash(fmt.Sprintf("%s:%d", node, i))
		delete(ch.ring, hash)
	}

	delete(ch.nodeReplicas, node)
	ch.sortedHashes = ch.getSortedHashes()
}

// GetNode 获取单个节点
func (ch *ConsistentHash) GetNode(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return ""
	}

	hash := ch.hash(key)
	idx := sort.Search(len(ch.sortedHashes), func(i int) bool {
		return ch.sortedHashes[i] >= hash
	})

	if idx == len(ch.sortedHashes) {
		idx = 0
	}

	return ch.ring[ch.sortedHashes[idx]]
}

// GetNodes 获取多个副本节点
func (ch *ConsistentHash) GetNodes(key string, count int) []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return []string{}
	}

	if count > len(ch.nodeReplicas) {
		count = len(ch.nodeReplicas)
	}

	hash := ch.hash(key)
	var nodes []string
	seen := make(map[string]bool)

	for i := 0; i < len(ch.sortedHashes) && len(nodes) < count; i++ {
		idx := (sort.Search(len(ch.sortedHashes), func(j int) bool {
			return ch.sortedHashes[j] >= hash
		}) + i) % len(ch.sortedHashes)

		node := ch.ring[ch.sortedHashes[idx]]
		if !seen[node] {
			nodes = append(nodes, node)
			seen[node] = true
		}
	}

	return nodes
}

// hash MD5哈希函数
func (ch *ConsistentHash) hash(key string) uint32 {
	data := md5.Sum([]byte(key))
	value := uint32(data[0])<<24 | uint32(data[1])<<16 | uint32(data[2])<<8 | uint32(data[3])
	return value
}

// getSortedHashes 获取排序后的哈希值
func (ch *ConsistentHash) getSortedHashes() []uint32 {
	var hashes []uint32
	for h := range ch.ring {
		hashes = append(hashes, h)
	}
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i] < hashes[j]
	})
	return hashes
}

// GetNodeAnalysis 获取节点分布分析
func (ch *ConsistentHash) GetNodeAnalysis() string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.nodeReplicas) == 0 {
		return "No nodes"
	}

	analysis := fmt.Sprintf("Nodes: %d, Virtual replicas: %d, Total hashes: %d\n",
		len(ch.nodeReplicas), ch.virtualNodes, len(ch.ring))

	for node, count := range ch.nodeReplicas {
		analysis += fmt.Sprintf("  %s: %d virtual nodes\n", node, count)
	}

	return analysis
}
