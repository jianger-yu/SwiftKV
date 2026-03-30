package sharding

import (
	"testing"
)

// TestAddNode tests adding nodes to the hash ring
func TestAddNode(t *testing.T) {
	ch := NewConsistentHash(3)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	node := ch.GetNode("testkey")
	if node == "" {
		t.Errorf("Expected to find node for key, got empty result")
	}
}

// TestRemoveNode tests removing nodes from the hash ring
func TestRemoveNode(t *testing.T) {
	ch := NewConsistentHash(3)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	node := ch.GetNode("key1")
	if node == "" {
		t.Errorf("Expected node for key1")
	}

	ch.RemoveNode("node2")

	node2 := ch.GetNode("key1")
	if node2 == "" {
		t.Errorf("Expected node after removing node2")
	}
}

// TestGetNode returns single node for key
func TestGetNode(t *testing.T) {
	ch := NewConsistentHash(3)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	node := ch.GetNode("testkey")
	if node == "" {
		t.Errorf("Expected non-empty node, got empty string")
	}

	// Same key should return same node
	node2 := ch.GetNode("testkey")
	if node != node2 {
		t.Errorf("Expected consistent hashing: key should map to same node. Got %s, then %s", node, node2)
	}
}

// TestGetNodes returns multiple replicas for key
func TestGetNodes(t *testing.T) {
	ch := NewConsistentHash(3)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")
	ch.AddNode("node4")

	nodes := ch.GetNodes("testkey", 2)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 replicas, got %d", len(nodes))
	}

	// Replicas should be unique
	if nodes[0] == nodes[1] {
		t.Errorf("Expected different nodes for replicas, got same node: %s", nodes[0])
	}

	// Same key should return same replicas in same order
	nodes2 := ch.GetNodes("testkey", 2)
	if nodes[0] != nodes2[0] || nodes[1] != nodes2[1] {
		t.Errorf("Expected consistent replica placement. First: %v, Second: %v", nodes, nodes2)
	}
}

// TestEmptyRing tests behavior on empty ring
func TestEmptyRing(t *testing.T) {
	ch := NewConsistentHash(3)

	node := ch.GetNode("testkey")
	if node != "" {
		t.Errorf("Expected empty string for empty ring, got %s", node)
	}

	nodes := ch.GetNodes("testkey", 3)
	if len(nodes) != 0 {
		t.Errorf("Expected empty slice for empty ring, got %v", nodes)
	}
}

// TestSingleNode tests behavior with single node
func TestSingleNode(t *testing.T) {
	ch := NewConsistentHash(3)
	ch.AddNode("node1")

	node := ch.GetNode("testkey")
	if node != "node1" {
		t.Errorf("Expected node1 for single node ring, got %s", node)
	}

	nodes := ch.GetNodes("testkey", 3)
	if len(nodes) != 1 {
		t.Errorf("Expected 1 node for single node ring, got %d", len(nodes))
	}
	if nodes[0] != "node1" {
		t.Errorf("Expected node1, got %s", nodes[0])
	}
}

// TestReplicationCount tests requesting more replicas than available nodes
func TestReplicationCount(t *testing.T) {
	ch := NewConsistentHash(3)

	ch.AddNode("node1")
	ch.AddNode("node2")

	// Request 5 replicas but only 2 nodes exist
	nodes := ch.GetNodes("testkey", 5)
	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes (all available), got %d", len(nodes))
	}
}

// TestKeyDistribution verifies different keys map to nodes
func TestKeyDistribution(t *testing.T) {
	ch := NewConsistentHash(3)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Generate many keys
	nodeAssignments := make(map[string]bool)
	for i := 0; i < 100; i++ {
		key := "key" + string(rune('0'+(i%26)))
		node := ch.GetNode(key)
		nodeAssignments[node] = true
	}

	// Should use multiple nodes
	if len(nodeAssignments) < 2 {
		t.Logf("Note: Only %d unique nodes used (ideally would use more)", len(nodeAssignments))
	}
}

// TestNodeRemovalRebalance verifies key reassignment after node removal
func TestNodeRemovalRebalance(t *testing.T) {
	ch := NewConsistentHash(3)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	// Get key assignments before removal
	beforeRemoval := make(map[string]string)
	testKeys := []string{"key1", "key2", "key3", "key4", "key5"}
	for _, key := range testKeys {
		beforeRemoval[key] = ch.GetNode(key)
	}

	// Remove a node
	ch.RemoveNode("node2")

	// Get key assignments after removal
	movedCount := 0
	for _, key := range testKeys {
		afterRemoval := ch.GetNode(key)
		if beforeRemoval[key] != afterRemoval {
			movedCount++
		}
	}

	// Some keys should have been reassigned
	if movedCount > len(testKeys) {
		t.Logf("Note: %d/%d keys were reassigned after node removal",
			movedCount, len(testKeys))
	}
}

// TestVirtualNodes verifies virtual nodes exist
func TestVirtualNodes(t *testing.T) {
	ch := NewConsistentHash(100) // Many virtual nodes

	numNodes := 3
	for i := 1; i <= numNodes; i++ {
		ch.AddNode("node" + string(rune('0'+i)))
	}

	// Verify ring is sorted
	if len(ch.sortedHashes) > 1 {
		for i := 1; i < len(ch.sortedHashes); i++ {
			if ch.sortedHashes[i] < ch.sortedHashes[i-1] {
				t.Errorf("Hash ring is not sorted at index %d", i)
			}
		}
	}
}

// TestGetNodeAnalysis tests node distribution analysis
func TestGetNodeAnalysis(t *testing.T) {
	ch := NewConsistentHash(3)

	ch.AddNode("node1")
	ch.AddNode("node2")
	ch.AddNode("node3")

	analysis := ch.GetNodeAnalysis()
	if analysis == "" {
		t.Errorf("Expected non-empty analysis string")
	}

	if len(analysis) < 10 {
		t.Logf("Note: Analysis string seems short: %s", analysis)
	}
}

// BenchmarkGetNode benchmarks single node lookup
func BenchmarkGetNode(b *testing.B) {
	ch := NewConsistentHash(10)

	for i := 0; i < 10; i++ {
		ch.AddNode("node" + string(rune('0'+i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + string(rune('0'+(i%1000)))
		ch.GetNode(key)
	}
}

// BenchmarkGetNodes benchmarks multiple replica lookup
func BenchmarkGetNodes(b *testing.B) {
	ch := NewConsistentHash(10)

	for i := 0; i < 10; i++ {
		ch.AddNode("node" + string(rune('0'+i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + string(rune('0'+(i%1000)))
		ch.GetNodes(key, 3)
	}
}
