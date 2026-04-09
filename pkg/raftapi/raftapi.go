package raftapi

type Raft interface {
	// Start 对一条新日志条目达成共识，返回该条目的日志索引、任期号以及本节点是否为 leader。
	Start(command interface{}) (int, int, bool)

	// GetState 返回本节点当前的任期号及是否为 leader。
	GetState() (int, bool)

	// Snapshot 触发快照功能。
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// Kill 告知本节点应杀死长环行 goroutine。
	Kill()
}

// ApplyMsg 是 Raft 向应用层报告已提交日志的消息。
// 当 Raft 节点识别出连续的日志条目已提交时，节点应通过 applyCh 发送 ApplyMsg。
// 设置 CommandValid 为真以表示 ApplyMsg 包含了新提交的日志条目。

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
