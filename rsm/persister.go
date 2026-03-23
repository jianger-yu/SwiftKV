package rsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// FilePersister 基于文件系统的持久化器实现
// 原子性写入：先写临时文件，再重命名，确保崩溃时不损坏
type FilePersister struct {
	mu            sync.RWMutex
	raftStateFile string
	snapshotFile  string
	tmpDir        string
}

// NewFilePersister 创建基于文件的持久化器
// dataDir: 数据目录，例如 "badger-127.0.0.1:15000/"
func NewFilePersister(dataDir string) (*FilePersister, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dataDir, err)
	}

	fp := &FilePersister{
		raftStateFile: filepath.Join(dataDir, "raft-state.bin"),
		snapshotFile:  filepath.Join(dataDir, "snapshot.bin"),
		tmpDir:        filepath.Join(dataDir, ".tmp"),
	}

	if err := os.MkdirAll(fp.tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir tmp dir: %w", err)
	}

	return fp, nil
}

// ReadRaftState 读取 Raft 状态（日志+任期+投票）
func (fp *FilePersister) ReadRaftState() []byte {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	data, _ := os.ReadFile(fp.raftStateFile)
	return data
}

// ReadSnapshot 读取快照数据
func (fp *FilePersister) ReadSnapshot() []byte {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	data, _ := os.ReadFile(fp.snapshotFile)
	return data
}

// Save 原子地保存 Raft 状态和快照
// 使用 write-then-rename 模式避免崩溃损坏
func (fp *FilePersister) Save(raftstate []byte, snapshot []byte) {
	fp.mu.Lock()
	defer fp.mu.Unlock()

	// 保存 Raft 状态
	if err := fp.atomicWrite(fp.raftStateFile, raftstate); err != nil {
		// 日志记录失败，但不 panic（保持兼容性）
		// 在生产环境应该立即告警
		fmt.Fprintf(os.Stderr, "[FilePersister] failed to save raft state: %v\n", err)
	}

	// 保存快照
	if err := fp.atomicWrite(fp.snapshotFile, snapshot); err != nil {
		fmt.Fprintf(os.Stderr, "[FilePersister] failed to save snapshot: %v\n", err)
	}
}

// atomicWrite 使用原子的 write-rename 写入文件
func (fp *FilePersister) atomicWrite(targetPath string, data []byte) error {
	// 1. 生成临时文件路径
	tmpFile := filepath.Join(fp.tmpDir, filepath.Base(targetPath)+".tmp")

	// 2. 写入临时文件
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("write temp file: %w", err)
	}

	// 3. 原子重命名（在同一文件系统上，这是原子操作）
	if err := os.Rename(tmpFile, targetPath); err != nil {
		_ = os.Remove(tmpFile) // 清理失败的临时文件
		return fmt.Errorf("rename %s -> %s: %w", tmpFile, targetPath, err)
	}

	return nil
}

// RaftStateSize 返回 Raft 状态的字节数
func (fp *FilePersister) RaftStateSize() int {
	fp.mu.RLock()
	defer fp.mu.RUnlock()

	info, err := os.Stat(fp.raftStateFile)
	if err != nil {
		return 0
	}
	return int(info.Size())
}

// Close 清理资源（如果需要）
func (fp *FilePersister) Close() error {
	// 文件系统无需显式关闭，但可以清理临时目录
	return nil
}
