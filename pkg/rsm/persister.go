package rsm

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	manifestFileName = "manifest.json"
	defaultBatchSize = 64
)

type persisterManifest struct {
	Generation   uint64 `json:"generation"`
	RaftStateRel string `json:"raft_state_rel"`
	SnapshotRel  string `json:"snapshot_rel"`
	UpdatedAt    int64  `json:"updated_at"`
}

type saveRequest struct {
	raftstate []byte
	snapshot  []byte
	done      chan error
}

// FilePersister 基于文件系统的持久化器实现
// 原子性写入：先写临时文件，再重命名，确保崩溃时不损坏
type FilePersister struct {
	mu            sync.RWMutex
	dataDir       string
	raftStateFile string
	snapshotFile  string
	manifestPath  string
	tmpDir        string

	manifest   persisterManifest
	batchSize  int
	reqCh      chan saveRequest
	stopCh     chan struct{}
	workerDone chan struct{}
	closed     bool
}

// NewFilePersister 创建基于文件的持久化器
// dataDir: 数据目录，例如 "badger-127.0.0.1:15000/"
func NewFilePersister(dataDir string) (*FilePersister, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dataDir, err)
	}

	fp := &FilePersister{
		dataDir:       dataDir,
		raftStateFile: filepath.Join(dataDir, "raft-state.bin"),
		snapshotFile:  filepath.Join(dataDir, "snapshot.bin"),
		manifestPath:  filepath.Join(dataDir, manifestFileName),
		tmpDir:        filepath.Join(dataDir, ".tmp"),
		batchSize:     defaultBatchSize,
	}

	if err := os.MkdirAll(fp.tmpDir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir tmp dir: %w", err)
	}

	if err := fp.loadManifest(); err != nil {
		return nil, err
	}

	fp.reqCh = make(chan saveRequest, 1024)
	fp.stopCh = make(chan struct{})
	fp.workerDone = make(chan struct{})
	go fp.runSaveWorker()

	return fp, nil
}

// ReadRaftState 读取 Raft 状态（日志+任期+投票）
func (fp *FilePersister) ReadRaftState() []byte {
	fp.mu.RLock()
	path := fp.currentRaftStatePathLocked()
	defer fp.mu.RUnlock()

	data, _ := os.ReadFile(path)
	return data
}

// ReadSnapshot 读取快照数据
func (fp *FilePersister) ReadSnapshot() []byte {
	fp.mu.RLock()
	path := fp.currentSnapshotPathLocked()
	defer fp.mu.RUnlock()

	data, _ := os.ReadFile(path)
	return data
}

// Save 原子地保存 Raft 状态和快照
// 使用 write-then-rename 模式避免崩溃损坏
func (fp *FilePersister) Save(raftstate []byte, snapshot []byte) {
	fp.mu.RLock()
	if fp.closed || fp.reqCh == nil || fp.stopCh == nil {
		fp.mu.RUnlock()
		fmt.Fprintf(os.Stderr, "[FilePersister] save ignored: persister closed\n")
		return
	}
	reqCh := fp.reqCh
	stopCh := fp.stopCh
	fp.mu.RUnlock()

	req := saveRequest{
		raftstate: append([]byte(nil), raftstate...),
		snapshot:  append([]byte(nil), snapshot...),
		done:      make(chan error, 1),
	}

	select {
	case reqCh <- req:
	case <-stopCh:
		fmt.Fprintf(os.Stderr, "[FilePersister] save dropped: persister stopping\n")
		return
	}

	if err := <-req.done; err != nil {
		fmt.Fprintf(os.Stderr, "[FilePersister] failed to save state/snapshot: %v\n", err)
	}
}

func (fp *FilePersister) runSaveWorker() {
	defer close(fp.workerDone)

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	batch := make([]saveRequest, 0, fp.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		latest := batch[len(batch)-1]
		err := fp.commitGeneration(latest.raftstate, latest.snapshot)
		for i := range batch {
			batch[i].done <- err
			close(batch[i].done)
		}
		batch = batch[:0]
	}

	for {
		select {
		case req := <-fp.reqCh:
			batch = append(batch, req)
			if len(batch) >= fp.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-fp.stopCh:
			flush()
			return
		}
	}
}

func (fp *FilePersister) commitGeneration(raftstate []byte, snapshot []byte) error {
	fp.mu.Lock()
	nextGen := fp.manifest.Generation + 1
	fp.mu.Unlock()

	raftRel := fmt.Sprintf("raft-state.%020d.bin", nextGen)
	snapshotRel := fmt.Sprintf("snapshot.%020d.bin", nextGen)
	raftPath := filepath.Join(fp.dataDir, raftRel)
	snapshotPath := filepath.Join(fp.dataDir, snapshotRel)

	if err := fp.durableWrite(raftPath, raftstate); err != nil {
		return fmt.Errorf("durable write raft state: %w", err)
	}
	if err := fp.durableWrite(snapshotPath, snapshot); err != nil {
		return fmt.Errorf("durable write snapshot: %w", err)
	}

	m := persisterManifest{
		Generation:   nextGen,
		RaftStateRel: raftRel,
		SnapshotRel:  snapshotRel,
		UpdatedAt:    time.Now().UnixNano(),
	}
	manifestBytes, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := fp.durableWrite(fp.manifestPath, manifestBytes); err != nil {
		return fmt.Errorf("durable write manifest: %w", err)
	}

	fp.mu.Lock()
	fp.manifest = m
	fp.mu.Unlock()
	return nil
}

// durableWrite performs write+file sync+rename+directory sync.
func (fp *FilePersister) durableWrite(targetPath string, data []byte) error {
	targetDir := filepath.Dir(targetPath)
	tmpPath := filepath.Join(targetDir, fmt.Sprintf(".%s.tmp.%d", filepath.Base(targetPath), time.Now().UnixNano()))

	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp file: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename %s -> %s: %w", tmpPath, targetPath, err)
	}

	if err := syncDir(targetDir); err != nil {
		return fmt.Errorf("sync target dir: %w", err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (fp *FilePersister) loadManifest() error {
	raw, err := os.ReadFile(fp.manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			fp.manifest = fp.discoverLatestCompleteGeneration()
			return nil
		}
		return fmt.Errorf("read manifest: %w", err)
	}

	var m persisterManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		fp.manifest = fp.discoverLatestCompleteGeneration()
		return nil
	}

	if m.RaftStateRel == "" || m.SnapshotRel == "" {
		fp.manifest = fp.discoverLatestCompleteGeneration()
		return nil
	}

	if _, err := os.Stat(filepath.Join(fp.dataDir, m.RaftStateRel)); err != nil {
		fp.manifest = fp.discoverLatestCompleteGeneration()
		return nil
	}
	if _, err := os.Stat(filepath.Join(fp.dataDir, m.SnapshotRel)); err != nil {
		fp.manifest = fp.discoverLatestCompleteGeneration()
		return nil
	}

	fp.manifest = m
	return nil
}

func (fp *FilePersister) discoverLatestCompleteGeneration() persisterManifest {
	entries, err := os.ReadDir(fp.dataDir)
	if err != nil {
		return persisterManifest{}
	}

	raftByGen := make(map[uint64]string)
	snapByGen := make(map[uint64]string)

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if strings.HasPrefix(name, "raft-state.") && strings.HasSuffix(name, ".bin") {
			gen, ok := parseGeneration(name, "raft-state.", ".bin")
			if ok {
				raftByGen[gen] = name
			}
			continue
		}
		if strings.HasPrefix(name, "snapshot.") && strings.HasSuffix(name, ".bin") {
			gen, ok := parseGeneration(name, "snapshot.", ".bin")
			if ok {
				snapByGen[gen] = name
			}
		}
	}

	var best uint64
	for gen := range raftByGen {
		if _, ok := snapByGen[gen]; !ok {
			continue
		}
		if gen > best {
			best = gen
		}
	}

	if best == 0 {
		return persisterManifest{}
	}

	return persisterManifest{
		Generation:   best,
		RaftStateRel: raftByGen[best],
		SnapshotRel:  snapByGen[best],
		UpdatedAt:    time.Now().UnixNano(),
	}
}

func parseGeneration(name, prefix, suffix string) (uint64, bool) {
	if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	raw := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
	gen, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return gen, true
}

func (fp *FilePersister) currentRaftStatePathLocked() string {
	if fp.manifest.RaftStateRel != "" {
		return filepath.Join(fp.dataDir, fp.manifest.RaftStateRel)
	}
	return fp.raftStateFile
}

func (fp *FilePersister) currentSnapshotPathLocked() string {
	if fp.manifest.SnapshotRel != "" {
		return filepath.Join(fp.dataDir, fp.manifest.SnapshotRel)
	}
	return fp.snapshotFile
}

// RaftStateSize 返回 Raft 状态的字节数
func (fp *FilePersister) RaftStateSize() int {
	fp.mu.RLock()
	path := fp.currentRaftStatePathLocked()
	defer fp.mu.RUnlock()

	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return int(info.Size())
}

// Close 清理资源（如果需要）
func (fp *FilePersister) Close() error {
	fp.mu.Lock()
	if fp.closed {
		fp.mu.Unlock()
		return nil
	}
	fp.closed = true
	stopCh := fp.stopCh
	workerDone := fp.workerDone
	fp.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
	}
	if workerDone != nil {
		<-workerDone
	}
	return nil
}
