package wal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Entry 记录了已被 Raft 提交的状态机操作。
type Entry struct {
	RaftIndex int64  `json:"raft_index"`
	Term      int    `json:"term"`
	NodeID    int    `json:"node_id"`
	ReqID     int64  `json:"req_id"`
	OpType    string `json:"op_type"`
	Key       string `json:"key,omitempty"`
	Timestamp int64  `json:"timestamp"`
}

// Logger persists entries in JSONL format for easy audit and recovery.
type Logger struct {
	mu      sync.Mutex
	path    string
	file    *os.File
	writer  *bufio.Writer
	enabled bool
}

func NewLogger(path string, enabled bool) (*Logger, error) {
	l := &Logger{path: path, enabled: enabled}
	if !enabled {
		return l, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal file: %w", err)
	}
	l.file = f
	l.writer = bufio.NewWriterSize(f, 64*1024)
	return l, nil
}

func (l *Logger) Enabled() bool {
	return l != nil && l.enabled
}

func (l *Logger) Append(entry Entry) error {
	if l == nil || !l.enabled {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil || l.writer == nil {
		return errors.New("wal logger is closed")
	}

	if entry.Timestamp == 0 {
		entry.Timestamp = time.Now().UnixNano()
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal wal entry: %w", err)
	}
	if _, err := l.writer.Write(data); err != nil {
		return fmt.Errorf("write wal entry: %w", err)
	}
	if err := l.writer.WriteByte('\n'); err != nil {
		return fmt.Errorf("write wal newline: %w", err)
	}
	if err := l.writer.Flush(); err != nil {
		return fmt.Errorf("flush wal entry: %w", err)
	}
	return nil
}

// TruncateUpTo 保留 raft_index > upToIndex 的条目。
func (l *Logger) TruncateUpTo(upToIndex int64) error {
	if l == nil || !l.enabled {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file == nil || l.writer == nil {
		return errors.New("wal logger is closed")
	}
	if err := l.writer.Flush(); err != nil {
		return fmt.Errorf("flush wal before truncate: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("close wal before truncate: %w", err)
	}

	input, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("open wal for truncate: %w", err)
	}
	defer input.Close()

	tmpPath := l.path + ".tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open wal tmp: %w", err)
	}

	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry Entry
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if entry.RaftIndex <= upToIndex {
			continue
		}
		if _, err := tmp.Write(line); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("write wal tmp: %w", err)
		}
		if _, err := tmp.Write([]byte("\n")); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("write wal tmp newline: %w", err)
		}
	}
	if err := scanner.Err(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("scan wal for truncate: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close wal tmp: %w", err)
	}
	if err := os.Rename(tmpPath, l.path); err != nil {
		return fmt.Errorf("replace wal after truncate: %w", err)
	}

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("reopen wal after truncate: %w", err)
	}
	l.file = f
	l.writer = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func (l *Logger) Close() error {
	if l == nil || !l.enabled {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil || l.writer == nil {
		return nil
	}
	if err := l.writer.Flush(); err != nil {
		_ = l.file.Close()
		l.file = nil
		l.writer = nil
		return fmt.Errorf("flush wal close: %w", err)
	}
	err := l.file.Close()
	l.file = nil
	l.writer = nil
	if err != nil {
		return fmt.Errorf("close wal file: %w", err)
	}
	return nil
}
