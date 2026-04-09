package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
)

// kvEntry 代表存储中的一个键值对条目
type kvEntry struct {
	Value   string `json:"value"`
	Version uint64 `json:"version"`
	Expires int64  `json:"expires"`
}

// Store 是基于BadgerDB的持久化存储实现
type Store struct {
	mu     sync.RWMutex
	db     *badger.DB
	dbPath string
}

type PutCASStatus int

const (
	PutCASOK PutCASStatus = iota
	PutCASNoKey
	PutCASVersionMismatch
)

func badgerOptions(dbPath string) badger.Options {
	return badger.DefaultOptions(dbPath).WithLoggingLevel(badger.WARNING)
}

func openBadger(dbPath string) (*badger.DB, error) {
	return badger.Open(badgerOptions(dbPath))
}

// NewStore 创建一个新的Store实例
func NewStore(dbPath string) (*Store, error) {
	// 确保目录存在
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dbPath, 0755); err != nil {
			return nil, fmt.Errorf("创建数据库目录失败: %w", err)
		}
	}

	// 打开BadgerDB数据库
	db, err := openBadger(dbPath)
	if err != nil {
		return nil, fmt.Errorf("打开BadgerDB失败: %w", err)
	}

	return &Store{
		db:     db,
		dbPath: dbPath,
	}, nil
}

// Get 返回一个键的值、版本号和绝对过期时间戳。
func (s *Store) Get(key string) (value string, version uint64, expires int64, exists bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err == badger.ErrKeyNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		data, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}

		var entry kvEntry
		if err := json.Unmarshal(data, &entry); err != nil {
			return err
		}

		value = entry.Value
		version = entry.Version
		expires = entry.Expires
		exists = true
		return nil
	})

	return
}

// Put stores key/value/version with optional absolute expiry unix nano.
func (s *Store) Put(key, value string, version uint64, expires int64) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry := kvEntry{
		Value:   value,
		Version: version,
		Expires: expires,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("序列化失败: %w", err)
	}

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), data)
	})
}

// PutCASWithTTL performs version-check and write in a single transaction.
func (s *Store) PutCASWithTTL(key, value string, expectedVersion uint64, expires int64) (oldValue string, status PutCASStatus, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	status = PutCASOK
	err = s.db.Update(func(txn *badger.Txn) error {
		item, getErr := txn.Get([]byte(key))
		if getErr == badger.ErrKeyNotFound {
			if expectedVersion != 0 {
				status = PutCASNoKey
				return nil
			}
			entry := kvEntry{Value: value, Version: 1, Expires: expires}
			data, marshalErr := json.Marshal(entry)
			if marshalErr != nil {
				return fmt.Errorf("序列化失败: %w", marshalErr)
			}
			return txn.Set([]byte(key), data)
		}
		if getErr != nil {
			return getErr
		}

		raw, copyErr := item.ValueCopy(nil)
		if copyErr != nil {
			return copyErr
		}
		var cur kvEntry
		if unmarshalErr := json.Unmarshal(raw, &cur); unmarshalErr != nil {
			return unmarshalErr
		}

		now := time.Now().UnixNano()
		exists := !(cur.Expires > 0 && cur.Expires <= now)
		if !exists {
			if expectedVersion != 0 {
				status = PutCASNoKey
				return nil
			}
			entry := kvEntry{Value: value, Version: 1, Expires: expires}
			data, marshalErr := json.Marshal(entry)
			if marshalErr != nil {
				return fmt.Errorf("序列化失败: %w", marshalErr)
			}
			return txn.Set([]byte(key), data)
		}

		if cur.Version != expectedVersion {
			status = PutCASVersionMismatch
			return nil
		}

		oldValue = cur.Value
		entry := kvEntry{Value: value, Version: cur.Version + 1, Expires: expires}
		data, marshalErr := json.Marshal(entry)
		if marshalErr != nil {
			return fmt.Errorf("序列化失败: %w", marshalErr)
		}
		return txn.Set([]byte(key), data)
	})

	return oldValue, status, err
}

// GetExpiredKeys 返回所有过期时间 <= cutoff 阀值的键。
func (s *Store) GetExpiredKeys(cutoff int64, limit int) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if limit <= 0 {
		limit = 128
	}
	keys := make([]string, 0, limit)
	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			if len(keys) >= limit {
				break
			}
			item := it.Item()
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			var entry kvEntry
			if err := json.Unmarshal(val, &entry); err != nil {
				continue
			}
			if entry.Expires > 0 && entry.Expires <= cutoff {
				keys = append(keys, string(item.Key()))
			}
		}
		return nil
	})
	return keys, err
}

// Delete 从存储中删除一个键
func (s *Store) Delete(key string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

// GetAll 获取所有键值对（用于加载快照或导出）
func (s *Store) GetAll() (map[string]kvEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data := make(map[string]kvEntry)

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := string(item.Key())

			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			var entry kvEntry
			if err := json.Unmarshal(val, &entry); err != nil {
				return err
			}

			data[key] = entry
		}
		return nil
	})

	return data, err
}

// LoadSnapshot 从编码的快照数据中恢复存储
func (s *Store) LoadSnapshot(snapshotData []byte) error {
	var snapshot map[string]kvEntry
	if err := json.Unmarshal(snapshotData, &snapshot); err != nil {
		return fmt.Errorf("反序列化快照失败: %w", err)
	}

	ts := time.Now().UnixNano()
	tmpPath := fmt.Sprintf("%s.restore.%d", s.dbPath, ts)
	backupPath := fmt.Sprintf("%s.backup.%d", s.dbPath, ts)

	if err := os.RemoveAll(tmpPath); err != nil {
		return fmt.Errorf("清理临时恢复目录失败: %w", err)
	}

	tmpDB, err := openBadger(tmpPath)
	if err != nil {
		return fmt.Errorf("打开临时恢复数据库失败: %w", err)
	}

	for key, entry := range snapshot {
		entryCopy := entry
		data, marshalErr := json.Marshal(entryCopy)
		if marshalErr != nil {
			_ = tmpDB.Close()
			_ = os.RemoveAll(tmpPath)
			return fmt.Errorf("序列化快照条目失败: %w", marshalErr)
		}
		if putErr := tmpDB.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte(key), data)
		}); putErr != nil {
			_ = tmpDB.Close()
			_ = os.RemoveAll(tmpPath)
			return fmt.Errorf("写入临时恢复数据库失败: %w", putErr)
		}
	}

	if err := tmpDB.Close(); err != nil {
		_ = os.RemoveAll(tmpPath)
		return fmt.Errorf("关闭临时恢复数据库失败: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			_ = os.RemoveAll(tmpPath)
			return fmt.Errorf("关闭当前数据库失败: %w", err)
		}
		s.db = nil
	}

	if err := os.RemoveAll(backupPath); err != nil {
		_ = os.RemoveAll(tmpPath)
		return fmt.Errorf("清理旧备份目录失败: %w", err)
	}

	if _, err := os.Stat(s.dbPath); err == nil {
		if err := os.Rename(s.dbPath, backupPath); err != nil {
			_ = os.RemoveAll(tmpPath)
			return fmt.Errorf("备份当前数据库失败: %w", err)
		}
	}

	if err := os.Rename(tmpPath, s.dbPath); err != nil {
		_ = os.Rename(backupPath, s.dbPath)
		return fmt.Errorf("切换恢复数据库失败: %w", err)
	}

	newDB, err := openBadger(s.dbPath)
	if err != nil {
		_ = os.RemoveAll(s.dbPath)
		_ = os.Rename(backupPath, s.dbPath)
		fallbackDB, fallbackErr := openBadger(s.dbPath)
		if fallbackErr == nil {
			s.db = fallbackDB
		}
		return fmt.Errorf("恢复后重新打开数据库失败: %w", err)
	}

	s.db = newDB
	_ = os.RemoveAll(backupPath)
	return nil
}

// SaveSnapshot 将当前存储导出为快照数据
func (s *Store) SaveSnapshot() ([]byte, error) {
	data, err := s.GetAll()
	if err != nil {
		return nil, err
	}

	return json.Marshal(data)
}

// Close 关闭数据库连接
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// GetStats 获取数据库统计信息
func (s *Store) GetStats() string {
	return "BadgerDB storage"
}

// Clear 清空所有数据（用于测试）
func (s *Store) Clear() error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.db.DropAll()
}

// DBPath 返回数据库路径
func (s *Store) DBPath() string {
	return s.dbPath
}

// DataSize 返回数据库大小（字节）
func (s *Store) DataSize() (int64, error) {
	var size int64
	err := filepath.Walk(s.dbPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})
	return size, err
}
