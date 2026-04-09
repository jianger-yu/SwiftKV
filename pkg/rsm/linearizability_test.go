package rsm

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kvraftapi "kvraft/pkg/raftapi"
	"kvraft/pkg/storage"
)

type linearOp struct {
	ID           int64
	ClientID     int
	Type         string
	Key          string
	Value        string
	PutVersion   kvraftapi.Tversion
	InvokeTime   time.Time
	ResponseTime time.Time
	Err          kvraftapi.Err
	GetValue     string
	GetVersion   kvraftapi.Tversion
}

type linearizabilityConfig struct {
	NumClients         int
	OpsPerClient       int
	KeySpace           int
	MaxLatencyMs       int
	EnableTimeoutCheck bool
	TestTimeout        time.Duration

	mu        sync.Mutex
	ops       []*linearOp
	timedOut  int32
	timeoutCh chan struct{}
}

func newLinearizabilityConfig() *linearizabilityConfig {
	return &linearizabilityConfig{
		NumClients:         6,
		OpsPerClient:       80,
		KeySpace:           24,
		MaxLatencyMs:       50,
		EnableTimeoutCheck: true,
		TestTimeout:        20 * time.Second,
		timeoutCh:          make(chan struct{}, 1),
	}
}

func (cfg *linearizabilityConfig) record(op *linearOp) {
	cfg.mu.Lock()
	cfg.ops = append(cfg.ops, op)
	cfg.mu.Unlock()
}

func (cfg *linearizabilityConfig) signalTimeout() {
	atomic.StoreInt32(&cfg.timedOut, 1)
	select {
	case cfg.timeoutCh <- struct{}{}:
	default:
	}
}

func (cfg *linearizabilityConfig) isTimedOut() bool {
	return atomic.LoadInt32(&cfg.timedOut) == 1
}

// CheckTimeout 检查是否存在活锁或全局超时。
func (cfg *linearizabilityConfig) CheckTimeout() error {
	if !cfg.EnableTimeoutCheck {
		return nil
	}
	select {
	case <-cfg.timeoutCh:
		return fmt.Errorf("detected timeout/livelock during linearizability run")
	default:
		return nil
	}
}

func randomKey(r *rand.Rand, keySpace int) string {
	return fmt.Sprintf("k-%d", r.Intn(keySpace))
}

func randomValue(r *rand.Rand) string {
	return fmt.Sprintf("v-%d", r.Int63())
}

func randomOpType(r *rand.Rand) string {
	// 45% get, 40% put, 15% delete
	x := r.Intn(100)
	if x < 45 {
		return "get"
	}
	if x < 85 {
		return "put"
	}
	return "delete"
}

func runOneOperation(kv *KVServer, cfg *linearizabilityConfig, r *rand.Rand, clientID int, id int64) {
	opType := randomOpType(r)
	key := randomKey(r, cfg.KeySpace)
	value := randomValue(r)
	putVersion := kvraftapi.Tversion(r.Intn(6))

	op := &linearOp{
		ID:         id,
		ClientID:   clientID,
		Type:       opType,
		Key:        key,
		Value:      value,
		PutVersion: putVersion,
		InvokeTime: time.Now(),
	}

	if cfg.MaxLatencyMs > 0 {
		time.Sleep(time.Duration(r.Intn(cfg.MaxLatencyMs)) * time.Millisecond)
	}

	switch opType {
	case "get":
		reply := kv.DoOp(&kvraftapi.GetArgs{Key: key}).(kvraftapi.GetReply)
		op.Err = reply.Err
		op.GetValue = reply.Value
		op.GetVersion = reply.Version
	case "put":
		reply := kv.DoOp(&kvraftapi.PutArgs{Key: key, Value: value, Version: putVersion}).(kvraftapi.PutReply)
		op.Err = reply.Err
	case "delete":
		reply := kv.DoOp(&kvraftapi.DeleteArgs{Key: key}).(kvraftapi.DeleteReply)
		op.Err = reply.Err
	}

	op.ResponseTime = time.Now()
	cfg.record(op)
}

func checkLinearizabilityWithOracle(ops []*linearOp) (bool, []string) {
	ordered := append([]*linearOp(nil), ops...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].ResponseTime.Equal(ordered[j].ResponseTime) {
			return ordered[i].ID < ordered[j].ID
		}
		return ordered[i].ResponseTime.Before(ordered[j].ResponseTime)
	})

	// Oracle: 内存中的逻辑状态机（key -> value/version）。
	type state struct {
		Value   string
		Version kvraftapi.Tversion
	}
	oracle := make(map[string]state)
	violations := make([]string, 0)

	for _, op := range ordered {
		s, exists := oracle[op.Key]

		switch op.Type {
		case "get":
			if op.Err == kvraftapi.OK {
				if !exists {
					violations = append(violations, fmt.Sprintf("get(%s)=OK but oracle has no key", op.Key))
					continue
				}
				if s.Value != op.GetValue || s.Version != op.GetVersion {
					violations = append(violations,
						fmt.Sprintf("get(%s) mismatch: got=(%q,v=%d), oracle=(%q,v=%d)",
							op.Key, op.GetValue, op.GetVersion, s.Value, s.Version))
				}
			} else if op.Err == kvraftapi.ErrNoKey {
				if exists {
					violations = append(violations, fmt.Sprintf("get(%s)=ErrNoKey but oracle has key", op.Key))
				}
			}
		case "put":
			if op.Err == kvraftapi.OK {
				expected := kvraftapi.Tversion(0)
				if exists {
					expected = s.Version
				}
				if op.PutVersion != expected {
					violations = append(violations,
						fmt.Sprintf("put(%s) success with version=%d, expected=%d", op.Key, op.PutVersion, expected))
					continue
				}
				oracle[op.Key] = state{Value: op.Value, Version: expected + 1}
			}
		case "delete":
			if op.Err == kvraftapi.OK {
				if !exists {
					violations = append(violations, fmt.Sprintf("delete(%s)=OK but oracle has no key", op.Key))
					continue
				}
				delete(oracle, op.Key)
			}
		}
	}

	return len(violations) == 0, violations
}

func runLinearizabilityScenario(t *testing.T, cfg *linearizabilityConfig) {
	t.Helper()

	storePath := rsmTestStorePath(t, fmt.Sprintf("test-db-linear-%d", time.Now().UnixNano()))
	store, err := storage.NewStore(storePath)
	if err != nil {
		t.Fatalf("create store failed: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	kv := &KVServer{
		me:    0,
		dead:  0,
		store: store,
		stats: &ServerStats{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.TestTimeout)
	defer cancel()

	var idCounter int64
	var wg sync.WaitGroup
	for cid := 0; cid < cfg.NumClients; cid++ {
		cid := cid
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(cid)*7919))
			for i := 0; i < cfg.OpsPerClient; i++ {
				if cfg.isTimedOut() {
					return
				}
				select {
				case <-ctx.Done():
					cfg.signalTimeout()
					return
				default:
				}
				id := atomic.AddInt64(&idCounter, 1)
				runOneOperation(kv, cfg, r, cid, id)
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		cfg.signalTimeout()
		<-done
	}

	if err := cfg.CheckTimeout(); err != nil {
		t.Fatalf("timeout/livelock check failed: %v", err)
	}

	cfg.mu.Lock()
	recorded := append([]*linearOp(nil), cfg.ops...)
	cfg.mu.Unlock()

	ok, violations := checkLinearizabilityWithOracle(recorded)
	if !ok {
		maxShow := 10
		if len(violations) < maxShow {
			maxShow = len(violations)
		}
		for i := 0; i < maxShow; i++ {
			t.Logf("violation[%d]: %s", i, violations[i])
		}
		t.Fatalf("linearizability check failed: %d violations, operations=%d", len(violations), len(recorded))
	}

	t.Logf("linearizability passed: operations=%d", len(recorded))
}

func TestLinearizability(t *testing.T) {
	cfg := newLinearizabilityConfig()
	runLinearizabilityScenario(t, cfg)
}

func TestLinearizabilityWithNetworkFaults(t *testing.T) {
	cfg := newLinearizabilityConfig()
	cfg.NumClients = 4
	cfg.OpsPerClient = 60
	cfg.KeySpace = 16
	cfg.MaxLatencyMs = 180
	cfg.TestTimeout = 25 * time.Second
	runLinearizabilityScenario(t, cfg)
}
