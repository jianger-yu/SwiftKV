package sharding

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"

	"google.golang.org/grpc"
)

type testKVService struct {
	pb.UnimplementedKVServiceServer

	mu                sync.Mutex
	kv                map[string]*pb.KeyValue
	alwaysWrongLeader bool
}

func (s *testKVService) Get(_ context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.alwaysWrongLeader {
		return &pb.GetResponse{Error: "ErrWrongLeader"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	it, ok := s.kv[req.GetKey()]
	if !ok {
		return &pb.GetResponse{Error: "ErrNoKey"}, nil
	}
	return &pb.GetResponse{Value: it.GetValue(), Version: it.GetVersion(), Error: "OK"}, nil
}

func (s *testKVService) Put(_ context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.alwaysWrongLeader {
		return &pb.PutResponse{Error: "ErrWrongLeader"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.kv[req.GetKey()]
	if !ok {
		if req.GetVersion() != 0 {
			return &pb.PutResponse{Error: "ErrNoKey"}, nil
		}
		s.kv[req.GetKey()] = &pb.KeyValue{Key: req.GetKey(), Value: req.GetValue(), Version: 1}
		return &pb.PutResponse{Error: "OK"}, nil
	}
	if cur.GetVersion() != req.GetVersion() {
		return &pb.PutResponse{Error: "ErrVersion"}, nil
	}
	next := cur.GetVersion() + 1
	s.kv[req.GetKey()] = &pb.KeyValue{Key: req.GetKey(), Value: req.GetValue(), Version: next}
	return &pb.PutResponse{Error: "OK"}, nil
}

func (s *testKVService) Delete(_ context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.alwaysWrongLeader {
		return &pb.DeleteResponse{Error: "ErrWrongLeader"}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.kv[req.GetKey()]; !ok {
		return &pb.DeleteResponse{Error: "ErrNoKey"}, nil
	}
	delete(s.kv, req.GetKey())
	return &pb.DeleteResponse{Error: "OK"}, nil
}

func (s *testKVService) Scan(_ context.Context, _ *pb.ScanRequest) (*pb.ScanResponse, error) {
	return &pb.ScanResponse{Error: "OK"}, nil
}

func (s *testKVService) Watch(_ grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchEvent]) error {
	return nil
}

func (s *testKVService) GetClusterStatus(_ context.Context, _ *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	return &pb.ClusterStatusResponse{CurrentTerm: 1}, nil
}

func startTestKVServer(t *testing.T, svc *testKVService) (addr string, stop func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen failed: %v", err)
	}

	gs := grpc.NewServer()
	pb.RegisterKVServiceServer(gs, svc)

	go func() {
		_ = gs.Serve(lis)
	}()

	return lis.Addr().String(), func() {
		gs.Stop()
		_ = lis.Close()
	}
}

func findKeyForGroup(t *testing.T, r *ShardRouter, gid int, prefix string) string {
	t.Helper()
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("%s-%d", prefix, i)
		if r.Resolve(k) == gid {
			return k
		}
	}
	t.Fatalf("cannot find key for group %d", gid)
	return ""
}

func TestShardRouterResolveAndCRUD(t *testing.T) {
	s1 := &testKVService{kv: map[string]*pb.KeyValue{}}
	a1, stop1 := startTestKVServer(t, s1)
	defer stop1()

	s2 := &testKVService{kv: map[string]*pb.KeyValue{}}
	a2, stop2 := startTestKVServer(t, s2)
	defer stop2()

	r, err := NewShardRouter(ShardingConfig{
		Groups: []RaftGroupConfig{
			{GroupID: 1, Replicas: []string{a1}, LeaderIdx: 0},
			{GroupID: 2, Replicas: []string{a2}, LeaderIdx: 0},
		},
		VirtualNodeCount: 64,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new shard router failed: %v", err)
	}
	defer r.Close()

	key1 := findKeyForGroup(t, r, 1, "g1")
	key2 := findKeyForGroup(t, r, 2, "g2")

	ctx := context.Background()
	if _, err := r.PutRoute(ctx, key1, "v1", 0); err != nil {
		t.Fatalf("put key1 failed: %v", err)
	}
	if _, err := r.PutRoute(ctx, key2, "v2", 0); err != nil {
		t.Fatalf("put key2 failed: %v", err)
	}

	g1, err := r.GetRoute(ctx, key1)
	if err != nil || g1.GetError() != "OK" || g1.GetValue() != "v1" {
		t.Fatalf("get key1 unexpected: resp=%+v err=%v", g1, err)
	}
	g2, err := r.GetRoute(ctx, key2)
	if err != nil || g2.GetError() != "OK" || g2.GetValue() != "v2" {
		t.Fatalf("get key2 unexpected: resp=%+v err=%v", g2, err)
	}
}

func TestShardRouterFailoverWithinGroup(t *testing.T) {
	wrongLeaderSvc := &testKVService{kv: map[string]*pb.KeyValue{}, alwaysWrongLeader: true}
	wrongAddr, stopWrong := startTestKVServer(t, wrongLeaderSvc)
	defer stopWrong()

	leaderSvc := &testKVService{kv: map[string]*pb.KeyValue{}}
	leaderAddr, stopLeader := startTestKVServer(t, leaderSvc)
	defer stopLeader()

	r, err := NewShardRouter(ShardingConfig{
		Groups: []RaftGroupConfig{
			{GroupID: 1, Replicas: []string{wrongAddr, leaderAddr}, LeaderIdx: 0},
		},
		VirtualNodeCount: 32,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new shard router failed: %v", err)
	}
	defer r.Close()

	key := findKeyForGroup(t, r, 1, "failover")

	putResp, err := r.PutRoute(context.Background(), key, "ok-after-failover", 0)
	if err != nil {
		t.Fatalf("put with failover failed: %v", err)
	}
	if putResp.GetError() != "OK" {
		t.Fatalf("put error: %s", putResp.GetError())
	}

	if got := r.leaderCache[1]; got != leaderAddr {
		t.Fatalf("leader cache not updated, want=%s got=%s", leaderAddr, got)
	}
}

func TestShardRouterBatchGet(t *testing.T) {
	s1 := &testKVService{kv: map[string]*pb.KeyValue{}}
	a1, stop1 := startTestKVServer(t, s1)
	defer stop1()

	s2 := &testKVService{kv: map[string]*pb.KeyValue{}}
	a2, stop2 := startTestKVServer(t, s2)
	defer stop2()

	r, err := NewShardRouter(ShardingConfig{
		Groups: []RaftGroupConfig{
			{GroupID: 1, Replicas: []string{a1}, LeaderIdx: 0},
			{GroupID: 2, Replicas: []string{a2}, LeaderIdx: 0},
		},
		VirtualNodeCount: 64,
		ConnectTimeout:   2 * time.Second,
		RequestTimeout:   1200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new shard router failed: %v", err)
	}
	defer r.Close()

	key1 := findKeyForGroup(t, r, 1, "batch-g1")
	key2 := findKeyForGroup(t, r, 2, "batch-g2")
	key3 := findKeyForGroup(t, r, 1, "batch-g1-extra")

	_, _ = r.PutRoute(context.Background(), key1, "v1", 0)
	_, _ = r.PutRoute(context.Background(), key2, "v2", 0)
	_, _ = r.PutRoute(context.Background(), key3, "v3", 0)

	results := r.BatchGet(context.Background(), []string{key1, key2, key3, "missing-key"})
	if len(results) != 4 {
		t.Fatalf("batch get result size mismatch, want=4 got=%d", len(results))
	}
	if results[key1].GetValue() != "v1" || results[key2].GetValue() != "v2" || results[key3].GetValue() != "v3" {
		t.Fatalf("batch get returned unexpected values")
	}
	if miss := results["missing-key"]; miss == nil || miss.GetError() != "ErrNoKey" {
		t.Fatalf("missing key response mismatch: %+v", miss)
	}
}
