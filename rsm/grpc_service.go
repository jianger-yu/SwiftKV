package rsm

import (
	"context"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	kvraftapi "kvraft/raftkv/rpc"
	"kvraft/watch"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// grpcKVService 将现有 KVServer 能力暴露为 gRPC 接口。
type grpcKVService struct {
	pb.UnimplementedKVServiceServer
	kv *KVServer
}

func grpcAddrFromRPC(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return addr
	}
	return net.JoinHostPort(host, strconv.Itoa(p+1000))
}

func errReply(e kvraftapi.Err) string {
	return string(e)
}

func (s *grpcKVService) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if s.kv.killed() {
		return &pb.GetResponse{Error: errReply(kvraftapi.ErrWrongLeader)}, nil
	}

	err, ret := s.kv.rsm.Submit(&kvraftapi.GetArgs{Key: req.GetKey()})
	if err != kvraftapi.OK {
		return &pb.GetResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(kvraftapi.GetReply)
	if !ok {
		return &pb.GetResponse{Error: "ErrInternal"}, nil
	}

	return &pb.GetResponse{
		Value:   reply.Value,
		Version: int64(reply.Version),
		Error:   errReply(reply.Err),
	}, nil
}

func (s *grpcKVService) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if s.kv.killed() {
		return &pb.PutResponse{Error: errReply(kvraftapi.ErrWrongLeader)}, nil
	}

	args := &kvraftapi.PutArgs{Key: req.GetKey(), Value: req.GetValue(), Version: kvraftapi.Tversion(req.GetVersion())}
	err, ret := s.kv.rsm.Submit(args)
	if err != kvraftapi.OK {
		return &pb.PutResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(kvraftapi.PutReply)
	if !ok {
		return &pb.PutResponse{Error: "ErrInternal"}, nil
	}

	return &pb.PutResponse{Error: errReply(reply.Err)}, nil
}

func (s *grpcKVService) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if s.kv.killed() {
		return &pb.DeleteResponse{Error: errReply(kvraftapi.ErrWrongLeader)}, nil
	}

	err, ret := s.kv.rsm.Submit(&kvraftapi.DeleteArgs{Key: req.GetKey()})
	if err != kvraftapi.OK {
		return &pb.DeleteResponse{Error: errReply(err)}, nil
	}

	reply, ok := ret.(kvraftapi.DeleteReply)
	if !ok {
		return &pb.DeleteResponse{Error: "ErrInternal"}, nil
	}

	return &pb.DeleteResponse{Error: errReply(reply.Err)}, nil
}

func (s *grpcKVService) Scan(ctx context.Context, req *pb.ScanRequest) (*pb.ScanResponse, error) {
	if s.kv.killed() {
		return &pb.ScanResponse{Error: errReply(kvraftapi.ErrWrongLeader)}, nil
	}

	all, err := s.kv.store.GetAll()
	if err != nil {
		return &pb.ScanResponse{Error: errReply(kvraftapi.ErrWrongLeader)}, nil
	}

	prefix := req.GetPrefix()
	keys := make([]string, 0, len(all))
	for k := range all {
		if prefix == "" || strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	limit := int(req.GetLimit())
	if limit <= 0 || limit > len(keys) {
		limit = len(keys)
	}

	items := make([]*pb.KeyValue, 0, limit)
	for i := 0; i < limit; i++ {
		k := keys[i]
		v := all[k]
		items = append(items, &pb.KeyValue{Key: k, Value: v.Value, Version: int64(v.Version)})
	}

	return &pb.ScanResponse{Items: items, Error: errReply(kvraftapi.OK)}, nil
}

func (s *grpcKVService) Watch(stream grpc.BidiStreamingServer[pb.WatchRequest, pb.WatchEvent]) error {
	watchMgr := s.kv.rsm.GetWatchManager()
	if watchMgr == nil {
		return nil
	}

	var currentID int64
	var currentStop chan struct{}

	stopCurrent := func() {
		if currentStop != nil {
			close(currentStop)
			currentStop = nil
		}
		if currentID != 0 {
			_ = watchMgr.Unsubscribe(currentID)
			currentID = 0
		}
	}
	defer stopCurrent()

	for {
		req, err := stream.Recv()
		if err != nil {
			stopCurrent()
			return nil
		}

		switch t := req.GetRequestType().(type) {
		case *pb.WatchRequest_Create:
			// 同一流内重复 Create 时，先清理旧订阅，确保仅保留当前订阅。
			stopCurrent()

			w, subErr := watchMgr.Subscribe(t.Create.GetKey(), t.Create.GetPrefix())
			if subErr != nil {
				_ = stream.Send(&pb.WatchEvent{EventType: "error", NewValue: subErr.Error()})
				continue
			}
			currentID = w.ID

			stopCh := make(chan struct{})
			currentStop = stopCh
			go func(src <-chan watch.Event, watchID int64, stop <-chan struct{}) {
				for {
					select {
					case <-stop:
						return
					case e, ok := <-src:
						if !ok {
							return
						}
						ev := &pb.WatchEvent{
							WatchId:    watchID,
							Key:        e.Key,
							OldValue:   e.OldValue,
							NewValue:   e.NewValue,
							NewVersion: e.NewVersion,
							EventType:  strings.ToLower(e.EventType),
						}
						if sendErr := stream.Send(ev); sendErr != nil {
							return
						}
					}
				}
			}(w.Channel, w.ID, stopCh)
		case *pb.WatchRequest_Cancel:
			id := t.Cancel.GetWatchId()
			if id == 0 {
				id = currentID
			}
			if id != 0 {
				_ = watchMgr.Unsubscribe(id)
			}
			if currentStop != nil {
				close(currentStop)
				currentStop = nil
			}
			currentID = 0
			return nil
		}
	}
}

func (s *grpcKVService) GetClusterStatus(ctx context.Context, req *pb.ClusterStatusRequest) (*pb.ClusterStatusResponse, error) {
	term, isLeader := s.kv.rsm.Raft().GetState()
	node := &pb.NodeStatus{Id: int32(s.kv.me), Address: grpcAddrFromRPC(s.kv.address), IsLeader: isLeader, IsAlive: !s.kv.killed()}
	return &pb.ClusterStatusResponse{
		LeaderId:    fmt.Sprintf("node-%d", s.kv.me),
		Nodes:       []*pb.NodeStatus{node},
		CurrentTerm: int64(term),
		LastApplied: 0,
	}, nil
}

func startGRPCServer(kv *KVServer, rpcAddr string) (*grpc.Server, net.Listener) {
	grpcAddr := grpcAddrFromRPC(rpcAddr)
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("start grpc listener %s failed: %v", grpcAddr, err)
	}
	gs := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    2 * time.Minute,
			Timeout: 20 * time.Second,
		}),
	)
	pb.RegisterKVServiceServer(gs, &grpcKVService{kv: kv})
	go func() {
		if serveErr := gs.Serve(lis); serveErr != nil {
			log.Printf("grpc serve stopped on %s: %v", grpcAddr, serveErr)
		}
	}()

	// 避免服务刚启动时客户端短暂拨号失败。
	time.Sleep(20 * time.Millisecond)
	return gs, lis
}
