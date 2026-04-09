package rsm

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	kvraftapi "kvraft/pkg/raftapi"
	"kvraft/pkg/sharding"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

type Clerk struct {
	servers     []string
	grpcServers []string
	leader      int // 最近成功 leader 在 grpcServers 中的索引
	hasher      *sharding.ConsistentHash
	router      *sharding.ShardRouter
	serverToIdx map[string]int

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	clients map[string]pb.KVServiceClient
}

func rpcTimeout(defaultMs int) time.Duration {
	raw := strings.TrimSpace(os.Getenv("KV_RPC_TIMEOUT_MS"))
	if raw == "" {
		return time.Duration(defaultMs) * time.Millisecond
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return time.Duration(defaultMs) * time.Millisecond
	}
	return time.Duration(ms) * time.Millisecond
}

// WatchSubscription 表示一个可取消的 Watch 订阅。
type WatchSubscription struct {
	Events <-chan *pb.WatchEvent
	cancel context.CancelFunc
}

// Cancel 主动取消订阅。
func (s *WatchSubscription) Cancel() {
	if s != nil && s.cancel != nil {
		s.cancel()
	}
}

func MakeClerk(servers []string) *Clerk {
	grpcServers := make([]string, 0, len(servers))
	for _, s := range servers {
		grpcServers = append(grpcServers, toGRPCAddress(s))
	}

	ck := &Clerk{
		servers:     servers,
		grpcServers: grpcServers,
		leader:      0,
		hasher:      sharding.NewConsistentHash(32),
		serverToIdx: make(map[string]int, len(grpcServers)),
		conns:       make(map[string]*grpc.ClientConn, len(grpcServers)),
		clients:     make(map[string]pb.KVServiceClient, len(grpcServers)),
	}
	for i, s := range grpcServers {
		ck.serverToIdx[s] = i
		ck.hasher.AddNode(s)
	}
	// 预热连接池，减少首包延迟。
	for _, s := range grpcServers {
		_, _ = ck.getClient(s)
	}
	return ck
}

// MakeShardedClerk 创建基于分组路由器的 Clerk。
// 该模式下每个请求按 key 定位 group，并在组内自动故障切换。
func MakeShardedClerk(cfg sharding.ShardingConfig) (*Clerk, error) {
	router, err := sharding.NewShardRouter(cfg)
	if err != nil {
		return nil, err
	}

	ck := &Clerk{
		leader:      0,
		hasher:      nil,
		router:      router,
		serverToIdx: map[string]int{},
		conns:       map[string]*grpc.ClientConn{},
		clients:     map[string]pb.KVServiceClient{},
	}
	return ck, nil
}

func toGRPCAddress(raftAddr string) string {
	parts := strings.Split(raftAddr, ":")
	if len(parts) != 2 {
		return raftAddr
	}
	p, err := strconv.Atoi(parts[1])
	if err != nil {
		return raftAddr
	}
	return fmt.Sprintf("%s:%d", parts[0], p+1000)
}

// 返回业务逻辑存根
func (ck *Clerk) getClient(addr string) (pb.KVServiceClient, error) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if c, ok := ck.clients[addr]; ok {
		if conn := ck.conns[addr]; conn != nil && conn.GetState() == connectivity.Shutdown {
			delete(ck.clients, addr)
			delete(ck.conns, addr)
		} else {
			return c, nil
		}
	}

	// 若连接失效，新建 gRPC 连接与性能配置
	target := "passthrough:///" + addr // 使用 passthrough 方案绕过复杂的负载均衡解析，直接连接 IP
	conn, err := grpc.NewClient(
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()), // 使用不加密的明文传输。
		grpc.WithKeepaliveParams(keepalive.ClientParameters{ // 配置保活参数。
			Time:                2 * time.Minute,
			Timeout:             3 * time.Second,
			PermitWithoutStream: false,
		}),
	)
	if err != nil {
		return nil, err
	}

	// 显式触发连接建立并等待 Ready（受超时控制）。
	conn.Connect()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for conn.GetState() != connectivity.Ready {
		if !conn.WaitForStateChange(ctx, conn.GetState()) {
			break
		}
	}

	client := pb.NewKVServiceClient(conn)
	ck.conns[addr] = conn
	ck.clients[addr] = client
	return client, nil
}

// 为特定的 key 生成一个优先访问列表
func (ck *Clerk) preferredCandidates(key string) []int {
	if len(ck.grpcServers) == 0 || ck.hasher == nil {
		return []int{0}
	}

	// 计算一致性哈希推荐节点
	count := 3
	if len(ck.grpcServers) < count {
		count = len(ck.grpcServers)
	}

	nodes := ck.hasher.GetNodes(key, count)         // 哈希环推荐的节点地址切片
	seen := make(map[int]bool, len(ck.grpcServers)) // 去重查重表
	idxs := make([]int, 0, len(ck.grpcServers))     // 存储排序后的服务器下标，调用者会按这个顺序挨个尝试发请求。

	// 将哈希环返回的字符串地址转回数字下标
	for _, n := range nodes {
		if idx, ok := ck.serverToIdx[n]; ok && !seen[idx] {
			idxs = append(idxs, idx)
			seen[idx] = true
		}
	}

	// 单 Raft 组场景下，优先走最近成功 leader（把leader插入列表头部），可显著减少 WrongLeader 往返。
	if ck.leader >= 0 && ck.leader < len(ck.grpcServers) {
		if !seen[ck.leader] {
			idxs = append([]int{ck.leader}, idxs...)
			seen[ck.leader] = true
		} else {
			for i, v := range idxs {
				if v == ck.leader {
					idxs = append([]int{v}, append(idxs[:i], idxs[i+1:]...)...)
					break
				}
			}
		}
	}

	for i := range ck.grpcServers {
		if !seen[i] {
			idxs = append(idxs, i)
		}
	}

	if len(idxs) == 0 {
		return []int{0}
	}
	return idxs
}

// 解析报错信息中的 IP:Port
func parseRedirectLeaderAddr(msg string) string {
	re := regexp.MustCompile(`([a-zA-Z0-9._-]+:\d{2,5})`)
	m := re.FindStringSubmatch(msg)
	if len(m) == 0 {
		return ""
	}
	return m[0]
}

// 错误类型转换器
func mapPBErr(errText string) kvraftapi.Err {
	switch errText {
	case string(kvraftapi.OK), "":
		return kvraftapi.OK
	case string(kvraftapi.ErrNoKey):
		return kvraftapi.ErrNoKey
	case string(kvraftapi.ErrWrongLeader):
		return kvraftapi.ErrWrongLeader
	case string(kvraftapi.ErrVersion):
		return kvraftapi.ErrVersion
	case string(kvraftapi.ErrMaybe):
		return kvraftapi.ErrMaybe
	default:
		if strings.Contains(strings.ToLower(errText), "unimplemented") {
			return kvraftapi.ErrWrongLeader
		}
		if strings.Contains(strings.ToLower(errText), "wrong") && strings.Contains(strings.ToLower(errText), "leader") {
			return kvraftapi.ErrWrongLeader
		}
		return kvraftapi.ErrWrongLeader
	}
}

// Get 获取一个键的当前值、版本和过期时间（UnixNano 绝对时间）。如果键不存在，返回 ErrNoKey。
// 在面对所有其他错误时，它会不断重试。
//
// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数的
// 参数的声明类型相匹配。此外，reply 必须作为指针传递。
func (ck *Clerk) Get(key string) (string, kvraftapi.Tversion, int64, kvraftapi.Err) {
	if ck.router != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := ck.router.GetRoute(ctx, key)
		if err != nil || resp == nil {
			return "", 0, 0, kvraftapi.ErrWrongLeader
		}
		errCode := mapPBErr(resp.GetError())
		if errCode == kvraftapi.OK {
			return resp.GetValue(), kvraftapi.Tversion(resp.GetVersion()), resp.GetExpires(), kvraftapi.OK
		}
		if errCode == kvraftapi.ErrNoKey {
			return "", 0, 0, kvraftapi.ErrNoKey
		}
		return "", 0, 0, errCode
	}

	// 不是分片模式或分片路由未启用，进入传统模式
	args := kvraftapi.GetArgs{Key: key}
	timeout := time.After(10 * time.Second)
	attempts := 0

	for {
		select {
		case <-timeout:
			fmt.Println("\n⚠️  获取操作超时（10秒）")
			fmt.Println("提示: 请确保 KVraft 服务器已启动:")
			return "", 0, 0, kvraftapi.ErrWrongLeader
		default:
		}

		candidates := ck.preferredCandidates(key)
		for _, index := range candidates {
			addr := ck.grpcServers[index]
			client, err := ck.getClient(addr)
			if err != nil {
				attempts++
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout(250))
			resp, rpcErr := client.Get(ctx, &pb.GetRequest{Key: args.Key})
			cancel()

			if rpcErr == nil && resp != nil {
				errCode := mapPBErr(resp.GetError())
				if errCode == kvraftapi.OK {
					ck.leader = index
					return resp.GetValue(), kvraftapi.Tversion(resp.GetVersion()), resp.GetExpires(), kvraftapi.OK
				}
				if errCode == kvraftapi.ErrNoKey {
					return "", 0, 0, kvraftapi.ErrNoKey
				}
				if errCode == kvraftapi.ErrWrongLeader {
					if leader := parseRedirectLeaderAddr(resp.GetError()); leader != "" {
						leader = toGRPCAddress(leader)
						if idx, ok := ck.serverToIdx[leader]; ok {
							ck.leader = idx
						}
					}
				}
			} else if rpcErr != nil {
				if leader := parseRedirectLeaderAddr(rpcErr.Error()); leader != "" {
					leader = toGRPCAddress(leader)
					if idx, ok := ck.serverToIdx[leader]; ok {
						ck.leader = idx
					}
				}
			}

			attempts++
			if attempts > 100 {
				fmt.Printf("\n⚠️  Get 已尝试 %d 次，仍无可用服务器\n", attempts)
				fmt.Println("提示: 请确保 KVraft 服务器已启动:")
				return "", 0, 0, kvraftapi.ErrWrongLeader
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Put 仅当请求中的版本与服务器上该键的版本匹配时，才会使用值更新键。
// 如果版本号不匹配，服务器应返回 ErrVersion。如果 Put 在其第一个 RPC
// 上收到 ErrVersion，Put 应返回 ErrVersion，因为 Put 肯定没有在服务器上
// 执行。如果服务器在重新发送 RPC 时返回 ErrVersion，那么 Put 必须向应用
// 返回 ErrMaybe，因为其较早的 RPC 可能已被服务器成功处理，但响应丢失了，
// Clerk 不知道 Put 是否被执行了。
//
// 你可以使用如下代码向服务器 i 发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数的
// 参数的声明类型相匹配。此外，reply 必须作为指针传递。
func (ck *Clerk) Put(key string, value string, version kvraftapi.Tversion) kvraftapi.Err {
	if ck.router != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := ck.router.PutRoute(ctx, key, value, int64(version))
		if err != nil || resp == nil {
			return kvraftapi.ErrWrongLeader
		}
		return mapPBErr(resp.GetError())
	}

	args := kvraftapi.PutArgs{Key: key, Value: value, Version: version}
	retry := false
	timeout := time.After(10 * time.Second)
	attempts := 0

	for {
		select {
		case <-timeout:
			fmt.Println("\n⚠️  写入操作超时（10秒）")
			fmt.Println("提示: 请确保 KVraft 服务器已启动:")
			return kvraftapi.ErrWrongLeader
		default:
		}

		candidates := ck.preferredCandidates(key)
		for _, index := range candidates {
			addr := ck.grpcServers[index]
			client, err := ck.getClient(addr)
			if err != nil {
				attempts++
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout(250))
			resp, rpcErr := client.Put(ctx, &pb.PutRequest{Key: args.Key, Value: args.Value, Version: int64(args.Version)})
			cancel()
			if rpcErr == nil && resp != nil {
				errCode := mapPBErr(resp.GetError())
				switch errCode {
				case kvraftapi.OK, kvraftapi.ErrNoKey:
					ck.leader = index
					return errCode
				case kvraftapi.ErrVersion:
					ck.leader = index
					if !retry {
						return kvraftapi.ErrVersion
					}
					return kvraftapi.ErrMaybe
				case kvraftapi.ErrWrongLeader:
					if leader := parseRedirectLeaderAddr(resp.GetError()); leader != "" {
						leader = toGRPCAddress(leader)
						if idx, ok := ck.serverToIdx[leader]; ok {
							ck.leader = idx
						}
					}
				}
			} else if rpcErr != nil {
				if leader := parseRedirectLeaderAddr(rpcErr.Error()); leader != "" {
					leader = toGRPCAddress(leader)
					if idx, ok := ck.serverToIdx[leader]; ok {
						ck.leader = idx
					}
				}
			}

			attempts++
			if attempts > 100 {
				fmt.Printf("\n⚠️  Put 已尝试 %d 次，仍无可用服务器\n", attempts)
				fmt.Println("提示: 请确保 KVraft 服务器已启动:")
				return kvraftapi.ErrWrongLeader
			}
		}

		retry = true
		time.Sleep(10 * time.Millisecond)
	}
}

// PutWithTTL 修改一个键值对，带有TTL支持
func (ck *Clerk) PutWithTTL(key string, value string, version kvraftapi.Tversion, ttlSeconds int64) kvraftapi.Err {
	if ck.router != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := ck.router.PutRouteWithTTL(ctx, key, value, int64(version), ttlSeconds)
		if err != nil || resp == nil {
			return kvraftapi.ErrWrongLeader
		}
		return mapPBErr(resp.GetError())
	}

	args := kvraftapi.PutArgs{Key: key, Value: value, Version: version, TTL: ttlSeconds}
	retry := false
	timeout := time.After(10 * time.Second)
	attempts := 0

	for {
		select {
		case <-timeout:
			fmt.Println("\n⚠️  写入操作超时（10秒）")
			fmt.Println("提示: 请确保 KVraft 服务器已启动:")
			return kvraftapi.ErrWrongLeader
		default:
		}

		candidates := ck.preferredCandidates(key)
		for _, index := range candidates {
			addr := ck.grpcServers[index]
			client, err := ck.getClient(addr)
			if err != nil {
				attempts++
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout(250))
			resp, rpcErr := client.Put(ctx, &pb.PutRequest{Key: args.Key, Value: args.Value, Version: int64(args.Version), TtlSeconds: args.TTL})
			cancel()
			if rpcErr == nil && resp != nil {
				errCode := mapPBErr(resp.GetError())
				switch errCode {
				case kvraftapi.OK, kvraftapi.ErrNoKey:
					ck.leader = index
					return errCode
				case kvraftapi.ErrVersion:
					ck.leader = index
					if !retry {
						return kvraftapi.ErrVersion
					}
					return kvraftapi.ErrMaybe
				case kvraftapi.ErrWrongLeader:
					if leader := parseRedirectLeaderAddr(resp.GetError()); leader != "" {
						leader = toGRPCAddress(leader)
						if idx, ok := ck.serverToIdx[leader]; ok {
							ck.leader = idx
						}
					}
				}
			} else if rpcErr != nil {
				if leader := parseRedirectLeaderAddr(rpcErr.Error()); leader != "" {
					leader = toGRPCAddress(leader)
					if idx, ok := ck.serverToIdx[leader]; ok {
						ck.leader = idx
					}
				}
			}

			attempts++
			if attempts > 100 {
				fmt.Printf("\n⚠️  Put 已尝试 %d 次，仍无可用服务器\n", attempts)
				fmt.Println("提示: 请确保 KVraft 服务器已启动:")
				return kvraftapi.ErrWrongLeader
			}
		}

		retry = true
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Delete(key string) kvraftapi.Err {
	if ck.router != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := ck.router.DeleteRoute(ctx, key)
		if err != nil || resp == nil {
			return kvraftapi.ErrWrongLeader
		}
		return mapPBErr(resp.GetError())
	}

	timeout := time.After(10 * time.Second)
	attempts := 0

	for {
		select {
		case <-timeout:
			return kvraftapi.ErrWrongLeader
		default:
		}

		for _, index := range ck.preferredCandidates(key) {
			addr := ck.grpcServers[index]
			client, err := ck.getClient(addr)
			if err != nil {
				attempts++
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout(250))
			resp, rpcErr := client.Delete(ctx, &pb.DeleteRequest{Key: key})
			cancel()
			if rpcErr == nil && resp != nil {
				errCode := mapPBErr(resp.GetError())
				if errCode == kvraftapi.OK || errCode == kvraftapi.ErrNoKey {
					ck.leader = index
					return errCode
				}
			}

			attempts++
			if attempts > 100 {
				return kvraftapi.ErrWrongLeader
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// Scan 按前缀扫描键值。
func (ck *Clerk) Scan(prefix string, limit int32) ([]*pb.KeyValue, kvraftapi.Err) {
	if ck.router != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		items, err := ck.router.ScanRoute(ctx, prefix, limit)
		if err != nil {
			return nil, kvraftapi.ErrWrongLeader
		}
		return items, kvraftapi.OK
	}

	timeout := time.After(10 * time.Second)
	attempts := 0

	for {
		select {
		case <-timeout:
			return nil, kvraftapi.ErrWrongLeader
		default:
		}

		for _, index := range ck.preferredCandidates(prefix) {
			addr := ck.grpcServers[index]
			client, err := ck.getClient(addr)
			if err != nil {
				attempts++
				continue
			}

			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout(300))
			resp, rpcErr := client.Scan(ctx, &pb.ScanRequest{Prefix: prefix, Limit: limit})
			cancel()
			if rpcErr == nil && resp != nil {
				errCode := mapPBErr(resp.GetError())
				if errCode == kvraftapi.OK {
					ck.leader = index
					items := append([]*pb.KeyValue(nil), resp.GetItems()...)
					sort.Slice(items, func(i, j int) bool {
						return items[i].GetKey() < items[j].GetKey()
					})
					return items, kvraftapi.OK
				}
			}

			attempts++
			if attempts > 100 {
				return nil, kvraftapi.ErrWrongLeader
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) watchSingleServer(ctx context.Context, addr string, key string, prefix bool, out chan<- *pb.WatchEvent) error {
	client, err := ck.getClient(addr)
	if err != nil {
		return err
	}

	stream, err := client.Watch(ctx)
	if err != nil {
		return err
	}

	if err := stream.Send(&pb.WatchRequest{RequestType: &pb.WatchRequest_Create{Create: &pb.WatchCreateRequest{Key: key, Prefix: prefix}}}); err != nil {
		return err
	}

	for {
		ev, recvErr := stream.Recv()
		if recvErr != nil {
			return recvErr
		}
		if ev == nil {
			continue
		}

		select {
		case out <- ev:
		case <-ctx.Done():
			return nil
		}
	}
}

// Watch 订阅指定 key 或 prefix 的变化。
// prefix=false 时仅监听一个 key；prefix=true 时监听指定前缀。
func (ck *Clerk) Watch(key string, prefix bool) (*WatchSubscription, error) {
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *pb.WatchEvent, 128)

	if ck.router != nil {
		go func() {
			defer close(out)
			_ = ck.router.WatchRoute(ctx, key, prefix, func(groupID int, event *pb.WatchEvent) {
				if event == nil {
					return
				}
				ev := &pb.WatchEvent{
					WatchId:    event.GetWatchId(),
					Key:        event.GetKey(),
					OldValue:   event.GetOldValue(),
					NewValue:   event.GetNewValue(),
					NewVersion: event.GetNewVersion(),
					EventType:  fmt.Sprintf("group-%d:%s", groupID, event.GetEventType()),
				}
				select {
				case out <- ev:
				case <-ctx.Done():
				}
			})
		}()
		return &WatchSubscription{Events: out, cancel: cancel}, nil
	}

	go func() {
		defer close(out)
		for {
			if ctx.Err() != nil {
				return
			}

			candidates := ck.preferredCandidates(key)
			for _, index := range candidates {
				if ctx.Err() != nil {
					return
				}

				addr := ck.grpcServers[index]
				err := ck.watchSingleServer(ctx, addr, key, prefix, out)
				if err == nil || ctx.Err() != nil {
					return
				}
			}

			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
	}()

	return &WatchSubscription{Events: out, cancel: cancel}, nil
}

func (ck *Clerk) Close() {
	if ck.router != nil {
		ck.router.Close()
	}

	ck.mu.Lock()
	defer ck.mu.Unlock()
	for addr, conn := range ck.conns {
		if conn != nil {
			_ = conn.Close()
		}
		delete(ck.conns, addr)
		delete(ck.clients, addr)
	}
}
