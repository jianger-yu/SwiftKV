package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "kvraft/api/pb/kvraft/api/pb"
	kvraftapi "kvraft/raftkv/rpc"
	"kvraft/rsm"
	"kvraft/sharding"
)

type jsonGroup struct {
	GroupID   int      `json:"group_id"`
	Replicas  []string `json:"replicas"`
	LeaderIdx int      `json:"leader_idx"`
}

type jsonShardingConfig struct {
	Groups            []jsonGroup `json:"groups"`
	VirtualNodeCount  int         `json:"virtual_node_count"`
	ConnectTimeoutMS  int         `json:"connect_timeout_ms"`
	RequestTimeoutMS  int         `json:"request_timeout_ms"`
	PreferredReplicas int         `json:"preferred_replicas"`
}

func loadShardingConfig(path string) (sharding.ShardingConfig, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return sharding.ShardingConfig{}, err
	}

	var jc jsonShardingConfig
	if err := json.Unmarshal(content, &jc); err != nil {
		return sharding.ShardingConfig{}, err
	}

	cfg := sharding.ShardingConfig{
		VirtualNodeCount:  jc.VirtualNodeCount,
		PreferredReplicas: jc.PreferredReplicas,
	}
	if jc.ConnectTimeoutMS > 0 {
		cfg.ConnectTimeout = time.Duration(jc.ConnectTimeoutMS) * time.Millisecond
	}
	if jc.RequestTimeoutMS > 0 {
		cfg.RequestTimeout = time.Duration(jc.RequestTimeoutMS) * time.Millisecond
	}

	cfg.Groups = make([]sharding.RaftGroupConfig, 0, len(jc.Groups))
	for _, g := range jc.Groups {
		cfg.Groups = append(cfg.Groups, sharding.RaftGroupConfig{
			GroupID:   g.GroupID,
			Replicas:  append([]string(nil), g.Replicas...),
			LeaderIdx: g.LeaderIdx,
		})
	}
	return cfg, nil
}

func printHelp() {
	fmt.Println("commands:")
	fmt.Println("  help")
	fmt.Println("  get <key>")
	fmt.Println("  put <key> <value> [version]")
	fmt.Println("  del <key>")
	fmt.Println("  scan <prefix> [limit]")
	fmt.Println("  watch key <key>")
	fmt.Println("  watch prefix <prefix>")
	fmt.Println("  unwatch <id|all>")
	fmt.Println("  list-watch")
	fmt.Println("  exit")
}

func parseLimit(raw string) (int32, error) {
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, err
	}
	return int32(v), nil
}

func main() {
	serversFlag := flag.String("servers", "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003", "comma-separated rpc server addresses for single-raft mode")
	shardingConfig := flag.String("sharding-config", "", "path to sharding config json for sharded mode")
	flag.Parse()

	var ck *rsm.Clerk
	var err error
	mode := "single-raft"
	if *shardingConfig != "" {
		cfg, cfgErr := loadShardingConfig(*shardingConfig)
		if cfgErr != nil {
			fmt.Fprintf(os.Stderr, "load sharding config failed: %v\n", cfgErr)
			os.Exit(1)
		}
		ck, err = rsm.MakeShardedClerk(cfg)
		mode = "sharded"
	} else {
		servers := strings.Split(*serversFlag, ",")
		for i := range servers {
			servers[i] = strings.TrimSpace(servers[i])
		}
		ck = rsm.MakeClerk(servers)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "create clerk failed: %v\n", err)
		os.Exit(1)
	}
	defer ck.Close()

	fmt.Printf("KV CLI started, mode=%s\n", mode)
	printHelp()

	var outMu sync.Mutex
	watchers := map[int]*rsm.WatchSubscription{}
	nextWatchID := 1
	localVersion := map[string]kvraftapi.Tversion{}

	scanner := bufio.NewScanner(os.Stdin)
	for {
		outMu.Lock()
		fmt.Print("kv> ")
		outMu.Unlock()

		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := strings.ToLower(parts[0])

		switch cmd {
		case "help":
			printHelp()
		case "exit", "quit":
			for id, sub := range watchers {
				sub.Cancel()
				delete(watchers, id)
			}
			fmt.Println("bye")
			return
		case "get":
			if len(parts) != 2 {
				fmt.Println("usage: get <key>")
				continue
			}
			val, ver, e := ck.Get(parts[1])
			fmt.Printf("value=%q version=%d err=%s\n", val, ver, e)
		case "put":
			if len(parts) < 3 || len(parts) > 4 {
				fmt.Println("usage: put <key> <value> [version]")
				continue
			}
			key := parts[1]
			value := parts[2]
			var version kvraftapi.Tversion
			explicitVersion := false
			if len(parts) == 4 {
				v, parseErr := strconv.ParseInt(parts[3], 10, 64)
				if parseErr != nil {
					fmt.Printf("invalid version: %v\n", parseErr)
					continue
				}
				version = kvraftapi.Tversion(v)
				explicitVersion = true
			} else {
				if v, ok := localVersion[key]; ok {
					version = v
				} else {
					_, ver, gerr := ck.Get(key)
					if gerr == kvraftapi.OK {
						version = ver
					} else if gerr == kvraftapi.ErrNoKey {
						version = 0
					} else {
						fmt.Printf("cannot determine version automatically: %v\n", gerr)
						continue
					}
				}
			}

			attempts := 0
			maxAttempts := 1
			deadline := time.Now().Add(1200 * time.Millisecond)
			if !explicitVersion {
				maxAttempts = 200
			}

			for {
				attempts++
				errCode := ck.Put(key, value, version)
				if errCode == kvraftapi.OK {
					localVersion[key] = version + 1
					if attempts == 1 {
						fmt.Printf("put err=%s\n", errCode)
					} else {
						fmt.Printf("put err=%s retries=%d\n", errCode, attempts-1)
					}
					break
				}

				if explicitVersion || (errCode != kvraftapi.ErrVersion && errCode != kvraftapi.ErrMaybe) || attempts >= maxAttempts || time.Now().After(deadline) {
					fmt.Printf("put err=%s\n", errCode)
					break
				}

				_, ver, gerr := ck.Get(key)
				if gerr == kvraftapi.OK {
					version = ver
				} else if gerr == kvraftapi.ErrNoKey {
					version = 0
				} else {
					fmt.Printf("put err=%s (refresh failed: %v)\n", errCode, gerr)
					break
				}

				time.Sleep(2 * time.Millisecond)
			}
		case "del", "delete":
			if len(parts) != 2 {
				fmt.Println("usage: del <key>")
				continue
			}
			errCode := ck.Delete(parts[1])
			fmt.Printf("delete err=%s\n", errCode)
		case "scan":
			if len(parts) < 2 || len(parts) > 3 {
				fmt.Println("usage: scan <prefix> [limit]")
				continue
			}
			limit := int32(0)
			if len(parts) == 3 {
				l, parseErr := parseLimit(parts[2])
				if parseErr != nil {
					fmt.Printf("invalid limit: %v\n", parseErr)
					continue
				}
				limit = l
			}
			items, e := ck.Scan(parts[1], limit)
			if e != kvraftapi.OK {
				fmt.Printf("scan err=%s\n", e)
				continue
			}
			sort.Slice(items, func(i, j int) bool {
				return items[i].GetKey() < items[j].GetKey()
			})
			fmt.Printf("scan count=%d\n", len(items))
			for _, item := range items {
				fmt.Printf("  %s => %q (v=%d)\n", item.GetKey(), item.GetValue(), item.GetVersion())
			}
		case "watch":
			if len(parts) != 3 {
				fmt.Println("usage: watch key <key> | watch prefix <prefix>")
				continue
			}
			mode := strings.ToLower(parts[1])
			isPrefix := false
			switch mode {
			case "key":
				isPrefix = false
			case "prefix":
				isPrefix = true
			default:
				fmt.Println("usage: watch key <key> | watch prefix <prefix>")
				continue
			}

			sub, subErr := ck.Watch(parts[2], isPrefix)
			if subErr != nil {
				fmt.Printf("watch failed: %v\n", subErr)
				continue
			}
			watchID := nextWatchID
			nextWatchID++
			watchers[watchID] = sub
			fmt.Printf("watch started id=%d mode=%s pattern=%s\n", watchID, mode, parts[2])

			go func(id int, ch <-chan *pb.WatchEvent) {
				for ev := range ch {
					if ev == nil {
						continue
					}
					outMu.Lock()
					fmt.Printf("\n[watch:%d] key=%s type=%s old=%q new=%q ver=%d\n", id, ev.GetKey(), ev.GetEventType(), ev.GetOldValue(), ev.GetNewValue(), ev.GetNewVersion())
					fmt.Print("kv> ")
					outMu.Unlock()
				}
			}(watchID, sub.Events)
		case "unwatch":
			if len(parts) != 2 {
				fmt.Println("usage: unwatch <id|all>")
				continue
			}
			if strings.ToLower(parts[1]) == "all" {
				for id, sub := range watchers {
					sub.Cancel()
					delete(watchers, id)
				}
				fmt.Println("all watches canceled")
				continue
			}

			id, parseErr := strconv.Atoi(parts[1])
			if parseErr != nil {
				fmt.Printf("invalid watch id: %v\n", parseErr)
				continue
			}
			sub, ok := watchers[id]
			if !ok {
				fmt.Printf("watch id %d not found\n", id)
				continue
			}
			sub.Cancel()
			delete(watchers, id)
			fmt.Printf("watch %d canceled\n", id)
		case "list-watch":
			if len(watchers) == 0 {
				fmt.Println("no active watch")
				continue
			}
			ids := make([]int, 0, len(watchers))
			for id := range watchers {
				ids = append(ids, id)
			}
			sort.Ints(ids)
			for _, id := range ids {
				fmt.Printf("watch id=%d active\n", id)
			}
		default:
			fmt.Println("unknown command, use: help")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "read command failed: %v\n", err)
	}
}
