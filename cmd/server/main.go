package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"kvraft/pkg/rsm"
)

func getenvInt(name string, def int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}

func getenvStr(name string, def string) string {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	return raw
}

func runtimeDataRoot() string {
	v := strings.TrimSpace(os.Getenv("KV_DATA_DIR"))
	if v != "" {
		return v
	}
	if info, err := os.Stat("/data"); err == nil && info.IsDir() {
		return "/data"
	}
	return "data"
}

func startHTTP(addr string, kv *rsm.KVServer) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		if kv.IsAlive() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
			return
		}
		http.Error(w, "server stopped", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		if kv.IsAlive() {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ready"))
			return
		}
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http server stopped on %s: %v", addr, err)
		}
	}()
	return srv
}

func startMetricsHTTP(addr string, kv *rsm.KVServer) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		stats := kv.StatsSnapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = fmt.Fprintln(w, "# HELP kvraft_total_requests Total requests processed")
		_, _ = fmt.Fprintln(w, "# TYPE kvraft_total_requests counter")
		_, _ = fmt.Fprintf(w, "kvraft_total_requests %d\n", stats.TotalRequests)
		_, _ = fmt.Fprintln(w, "# HELP kvraft_total_reads Total read requests")
		_, _ = fmt.Fprintln(w, "# TYPE kvraft_total_reads counter")
		_, _ = fmt.Fprintf(w, "kvraft_total_reads %d\n", stats.TotalReads)
		_, _ = fmt.Fprintln(w, "# HELP kvraft_total_writes Total write requests")
		_, _ = fmt.Fprintln(w, "# TYPE kvraft_total_writes counter")
		_, _ = fmt.Fprintf(w, "kvraft_total_writes %d\n", stats.TotalWrites)
		_, _ = fmt.Fprintln(w, "# HELP kvraft_failed_requests Total failed requests")
		_, _ = fmt.Fprintln(w, "# TYPE kvraft_failed_requests counter")
		_, _ = fmt.Fprintf(w, "kvraft_failed_requests %d\n", stats.FailedRequests)
		_, _ = fmt.Fprintln(w, "# HELP kvraft_watch_notifies Total watch notifications")
		_, _ = fmt.Fprintln(w, "# TYPE kvraft_watch_notifies counter")
		_, _ = fmt.Fprintf(w, "kvraft_watch_notifies %d\n", stats.WatchNotifies)
	})

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("metrics server stopped on %s: %v", addr, err)
		}
	}()
	return srv
}

func main() {
	nodeID := getenvInt("NODE_ID", 0)
	peers := strings.TrimSpace(os.Getenv("RAFT_PEERS"))
	if peers == "" {
		log.Fatal("RAFT_PEERS is required, e.g. kvraft-node-0:50000,kvraft-node-1:50001,kvraft-node-2:50002")
	}
	servers := strings.Split(peers, ",")
	for i := range servers {
		servers[i] = strings.TrimSpace(servers[i])
	}
	if nodeID < 0 || nodeID >= len(servers) {
		log.Fatalf("invalid NODE_ID=%d, peers=%d", nodeID, len(servers))
	}

	rpcAddr := servers[nodeID]
	httpAddr := getenvStr("REST_LISTEN", "0.0.0.0:8001")
	metricsAddr := getenvStr("METRICS_LISTEN", "0.0.0.0:9100")
	maxRaftState := getenvInt("MAX_RAFT_STATE", 1048576)

	dataDir := filepath.Join(runtimeDataRoot(), fmt.Sprintf("node-%d", nodeID))
	persister, err := rsm.NewFilePersister(dataDir)
	if err != nil {
		log.Fatalf("create persister failed: %v", err)
	}
	defer persister.Close()

	kv := rsm.StartKVServer(servers, 1, nodeID, persister, maxRaftState, rpcAddr)
	log.Printf("kvraft node started: node_id=%d rpc=%s rest=%s metrics=%s", nodeID, rpcAddr, httpAddr, metricsAddr)

	httpSrv := startHTTP(httpAddr, kv)
	metricsSrv := startMetricsHTTP(metricsAddr, kv)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutting down node %d", nodeID)
	if httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = httpSrv.Shutdown(ctx)
		cancel()
	}
	if metricsSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = metricsSrv.Shutdown(ctx)
		cancel()
	}
	kv.Kill()
}
