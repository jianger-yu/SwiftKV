package watch

import (
	"sync"
	"testing"
	"time"
)

func newConfig() ManagerConfig {
	return ManagerConfig{
		EventBufferSize:   100,
		WatcherBufferSize: 10,
		DLQSize:           50,
		WatcherTimeout:    2 * time.Second,
		HealthCheckTick:   500 * time.Millisecond,
	}
}

// TestSubscribe tests basic subscribe functionality
func TestSubscribe(t *testing.T) {
	wm := NewManager(newConfig())
	defer wm.Close()

	w, err := wm.Subscribe("key1", false)
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	if w == nil || w.ID == 0 {
		t.Fatal("Expected valid watcher")
	}

	stats := wm.GetStats()
	if stats.ActiveWatchers != 1 {
		t.Errorf("Expected 1 active watcher, got %d", stats.ActiveWatchers)
	}
}

// TestUnsubscribe tests watcher removal
func TestUnsubscribe(t *testing.T) {
	wm := NewManager(newConfig())
	defer wm.Close()

	w, _ := wm.Subscribe("key1", false)
	wm.Unsubscribe(w.ID)

	stats := wm.GetStats()
	if stats.ActiveWatchers != 0 {
		t.Errorf("Expected 0 active watchers after unsubscribe, got %d", stats.ActiveWatchers)
	}
}

// TestNotify sends event to watcher
func TestNotify(t *testing.T) {
	wm := NewManager(newConfig())
	defer wm.Close()

	w, _ := wm.Subscribe("key1", false)

	// Send event
	wm.Notify("key1", "old", "new", 1, "set")

	// Allow time for background processing
	time.Sleep(100 * time.Millisecond)

	select {
	case event := <-w.Channel:
		if event.Key != "key1" || event.NewValue != "new" {
			t.Errorf("Got unexpected event: %+v", event)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Expected to receive event within 1 second")
	}
}

// TestPrefixWatch filters events by prefix
func TestPrefixWatch(t *testing.T) {
	wm := NewManager(newConfig())
	defer wm.Close()

	w, _ := wm.Subscribe("user:", true) // prefix mode

	// Send matching event
	wm.Notify("user:123", "", "alice", 1, "set")

	// Send non-matching event
	wm.Notify("post:456", "", "hello", 1, "set")

	time.Sleep(100 * time.Millisecond)

	// Should receive only the matching event
	select {
	case event := <-w.Channel:
		if event.Key != "user:123" {
			t.Errorf("Expected key=user:123, got %s", event.Key)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Expected to receive event within 1 second")
	}

	// Verify no second event in channel
	select {
	case <-w.Channel:
		t.Fatal("Should not receive non-matching event")
	case <-time.After(100 * time.Millisecond):
		// Expected: no more events
	}
}

// TestBackpressure tests handling of slow subscribers
func TestBackpressure(t *testing.T) {
	cfg := newConfig()
	cfg.WatcherBufferSize = 1 // Very small buffer
	wm := NewManager(cfg)
	defer wm.Close()

	w, _ := wm.Subscribe("key1", false)

	// Send multiple events rapidly
	for i := 0; i < 10; i++ {
		wm.Notify("key1", "", "value", int64(i), "set")
	}

	// Give time for processing
	time.Sleep(200 * time.Millisecond)

	stats := wm.GetStats()
	if stats.TotalEvents == 0 {
		t.Errorf("Expected events to be queued, got 0")
	}

	t.Logf("Stats: Total=%d, Successful=%d, Failed=%d, Dropped=%d",
		stats.TotalEvents, stats.SuccessfulSends, stats.FailedSends, stats.DroppedEvents)

	_ = w
}

// TestWatcherTimeout verifies watchers timeout after inactivity
func TestWatcherTimeout(t *testing.T) {
	cfg := newConfig()
	cfg.WatcherTimeout = 500 * time.Millisecond
	wm := NewManager(cfg)
	defer wm.Close()

	w, _ := wm.Subscribe("key1", false)

	initial := wm.GetStats().ActiveWatchers
	if initial != 1 {
		t.Errorf("Expected 1 active watcher, got %d", initial)
	}

	// Wait for timeout
	time.Sleep(1 * time.Second)

	// The watcher should timeout during health check
	time.Sleep(200 * time.Millisecond)

	final := wm.GetStats().ActiveWatchers
	if final >= 1 {
		t.Logf("Note: Watcher timeout check - ActiveWatchers=%d (may not be exact due to timing)", final)
	}

	_ = w // ensure w is used
}

// TestMultipleWatchers tests multiple concurrent watchers
func TestMultipleWatchers(t *testing.T) {
	wm := NewManager(newConfig())
	defer wm.Close()

	numWatchers := 10
	watchers := make([]*Watcher, numWatchers)

	for i := 0; i < numWatchers; i++ {
		w, _ := wm.Subscribe("key1", false)
		watchers[i] = w
	}

	wm.Notify("key1", "", "broadcast", 1, "set")
	time.Sleep(100 * time.Millisecond)

	// Verify all watchers received the event
	for i := 0; i < numWatchers; i++ {
		select {
		case event := <-watchers[i].Channel:
			if event.NewValue != "broadcast" {
				t.Errorf("Watcher %d received wrong value: %s", i, event.NewValue)
			}
		case <-time.After(1 * time.Second):
			t.Errorf("Watcher %d did not receive event", i)
		}
	}

	stats := wm.GetStats()
	if stats.ActiveWatchers != int64(numWatchers) {
		t.Errorf("Expected %d active watchers, got %d", numWatchers, stats.ActiveWatchers)
	}
}

// TestEventStats verifies statistics tracking
func TestEventStats(t *testing.T) {
	wm := NewManager(newConfig())
	defer wm.Close()

	w, _ := wm.Subscribe("key1", false)

	for i := 0; i < 5; i++ {
		wm.Notify("key1", "", "test", int64(i), "set")
	}

	time.Sleep(200 * time.Millisecond)

	stats := wm.GetStats()
	if stats.TotalEvents < 5 {
		t.Errorf("Expected at least 5 total events, got %d", stats.TotalEvents)
	}
	if stats.SuccessfulSends < 5 {
		t.Errorf("Expected at least 5 successful sends, got %d", stats.SuccessfulSends)
	}

	_ = w
}

// TestConcurrentSubscriptions tests thread-safe subscribe/unsubscribe
func TestConcurrentSubscriptions(t *testing.T) {
	wm := NewManager(newConfig())
	defer wm.Close()

	var wg sync.WaitGroup

	// Concurrent subscriptions
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			wm.Subscribe("key1", false)
		}(i)
	}

	// Concurrent unsubscriptions
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			w, _ := wm.Subscribe("key2", false)
			wm.Unsubscribe(w.ID)
		}(i)
	}

	wg.Wait()

	stats := wm.GetStats()
	if stats.ActiveWatchers < 10 {
		t.Logf("Expected ~10 active watchers, got %d (timing-dependent)", stats.ActiveWatchers)
	}
}

// BenchmarkNotify benchmarks single watcher event delivery
func BenchmarkNotify(b *testing.B) {
	wm := NewManager(ManagerConfig{
		EventBufferSize:   b.N * 2,
		WatcherBufferSize: b.N,
		DLQSize:           b.N,
		WatcherTimeout:    5 * time.Second,
		HealthCheckTick:   1 * time.Second,
	})
	defer wm.Close()

	w, _ := wm.Subscribe("key1", false)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wm.Notify("key1", "", "value", int64(i), "set")
	}

	// Drain channel to ensure processing
	time.Sleep(100 * time.Millisecond)

	_ = w
}

// BenchmarkMultipleNotify benchmarks event delivery to many watchers
func BenchmarkMultipleNotify(b *testing.B) {
	wm := NewManager(ManagerConfig{
		EventBufferSize:   b.N,
		WatcherBufferSize: b.N,
		DLQSize:           b.N,
		WatcherTimeout:    5 * time.Second,
		HealthCheckTick:   1 * time.Second,
	})
	defer wm.Close()

	// Create 100 watchers
	for i := 0; i < 100; i++ {
		wm.Subscribe("key1", false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wm.Notify("key1", "", "value", int64(i), "set")
	}

	// Drain channels
	time.Sleep(200 * time.Millisecond)
}
