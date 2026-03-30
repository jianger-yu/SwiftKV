package storage

import (
	"encoding/json"
	"os"
	"testing"
)

func TestLoadSnapshotAtomicSwitch(t *testing.T) {
	path := "test-db-snapshot-atomic"
	defer os.RemoveAll(path)

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	defer store.Close()

	if err := store.Put("k1", "v1", 1); err != nil {
		t.Fatalf("seed put failed: %v", err)
	}

	snapshot := map[string]kvEntry{
		"k2": {Value: "v2", Version: 9},
		"k3": {Value: "v3", Version: 1},
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatalf("marshal snapshot failed: %v", err)
	}

	if err := store.LoadSnapshot(raw); err != nil {
		t.Fatalf("load snapshot failed: %v", err)
	}

	if _, _, exists, err := store.Get("k1"); err != nil {
		t.Fatalf("get k1 failed: %v", err)
	} else if exists {
		t.Fatalf("k1 should be replaced by snapshot")
	}

	v2, ver2, ok2, err := store.Get("k2")
	if err != nil {
		t.Fatalf("get k2 failed: %v", err)
	}
	if !ok2 || v2 != "v2" || ver2 != 9 {
		t.Fatalf("k2 mismatch: value=%q version=%d exists=%v", v2, ver2, ok2)
	}
}

func TestLoadSnapshotInvalidInputKeepsData(t *testing.T) {
	path := "test-db-snapshot-invalid"
	defer os.RemoveAll(path)

	store, err := NewStore(path)
	if err != nil {
		t.Fatalf("new store failed: %v", err)
	}
	defer store.Close()

	if err := store.Put("origin", "value", 1); err != nil {
		t.Fatalf("seed put failed: %v", err)
	}

	if err := store.LoadSnapshot([]byte("{bad json")); err == nil {
		t.Fatalf("expected invalid snapshot error, got nil")
	}

	v, ver, ok, err := store.Get("origin")
	if err != nil {
		t.Fatalf("get origin failed: %v", err)
	}
	if !ok || v != "value" || ver != 1 {
		t.Fatalf("origin data should stay unchanged, value=%q version=%d exists=%v", v, ver, ok)
	}
}
