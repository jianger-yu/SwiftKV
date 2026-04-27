package rsm

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func buildTestAppendPayload(prevState []byte, newHeader []byte, delta []byte) []byte {
	payload := make([]byte, raftAppendPayloadHeaderSize+len(newHeader)+len(delta))
	binary.LittleEndian.PutUint32(payload[raftAppendOffMagic:raftAppendOffMagic+4], raftAppendPayloadMagic)
	binary.LittleEndian.PutUint64(payload[raftAppendOffPrevLen:raftAppendOffPrevLen+8], uint64(len(prevState)))
	binary.LittleEndian.PutUint64(payload[raftAppendOffNewLen:raftAppendOffNewLen+8], uint64(len(prevState)+len(delta)))
	binary.LittleEndian.PutUint32(payload[raftAppendOffHeaderLen:raftAppendOffHeaderLen+4], uint32(len(newHeader)))
	binary.LittleEndian.PutUint32(payload[raftAppendOffDeltaLen:raftAppendOffDeltaLen+4], uint32(len(delta)))
	copy(payload[raftAppendPayloadHeaderSize:raftAppendPayloadHeaderSize+len(newHeader)], newHeader)
	copy(payload[raftAppendPayloadHeaderSize+len(newHeader):], delta)
	return payload
}

func TestPersisterAppendRaftStateRoundTrip(t *testing.T) {
	dataDir := rsmTestStorePath(t, "test-persister-append-roundtrip")
	fp, err := NewFilePersister(dataDir)
	if err != nil {
		t.Fatalf("create persister failed: %v", err)
	}
	defer func() { _ = fp.Close() }()

	base := bytes.Repeat([]byte{'B'}, 44)
	fp.Save(base, nil)

	headerPatch := bytes.Repeat([]byte{'H'}, 44)
	delta := []byte("-delta-append")
	payload := buildTestAppendPayload(base, headerPatch, delta)
	fp.AppendRaftState(payload)

	got := fp.ReadRaftState()
	want := append(append([]byte(nil), headerPatch...), delta...)
	if !bytes.Equal(got, want) {
		t.Fatalf("append roundtrip mismatch: got=%q want=%q", string(got), string(want))
	}
}

func TestPersisterAppendRaftStateIgnoresPartialTailFrame(t *testing.T) {
	dataDir := rsmTestStorePath(t, "test-persister-append-partial-tail")
	fp, err := NewFilePersister(dataDir)
	if err != nil {
		t.Fatalf("create persister failed: %v", err)
	}
	defer func() { _ = fp.Close() }()

	base := bytes.Repeat([]byte{'B'}, 44)
	fp.Save(base, nil)

	headerPatch := bytes.Repeat([]byte{'H'}, 44)
	delta := []byte("good")
	fp.AppendRaftState(buildTestAppendPayload(base, headerPatch, delta))

	fp.mu.RLock()
	deltaPath := fp.currentRaftDeltaPathLocked()
	fp.mu.RUnlock()

	f, err := os.OpenFile(deltaPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open delta file failed: %v", err)
	}
	// 写入一个不完整的帧头尾巴，模拟掉电中断。
	_, _ = f.Write([]byte{0x44, 0x46, 0x41})
	_ = f.Close()

	got := fp.ReadRaftState()
	want := append(append([]byte(nil), headerPatch...), delta...)
	if !bytes.Equal(got, want) {
		t.Fatalf("partial tail should be ignored: got=%q want=%q", string(got), string(want))
	}
}

func TestPersisterAppendCompactsToNewBase(t *testing.T) {
	dataDir := rsmTestStorePath(t, "test-persister-append-compact")
	fp, err := NewFilePersister(dataDir)
	if err != nil {
		t.Fatalf("create persister failed: %v", err)
	}
	defer func() { _ = fp.Close() }()

	fp.deltaCompactBytes = 64

	base := bytes.Repeat([]byte{'B'}, 44)
	fp.Save(base, nil)

	headerPatch := bytes.Repeat([]byte{'H'}, 44)
	delta := bytes.Repeat([]byte("x"), 128)
	fp.AppendRaftState(buildTestAppendPayload(base, headerPatch, delta))

	fp.mu.RLock()
	gen := fp.manifest.Generation
	deltaPath := filepath.Join(fp.dataDir, fp.manifest.RaftDeltaRel)
	fp.mu.RUnlock()

	if gen < 2 {
		t.Fatalf("expected compaction to bump generation, got %d", gen)
	}

	info, err := os.Stat(deltaPath)
	if err != nil {
		t.Fatalf("stat compacted delta failed: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("expected compacted delta to be empty, got %d bytes", info.Size())
	}

	got := fp.ReadRaftState()
	want := append(append([]byte(nil), headerPatch...), delta...)
	if !bytes.Equal(got, want) {
		t.Fatalf("compacted state mismatch")
	}
}
