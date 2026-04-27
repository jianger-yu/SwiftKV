package raft

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestPersistV3RoundTrip(t *testing.T) {
	rf := &Raft{
		CurrentTerm:       5,
		VotedFor:          2,
		lastIncludedIndex: 7,
		lastIncludedTerm:  3,
		log: []LogEntry{
			{Term: 3},
			{Term: 4, Command: "cmd-a"},
			{Term: 5, Command: "cmd-b"},
		},
	}

	data, ok := rf.encodePersistentStateIncrementalLocked()
	if !ok {
		t.Fatalf("encodePersistentStateIncrementalLocked failed")
	}

	var restored Raft
	if !restored.readPersistV3(append([]byte(nil), data...)) {
		t.Logf("encoded_len=%d magic=%#x logCount=%d", len(data), binary.LittleEndian.Uint32(data[persistV3OffMagic:persistV3OffMagic+4]), binary.LittleEndian.Uint64(data[persistV3OffLogCount:persistV3OffLogCount+8]))
		t.Fatalf("readPersistV3 failed")
	}

	if restored.CurrentTerm != rf.CurrentTerm {
		t.Fatalf("CurrentTerm mismatch: got %d want %d", restored.CurrentTerm, rf.CurrentTerm)
	}
	if restored.VotedFor != rf.VotedFor {
		t.Fatalf("VotedFor mismatch: got %d want %d", restored.VotedFor, rf.VotedFor)
	}
	if restored.lastIncludedIndex != rf.lastIncludedIndex {
		t.Fatalf("lastIncludedIndex mismatch: got %d want %d", restored.lastIncludedIndex, rf.lastIncludedIndex)
	}
	if restored.lastIncludedTerm != rf.lastIncludedTerm {
		t.Fatalf("lastIncludedTerm mismatch: got %d want %d", restored.lastIncludedTerm, rf.lastIncludedTerm)
	}
	if len(restored.log) != len(rf.log) {
		t.Fatalf("log length mismatch: got %d want %d", len(restored.log), len(rf.log))
	}

	if got, ok := restored.log[1].Command.(string); !ok || got != "cmd-a" {
		t.Fatalf("restored log[1] command mismatch: %#v", restored.log[1].Command)
	}
	if got, ok := restored.log[2].Command.(string); !ok || got != "cmd-b" {
		t.Fatalf("restored log[2] command mismatch: %#v", restored.log[2].Command)
	}
}

func TestPersistV3AppendKeepsEncodedPrefix(t *testing.T) {
	rf := &Raft{
		CurrentTerm:       2,
		VotedFor:          1,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: "first"},
		},
	}

	data1, ok := rf.encodePersistentStateIncrementalLocked()
	if !ok {
		t.Fatalf("first incremental encode failed")
	}

	prefix := append([]byte(nil), data1[persistV3HeaderSize:]...)
	appendFrom := len(rf.log)
	rf.markPersistV3DirtyFromLocked(appendFrom)
	rf.log = append(rf.log, LogEntry{Term: 2, Command: "second"})

	data2, ok := rf.encodePersistentStateIncrementalLocked()
	if !ok {
		t.Fatalf("second incremental encode failed")
	}

	if len(data2) <= len(data1) {
		t.Fatalf("encoded size did not grow after append: before=%d after=%d", len(data1), len(data2))
	}

	if !bytes.Equal(prefix, data2[persistV3HeaderSize:persistV3HeaderSize+len(prefix)]) {
		t.Fatalf("existing encoded log prefix changed after pure append")
	}
}

func TestPersistV3TruncateThenAppend(t *testing.T) {
	rf := &Raft{
		CurrentTerm:       3,
		VotedFor:          1,
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
		log: []LogEntry{
			{Term: 0},
			{Term: 1, Command: "a"},
			{Term: 2, Command: "b"},
			{Term: 2, Command: "c"},
		},
	}

	if _, ok := rf.encodePersistentStateIncrementalLocked(); !ok {
		t.Fatalf("initial incremental encode failed")
	}

	rf.markPersistV3DirtyFromLocked(2)
	rf.log = rf.log[:2]
	rf.log = append(rf.log, LogEntry{Term: 9, Command: "x"})

	data2, ok := rf.encodePersistentStateIncrementalLocked()
	if !ok {
		t.Fatalf("encode after truncate+append failed")
	}

	var restored Raft
	if !restored.readPersistV3(append([]byte(nil), data2...)) {
		t.Logf("encoded_len=%d magic=%#x logCount=%d", len(data2), binary.LittleEndian.Uint32(data2[persistV3OffMagic:persistV3OffMagic+4]), binary.LittleEndian.Uint64(data2[persistV3OffLogCount:persistV3OffLogCount+8]))
		t.Fatalf("readPersistV3 after truncate+append failed")
	}

	if len(restored.log) != 3 {
		t.Fatalf("restored log length mismatch: got %d want 3", len(restored.log))
	}
	if restored.log[2].Term != 9 {
		t.Fatalf("restored tail term mismatch: got %d want 9", restored.log[2].Term)
	}
	if got, ok := restored.log[2].Command.(string); !ok || got != "x" {
		t.Fatalf("restored tail command mismatch: %#v", restored.log[2].Command)
	}
}
