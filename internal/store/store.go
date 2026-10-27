// Package store is the storage interface for executed writes (C++ storage_backend.h,
// value_codec.h, in_memory_backend.*).
//
// Durability model: commit never waits on the store (invariant 2). A committed txn
// already lives in memory on f+1 replicas, so the store is not fsynced per write.
// Every value is kept with the commit t of the write that produced it, encoded as
// [t:8 bytes LE][value], so recovery can select key-values by timestamp.
package store

import (
	"encoding/binary"
	"sync"
)

const ValueHeaderBytes = 8

type VersionedValue struct {
	Value string
	T     uint64 // commit timestamp of the write that produced Value
}

// Store is touched only by the executor goroutine in a running replica.
type Store interface {
	// Apply: key := value, committed at t. Visible to Read as soon as it returns.
	Apply(key, value string, t uint64)
	// Read returns the current value and its commit t; ok=false if absent.
	Read(key string) (VersionedValue, bool)
}

// Stager is an optional Store extension: Stage buffers a write (not yet readable) and
// Flush makes every staged write visible at once (one disk-store commit per executor
// pass instead of one per key).
type Stager interface {
	Stage(key, value string, t uint64)
	Flush()
}

// EncodeVersioned appends [t LE][value] to dst.
func EncodeVersioned(dst []byte, value string, t uint64) []byte {
	dst = binary.LittleEndian.AppendUint64(dst, t)
	return append(dst, value...)
}

// DecodeVersioned splits a stored blob. One shorter than the header was not written by
// DYNA (e.g. preloaded) and comes back as-is with t = 0.
func DecodeVersioned(stored []byte) VersionedValue {
	if len(stored) < ValueHeaderBytes {
		return VersionedValue{Value: string(stored)}
	}
	return VersionedValue{
		T:     binary.LittleEndian.Uint64(stored),
		Value: string(stored[ValueHeaderBytes:]),
	}
}

// Mem is an in-memory Store (tests, and the replica default).
type Mem struct {
	mu         sync.Mutex
	kv         map[string]VersionedValue
	applyCount uint64
}

func NewMem() *Mem { return &Mem{kv: make(map[string]VersionedValue)} }

func (m *Mem) Apply(key, value string, t uint64) {
	m.mu.Lock()
	m.kv[key] = VersionedValue{Value: value, T: t}
	m.applyCount++
	m.mu.Unlock()
}

func (m *Mem) Read(key string) (VersionedValue, bool) {
	m.mu.Lock()
	v, ok := m.kv[key]
	m.mu.Unlock()
	return v, ok
}

// ValueOrEmpty is a test convenience: just the value ("" if absent).
func (m *Mem) ValueOrEmpty(key string) string {
	v, _ := m.Read(key)
	return v.Value
}

func (m *Mem) ApplyCount() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.applyCount
}
