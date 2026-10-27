// Package topology is the leadership map for one view (C++ dyna/topology.h).
//
// Shared-replica model: N = 2f+1 servers, the key space split into N home partitions;
// server i leads partition i and follows the rest. Identical on every node in the
// same view, so InvolvedLeaders is computed the same way on client and server.
//
// A view carries the set of ALIVE servers. A key whose home leader is alive keeps it.
// A key whose home leader failed is re-assigned by consistent hashing with virtual
// nodes over the alive servers (first alive point clockwise of the key's hash), so
// survivors keep their ranges and split the failed one; each survivor's share is its
// "delta-p".
//
// Hashing is FNV-1a for both partitions and the ring (the C++ version used
// std::hash for partitions; the Go port is not wire/placement compatible).
package topology

import (
	"fmt"
	"sort"
	"sync"

	"dynago/internal/types"
)

const VnodesPerServer = 16

type point struct {
	h uint64
	s uint32
}

// Topology is a small value; the ring is shared and immutable.
type Topology struct {
	N     uint32
	Alive types.ServerSet
	ring  []point
}

func AllAlive(n uint32) types.ServerSet {
	if n >= 64 {
		return ^types.ServerSet(0)
	}
	return types.ServerSet(1)<<n - 1
}

var (
	ringMu    sync.Mutex
	ringCache = map[uint32][]point{}
)

func New(n uint32) Topology {
	if n == 0 {
		n = 1
	}
	return Topology{N: n, Alive: AllAlive(n), ring: buildRing(n)}
}

func buildRing(n uint32) []point {
	ringMu.Lock()
	defer ringMu.Unlock()
	if r, ok := ringCache[n]; ok { // built per view change only; not hot
		return r
	}
	var r []point
	for s := uint32(0); s < n; s++ {
		for v := 0; v < VnodesPerServer; v++ {
			r = append(r, point{Fnv1a(fmt.Sprintf("dyna-vnode-%d-%d", s, v)), s})
		}
	}
	sort.Slice(r, func(i, j int) bool {
		if r[i].h != r[j].h {
			return r[i].h < r[j].h
		}
		return r[i].s < r[j].s
	})
	ringCache[n] = r
	return r
}

// WithAlive: same servers, different alive set (a new view).
func (t Topology) WithAlive(mask types.ServerSet) Topology {
	t.Alive = mask & AllAlive(t.N)
	return t
}

func (t Topology) IsAlive(s uint32) bool { return t.Alive.Has(s) }
func (t Topology) AliveCount() int       { return t.Alive.Count() }

func Fnv1a(s string) uint64 {
	h := uint64(1469598103934665603)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return h
}

// PartitionOf is the deterministic key -> home partition map, known to all.
func PartitionOf(key string, n uint32) uint32 {
	h := Fnv1a(key)
	h ^= h >> 33 // FNV's low bits are weak for short keys; fold before the modulo
	return uint32(h % uint64(n))
}

func (t Topology) PartitionOf(key string) uint32 { return PartitionOf(key, t.N) }

// OwnerOf is the server that leads key in this view: its home leader if alive,
// otherwise the consistent-hash successor among the alive servers.
func (t Topology) OwnerOf(key string) uint32 {
	home := t.PartitionOf(key) // leaderOf(partition) is the identity
	if t.Alive.Has(home) || t.Alive == 0 {
		return home
	}
	h := Fnv1a(key)
	i := sort.Search(len(t.ring), func(i int) bool { return t.ring[i].h >= h })
	for k := 0; k < len(t.ring); k++ {
		p := t.ring[(i+k)%len(t.ring)]
		if t.Alive.Has(p.s) {
			return p.s
		}
	}
	return home
}

// InvolvedLeaders: owners over RSet ∪ keys(WSet).
func (t Topology) InvolvedLeaders(txn *types.Txn) types.ServerSet {
	var ls types.ServerSet
	for _, k := range txn.RSet {
		ls.Add(t.OwnerOf(k))
	}
	for _, kv := range txn.WSet {
		ls.Add(t.OwnerOf(kv.Key))
	}
	return ls
}

// InvolvedPartitions: home partitions over RSet ∪ keys(WSet).
func (t Topology) InvolvedPartitions(txn *types.Txn) types.ServerSet {
	var ps types.ServerSet
	for _, k := range txn.RSet {
		ps.Add(t.PartitionOf(k))
	}
	for _, kv := range txn.WSet {
		ps.Add(t.PartitionOf(kv.Key))
	}
	return ps
}

// KeyForPartition brute-forces a key "k<i>" homed at target (tests, smoke tools).
func KeyForPartition(target, n uint32) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("k%d", i)
		if PartitionOf(k, n) == target {
			return k
		}
	}
}
