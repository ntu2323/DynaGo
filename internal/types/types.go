// Package types holds DYNA's identity and ordering types (C++ dyna/txn_types.h):
// TxnId, OrderKey, the transaction body Txn and the per-server TxnEntry.
package types

import (
	"math"
	"math/bits"
	"net/netip"
	"sort"
)

// TxnId = (clientId, requestId): unique per transaction across the system.
type TxnId struct {
	ClientID  uint64
	RequestID uint64
}

// Hash is a well-mixed 64-bit hash (splitmix64 finalizer), used for lane sharding.
// (Go maps hash struct keys themselves; the C++ framework's hash returned 0.)
func (id TxnId) Hash() uint64 {
	h := id.ClientID*0x9E3779B97F4A7C15 ^ id.RequestID
	h ^= h >> 30
	h *= 0xBF58476D1CE4E5B9
	h ^= h >> 27
	h *= 0x94D049BB133111EB
	return h ^ (h >> 31)
}

// Status of a txn in specLog.
type Status uint8

const (
	Speculative Status = iota
	Committing
	Committed
)

func (s Status) String() string {
	switch s {
	case Speculative:
		return "SPECULATIVE"
	case Committing:
		return "COMMITTING"
	case Committed:
		return "COMMITTED"
	}
	return "?"
}

// OrderKey = (t, clientId, requestId), compared lexicographically. Every replica orders
// by it (invariant 1). Re-timestamping changes only t, so distinct txns never collide.
type OrderKey struct {
	T         uint64
	ClientID  uint64
	RequestID uint64
}

func MakeOrderKey(t uint64, id TxnId) OrderKey { return OrderKey{t, id.ClientID, id.RequestID} }

// Less reports a < b.
func (a OrderKey) Less(b OrderKey) bool {
	if a.T != b.T {
		return a.T < b.T
	}
	if a.ClientID != b.ClientID {
		return a.ClientID < b.ClientID
	}
	return a.RequestID < b.RequestID
}

// MaxOrderKey sorts after every real key.
var MaxOrderKey = OrderKey{math.MaxUint64, math.MaxUint64, math.MaxUint64}

func (a OrderKey) Id() TxnId { return TxnId{a.ClientID, a.RequestID} }

type TxnType uint8

const (
	Write TxnType = 0 // pure blind write (WSet only)
	Read  TxnType = 1 // pure read (RSet only); RMW is out of scope
)

type KV struct {
	Key   string
	Value string
}

// Txn is the transaction body as the client sent it.
type Txn struct {
	ClientID  uint64
	RequestID uint64
	Type      TxnType
	RSet      []string // keys read (empty for a pure write)
	WSet      []KV     // keys + values written (empty for a pure read)
	T         uint64   // client-assigned deadline (µs)
	ViewID    uint64
	SendTime  uint64 // µs, client send stamp (replica learns OWD); 0 = absent
}

func (t *Txn) Id() TxnId { return TxnId{t.ClientID, t.RequestID} }

// Keys returns every key the txn touches (reads then writes).
func (t *Txn) Keys() []string {
	out := make([]string, 0, len(t.RSet)+len(t.WSet))
	out = append(out, t.RSet...)
	for _, kv := range t.WSet {
		out = append(out, kv.Key)
	}
	return out
}

// Clone deep-copies the slices (strings are immutable and shared).
func (t *Txn) Clone() Txn {
	c := *t
	c.RSet = append([]string(nil), t.RSet...)
	c.WSet = append([]KV(nil), t.WSet...)
	return c
}

// ServerSet is a bitmask of server ids (N <= 64).
type ServerSet uint64

func (s ServerSet) Has(i uint32) bool          { return i < 64 && s&(1<<i) != 0 }
func (s *ServerSet) Add(i uint32)              { *s |= 1 << i }
func (s ServerSet) Count() int                 { return bits.OnesCount64(uint64(s)) }
func (s ServerSet) With(i uint32) ServerSet    { return s | 1<<i }
func (s ServerSet) Without(i uint32) ServerSet { return s &^ (1 << i) }

// Members lists the ids in ascending order.
func (s ServerSet) Members() []uint32 {
	var out []uint32
	for x := uint64(s); x != 0; x &= x - 1 {
		out = append(out, uint32(bits.TrailingZeros64(x)))
	}
	return out
}

// NoServer marks an unset server id (C++ UINT32_MAX).
const NoServer = math.MaxUint32

// TxnEntry is a txn's per-server state. It lives in exactly one of earlyBuffer,
// lateBuffer or specLog.
type TxnEntry struct {
	Txn    Txn
	T      uint64 // LIVE timestamp; orders the entry (may differ from Txn.T after a bump)
	Status Status

	IsLeaderForThis bool     // this server leads a key in the txn (current view)
	LedKeys         []string // sorted: the keys this server leads
	Primary         uint32   // min-id involved leader (drives replication); NoServer if unset

	Results map[string]string // READ at a leader: disk-read values

	TsProposals    map[uint32]uint64 // serverId -> proposed t (TsAgree)
	PrepareReplies ServerSet         // servers that acked ReplicatePrepare

	OwdEstimateUS   uint64
	ArrivalUS       uint64
	ReleasedUS      uint64 // clock() when it entered specLog SPECULATIVE
	CommittingUS    uint64 // clock() when it became COMMITTING
	LastReplicateUS uint64 // primary: last prepare broadcast; others: last re-ack
	SyncNow         bool   // post-recovery: leader-sync without waiting for the timeout

	ClientAddr netip.AddrPort // where to answer (invalid if unknown)
}

func (e *TxnEntry) Key() TxnId            { return TxnId{e.Txn.ClientID, e.Txn.RequestID} }
func (e *TxnEntry) Order() OrderKey       { return OrderKey{e.T, e.Txn.ClientID, e.Txn.RequestID} }
func (e *TxnEntry) Less(o *TxnEntry) bool { return e.Order().Less(o.Order()) }

// NewEntry builds an entry whose live t is the body's t.
func NewEntry(txn Txn) *TxnEntry {
	return &TxnEntry{Txn: txn, T: txn.T, Primary: NoServer}
}

// SortStrings sorts in place and drops duplicates.
func SortStrings(s []string) []string {
	sort.Strings(s)
	out := s[:0]
	for i, k := range s {
		if i == 0 || k != s[i-1] {
			out = append(out, k)
		}
	}
	return out
}

// ID builds a TxnId.
func ID(clientID, requestID uint64) TxnId { return TxnId{clientID, requestID} }
