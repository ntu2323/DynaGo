// Package recovery is the data model + pure helpers for partition-leader recovery
// (C++ dyna/recovery.{h,cc}; design doc § Recovery).
//
// After a server fails, every survivor becomes Partition Leader (PL) for its delta-p: the
// keys of the failed range that consistent hashing now assigns to it. The PL collects,
// from f+1 servers (itself included), every speculative / committing / committed txn
// touching delta-p and every executed key-value in delta-p, and merges them into M
// (Merge). M is prepared on f+1 servers and, at StartView, installed everywhere.
//
// Why this is safe: followers only ever hold a txn SPECULATIVE at the client's original
// t, so a txn fast-committed with its leader dead has ≥1 SPECULATIVE copy at that t among
// any f+1 of the 2f survivors; a slow-committed txn has ≥1 COMMITTING copy at agreedT
// among them. Merge precedence keeps the most-decided copy.
package recovery

import (
	"sort"

	"dynago/internal/topology"
	"dynago/internal/types"
	"dynago/internal/wire"
)

// Data is one server's contribution to M (or M itself).
type Data struct {
	Txns []wire.RecTxn
	KVs  []wire.RecKV
}

func (d *Data) Empty() bool { return len(d.Txns) == 0 && len(d.KVs) == 0 }

// InDeltaP: is key part of PL pl's delta-p for the change old → new (it moved to pl)?
func InDeltaP(oldT, newT topology.Topology, key string, pl uint32) bool {
	return newT.OwnerOf(key) == pl && oldT.OwnerOf(key) != pl
}

// InFailedRange: was key led by a server gone in the new view (any PL's delta-p)?
func InFailedRange(oldT, newT topology.Topology, key string) bool {
	return !newT.IsAlive(oldT.OwnerOf(key))
}

// TxnTouches: does the txn touch any key satisfying pred?
func TxnTouches(t *types.Txn, pred func(string) bool) bool {
	for _, k := range t.RSet {
		if pred(k) {
			return true
		}
	}
	for _, kv := range t.WSet {
		if pred(kv.Key) {
			return true
		}
	}
	return false
}

// StatusRank orders statuses for the merge and for install (never downgrade).
func StatusRank(s types.Status) int { return int(s) }

// Merge builds M from the f+1 replies.
//   - Txns, per TxnId: COMMITTED > COMMITTING > SPECULATIVE; within one status the
//     larger t (COMMITTING/COMMITTED copies all carry the agreed t; a surviving
//     co-leader's SPECULATIVE copy may be bumped — post-recovery leader sync resolves
//     those to the max anyway).
//   - Key-values, per key: the largest t.
//
// The output is sorted (by TxnId, by key), so the same replies give the same M.
func Merge(replies []Data) Data {
	txns := map[types.TxnId]wire.RecTxn{}
	kvs := map[string]wire.RecKV{}
	for _, r := range replies {
		for _, c := range r.Txns {
			id := c.Txn.Id()
			cur, ok := txns[id]
			if !ok || StatusRank(c.Status) > StatusRank(cur.Status) ||
				(c.Status == cur.Status && c.T > cur.T) {
				txns[id] = c
			}
		}
		for _, kv := range r.KVs {
			if cur, ok := kvs[kv.Key]; !ok || kv.T > cur.T {
				kvs[kv.Key] = kv
			}
		}
	}
	var m Data
	for _, t := range txns {
		m.Txns = append(m.Txns, t)
	}
	sort.Slice(m.Txns, func(i, j int) bool {
		a, b := m.Txns[i].Txn.Id(), m.Txns[j].Txn.Id()
		return a.ClientID < b.ClientID || (a.ClientID == b.ClientID && a.RequestID < b.RequestID)
	})
	for _, kv := range kvs {
		m.KVs = append(m.KVs, kv)
	}
	sort.Slice(m.KVs, func(i, j int) bool { return m.KVs[i].Key < m.KVs[j].Key })
	return m
}
