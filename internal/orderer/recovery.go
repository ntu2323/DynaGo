package orderer

import (
	"sort"

	"dynago/internal/clock"
	"dynago/internal/hmap"
	"dynago/internal/recovery"
	"dynago/internal/topology"
	"dynago/internal/types"
	"dynago/internal/wire"
)

// InstallStats summarizes one InstallView.
type InstallStats struct {
	TxnsInstalled int
	KVsApplied    int
	Leftovers     int // uncommitted entries in the failed range no M carried (kept)
	SpecToSync    int // inherited SPECULATIVE writes we now lead, marked for leader sync
}

// CollectRecovery is a survivor's contribution to a PL's M: every SPECULATIVE /
// COMMITTING / COMMITTED write in specLog touching a key in delta-p, plus every executed
// key-value in delta-p with its commit t (read on the store goroutine).
func (o *Orderer) CollectRecovery(inDelta func(string) bool) recovery.Data {
	var d recovery.Data
	// Txns first: an entry the executor erases after this snapshot is applied to the
	// store before the store scan below runs (same serialization).
	o.lockAll()
	for _, s := range o.shards {
		s.spec.ForEach(func(_ types.TxnId, e *types.TxnEntry) {
			if e.Txn.Type == types.Write && recovery.TxnTouches(&e.Txn, inDelta) {
				d.Txns = append(d.Txns, wire.RecTxn{Txn: e.Txn, T: e.T, Status: e.Status})
			}
		})
	}
	o.unlockAll()
	o.RunOnStoreThread(func() {
		if o.store == nil {
			return
		}
		for k := range o.exec.writtenKeys {
			if !inDelta(k) {
				continue
			}
			if v, ok := o.store.Read(k); ok {
				d.KVs = append(d.KVs, wire.RecKV{Key: k, Value: v.Value, T: v.T})
			}
		}
	})
	return d
}

// installRecoveredLocked adopts one M txn (status precedence; never downgrade, never
// re-install an executed txn). True if it changed local state.
func (o *Orderer) installRecoveredLocked(s *shard, r *wire.RecTxn, now uint64, kick *bool) bool {
	id := r.Txn.Id()
	if _, ok := s.executed.Get(id); ok {
		return false
	}
	if e := s.spec.Find(id); e != nil {
		if recovery.StatusRank(r.Status) <= recovery.StatusRank(e.Status) {
			return false
		}
		if e.Status == types.Speculative {
			e.CommittingUS = now
		}
		e.T = r.T
		e.Status = r.Status
		o.bumpHighestLocked(s, e.Order())
		s.spec.UpdateKey(id)
		if r.Status == types.Committed {
			*kick = true
		}
		return true
	}
	var e *types.TxnEntry
	if s.early.Contains(id) {
		// Not released yet here: it releases on its own at the client's t, which is what a
		// SPECULATIVE copy in M carries anyway.
		if r.Status == types.Speculative {
			return false
		}
		e = s.early.Erase(id)
	} else if e = s.late.At(id); e != nil {
		s.late.Delete(id)
	} else {
		e = types.NewEntry(r.Txn)
		o.computeLeadership(e)
	}
	e.T = r.T
	e.Status = r.Status
	e.ReleasedUS = now
	if r.Status != types.Speculative {
		e.CommittingUS = now
	}
	o.bumpHighestLocked(s, e.Order())
	s.spec.Push(id, e)
	if r.Status == types.Committed {
		*kick = true
	}
	return true
}

// applyKVsGuarded applies key-values iff newer than the store's (t-guard) and remembers
// their t so an older execution can't overwrite them. Requires exec.runMu.
func (o *Orderer) applyKVsGuarded(kvs []wire.RecKV) int {
	applied := 0
	for _, kv := range kvs {
		if cur := o.exec.appliedOK[kv.Key]; cur.T <= kv.T {
			o.exec.appliedOK[kv.Key] = types.OrderKey{T: kv.T}
		}
		if o.store == nil {
			continue
		}
		if cur, ok := o.store.Read(kv.Key); ok && cur.T >= kv.T {
			continue
		}
		o.store.Apply(kv.Key, kv.Value, kv.T)
		o.exec.writtenKeys[kv.Key] = struct{}{}
		applied++
	}
	return applied
}

// InstallView switches to a new view's topology (recomputing every held entry's
// leadership) and installs the Ms buffered from each PL:
//   - key-values go to the store t-guarded;
//   - txns are adopted with their M status and t, never downgrading a local copy;
//   - entries absent from every M are KEPT (user decision); Leftovers counts the
//     uncommitted ones in the failed range;
//   - inherited SPECULATIVE writes we now lead are marked SyncNow, so the next
//     ts-agreement pass leader-syncs them immediately.
func (o *Orderer) InstallView(topo topology.Topology, viewID uint64, ms []recovery.Data,
	inFailedRange func(string) bool) InstallStats {
	var st InstallStats
	o.RunOnStoreThread(func() {
		for i := range ms {
			st.KVsApplied += o.applyKVsGuarded(ms[i].KVs)
		}
	})
	kick := false
	o.lockAll()
	now := o.clk.Now()
	o.topo = topo
	o.viewID = viewID
	for _, s := range o.shards {
		s.spec.ForEach(func(_ types.TxnId, e *types.TxnEntry) {
			o.computeLeadership(e) // leadership doesn't affect the OrderKey
			e.LastReplicateUS = 0  // the (new) primary prepares promptly
			e.PrepareReplies = 0
		})
		s.early.ForEach(func(_ types.TxnId, e *types.TxnEntry) { o.computeLeadership(e) })
		s.late.Range(func(_ types.TxnId, e **types.TxnEntry) bool {
			o.computeLeadership(*e)
			return true
		})
	}
	inM := map[types.TxnId]struct{}{}
	for i := range ms {
		for j := range ms[i].Txns {
			r := &ms[i].Txns[j]
			inM[r.Txn.Id()] = struct{}{}
			if o.installRecoveredLocked(o.shardOf(r.Txn.Id()), r, now, &kick) {
				st.TxnsInstalled++
			}
		}
	}
	leftover := func(e *types.TxnEntry) bool {
		if e.Status == types.Committed || e.Txn.Type != types.Write || inFailedRange == nil {
			return false
		}
		_, ok := inM[e.Key()]
		return !ok && recovery.TxnTouches(&e.Txn, inFailedRange)
	}
	for _, s := range o.shards {
		s.spec.ForEach(func(_ types.TxnId, e *types.TxnEntry) {
			if leftover(e) {
				st.Leftovers++
			}
			if e.Status == types.Speculative && e.IsLeaderForThis && e.Txn.Type == types.Write {
				e.SyncNow = true
				st.SpecToSync++
			}
		})
		s.late.Range(func(_ types.TxnId, e **types.TxnEntry) bool {
			if leftover(*e) {
				st.Leftovers++
			}
			return true
		})
	}
	o.unlockAll()
	if kick {
		o.KickExecutor()
	}
	o.interrupt()
	return st
}

// Reset drops all ordering state (a server left out of a view rejoins from scratch).
// The store is kept: catch-up applies newer values t-guarded.
func (o *Orderer) Reset() {
	o.exec.runMu.Lock()
	defer o.exec.runMu.Unlock()
	o.lockAll()
	for _, s := range o.shards {
		s.early.Clear()
		s.spec.Clear()
		s.late.Clear()
		s.executed = hmap.New[types.TxnId, executedInfo](0)
		s.highestReleased = types.OrderKey{}
		s.nextDue.Store(clock.Never)
	}
	o.highestTMax.Store(0)
	o.unlockAll()
}

// SnapshotKeys (donor side of catch-up): every key applied here, sorted so a joiner can
// page through it by offset.
func (o *Orderer) SnapshotKeys() []string {
	var keys []string
	o.RunOnStoreThread(func() {
		keys = make([]string, 0, len(o.exec.writtenKeys))
		for k := range o.exec.writtenKeys {
			keys = append(keys, k)
		}
	})
	sort.Strings(keys)
	return keys
}

// SnapshotRead: the stored (value, t) of keys[off : off+count].
func (o *Orderer) SnapshotRead(keys []string, off, count int) []wire.RecKV {
	var out []wire.RecKV
	o.RunOnStoreThread(func() {
		if o.store == nil {
			return
		}
		for i := off; i < len(keys) && i < off+count; i++ {
			if v, ok := o.store.Read(keys[i]); ok {
				out = append(out, wire.RecKV{Key: keys[i], Value: v.Value, T: v.T})
			}
		}
	})
	return out
}

// CommittedTxns: COMMITTING / COMMITTED writes not yet executed (a snapshot's last chunk).
func (o *Orderer) CommittedTxns() []wire.RecTxn {
	var out []wire.RecTxn
	for _, s := range o.shards {
		s.mu.Lock()
		s.spec.ForEach(func(_ types.TxnId, e *types.TxnEntry) {
			if e.Txn.Type == types.Write && e.Status != types.Speculative {
				out = append(out, wire.RecTxn{Txn: e.Txn, T: e.T, Status: e.Status})
			}
		})
		s.mu.Unlock()
	}
	return out
}

// InstallSnapshot (joiner): key-values t-guarded, txns as in InstallView. Returns the
// number of key-values applied.
func (o *Orderer) InstallSnapshot(d recovery.Data) int {
	applied := 0
	if len(d.KVs) > 0 {
		o.RunOnStoreThread(func() { applied = o.applyKVsGuarded(d.KVs) })
	}
	kick := false
	o.lockAll()
	now := o.clk.Now()
	for i := range d.Txns {
		o.installRecoveredLocked(o.shardOf(d.Txns[i].Txn.Id()), &d.Txns[i], now, &kick)
	}
	o.unlockAll()
	if kick {
		o.KickExecutor()
	}
	return applied
}
