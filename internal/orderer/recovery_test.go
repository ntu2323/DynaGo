package orderer

import (
	"fmt"
	"slices"
	"testing"

	"dynago/internal/clock"
	"dynago/internal/recovery"
	"dynago/internal/store"
	"dynago/internal/topology"
	"dynago/internal/types"
	"dynago/internal/wire"
)

const kN = 3

// keyForPartition: the skip-th key "k<i>" homed at target.
func keyForPartition(target uint32, skip int) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("k%d", i)
		if topology.PartitionOf(k, kN) == target {
			if skip == 0 {
				return k
			}
			skip--
		}
	}
}

// failedKeyOwnedBy: a key of failed partition 1 whose new owner (alive {0,2}) is pl.
func failedKeyOwnedBy(pl uint32) string {
	t := topology.New(kN).WithAlive(0b101)
	for skip := 0; ; skip++ {
		if k := keyForPartition(1, skip); t.OwnerOf(k) == pl {
			return k
		}
	}
}

// rcluster: three orderers on one mock clock, server 1 about to fail.
type rcluster struct {
	clk        *clock.Mock
	st         [kN]*store.Mem
	col        [kN]*collector
	o          [kN]*Orderer
	oldT, newT topology.Topology
}

func newRCluster() *rcluster {
	c := &rcluster{clk: clock.NewMock(0), oldT: topology.New(kN), newT: topology.New(kN).WithAlive(0b101)}
	for s := 0; s < kN; s++ {
		c.st[s] = store.NewMem()
		c.col[s] = newCollector()
		c.o[s] = New(uint32(s), 0, c.clk, topology.New(kN), c.col[s].cb(), c.st[s], 0)
	}
	return c
}

// release e (on time) at the given servers.
func (c *rcluster) release(e *types.TxnEntry, at ...int) {
	c.clk.Set(e.T - 10)
	for _, s := range at {
		cp := *e
		cp.Txn = e.Txn.Clone()
		c.o[s].SubmitTxn(&cp, false)
	}
	c.clk.Set(e.T)
	for _, s := range at {
		c.o[s].ReleaseDue()
	}
}

// recover among survivors {0,2}: each PL merges both servers' data; both install all Ms.
func (c *rcluster) recover() []InstallStats {
	var ms []recovery.Data
	for _, pl := range []uint32{0, 2} {
		inDelta := func(k string) bool { return recovery.InDeltaP(c.oldT, c.newT, k, pl) }
		ms = append(ms, recovery.Merge([]recovery.Data{c.o[0].CollectRecovery(inDelta), c.o[2].CollectRecovery(inDelta)}))
	}
	failed := func(k string) bool { return recovery.InFailedRange(c.oldT, c.newT, k) }
	return []InstallStats{c.o[0].InstallView(c.newT, 1, ms, failed), c.o[2].InstallView(c.newT, 1, ms, failed)}
}

func writeKV(c, r, t uint64, k, v string) *types.TxnEntry { return mkWriteV(c, r, t, k, v) }

func TestRecoveryOrderer_FastCommittedLeaderDeadSurvivesAtT(t *testing.T) {
	for _, endorserIsPL := range []bool{false, true} {
		c := newRCluster()
		pl := uint32(2)
		k := failedKeyOwnedBy(pl)
		w := writeKV(7, 1, 1000, k, "v")
		other := 0
		if endorserIsPL {
			other = int(pl)
		}
		c.release(w, 1, other)
		st := c.recover()
		if st[0].Leftovers+st[1].Leftovers != 0 {
			t.Fatal(st)
		}
		for _, s := range []int{0, 2} {
			if sts, ts := mustEntry(t, c.o[s], w.Key()); sts != types.Speculative || ts != 1000 {
				t.Fatal(s, sts, ts)
			}
		}
		c.clk.Set(1000 + 200000)
		_, done := c.o[pl].DriveTsAgreement(100000, 0)
		if len(done) != 1 || done[0].T != 1000 || !done[0].IsLeaderForThis {
			t.Fatal(done)
		}
		if _, ok := c.o[pl].TakeReplicateNow(w.Key()); !ok {
			t.Fatal("the new primary replicates")
		}
	}
}

func TestRecoveryOrderer_InheritedSpecSyncedImmediately(t *testing.T) {
	c := newRCluster()
	pl := uint32(2)
	w := writeKV(7, 1, 1000, failedKeyOwnedBy(pl), "v")
	c.release(w, 0, 1, 2)
	st := c.recover()
	if st[1].SpecToSync != 1 || st[0].SpecToSync != 0 {
		t.Fatal(st)
	}
	acts, done := c.o[pl].DriveTsAgreement(100000, 0) // no time passed
	if len(done) != 1 || done[0].Key() != w.Key() || done[0].T != 1000 || done[0].Status != types.Committing || len(acts) != 0 {
		t.Fatal(done, acts)
	}
	if _, d0 := c.o[0].DriveTsAgreement(100000, 0); len(d0) != 0 {
		t.Fatal("not a leader of it")
	}
	w2 := writeKV(7, 2, 5000, failedKeyOwnedBy(pl), "v")
	w2.Txn.ViewID = 1
	c.release(w2, 0, 2)
	if _, d := c.o[pl].DriveTsAgreement(100000, 0); len(d) != 0 {
		t.Fatal("a fresh write waits out the timeout")
	}
}

func TestRecoveryOrderer_InheritedMultiLeaderSpecSyncsImmediately(t *testing.T) {
	c := newRCluster()
	w := writeKV(7, 3, 1000, failedKeyOwnedBy(2), "v")
	w.Txn.WSet = append(w.Txn.WSet, types.KV{Key: keyForPartition(0, 0), Value: "v0"})
	c.release(w, 0, 1, 2)
	st := c.recover()
	if st[0].SpecToSync != 1 || st[1].SpecToSync != 1 {
		t.Fatal(st)
	}
	a0, d0 := c.o[0].DriveTsAgreement(100000, 0)
	a2, _ := c.o[2].DriveTsAgreement(100000, 0)
	if len(a0) != 1 || len(a2) != 1 || len(d0) != 0 {
		t.Fatal(a0, a2)
	}
	r2 := c.o[2].RecordTsProposal(w.Key(), 0, a0[0].ProposedT, false)
	r0 := c.o[0].RecordTsProposal(w.Key(), 2, a2[0].ProposedT, false)
	if !r2.Transitioned || !r0.Transitioned || r0.Entry.T != 1000 || r2.Entry.T != 1000 {
		t.Fatal(r0, r2)
	}
}

func TestRecoveryOrderer_InstallNeverDowngrades(t *testing.T) {
	c := newRCluster()
	w := writeKV(7, 8, 1000, keyForPartition(1, 0), "v")
	c.release(w, 0)
	c.o[0].ApplyCommit(w.Key(), 1500)
	c.o[0].MarkCommitted(w.Key())
	m := recovery.Data{Txns: []wire.RecTxn{{Txn: w.Txn, T: 1000, Status: types.Speculative}}}
	c.o[0].InstallView(c.newT, 1, []recovery.Data{m}, func(string) bool { return true })
	if st, ts := mustEntry(t, c.o[0], w.Key()); st != types.Committed || ts != 1500 {
		t.Fatal(st, ts)
	}
}

func TestRecoveryOrderer_SlowCommittedSurvivesAtAgreedT(t *testing.T) {
	c := newRCluster()
	w := writeKV(7, 2, 1000, keyForPartition(1, 0), "v")
	c.release(w, 0, 1, 2)
	for _, s := range []int{0, 1} {
		if r, _ := c.o[s].ApplyCommit(w.Key(), 1500); r != Applied {
			t.Fatal()
		}
	}
	c.recover()
	for _, s := range []int{0, 2} {
		if st, ts := mustEntry(t, c.o[s], w.Key()); st != types.Committing || ts != 1500 {
			t.Fatal(s, st, ts)
		}
	}
}

func TestRecoveryOrderer_ExecutedKeyValuesRecoveredWithTGuard(t *testing.T) {
	c := newRCluster()
	k, k2 := keyForPartition(1, 0), keyForPartition(1, 1)
	for _, e := range []*types.TxnEntry{writeKV(7, 3, 1000, k, "val"), writeKV(7, 4, 1100, k2, "old")} {
		c.release(e, 0)
		c.o[0].ApplyCommit(e.Key(), e.T)
		c.o[0].MarkCommitted(e.Key())
	}
	c.o[0].ExecuteReady()
	if c.st[0].ValueOrEmpty(k) != "val" {
		t.Fatal()
	}
	c.st[2].Apply(k2, "newer", 5000)
	st := c.recover()
	if v, _ := c.st[2].Read(k); v.Value != "val" || v.T != 1000 {
		t.Fatal(v)
	}
	if c.st[2].ValueOrEmpty(k2) != "newer" {
		t.Fatal("t-guard")
	}
	if st[1].KVsApplied != 1 || st[0].KVsApplied != 0 {
		t.Fatal(st)
	}
}

func TestRecoveryOrderer_LeftoversKeptAndCounted(t *testing.T) {
	c := newRCluster()
	w := writeKV(7, 5, 1000, keyForPartition(1, 0), "v")
	c.clk.Set(2000)
	c.o[2].SubmitTxn(w, false) // late at a pure follower → lateBuffer
	if c.o[2].LateSize() != 1 {
		t.Fatal()
	}
	st := c.recover()
	if st[1].Leftovers != 1 || c.o[2].LateSize() != 1 {
		t.Fatal(st)
	}
}

func TestRecoveryOrderer_RecoveredValueNotOverwrittenByOlderExecution(t *testing.T) {
	c := newRCluster()
	k := keyForPartition(1, 0)
	m := recovery.Data{KVs: []wire.RecKV{{Key: k, Value: "recovered", T: 2000}}}
	c.o[2].InstallView(c.newT, 1, []recovery.Data{m}, func(string) bool { return false })
	if c.st[2].ValueOrEmpty(k) != "recovered" {
		t.Fatal()
	}
	old := writeKV(9, 1, 1500, k, "stale")
	c.clk.Set(1600)
	c.o[2].InstallFetched(&old.Txn)
	c.o[2].ApplyCommit(old.Key(), 1500)
	c.o[2].MarkCommitted(old.Key())
	c.o[2].ExecuteReady()
	if !c.o[2].Executed(old.Key()) || c.st[2].ValueOrEmpty(k) != "recovered" {
		t.Fatal(c.st[2].ValueOrEmpty(k))
	}
}

func TestRecoveryOrderer_ResendAfterInstallNotDuplicated(t *testing.T) {
	c := newRCluster()
	k := keyForPartition(1, 0)
	w := writeKV(7, 6, 1000, k, "v")
	c.release(w, 0, 1)
	c.recover()
	pl := c.newT.OwnerOf(k)
	before := c.o[pl].SpecLogSize()
	c.col[pl].reset()
	c.clk.Set(9000) // the resend is now late at the (new) leader
	c.o[pl].SubmitTxn(writeKV(7, 6, 1000, k, "v"), false)
	if c.o[pl].SpecLogSize() != before || c.col[pl].count() != 1 || c.col[pl].at(0).t != 1000 {
		t.Fatal(c.col[pl].got)
	}
}

// --- rejoin (orderer level) ---

func TestRejoin_SnapshotPagesAndInstalls(t *testing.T) {
	c := newRCluster()
	var keys []string
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("snap%d", i)
		keys = append(keys, k)
		w := writeKV(3, uint64(i+1), uint64(1000+i), k, fmt.Sprintf("v%d", i))
		c.release(w, 0)
		c.o[0].ApplyCommit(w.Key(), w.T)
		c.o[0].MarkCommitted(w.Key())
	}
	c.o[0].ExecuteReady()
	pending := writeKV(3, 99, 5000, "snapP", "p")
	c.release(pending, 0)
	c.o[0].ApplyCommit(pending.Key(), 5000) // COMMITTING, not executed
	c.st[2].Apply("snap0", "newer", 9999)
	sk := c.o[0].SnapshotKeys()
	if !slices.Equal(sk, keys) {
		t.Fatal(sk)
	}
	applied := 0
	for off := 0; off < len(sk); off += 2 {
		d := recovery.Data{KVs: c.o[0].SnapshotRead(sk, off, 2)}
		if off+2 >= len(sk) {
			d.Txns = c.o[0].CommittedTxns()
		}
		applied += c.o[2].InstallSnapshot(d)
	}
	if applied != 4 || c.st[2].ValueOrEmpty("snap0") != "newer" || c.st[2].ValueOrEmpty("snap4") != "v4" {
		t.Fatal(applied)
	}
	if st, ts := mustEntry(t, c.o[2], pending.Key()); st != types.Committing || ts != 5000 {
		t.Fatal(st, ts)
	}
	c.o[2].Reset()
	if c.o[2].SpecLogSize() != 0 || c.st[2].ValueOrEmpty("snap4") != "v4" {
		t.Fatal("store kept, ordering state dropped")
	}
}

// Server 1 dies (view 1), a write on one of its keys is fast-committed in view 1;
// server 1 restarts empty, catches up from 0, and in the join recovery (view 1 → 2) is
// PL of its home range again — holding the write at its t as leader, and the value.
func TestRejoin_HomeRangeHandedBackWithState(t *testing.T) {
	c := newRCluster()
	kexec, kspec := keyForPartition(1, 0), keyForPartition(1, 1)
	w0 := writeKV(4, 1, 1000, kexec, "exec")
	c.release(w0, 0, 1, 2)
	for s := 0; s < kN; s++ {
		c.o[s].ApplyCommit(w0.Key(), 1000)
		c.o[s].MarkCommitted(w0.Key())
		c.o[s].ExecuteReady()
	}
	c.recover() // view 1, alive {0,2}
	w1 := writeKV(4, 2, 3000, kspec, "spec")
	c.release(w1, 0, 2)

	freshStore := store.NewMem()
	joiner := New(1, 0, c.clk, topology.New(kN), newCollector().cb(), freshStore, 0)
	joiner.InstallView(c.newT, 1, nil, func(string) bool { return false })
	snap := recovery.Data{KVs: c.o[0].SnapshotRead(c.o[0].SnapshotKeys(), 0, 100), Txns: c.o[0].CommittedTxns()}
	joiner.InstallSnapshot(snap)
	if freshStore.ValueOrEmpty(kexec) != "exec" {
		t.Fatal()
	}
	v1, v2 := c.newT, topology.New(kN)
	srv := []*Orderer{c.o[0], joiner, c.o[2]}
	var ms []recovery.Data
	for pl := uint32(0); pl < kN; pl++ {
		inDelta := func(k string) bool { return recovery.InDeltaP(v1, v2, k, pl) }
		if pl != 1 {
			if d := srv[pl].CollectRecovery(inDelta); !d.Empty() {
				t.Fatal("pl", pl, d)
			}
			continue
		}
		ms = append(ms, recovery.Merge([]recovery.Data{joiner.CollectRecovery(inDelta), c.o[0].CollectRecovery(inDelta)}))
	}
	for _, o := range srv {
		o.InstallView(v2, 2, ms, func(string) bool { return false })
	}
	if st, ts := mustEntry(t, joiner, w1.Key()); st != types.Speculative || ts != 3000 {
		t.Fatal(st, ts)
	}
	c.clk.Set(3000 + 200000)
	_, done := joiner.DriveTsAgreement(100000, 0)
	if len(done) != 1 || done[0].T != 3000 || !done[0].IsLeaderForThis {
		t.Fatal(done)
	}
}

func TestRejoin_ResendOfExecutedWriteAnsweredWithFinalT(t *testing.T) {
	c := newRCluster()
	w := writeKV(5, 1, 1000, "kx", "v")
	c.clk.Set(1500)
	if !c.o[1].HandleReplicatePrepare(w.Key(), 1200, &w.Txn) {
		t.Fatal()
	}
	c.o[1].MarkCommitted(w.Key())
	c.o[1].ExecuteReady()
	if !c.o[1].Executed(w.Key()) {
		t.Fatal()
	}
	c.col[1].reset()
	c.o[1].SubmitTxn(writeKV(5, 1, 1000, "kx", "v"), false)
	if c.col[1].count() != 1 || c.col[1].at(0).t != 1200 || c.o[1].SpecLogContains(w.Key()) {
		t.Fatal(c.col[1].got)
	}
}
