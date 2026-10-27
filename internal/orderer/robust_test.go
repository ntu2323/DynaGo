package orderer

import (
	"testing"
	"time"

	"dynago/internal/clock"
	"dynago/internal/store"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func waitFor(pred func() bool) bool {
	for i := 0; i < 3000; i++ {
		if pred() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return pred()
}

func (c *collector) find(i types.TxnId) (released, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range c.got {
		if r.id == i {
			return r, true
		}
	}
	return released{}, false
}

func TestRobust_BufferedTxnOvertakenByCommitStillReleases(t *testing.T) {
	clk := clock.NewMock(90)
	c := newCollector()
	o := newOrderer(0, clk, 1, c)
	o.SubmitTxn(mkWrite(1, 1, 100, "a"), false)
	o.SubmitTxn(mkWrite(1, 2, 150, "b"), false)
	if r, _ := o.ApplyCommit(id(1, 2), 150); r != Applied || o.HighestReleased().T != 150 {
		t.Fatal()
	}
	clk.Set(100)
	if o.ReleaseDue() != 1 {
		t.Fatal()
	}
	if r, ok := c.find(id(1, 1)); !ok || r.t != 100 {
		t.Fatal()
	}
	if st, _ := mustEntry(t, o, id(1, 1)); st != types.Speculative || o.HighestReleased().T != 150 {
		t.Fatal()
	}
}

func TestRobust_ApplyCommitReheapAdoptsTheRightEntry(t *testing.T) {
	clk := clock.NewMock(50)
	o := newOrderer(0, clk, 1, newCollector())
	for r := uint64(1); r <= 3; r++ {
		o.SubmitTxn(mkWrite(1, r, 100*r, string(rune('a'+r))), false)
	}
	clk.Set(300)
	if o.ReleaseDue() != 3 {
		t.Fatal()
	}
	if r, b := o.ApplyCommit(id(1, 1), 400); r != Applied || b.T != 400 {
		t.Fatal()
	}
	if st, ts := mustEntry(t, o, id(1, 1)); st != types.Committing || ts != 400 {
		t.Fatal()
	}
	for r := uint64(2); r <= 3; r++ {
		if st, _ := mustEntry(t, o, id(1, r)); st != types.Speculative {
			t.Fatal(r)
		}
	}
}

func TestRobust_ApplyCommitNeverDowngradesCommitted(t *testing.T) {
	clk := clock.NewMock(50)
	o := newOrderer(0, clk, 1, newCollector())
	o.SubmitTxn(mkWrite(1, 1, 100, "a"), false)
	clk.Set(100)
	o.ReleaseDue()
	o.ApplyCommit(id(1, 1), 100)
	o.MarkCommitted(id(1, 1))
	if r, b := o.ApplyCommit(id(1, 1), 100); r != Applied || b.Status != types.Committed {
		t.Fatal(b)
	}
	if st, _ := mustEntry(t, o, id(1, 1)); st != types.Committed {
		t.Fatal()
	}
}

func TestRobust_ExecutedTxnIsNeverReinstalledOrReapplied(t *testing.T) {
	clk := clock.NewMock(50)
	st := store.NewMem()
	o := New(0, 0, clk, topology.New(1), newCollector().cb(), st, 0)
	w1, w2 := id(1, 1), id(1, 2)
	o.SubmitTxn(mkWriteV(1, 1, 100, "K", "v1"), false)
	o.SubmitTxn(mkWriteV(1, 2, 200, "K", "v2"), false)
	clk.Set(200)
	o.ReleaseDue()
	o.MarkCommitted(w1)
	o.MarkCommitted(w2)
	if len(o.ExecuteReady()) != 2 || st.ValueOrEmpty("K") != "v2" || !o.Executed(w1) {
		t.Fatal()
	}
	if r, b := o.ApplyCommit(w1, 100); r != AlreadyExecuted || b.T != 100 {
		t.Fatal(r, b)
	}
	body := mkWriteV(1, 1, 100, "K", "v1").Txn
	o.InstallFetched(&body)
	if !o.HandleReplicatePrepare(w1, 100, &body) {
		t.Fatal("executed: still acked")
	}
	o.SubmitTxn(mkWriteV(1, 1, 100, "K", "v1"), false) // delayed client retry
	if o.LateSize() != 0 || o.EarlySize() != 0 || o.SpecLogContains(w1) {
		t.Fatal()
	}
	if len(o.ExecuteReady()) != 0 || st.ValueOrEmpty("K") != "v2" {
		t.Fatal()
	}
}

func TestRobust_LostTsAgreeStillConverges(t *testing.T) {
	kA, kB := topology.KeyForPartition(0, 3), topology.KeyForPartition(1, 3)
	i := id(1, 1)
	c0, c1 := clock.NewMock(50), clock.NewMock(200)
	o0 := New(0, 0, c0, topology.New(3), nil, nil, 0)
	o1 := New(1, 0, c1, topology.New(3), nil, nil, 0)
	o0.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false)
	c0.Set(100)
	o0.ReleaseDue()
	o1.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false)
	a0, _ := o0.DriveTsAgreement(0, 0) // 0 → 1: LOST
	if len(a0) != 1 {
		t.Fatal()
	}
	a1, _ := o1.DriveTsAgreement(0, 0)
	if len(a1) != 1 {
		t.Fatal()
	}
	r0 := o0.RecordTsProposal(i, 1, a1[0].ProposedT, a1[0].IsReply)
	if !r0.Transitioned || !r0.HasAction || !r0.Action.IsReply {
		t.Fatal(r0) // 0's reply to 1: LOST
	}
	a1, _ = o1.DriveTsAgreement(0, 0) // 1 still lacks 0's proposal → re-sends
	if len(a1) != 1 || !a1[0].To.Has(0) {
		t.Fatal()
	}
	r0b := o0.RecordTsProposal(i, 1, a1[0].ProposedT, a1[0].IsReply)
	if !r0b.HasAction {
		t.Fatal("answered even though complete")
	}
	r1 := o1.RecordTsProposal(i, 0, r0b.Action.ProposedT, r0b.Action.IsReply)
	if !r1.Transitioned || r1.HasAction {
		t.Fatal("a reply is never answered")
	}
	for _, o := range []*Orderer{o0, o1} {
		if st, ts := mustEntry(t, o, i); st != types.Committing || ts != 200 {
			t.Fatal(st, ts)
		}
	}
}

func TestRobust_StuckLeaderConvergesWithPeerAlreadyCommitted(t *testing.T) {
	kA, kB := topology.KeyForPartition(0, 3), topology.KeyForPartition(1, 3)
	i := id(1, 1)
	c0, c1 := clock.NewMock(50), clock.NewMock(50)
	o0 := New(0, 0, c0, topology.New(3), nil, nil, 0)
	o1 := New(1, 0, c1, topology.New(3), nil, nil, 0)
	o0.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false)
	o1.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false)
	c0.Set(100)
	c1.Set(100)
	o0.ReleaseDue()
	o1.ReleaseDue()
	o0.ApplyCommit(i, 100)
	a1, _ := o1.DriveTsAgreement(0, 0)
	if len(a1) != 1 {
		t.Fatal()
	}
	r0 := o0.RecordTsProposal(i, 1, a1[0].ProposedT, a1[0].IsReply)
	if r0.Transitioned || !r0.HasAction || r0.Action.ProposedT != 100 {
		t.Fatal(r0)
	}
	if !o1.RecordTsProposal(i, 0, r0.Action.ProposedT, r0.Action.IsReply).Transitioned {
		t.Fatal()
	}
	if st, ts := mustEntry(t, o1, i); st != types.Committing || ts != 100 {
		t.Fatal()
	}
}

func TestRobust_LostReplicateCommitRecoveredByReack(t *testing.T) {
	kA := topology.KeyForPartition(0, 3)
	i := id(1, 1)
	c0, c1 := clock.NewMock(50), clock.NewMock(50)
	o0 := New(0, 0, c0, topology.New(3), nil, store.NewMem(), 0)
	o1 := New(1, 0, c1, topology.New(3), nil, nil, 0)
	o0.SubmitTxn(mkWrite(1, 1, 100, kA), false)
	c0.Set(100)
	o0.ReleaseDue()
	o0.ApplyCommit(i, 100)
	prep, ok := o0.TakeReplicateNow(i)
	if !ok {
		t.Fatal()
	}
	if _, again := o0.TakeReplicateNow(i); again {
		t.Fatal("only once")
	}
	if !o1.HandleReplicatePrepare(i, prep.AgreedT, &prep.Body) {
		t.Fatal()
	}
	if _, ok := o1.TakeReplicateNow(i); ok {
		t.Fatal("follower never drives")
	}
	if o0.RecordPrepareReply(i, 1) != PrepCommitted {
		t.Fatal()
	}
	// ReplicateCommit to server 1 is LOST.
	c1.Set(100 + 500)
	if len(o1.CollectStaleCommitting(1000, 0)) != 0 {
		t.Fatal("not stale yet")
	}
	c1.Set(100 + 2000)
	re := o1.CollectStaleCommitting(1000, 0)
	if len(re) != 1 || re[0].Primary != 0 {
		t.Fatal(re)
	}
	if len(o1.CollectStaleCommitting(1000, 0)) != 0 {
		t.Fatal("throttled")
	}
	if o0.RecordPrepareReply(i, 1) != PrepResend {
		t.Fatal()
	}
	o0.ExecuteReady()
	if !o0.Executed(i) || o0.RecordPrepareReply(i, 1) != PrepResend {
		t.Fatal()
	}
	o1.MarkCommitted(i)
	if st, _ := mustEntry(t, o1, i); st != types.Committed {
		t.Fatal()
	}
}

func TestRobust_ExecutorServicesReadAfterCommitWithoutTick(t *testing.T) {
	clk := clock.NewMock(50)
	st := store.NewMem()
	c := newCollector()
	o := New(0, 0, clk, topology.New(1), c.cb(), st, 0)
	o.Start(true)
	defer o.Stop()
	w, r := id(1, 1), id(1, 2)
	o.SubmitTxn(mkWriteV(1, 1, 100, "K", "v1"), true)
	clk.Set(100)
	if !waitFor(func() bool { _, ok := c.find(w); return ok }) {
		t.Fatal("write not released")
	}
	if res, _ := o.ApplyCommit(w, 100); res != Applied {
		t.Fatal()
	}
	o.MarkCommitted(w)
	if !waitFor(func() bool { return st.ValueOrEmpty("K") == "v1" }) {
		t.Fatal("not executed")
	}
	o.SubmitTxn(mkRead(1, 2, 200, "K"), true)
	clk.Set(200)
	if !waitFor(func() bool { _, ok := c.find(r); return ok }) {
		t.Fatal("read not served")
	}
	if rr, _ := c.find(r); rr.results["K"] != "v1" {
		t.Fatal(rr)
	}
}
