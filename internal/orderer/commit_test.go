package orderer

import (
	"testing"

	"dynago/internal/clock"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func TestOrdererCommit_LateLeaderReTimestampsAndReleases(t *testing.T) {
	clk := clock.NewMock(1000)
	c := newCollector()
	o := newOrderer(0, clk, 1, c)
	o.SubmitTxn(mkWrite(1, 1, 500, "k"), false) // late
	if c.count() != 1 || c.at(0).id != id(1, 1) || c.at(0).t != 1000 {
		t.Fatal(c.got)
	}
	if o.EarlySize() != 0 || o.LateSize() != 0 {
		t.Fatal()
	}
	if st, ts := mustEntry(t, o, id(1, 1)); st != types.Speculative || ts != 1000 {
		t.Fatal(st, ts)
	}
	if o.HighestReleased().T != 1000 {
		t.Fatal()
	}
}

func TestOrdererCommit_OnTimeLeaderTxnBehindHighestReleasedKeepsItsT(t *testing.T) {
	clk := clock.NewMock(100)
	c := newCollector()
	o := newOrderer(0, clk, 1, c)
	o.SubmitTxn(mkWrite(5, 5, 100, "k"), false)
	if o.ReleaseDue() != 1 || o.HighestReleased().T != 100 {
		t.Fatal()
	}
	o.SubmitTxn(mkWrite(1, 1, 100, "k"), false)
	if o.ReleaseDue() != 1 || c.count() != 2 {
		t.Fatal()
	}
	if st, ts := mustEntry(t, o, id(1, 1)); st != types.Speculative || ts != 100 {
		t.Fatal("its own t, not bumped", st, ts)
	}
	if c.at(1).t != 100 {
		t.Fatal()
	}
	if o.Lanes() == 1 && o.HighestReleased() != (types.OrderKey{T: 100, ClientID: 5, RequestID: 5}) {
		t.Fatal("lowered")
	}
}

func TestOrdererCommit_LateFollowerBuffersSilently(t *testing.T) {
	clk := clock.NewMock(1000)
	c := newCollector()
	o := newOrderer(0, clk, 3, c)
	o.SubmitTxn(mkWrite(1, 1, 500, topology.KeyForPartition(1, 3)), false)
	if c.count() != 0 || o.LateSize() != 1 || o.SpecLogSize() != 0 || o.EarlySize() != 0 {
		t.Fatal()
	}
}

func TestOrdererCommit_ApplyCommitAdoptsAgreedTFromSpecLog(t *testing.T) {
	clk := clock.NewMock(1000)
	o := newOrderer(0, clk, 1, newCollector())
	o.SubmitTxn(mkWrite(1, 1, 1500, "k"), false)
	clk.Set(1500)
	if o.ReleaseDue() != 1 {
		t.Fatal()
	}
	r, b := o.ApplyCommit(id(1, 1), 1600)
	if r != Applied || b.T != 1600 || b.Status != types.Committing {
		t.Fatal(r, b)
	}
	if st, ts := mustEntry(t, o, id(1, 1)); st != types.Committing || ts != 1600 {
		t.Fatal()
	}
	if o.HighestReleased().T != 1600 {
		t.Fatal()
	}
}

func TestOrdererCommit_ApplyCommitMovesEarlyBufferToSpecLog(t *testing.T) {
	clk := clock.NewMock(1000)
	o := newOrderer(0, clk, 1, newCollector())
	o.SubmitTxn(mkWrite(1, 1, 2000, "k"), false)
	r, b := o.ApplyCommit(id(1, 1), 2100)
	if r != Applied || b.Status != types.Committing || o.EarlySize() != 0 || o.SpecLogSize() != 1 {
		t.Fatal()
	}
	if _, ts := mustEntry(t, o, id(1, 1)); ts != 2100 {
		t.Fatal()
	}
}

func TestOrdererCommit_ApplyCommitMovesLateBufferToSpecLog(t *testing.T) {
	clk := clock.NewMock(1000)
	o := newOrderer(0, clk, 3, newCollector())
	o.SubmitTxn(mkWrite(1, 1, 500, topology.KeyForPartition(1, 3)), false)
	if o.LateSize() != 1 {
		t.Fatal()
	}
	r, b := o.ApplyCommit(id(1, 1), 2000)
	if r != Applied || b.T != 2000 || b.Status != types.Committing || o.LateSize() != 0 || o.SpecLogSize() != 1 {
		t.Fatal()
	}
}

func TestOrdererCommit_CommitNeedsFetchThenInstallThenApply(t *testing.T) {
	clk := clock.NewMock(1000)
	o := newOrderer(0, clk, 3, newCollector())
	key := topology.KeyForPartition(1, 3)
	if r, _ := o.ApplyCommit(id(9, 9), 2000); r != NeedFetch {
		t.Fatal(r)
	}
	body := types.Txn{ClientID: 9, RequestID: 9, Type: types.Write,
		WSet: []types.KV{{Key: key, Value: "v"}}, T: 500}
	o.InstallFetched(&body)
	if o.LateSize() != 1 {
		t.Fatal()
	}
	r, b := o.ApplyCommit(id(9, 9), 2000)
	if r != Applied || b.T != 2000 || b.Status != types.Committing || !o.SpecLogContains(id(9, 9)) {
		t.Fatal()
	}
	got, ok := o.TryGetBody(id(9, 9))
	if !ok || got.ClientID != 9 || got.WSet[0].Key != key {
		t.Fatal()
	}
}

// Extra (Go port): a resend of an executed write is answered with its final t; a READ
// resend is ignored; a late READ at a pure follower is dropped.
func TestOrdererCommit_DuplicatesAndLateFollowerRead(t *testing.T) {
	clk := clock.NewMock(1000)
	c := newCollector()
	o := newOrderer(0, clk, 3, c)
	fk := topology.KeyForPartition(1, 3)
	o.SubmitTxn(mkRead(1, 1, 500, fk), false)
	if c.count() != 0 || o.LateSize() != 0 || o.SpecLogSize() != 0 {
		t.Fatal("late follower read must be dropped")
	}
	// A duplicate of a specLog write answers with the live entry.
	lk := topology.KeyForPartition(0, 3)
	o.SubmitTxn(mkWrite(1, 2, 500, lk), false) // late leader: released at 1000
	o.SubmitTxn(mkWrite(1, 2, 500, lk), false)
	if c.count() != 2 || c.at(1).t != 1000 || o.SpecLogSize() != 1 {
		t.Fatal(c.got)
	}
}
