package orderer

import (
	"testing"
	"time"

	"dynago/internal/clock"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func TestServerOrderer_ReleasesAtDeadline(t *testing.T) {
	clk := clock.NewMock(1000)
	c := newCollector()
	o := newOrderer(0, clk, 1, c)
	o.SubmitTxn(mkWrite(1, 1, 1500, "k"), false)
	if o.EarlySize() != 1 {
		t.Fatal()
	}
	if o.ReleaseDue() != 0 || c.count() != 0 {
		t.Fatal("not yet due")
	}
	clk.Set(1500)
	if o.ReleaseDue() != 1 || c.count() != 1 || o.EarlySize() != 0 || o.SpecLogSize() != 1 {
		t.Fatal()
	}
	if !o.SpecLogContains(id(1, 1)) || o.HighestReleased().T != 1500 {
		t.Fatal()
	}
}

func TestServerOrderer_ReleasesInOrderKeyOrder(t *testing.T) {
	clk := clock.NewMock(50)
	c := newCollector()
	o := newOrderer(0, clk, 1, c)
	o.SubmitTxn(mkWrite(1, 1, 300, "k"), false)
	o.SubmitTxn(mkWrite(1, 1001, 100, "k"), false)
	o.SubmitTxn(mkWrite(2, 1, 200, "k"), false)
	if o.EarlySize() != 3 {
		t.Fatal()
	}
	clk.Set(1000)
	if o.ReleaseDue() != 3 {
		t.Fatal()
	}
	if o.Lanes() == 1 { // one lane releases in global OrderKey order
		for i, want := range []types.TxnId{id(1, 1001), id(2, 1), id(1, 1)} {
			if c.at(i).id != want {
				t.Fatal(i, c.at(i).id)
			}
		}
	}
	if o.HighestReleased().T != 300 {
		t.Fatal()
	}
}

// No counter-veto: on-time by the clock is admitted and released even if its OrderKey
// is behind highestReleasedT; highestReleasedT never decreases.
func TestServerOrderer_OnTimeTxnBehindHighestReleasedStillReleases(t *testing.T) {
	clk := clock.NewMock(100)
	c := newCollector()
	o := newOrderer(0, clk, 3, c)
	fk := topology.KeyForPartition(1, 3)
	o.SubmitTxn(mkWrite(5, 5, 100, fk), false)
	if o.ReleaseDue() != 1 || o.HighestReleased() != (types.OrderKey{T: 100, ClientID: 5, RequestID: 5}) {
		t.Fatal()
	}
	o.SubmitTxn(mkWrite(1, 1, 100, fk), false)
	if o.EarlySize() != 1 || o.ReleaseDue() != 1 || c.count() != 2 || o.LateSize() != 0 {
		t.Fatal()
	}
	if !o.SpecLogContains(id(1, 1)) {
		t.Fatal()
	}
	if o.Lanes() == 1 && o.HighestReleased() != (types.OrderKey{T: 100, ClientID: 5, RequestID: 5}) {
		t.Fatal("lowered")
	}
}

func TestServerOrderer_BackgroundLoopReleasesAndStops(t *testing.T) {
	clk := clock.NewMock(1000)
	c := newCollector()
	o := newOrderer(0, clk, 1, c)
	o.Start(true)
	o.SubmitTxn(mkWrite(7, 7, 2000, "k"), true)
	if c.waitFor(1, 100*time.Millisecond) {
		t.Fatal("released early")
	}
	clk.Set(2000)
	if !c.waitFor(1, time.Second) {
		t.Fatal("not released")
	}
	o.Stop()
	if o.SpecLogSize() != 1 {
		t.Fatal()
	}
}

func TestOrdererRelease_ExternalDriverHintTracksEarliestDeadline(t *testing.T) {
	clk := clock.NewMock(50)
	c := newCollector()
	o := newOrderer(0, clk, 1, c)
	o.Start(false)
	defer o.Stop()
	if o.NextDueHint() != clock.Never {
		t.Fatal()
	}
	o.SubmitTxn(mkWrite(1, 1, 300, "k"), false)
	if o.NextDueHint() != 300 {
		t.Fatal()
	}
	o.SubmitTxn(mkWrite(1, 2, 200, "k"), false)
	if o.NextDueHint() != 200 {
		t.Fatal()
	}
	clk.Set(250)
	if c.waitFor(1, 50*time.Millisecond) {
		t.Fatal("no release goroutine: nothing may release")
	}
	if o.ReleaseDue() != 1 || c.count() != 1 || o.NextDueHint() != 300 {
		t.Fatal(o.NextDueHint())
	}
	clk.Set(300)
	if o.ReleaseDue() != 1 || o.NextDueHint() != clock.Never {
		t.Fatal()
	}
}

// The late-leader re-timestamp floor is server-wide: a commit in one lane pushes it to
// 5000; a late txn we lead in ANOTHER lane is re-timestamped above it.
func TestOrdererLanes_LateLeaderRetimestampAboveOtherLanes(t *testing.T) {
	clk := clock.NewMock(100)
	c := newCollector()
	o := New(0, 0, clk, topology.New(1), c.cb(), nil, 4)
	a := id(1, 1)
	rb := uint64(1)
	for o.LaneOf(id(2, rb)) == o.LaneOf(a) {
		rb++
	}
	b := id(2, rb)
	o.SubmitTxn(mkWrite(1, 1, 200, "k"), false)
	clk.Set(200)
	o.ReleaseDue()
	if r, _ := o.ApplyCommit(a, 5000); r != Applied || o.HighestReleased().T != 5000 {
		t.Fatal(r)
	}
	clk.Set(1000)
	o.SubmitTxn(mkWrite(2, rb, 300, "k"), false)
	if st, ts := mustEntry(t, o, b); st != types.Speculative || ts != 5001 {
		t.Fatal(st, ts)
	}
}
