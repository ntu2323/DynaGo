package orderer

import (
	"testing"

	"dynago/internal/clock"
	"dynago/internal/store"
	"dynago/internal/topology"
)

func readOrderer(n uint32, now uint64) (*Orderer, *clock.Mock, *store.Mem, *collector) {
	clk := clock.NewMock(now)
	st := store.NewMem()
	c := newCollector()
	return New(0, 0, clk, topology.New(n), c.cb(), st, 0), clk, st, c
}

func TestRead_LeaderReadReturnsCommittedValueAfterWriteExecutes(t *testing.T) {
	o, clk, st, c := readOrderer(1, 50)
	o.SubmitTxn(mkWriteV(1, 1, 100, "K", "v1"), false)
	clk.Set(100)
	o.ReleaseDue()
	o.MarkCommitted(id(1, 1))
	o.ExecuteReady()
	if st.ValueOrEmpty("K") != "v1" {
		t.Fatal()
	}
	c.reset()
	o.SubmitTxn(mkRead(1, 2, 200, "K"), false)
	clk.Set(200)
	o.ReleaseDue()
	if c.count() != 0 {
		t.Fatal("held for the executor")
	}
	o.ExecuteReady()
	if c.count() != 1 {
		t.Fatal(c.count())
	}
	r := c.at(0)
	if r.id != id(1, 2) || !r.leader || r.results["K"] != "v1" || o.SpecLogContains(id(1, 2)) {
		t.Fatal(r)
	}
}

func TestRead_ReadBlockedUntilConflictingWriteCommitsAndExecutes(t *testing.T) {
	o, clk, _, c := readOrderer(1, 50)
	o.SubmitTxn(mkWriteV(1, 1, 100, "K", "v"), false)
	o.SubmitTxn(mkRead(1, 2, 200, "K"), false)
	clk.Set(200)
	o.ReleaseDue()
	c.reset()
	o.ExecuteReady()
	if c.count() != 0 {
		t.Fatal("blocked by the un-committed smaller write")
	}
	o.MarkCommitted(id(1, 1))
	o.ExecuteReady()
	if c.count() != 1 || c.at(0).results["K"] != "v" {
		t.Fatal(c.got)
	}
}

func TestRead_LateFollowerReadDroppedSilently(t *testing.T) {
	o, _, _, c := readOrderer(3, 1000)
	o.SubmitTxn(mkRead(1, 1, 500, topology.KeyForPartition(1, 3)), false)
	if c.count() != 0 || o.LateSize() != 0 || o.SpecLogSize() != 0 || o.EarlySize() != 0 {
		t.Fatal()
	}
}

func TestRead_OnTimeFollowerReadEndorsesWithoutLogging(t *testing.T) {
	o, clk, _, c := readOrderer(3, 50)
	o.SubmitTxn(mkRead(1, 1, 100, topology.KeyForPartition(1, 3)), false)
	clk.Set(100)
	o.ReleaseDue()
	if c.count() != 1 || c.at(0).leader || len(c.at(0).results) != 0 || o.SpecLogSize() != 0 {
		t.Fatal(c.got)
	}
}

func TestRead_OnTimeLeaderReadHeldUntilExecuted(t *testing.T) {
	o, clk, _, c := readOrderer(1, 50)
	o.SubmitTxn(mkRead(1, 1, 100, "K"), false)
	clk.Set(100)
	o.ReleaseDue()
	if c.count() != 0 || !o.SpecLogContains(id(1, 1)) {
		t.Fatal()
	}
	o.ExecuteReady()
	if c.count() != 1 || !c.at(0).leader {
		t.Fatal()
	}
	if v, ok := c.at(0).results["K"]; !ok || v != "" {
		t.Fatal("empty value for a never-written key")
	}
}
