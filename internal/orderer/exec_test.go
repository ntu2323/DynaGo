package orderer

import (
	"slices"
	"testing"

	"dynago/internal/clock"
	"dynago/internal/store"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func mkWriteV(c, r, t uint64, k, v string) *types.TxnEntry {
	e := mkWrite(c, r, t, k)
	e.Txn.WSet[0].Value = v
	return e
}

func releaseAt(o *Orderer, clk *clock.Mock, e *types.TxnEntry, at uint64) {
	o.SubmitTxn(e, false)
	clk.Set(at)
	o.ReleaseDue()
}

func execOrderer(lanes int) (*Orderer, *clock.Mock, *store.Mem) {
	clk := clock.NewMock(50)
	st := store.NewMem()
	return New(0, 0, clk, topology.New(1), nil, st, lanes), clk, st
}

func TestExec_CommittedWriteExecutesAndApplies(t *testing.T) {
	o, clk, st := execOrderer(0)
	releaseAt(o, clk, mkWriteV(1, 1, 100, "k", "v1"), 100)
	o.MarkCommitted(id(1, 1))
	done := o.ExecuteReady()
	if len(done) != 1 || done[0] != id(1, 1) || st.ValueOrEmpty("k") != "v1" || o.SpecLogSize() != 0 {
		t.Fatal(done)
	}
}

func TestExec_WWConflictExecutesInOrderKeyOrder(t *testing.T) {
	o, clk, st := execOrderer(0)
	releaseAt(o, clk, mkWriteV(1, 1, 100, "K", "a"), 100)
	releaseAt(o, clk, mkWriteV(1, 2, 200, "K", "b"), 200)
	o.MarkCommitted(id(1, 2))
	if len(o.ExecuteReady()) != 0 {
		t.Fatal("blocked by the un-committed smaller write")
	}
	if _, ok := st.Read("K"); ok {
		t.Fatal()
	}
	o.MarkCommitted(id(1, 1))
	done := o.ExecuteReady()
	if !slices.Equal(done, []types.TxnId{id(1, 1), id(1, 2)}) || st.ValueOrEmpty("K") != "b" || o.SpecLogSize() != 0 {
		t.Fatal(done)
	}
}

func TestExec_NonConflictingExecutesDespiteUncommittedSmaller(t *testing.T) {
	o, clk, st := execOrderer(0)
	releaseAt(o, clk, mkWriteV(1, 1, 100, "A", "x"), 100)
	releaseAt(o, clk, mkWriteV(1, 2, 200, "B", "y"), 200)
	o.MarkCommitted(id(1, 2))
	done := o.ExecuteReady()
	if !slices.Equal(done, []types.TxnId{id(1, 2)}) || st.ValueOrEmpty("B") != "y" {
		t.Fatal(done)
	}
	if _, ok := st.Read("A"); ok || !o.SpecLogContains(id(1, 1)) || o.SpecLogContains(id(1, 2)) {
		t.Fatal()
	}
}

func TestExec_AppliedWithCommitTimestamp(t *testing.T) {
	o, clk, st := execOrderer(0)
	releaseAt(o, clk, mkWriteV(1, 1, 100, "k", "v"), 100)
	o.MarkCommitted(id(1, 1))
	if len(o.ExecuteReady()) != 1 {
		t.Fatal()
	}
	if v, ok := st.Read("k"); !ok || v.Value != "v" || v.T != 100 || st.ApplyCount() != 1 {
		t.Fatal(v)
	}
}

func TestExec_ShortcutsPreserveDependencyRule(t *testing.T) {
	o, clk, st := execOrderer(0)
	releaseAt(o, clk, mkWriteV(1, 1, 100, "K", "a"), 100)
	releaseAt(o, clk, mkWriteV(1, 2, 110, "K", "b"), 110)
	releaseAt(o, clk, mkWriteV(1, 3, 120, "J", "c"), 120)
	releaseAt(o, clk, mkWriteV(1, 4, 130, "K", "d"), 130)
	if len(o.ExecuteReady()) != 0 || o.SpecLogSize() != 4 {
		t.Fatal()
	}
	o.MarkCommitted(id(1, 2))
	o.MarkCommitted(id(1, 3))
	if done := o.ExecuteReady(); !slices.Equal(done, []types.TxnId{id(1, 3)}) {
		t.Fatal(done)
	}
	if st.ValueOrEmpty("J") != "c" || st.ValueOrEmpty("K") != "" {
		t.Fatal()
	}
	o.MarkCommitted(id(1, 1))
	if done := o.ExecuteReady(); !slices.Equal(done, []types.TxnId{id(1, 1), id(1, 2)}) {
		t.Fatal(done)
	}
	if st.ValueOrEmpty("K") != "b" || !o.SpecLogContains(id(1, 4)) || o.SpecLogSize() != 1 {
		t.Fatal()
	}
}

func TestExecLanes_CrossLaneConflictExecutesInOrderKeyOrder(t *testing.T) {
	o, clk, st := execOrderer(4)
	small := id(1, 1)
	r2 := uint64(2)
	for o.LaneOf(id(2, r2)) == o.LaneOf(small) {
		r2++
	}
	large := id(2, r2)
	releaseAt(o, clk, mkWriteV(1, 1, 100, "K", "a"), 100)
	releaseAt(o, clk, mkWriteV(2, r2, 200, "K", "b"), 200)
	o.MarkCommitted(large)
	if len(o.ExecuteReady()) != 0 || st.ValueOrEmpty("K") != "" {
		t.Fatal()
	}
	o.MarkCommitted(small)
	if done := o.ExecuteReady(); !slices.Equal(done, []types.TxnId{small, large}) {
		t.Fatal(done)
	}
	if st.ValueOrEmpty("K") != "b" || !o.Executed(small) || !o.Executed(large) || o.SpecLogSize() != 0 {
		t.Fatal()
	}
}

func TestExecLanes_LaneOfStableAndBalanced(t *testing.T) {
	clk := clock.NewMock(0)
	o := New(0, 0, clk, topology.New(3), nil, nil, 4)
	o2 := New(1, 0, clk, topology.New(3), nil, nil, 4)
	var count [4]int
	for c := uint64(1); c <= 8; c++ {
		for r := uint64(1); r <= 1000; r++ {
			i := id(c*0x1234567, r)
			if o.LaneOf(i) != o2.LaneOf(i) {
				t.Fatal("unstable")
			}
			count[o.LaneOf(i)]++
		}
	}
	for _, n := range count {
		if n < 1700 || n > 2300 {
			t.Fatal(count)
		}
	}
}

// A follower executes a COMMITTED write before a smaller conflicting write it only has
// in its lateBuffer (the design's dependency rule sees only the specLog); when that
// smaller write commits later at its own (smaller) t, execution must not overwrite the
// newer value (t-guard, DYNA_EXEC_T_GUARD).
func TestExec_LateSmallerWriteNeverOverwritesNewerValue(t *testing.T) {
	clk := clock.NewMock(50)
	st := store.NewMem()
	key := topology.KeyForPartition(0, 3) // led by server 0; we are follower 1
	o := New(1, 0, clk, topology.New(3), nil, st, 0)
	releaseAt(o, clk, mkWriteV(2, 1, 200, key, "new"), 200)
	o.MarkCommitted(id(2, 1))
	if done := o.ExecuteReady(); len(done) != 1 || st.ValueOrEmpty(key) != "new" {
		t.Fatal(done)
	}
	// The smaller write arrives late at this pure follower: lateBuffer, not specLog.
	clk.Set(300)
	old := mkWriteV(1, 1, 100, key, "old")
	o.SubmitTxn(old, false)
	if o.LateSize() != 1 {
		t.Fatal("expected a lateBuffer entry")
	}
	// The leader + the other follower committed it at t=100; replication reaches us.
	if !o.HandleReplicatePrepare(id(1, 1), 100, nil) {
		t.Fatal("prepare should adopt the lateBuffer copy")
	}
	o.MarkCommitted(id(1, 1))
	if done := o.ExecuteReady(); len(done) != 1 {
		t.Fatal(done)
	}
	if v, _ := st.Read(key); v.Value != "new" || v.T != 200 {
		t.Fatalf("older write overwrote the newer value: %+v", v)
	}
}
