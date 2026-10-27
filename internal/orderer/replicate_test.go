package orderer

import (
	"slices"
	"testing"

	"dynago/internal/clock"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func TestReplicate_PrimaryLeaderDrivesCommittedAcrossFollower(t *testing.T) {
	kA := topology.KeyForPartition(0, 3)
	i := id(1, 1)
	c0, c1 := clock.NewMock(50), clock.NewMock(50)
	o0 := New(0, 0, c0, topology.New(3), nil, nil, 0)
	o1 := New(1, 0, c1, topology.New(3), nil, nil, 0)
	o0.SubmitTxn(mkWrite(1, 1, 100, kA), false)
	c0.Set(100)
	o0.ReleaseDue()
	if r, _ := o0.ApplyCommit(i, 100); r != Applied {
		t.Fatal()
	}
	prep := o0.DriveReplication(0, 0)
	if len(prep) != 1 || prep[0].Id != i || prep[0].AgreedT != 100 {
		t.Fatal(prep)
	}
	if !o1.HandleReplicatePrepare(i, prep[0].AgreedT, &prep[0].Body) {
		t.Fatal()
	}
	if st, _ := mustEntry(t, o1, i); st != types.Committing {
		t.Fatal()
	}
	if o0.RecordPrepareReply(i, 0) != PrepNone {
		t.Fatal("self-ack must not count")
	}
	if o0.RecordPrepareReply(i, 1) != PrepCommitted {
		t.Fatal()
	}
	if st, _ := mustEntry(t, o0, i); st != types.Committed {
		t.Fatal()
	}
	o1.MarkCommitted(i)
	if st, _ := mustEntry(t, o1, i); st != types.Committed {
		t.Fatal()
	}
}

func TestReplicate_OnlyPrimaryLeaderDrives(t *testing.T) {
	kA, kB := topology.KeyForPartition(0, 3), topology.KeyForPartition(1, 3)
	i := id(1, 1)
	c0, c1 := clock.NewMock(50), clock.NewMock(50)
	o0 := New(0, 0, c0, topology.New(3), nil, nil, 0)
	o1 := New(1, 0, c1, topology.New(3), nil, nil, 0)
	for _, x := range []struct {
		o *Orderer
		c *clock.Mock
	}{{o0, c0}, {o1, c1}} {
		x.o.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false)
		x.c.Set(100)
		x.o.ReleaseDue()
		if r, _ := x.o.ApplyCommit(i, 100); r != Applied {
			t.Fatal()
		}
	}
	if len(o0.DriveReplication(0, 0)) != 1 || len(o1.DriveReplication(0, 0)) != 0 {
		t.Fatal("only the min-id involved leader drives")
	}
}

func TestReplicate_PrepareAdoptsHeldLateBufferEntry(t *testing.T) {
	kA := topology.KeyForPartition(0, 3)
	c2 := clock.NewMock(1000)
	o2 := New(2, 0, c2, topology.New(3), nil, nil, 0)
	o2.SubmitTxn(mkWrite(1, 1, 500, kA), false)
	if o2.LateSize() != 1 {
		t.Fatal()
	}
	body := mkWrite(1, 1, 500, kA).Txn
	if !o2.HandleReplicatePrepare(id(1, 1), 700, &body) || o2.LateSize() != 0 {
		t.Fatal()
	}
	if st, ts := mustEntry(t, o2, id(1, 1)); st != types.Committing || ts != 700 {
		t.Fatal()
	}
}

func TestReplicate_BudgetedScanPicksSmallestOrderKeysFirst(t *testing.T) {
	kA := topology.KeyForPartition(0, 3)
	c0 := clock.NewMock(50)
	o0 := New(0, 0, c0, topology.New(3), nil, nil, 0)
	for i := uint64(10); i > 0; i-- {
		o0.SubmitTxn(mkWrite(1, i, 100+i-1, kA), false)
	}
	c0.Set(200)
	if o0.ReleaseDue() != 10 {
		t.Fatal()
	}
	for i := uint64(0); i < 10; i++ {
		if r, _ := o0.ApplyCommit(id(1, i+1), 100+i); r != Applied {
			t.Fatal()
		}
	}
	ts := func(v []ReplicateAction) []uint64 {
		var r []uint64
		for _, a := range v {
			r = append(r, a.AgreedT)
		}
		slices.Sort(r)
		return r
	}
	if got := ts(o0.DriveReplication(1000, 3)); !slices.Equal(got, []uint64{100, 101, 102}) {
		t.Fatal(got)
	}
	if got := ts(o0.DriveReplication(1000, 3)); !slices.Equal(got, []uint64{103, 104, 105}) {
		t.Fatal(got)
	}
	if got := ts(o0.DriveReplication(1000, 0)); !slices.Equal(got, []uint64{106, 107, 108, 109}) {
		t.Fatal(got)
	}
}
