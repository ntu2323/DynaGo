package orderer

import (
	"testing"

	"dynago/internal/clock"
	"dynago/internal/quorum"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func mkWrite2(c, r, t uint64, k1, k2 string) *types.TxnEntry {
	e := mkWrite(c, r, t, k1)
	e.Txn.WSet = append(e.Txn.WSet, types.KV{Key: k2, Value: "v"})
	return e
}

// Leaders disagree on t → SlowCommit(max) → both adopt → the CommitReply quorum
// re-forms at the max.
func TestSlowPath_LeaderDisagreementResolvesToMaxAndCommits(t *testing.T) {
	kA, kB := topology.KeyForPartition(0, 3), topology.KeyForPartition(1, 3)
	i := id(1, 1)
	c0 := clock.NewMock(50)
	o0 := New(0, 0, c0, topology.New(3), nil, nil, 0)
	o0.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false)
	c0.Set(100)
	if o0.ReleaseDue() != 1 {
		t.Fatal()
	}
	c1 := clock.NewMock(200)
	o1 := New(1, 0, c1, topology.New(3), nil, nil, 0)
	o1.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false) // late → re-timestamped to 200
	if _, ts := mustEntry(t, o0, i); ts != 100 {
		t.Fatal(ts)
	}
	if _, ts := mustEntry(t, o1, i); ts != 200 {
		t.Fatal(ts)
	}
	var ls types.ServerSet
	ls.Add(0)
	ls.Add(1)
	q := quorum.New(ls, 2, 0)
	q.AddReply(0, 0, 100, true)
	q.AddReply(1, 0, 200, true)
	if q.FastReady() || !q.LeadersDisagree() || q.SlowAgreedT() != 200 {
		t.Fatal()
	}
	for _, o := range []*Orderer{o0, o1} {
		if r, _ := o.ApplyCommit(i, 200); r != Applied {
			t.Fatal(r)
		}
		if st, ts := mustEntry(t, o, i); st != types.Committing || ts != 200 {
			t.Fatal(st, ts)
		}
	}
	q.AddReply(0, 0, 200, true)
	q.AddReply(1, 0, 200, true)
	if !q.FastReady() || q.AgreedT() != 200 {
		t.Fatal()
	}
}

func TestSlowPath_FollowerAdoptsAgreedTFromLateBuffer(t *testing.T) {
	c2 := clock.NewMock(1000)
	o2 := New(2, 0, c2, topology.New(3), nil, nil, 0)
	o2.SubmitTxn(mkWrite(1, 1, 500, topology.KeyForPartition(0, 3)), false)
	if o2.LateSize() != 1 || o2.SpecLogSize() != 0 {
		t.Fatal()
	}
	if r, _ := o2.ApplyCommit(id(1, 1), 700); r != Applied || o2.LateSize() != 0 {
		t.Fatal(r)
	}
	if st, ts := mustEntry(t, o2, id(1, 1)); st != types.Committing || ts != 700 {
		t.Fatal(st, ts)
	}
}
