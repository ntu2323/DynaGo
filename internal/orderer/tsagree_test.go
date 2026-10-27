package orderer

import (
	"testing"

	"dynago/internal/clock"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func TestTsAgree_TwoLeadersConvergeToMax(t *testing.T) {
	kA, kB := topology.KeyForPartition(0, 3), topology.KeyForPartition(1, 3)
	i := id(1, 1)
	c0, c1 := clock.NewMock(50), clock.NewMock(200)
	o0 := New(0, 0, c0, topology.New(3), nil, nil, 0)
	o1 := New(1, 0, c1, topology.New(3), nil, nil, 0)
	o0.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false)
	c0.Set(100)
	if o0.ReleaseDue() != 1 {
		t.Fatal()
	}
	o1.SubmitTxn(mkWrite2(1, 1, 100, kA, kB), false) // late → 200
	if st, ts := mustEntry(t, o0, i); st != types.Speculative || ts != 100 {
		t.Fatal()
	}
	if st, ts := mustEntry(t, o1, i); st != types.Speculative || ts != 200 {
		t.Fatal()
	}
	a0, d0 := o0.DriveTsAgreement(0, 0)
	a1, d1 := o1.DriveTsAgreement(0, 0)
	if len(a0) != 1 || a0[0].ProposedT != 100 || !a0[0].To.Has(1) {
		t.Fatal(a0)
	}
	if len(a1) != 1 || a1[0].ProposedT != 200 || !a1[0].To.Has(0) {
		t.Fatal(a1)
	}
	if len(d0) != 0 || len(d1) != 0 {
		t.Fatal("not complete yet")
	}
	if !o1.RecordTsProposal(i, 0, a0[0].ProposedT, false).Transitioned ||
		!o0.RecordTsProposal(i, 1, a1[0].ProposedT, false).Transitioned {
		t.Fatal()
	}
	for _, o := range []*Orderer{o0, o1} {
		if st, ts := mustEntry(t, o, i); st != types.Committing || ts != 200 {
			t.Fatal(st, ts)
		}
	}
	if o0.HighestReleased().T != 200 {
		t.Fatal()
	}
}

func TestTsAgree_SingleLeaderCommitsImmediately(t *testing.T) {
	clk := clock.NewMock(50)
	o := New(0, 0, clk, topology.New(1), nil, nil, 0)
	o.SubmitTxn(mkWrite(1, 1, 100, "k"), false)
	clk.Set(100)
	o.ReleaseDue()
	a, d := o.DriveTsAgreement(0, 0)
	if len(a) != 0 || len(d) != 1 {
		t.Fatal(a, d)
	}
	if st, ts := mustEntry(t, o, id(1, 1)); st != types.Committing || ts != 100 {
		t.Fatal()
	}
}

func TestTsAgree_NotStuckBeforeTimeout(t *testing.T) {
	clk := clock.NewMock(1000)
	o := New(0, 0, clk, topology.New(1), nil, nil, 0)
	o.SubmitTxn(mkWrite(1, 1, 1000, "k"), false)
	o.ReleaseDue()
	if a, d := o.DriveTsAgreement(1_000_000, 0); len(a) != 0 || len(d) != 0 {
		t.Fatal()
	}
	if st, _ := mustEntry(t, o, id(1, 1)); st != types.Speculative {
		t.Fatal()
	}
	clk.Set(1000 + 1_000_000)
	if _, d := o.DriveTsAgreement(1_000_000, 0); len(d) != 1 {
		t.Fatal()
	}
	if st, _ := mustEntry(t, o, id(1, 1)); st != types.Committing {
		t.Fatal()
	}
}
