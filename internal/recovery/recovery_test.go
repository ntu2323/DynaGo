package recovery

import (
	"fmt"
	"testing"

	"dynago/internal/topology"
	"dynago/internal/types"
	"dynago/internal/wire"
)

func TestRecoveryTopology_SurvivorsKeepRangesFailedRangeSplit(t *testing.T) {
	all := topology.New(3)
	v1 := all.WithAlive(0b101)
	took := map[uint32]int{}
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("key%d", i)
		home := all.OwnerOf(k)
		if home != topology.PartitionOf(k, 3) {
			t.Fatal()
		}
		if home != 1 {
			if v1.OwnerOf(k) != home {
				t.Fatal("survivors keep their range")
			}
			continue
		}
		s := v1.OwnerOf(k)
		if s != 0 && s != 2 || !InFailedRange(all, v1, k) || !InDeltaP(all, v1, k, s) {
			t.Fatal(k, s)
		}
		took[s]++
	}
	if took[0] < 100 || took[2] < 100 {
		t.Fatal("failed range not split", took)
	}
}

func TestRecoveryMerge_PrecedenceThenMaxT(t *testing.T) {
	rt := func(r, ts uint64, s types.Status) wire.RecTxn {
		return wire.RecTxn{Txn: types.Txn{ClientID: 1, RequestID: r, Type: types.Write, T: ts}, T: ts, Status: s}
	}
	a := Data{
		Txns: []wire.RecTxn{rt(1, 100, types.Speculative), rt(2, 100, types.Speculative), rt(3, 100, types.Committed)},
		KVs:  []wire.RecKV{{Key: "x", Value: "old", T: 5}, {Key: "y", Value: "y1", T: 9}},
	}
	b := Data{
		Txns: []wire.RecTxn{rt(1, 150, types.Committing), rt(2, 120, types.Speculative), rt(3, 300, types.Speculative)},
		KVs:  []wire.RecKV{{Key: "x", Value: "new", T: 7}},
	}
	m := Merge([]Data{a, b})
	if len(m.Txns) != 3 {
		t.Fatal(m.Txns)
	}
	by := map[uint64]wire.RecTxn{}
	for _, x := range m.Txns {
		by[x.Txn.RequestID] = x
	}
	if by[1].Status != types.Committing || by[1].T != 150 ||
		by[2].Status != types.Speculative || by[2].T != 120 ||
		by[3].Status != types.Committed || by[3].T != 100 {
		t.Fatal(by)
	}
	if len(m.KVs) != 2 || m.KVs[0] != (wire.RecKV{Key: "x", Value: "new", T: 7}) {
		t.Fatal(m.KVs)
	}
	// Wire round trip.
	out, err := wire.Unmarshal(wire.Marshal(nil, &wire.RecoveryPrepare{Txns: m.Txns, KVs: m.KVs}))
	if err != nil {
		t.Fatal(err)
	}
	if p := out.(*wire.RecoveryPrepare); len(p.Txns) != 3 || len(p.KVs) != 2 || p.Txns[0].Status != m.Txns[0].Status {
		t.Fatal(p)
	}
}
