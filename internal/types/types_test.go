package types

import "testing"

func TestOrderKey_LexicographicByTimestampFirst(t *testing.T) {
	a := OrderKey{100, 9, 9}
	b := OrderKey{200, 1, 1}
	if !a.Less(b) || b.Less(a) || a == b {
		t.Fatal("t must dominate")
	}
}

func TestOrderKey_TieBreaksByClientThenRequest(t *testing.T) {
	base := OrderKey{100, 2, 5}
	if !(OrderKey{100, 1, 9}).Less(base) {
		t.Fatal("client 1 < client 2")
	}
	if !(OrderKey{100, 2, 1}).Less(base) {
		t.Fatal("request 1 < 5")
	}
	if !base.Less(OrderKey{100, 2, 6}) {
		t.Fatal("request 6 > 5")
	}
}

func TestOrderKey_EqualityAndComparators(t *testing.T) {
	a, b := OrderKey{100, 1, 1}, OrderKey{100, 1, 1}
	if a != b || a.Less(b) || b.Less(a) {
		t.Fatal("equal keys")
	}
}

func TestOrderKey_GloballyUniqueAcrossDistinctTxns(t *testing.T) {
	seen := map[OrderKey]bool{}
	for c := uint64(0); c < 20; c++ {
		for r := uint64(0); r < 20; r++ {
			k := MakeOrderKey(500, TxnId{c, r})
			if seen[k] {
				t.Fatal("duplicate OrderKey")
			}
			seen[k] = true
		}
	}
	if len(seen) != 400 {
		t.Fatal(len(seen))
	}
}

func TestOrderKey_BumpPreservesIdentity(t *testing.T) {
	id := TxnId{7, 42}
	before, after := MakeOrderKey(100, id), MakeOrderKey(999, id)
	if !before.Less(after) || after.Id() != before.Id() {
		t.Fatal("bump must keep identity")
	}
}

func TestTxnEntry_KeyAndOrderDeriveFromBody(t *testing.T) {
	e := NewEntry(Txn{ClientID: 7, RequestID: 42, Type: Write, T: 100})
	e.T = 250 // live t differs from the wire t after a bump
	if e.Key() != (TxnId{7, 42}) {
		t.Fatal(e.Key())
	}
	if e.Order() != (OrderKey{250, 7, 42}) {
		t.Fatal("Order() must use the LIVE t", e.Order())
	}
}

func TestTxnIdHash_DistinctForSequentialIds(t *testing.T) {
	seen := map[uint64]bool{}
	for c := uint64(1); c <= 24; c++ {
		for r := uint64(1); r <= 1000; r++ {
			seen[TxnId{c, r}.Hash()] = true
		}
	}
	if len(seen) != 24*1000 {
		t.Fatal(len(seen))
	}
	if (TxnId{1, 2}).Hash() == (TxnId{2, 1}).Hash() {
		t.Fatal("hash symmetric in (client, req)")
	}
}

func TestTxnIdHash_TxnMapLookupsWork(t *testing.T) {
	m := map[TxnId]int{}
	s := map[TxnId]struct{}{}
	for r := uint64(1); r <= 5000; r++ {
		m[TxnId{7, r}] = int(r)
		s[TxnId{7, r}] = struct{}{}
	}
	if len(m) != 5000 || m[TxnId{7, 4321}] != 4321 {
		t.Fatal("map")
	}
	if _, ok := s[TxnId{8, 1}]; ok {
		t.Fatal("set")
	}
}

func TestServerSet(t *testing.T) {
	var s ServerSet
	s.Add(0)
	s.Add(3)
	if !s.Has(3) || s.Has(1) || s.Count() != 2 {
		t.Fatal(s)
	}
	m := s.Members()
	if len(m) != 2 || m[0] != 0 || m[1] != 3 {
		t.Fatal(m)
	}
	if s.Without(3).Count() != 1 {
		t.Fatal("without")
	}
}
