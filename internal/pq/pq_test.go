package pq

import (
	"math/rand"
	"testing"

	"dynago/internal/types"
)

type intVal struct{ key uint64 }

// u64 is a test id with a (deliberately weak) hash: probe runs collide.
type u64 uint64

func (k u64) Hash() uint64 { return uint64(k) * 0x9E3779B97F4A7C15 }

func newIntPQ() *IndexedMinPQ[u64, intVal] {
	return New[u64, intVal](func(a, b *intVal) bool { return a.key < b.key })
}

func TestIndexedMinPQ_PushPeekPopOrder(t *testing.T) {
	q := newIntPQ()
	if !q.Empty() {
		t.Fatal()
	}
	q.Push(1, &intVal{300})
	q.Push(2, &intVal{100})
	q.Push(3, &intVal{200})
	if q.Len() != 3 || !q.CheckInvariants() {
		t.Fatal()
	}
	if q.PeekMin().key != 100 || q.PeekMinID() != 2 {
		t.Fatal()
	}
	for _, want := range []uint64{100, 200, 300} {
		if got := q.PopMin().key; got != want {
			t.Fatal(got, want)
		}
	}
	if !q.Empty() {
		t.Fatal()
	}
}

func TestIndexedMinPQ_DuplicatePushRejected(t *testing.T) {
	q := newIntPQ()
	if !q.Push(1, &intVal{10}) || q.Push(1, &intVal{5}) {
		t.Fatal()
	}
	if q.Len() != 1 || q.PeekMin().key != 10 {
		t.Fatal("original value retained")
	}
}

func TestIndexedMinPQ_ContainsAndFind(t *testing.T) {
	q := newIntPQ()
	q.Push(5, &intVal{50})
	if !q.Contains(5) || q.Contains(6) {
		t.Fatal()
	}
	if q.Find(5) == nil || q.Find(5).key != 50 || q.Find(6) != nil {
		t.Fatal()
	}
}

func TestIndexedMinPQ_EraseArbitrary(t *testing.T) {
	q := newIntPQ()
	for i := uint64(0); i < 10; i++ {
		q.Push(u64(i), &intVal{i * 10})
	}
	if q.Erase(4) == nil || q.Contains(4) || q.Erase(4) != nil || q.Len() != 9 || !q.CheckInvariants() {
		t.Fatal()
	}
	prev := uint64(0)
	for !q.Empty() {
		k := q.PopMin().key
		if k == 40 || k < prev {
			t.Fatal(k)
		}
		prev = k
	}
}

func TestIndexedMinPQ_UpdateKeyReheapifiesDown(t *testing.T) {
	q := newIntPQ()
	q.Push(1, &intVal{10})
	q.Push(2, &intVal{20})
	q.Push(3, &intVal{30})
	q.Find(1).key = 100
	if !q.UpdateKey(1) || !q.CheckInvariants() || q.PeekMinID() != 2 {
		t.Fatal()
	}
	for _, want := range []uint64{20, 30, 100} {
		if got := q.PopMin().key; got != want {
			t.Fatal(got, want)
		}
	}
}

func TestIndexedMinPQ_UpdateKeyReheapifiesUp(t *testing.T) {
	q := newIntPQ()
	q.Push(1, &intVal{10})
	q.Push(2, &intVal{20})
	q.Push(3, &intVal{30})
	q.Find(3).key = 1
	if !q.UpdateKey(3) || !q.CheckInvariants() || q.PeekMinID() != 3 || q.PopMin().key != 1 {
		t.Fatal()
	}
}

func TestIndexedMinPQ_UpdateKeyMissingReturnsFalse(t *testing.T) {
	q := newIntPQ()
	q.Push(1, &intVal{10})
	if q.UpdateKey(99) {
		t.Fatal()
	}
}

func TestIndexedMinPQ_RandomizedMatchesOracle(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))
	q := newIntPQ()
	oracle := map[u64]uint64{}
	var ids []u64 // live ids, for random picks
	nextID := u64(1)
	oracleMin := func() uint64 {
		best := ^uint64(0)
		for _, k := range oracle {
			if k < best {
				best = k
			}
		}
		return best
	}
	removeID := func(id u64) {
		for i, x := range ids {
			if x == id {
				ids = append(ids[:i], ids[i+1:]...)
				return
			}
		}
	}
	for step := 0; step < 5000; step++ {
		op := rng.Intn(4)
		switch {
		case op == 0 || len(oracle) == 0:
			id, key := nextID, uint64(rng.Intn(1000))
			nextID++
			q.Push(id, &intVal{key})
			oracle[id] = key
			ids = append(ids, id)
		case op == 1:
			if q.PeekMin().key != oracleMin() {
				t.Fatal("min mismatch")
			}
			id := q.PeekMinID()
			q.PopMin()
			delete(oracle, id)
			removeID(id)
		case op == 2:
			id := ids[rng.Intn(len(ids))]
			if q.Erase(id) == nil {
				t.Fatal("erase")
			}
			delete(oracle, id)
			removeID(id)
		default:
			id := ids[rng.Intn(len(ids))]
			key := uint64(rng.Intn(1000))
			q.Find(id).key = key
			if !q.UpdateKey(id) {
				t.Fatal("update")
			}
			oracle[id] = key
		}
		if !q.CheckInvariants() || q.Len() != len(oracle) {
			t.Fatalf("invariants at step %d", step)
		}
		if len(oracle) > 0 && q.PeekMin().key != oracleMin() {
			t.Fatalf("min at step %d", step)
		}
	}
}

func TestIndexedMinPQ_TxnEntryByOrderKey(t *testing.T) {
	spec := New[types.TxnId, types.TxnEntry](func(a, b *types.TxnEntry) bool { return a.Less(b) })
	mk := func(c, r, ts uint64) *types.TxnEntry {
		return types.NewEntry(types.Txn{ClientID: c, RequestID: r, Type: types.Write, T: ts})
	}
	spec.Push(types.ID(1, 1), mk(1, 1, 300))
	spec.Push(types.ID(1, 2), mk(1, 2, 100))
	spec.Push(types.ID(2, 1), mk(2, 1, 200))
	if !spec.CheckInvariants() || spec.PeekMin().T != 100 {
		t.Fatal()
	}
	spec.Find(types.ID(1, 2)).T = 999
	if !spec.UpdateKey(types.ID(1, 2)) || !spec.CheckInvariants() {
		t.Fatal()
	}
	for _, want := range []uint64{200, 300, 999} {
		if got := spec.PopMin().T; got != want {
			t.Fatal(got, want)
		}
	}
}
