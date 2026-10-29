package hmap

import (
	"math/rand"
	"testing"
	"time"

	"dynago/internal/types"
)

// Random puts / deletes / gets against a Go map, with a tiny key space so probe runs
// collide and wrap (backward-shift deletion is the delicate part).
func TestHmap_MatchesGoMap(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	for round := 0; round < 20; round++ {
		m := New[types.TxnId, int](0)
		ref := map[types.TxnId]int{}
		space := uint64(8 + rng.Intn(500))
		for op := 0; op < 20000; op++ {
			k := types.ID(rng.Uint64()%4, rng.Uint64()%space)
			switch rng.Intn(3) {
			case 0:
				m.Put(k, op)
				ref[k] = op
			case 1:
				_, want := ref[k]
				if got := m.Delete(k); got != want {
					t.Fatalf("delete %v: got %v want %v", k, got, want)
				}
				delete(ref, k)
			default:
				v, ok := m.Get(k)
				rv, rok := ref[k]
				if ok != rok || v != rv {
					t.Fatalf("get %v: got (%d,%v) want (%d,%v)", k, v, ok, rv, rok)
				}
			}
			if m.Len() != len(ref) {
				t.Fatalf("len %d want %d", m.Len(), len(ref))
			}
		}
		n := 0
		m.Range(func(k types.TxnId, v *int) bool {
			if ref[k] != *v {
				t.Fatalf("range %v", k)
			}
			n++
			return true
		})
		if n != len(ref) {
			t.Fatal(n)
		}
	}
}

// Growth is incremental: with the key space far larger than the table, nearly every
// operation runs while an old table is still being drained. Ptr writes, deletes and
// lookups of entries on either side must all see one consistent map.
func TestHmap_IncrementalResize(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	m := New[types.TxnId, int](0)
	ref := map[types.TxnId]int{}
	sawMigration := false
	for op := 0; op < 300000; op++ {
		k := types.ID(rng.Uint64()%3, rng.Uint64()%200000)
		switch r := rng.Intn(10); {
		case r < 6:
			m.Put(k, op)
			ref[k] = op
		case r < 7:
			_, want := ref[k]
			if got := m.Delete(k); got != want {
				t.Fatalf("op %d delete %v: got %v want %v", op, k, got, want)
			}
			delete(ref, k)
		case r < 8:
			p := m.Ptr(k)
			if _, ok := ref[k]; ok != (p != nil) {
				t.Fatalf("op %d ptr %v: got %v want present=%v", op, k, p, ok)
			}
			if p != nil {
				*p = -op
				ref[k] = -op
			}
		default:
			v, ok := m.Get(k)
			rv, rok := ref[k]
			if ok != rok || v != rv || m.Has(k) != rok || m.At(k) != rv {
				t.Fatalf("op %d get %v: got (%d,%v) want (%d,%v)", op, k, v, ok, rv, rok)
			}
		}
		if m.old != nil {
			sawMigration = true
			if op%997 == 0 { // Range sees both tables exactly once
				n := 0
				m.Range(func(k types.TxnId, v *int) bool {
					if ref[k] != *v {
						t.Fatalf("op %d range %v: %d want %d", op, k, *v, ref[k])
					}
					n++
					return true
				})
				if n != len(ref) {
					t.Fatalf("op %d range saw %d want %d", op, n, len(ref))
				}
			}
		}
		if m.Len() != len(ref) {
			t.Fatalf("op %d len %d want %d", op, m.Len(), len(ref))
		}
	}
	if !sawMigration {
		t.Fatal("no operation ran during a migration")
	}
}

// A resize no longer copies the whole table in one Put: the worst single Put while
// growing to ~4M entries stays far below a full rehash (which took ~100 ms).
func TestHmap_PutLatencyWhileGrowing(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates ~250 MB")
	}
	m := New[types.TxnId, uint64](1 << 10)
	var worst time.Duration
	for i := uint64(0); i < 4_000_000; i++ {
		t0 := time.Now()
		m.Put(types.ID(i%7, i), i)
		if d := time.Since(t0); d > worst {
			worst = d
		}
	}
	// Starting a resize allocates the new table (zeroed pages are faulted in lazily),
	// so allow a few ms for that, not a whole-table copy.
	if worst > 20*time.Millisecond {
		t.Fatalf("worst Put took %v", worst)
	}
	t.Logf("worst Put %v", worst)
}

func BenchmarkHmapPutGetDelete(b *testing.B) {
	m := New[types.TxnId, uint64](1 << 16)
	for i := 0; i < b.N; i++ {
		k := types.ID(7, uint64(i))
		m.Put(k, uint64(i))
		m.Get(k)
		if i >= 1<<15 {
			m.Delete(types.ID(7, uint64(i-1<<15)))
		}
	}
}

func BenchmarkGoMapPutGetDelete(b *testing.B) {
	m := make(map[types.TxnId]uint64, 1<<16)
	for i := 0; i < b.N; i++ {
		k := types.ID(7, uint64(i))
		m[k] = uint64(i)
		_ = m[k]
		if i >= 1<<15 {
			delete(m, types.ID(7, uint64(i-1<<15)))
		}
	}
}
