package hmap

import (
	"math/rand"
	"testing"

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
