package topology

import (
	"fmt"
	"testing"
)

func TestTopology_HomeLeadersWhenAllAlive(t *testing.T) {
	tp := New(3)
	counts := make([]int, 3)
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("key%d", i)
		if tp.OwnerOf(k) != tp.PartitionOf(k) {
			t.Fatal("all alive: owner must be home")
		}
		counts[tp.PartitionOf(k)]++
	}
	for _, c := range counts {
		if c < 800 {
			t.Fatal("partitions badly unbalanced", counts)
		}
	}
}

func TestTopology_FailedRangeSplitsAmongSurvivorsOnly(t *testing.T) {
	tp := New(5).WithAlive(New(5).Alive.Without(2))
	got := map[uint32]int{}
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key%d", i)
		o := tp.OwnerOf(k)
		if o == 2 {
			t.Fatal("dead server owns a key")
		}
		if home := PartitionOf(k, 5); home != 2 && o != home {
			t.Fatal("a survivor lost its range")
		} else if home == 2 {
			got[o]++
		}
	}
	if len(got) < 3 {
		t.Fatal("failed range not spread", got)
	}
}

func TestTopology_KeyForPartition(t *testing.T) {
	for p := uint32(0); p < 3; p++ {
		if PartitionOf(KeyForPartition(p, 3), 3) != p {
			t.Fatal(p)
		}
	}
}
