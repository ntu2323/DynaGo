package simcluster

import (
	"fmt"
	"testing"

	"dynago/internal/topology"
	"dynago/internal/types"
	"dynago/internal/vr"
)

// workload issues n single-key writes spread over every partition from all clients,
// stepping `gap` µs between them; returns the outcomes and the key of each write.
func workload(c *Cluster, n int, gap uint64, tag string) ([]*outcome, []string) {
	var outs []*outcome
	var keys []string
	for i := 0; i < n; i++ {
		o := &outcome{}
		key := fmt.Sprintf("%s-%d", tag, i)
		outs = append(outs, o)
		keys = append(keys, key)
		c.Clients[i%len(c.Clients)].Write([]types.KV{{Key: key, Value: tag}}, o.cb())
		c.Run(gap, gap)
	}
	return outs, keys
}

func allDone(outs []*outcome) int {
	n := 0
	for _, o := range outs {
		if o.done {
			n++
		}
	}
	return n
}

func viewOf(c *Cluster, i int) (vr.Phase, uint64, uint64) { return c.Replicas[i].ViewState() }

func requireView(t *testing.T, c *Cluster, view, alive uint64) {
	t.Helper()
	for i, r := range c.Replicas {
		if c.Down[i] {
			continue
		}
		if ph, v, a := r.ViewState(); ph != vr.Normal || v != view || a != alive {
			t.Fatalf("r%d: %v view %d alive %#x, want NORMAL view %d alive %#x", i, ph, v, a, view, alive)
		}
	}
}

// requireConverged: every live replica executed everything and holds the same values.
func requireConverged(t *testing.T, c *Cluster, keys []string) {
	t.Helper()
	for i, r := range c.Replicas {
		if c.Down[i] {
			continue
		}
		if n := r.Orderer().SpecLogSize(); n != 0 {
			t.Fatalf("r%d: %d entries left in specLog", i, n)
		}
	}
	var ref = -1
	for i := range c.Replicas {
		if !c.Down[i] {
			ref = i
			break
		}
	}
	for _, k := range keys {
		v0, ok0 := c.Stores[ref].Read(k)
		if !ok0 {
			t.Fatalf("r%d lacks %s", ref, k)
		}
		for i := range c.Replicas {
			if c.Down[i] {
				continue
			}
			if v, ok := c.Stores[i].Read(k); !ok || v != v0 {
				t.Fatalf("%s: r%d=%+v r%d=%+v", k, ref, v0, i, v)
			}
		}
	}
}

// A replica crashes under load: survivors detect it, change view, and every write —
// committed before, in flight during, or issued after — completes and executes.
func killAndContinue(t *testing.T, dead int) {
	c := New(1, 3, start, 0)
	defer c.Stop()
	before, kb := workload(c, 30, 200, "before")
	c.Run(5000, 500)
	if allDone(before) != 30 {
		t.Fatal("pre-crash writes")
	}
	inflight, ki := workload(c, 10, 50, "inflight")
	c.Kill(dead)
	c.Run(3_000_000, 2000) // > fail timeout + recovery + leader sync
	alive := uint64(0b111) &^ (1 << dead)
	requireView(t, c, 1, alive)
	after, ka := workload(c, 30, 200, "after")
	c.Run(3_000_000, 2000)
	if got := allDone(inflight); got != 10 {
		t.Fatalf("in-flight writes completed %d/10", got)
	}
	if got := allDone(after); got != 30 {
		t.Fatalf("post-recovery writes completed %d/30", got)
	}
	requireConverged(t, c, append(append(kb, ki...), ka...))
}

func TestE2ERecovery_FollowerDies(t *testing.T) { killAndContinue(t, 2) }
func TestE2ERecovery_VrLeaderDies(t *testing.T) { killAndContinue(t, 0) }

// A fast-committed write whose only leader dies before any server learned the commit:
// recovery keeps it at its t and the new PL's leader sync finishes it.
func TestE2ERecovery_FastCommittedWriteSurvivesLeaderDeath(t *testing.T) {
	c := New(1, 1, start, 0)
	defer c.Stop()
	key := topology.KeyForPartition(1, 3)
	var o outcome
	c.Clients[0].Write([]types.KV{{Key: key, Value: "keep"}}, o.cb())
	c.Run(600, 100) // released at t: the client fast-commits
	if !o.done {
		t.Fatal("not committed")
	}
	c.Kill(1) // the leader, before the FastCommit batch was flushed
	c.Run(3_000_000, 2000)
	requireView(t, c, 1, 0b101)
	for _, i := range []int{0, 2} {
		if v, ok := c.Stores[i].Read(key); !ok || v.Value != "keep" || v.T != o.res.T {
			t.Fatalf("r%d: %+v want t=%d", i, v, o.res.T)
		}
	}
}

// Kill + restart: the restarted replica rejoins (view 2), catches up its store, and
// takes its home range back.
func TestE2ERecovery_KillAndRestartRejoins(t *testing.T) {
	for _, dead := range []int{2, 0} {
		c := New(1, 3, start, 0)
		before, kb := workload(c, 30, 200, "before")
		c.Kill(dead)
		c.Run(3_000_000, 2000)
		requireView(t, c, 1, 0b111&^(1<<dead))
		mid, km := workload(c, 20, 200, "mid")
		c.Run(500_000, 2000)
		c.Restart(dead)
		c.Run(3_000_000, 2000)
		requireView(t, c, 2, 0b111)
		after, ka := workload(c, 20, 200, "after")
		c.Run(2_000_000, 2000)
		for name, outs := range map[string][]*outcome{"before": before, "mid": mid, "after": after} {
			if allDone(outs) != len(outs) {
				t.Fatalf("dead=%d: %s completed %d/%d", dead, name, allDone(outs), len(outs))
			}
		}
		requireConverged(t, c, append(append(kb, km...), ka...))
		// Its home range is back: it leads partition `dead` again.
		key := topology.KeyForPartition(uint32(dead), 3)
		var o outcome
		c.Clients[0].Write([]types.KV{{Key: key, Value: "home"}}, o.cb())
		c.Run(5000, 100)
		if !o.done || o.res.Slow {
			t.Fatalf("dead=%d: write to its home range: %+v", dead, o)
		}
		c.Stop()
	}
}

// Recovery messages forced through the chunked bulk path, with 30% chunk loss.
func TestE2ERecovery_BulkTransferUnderChunkLoss(t *testing.T) {
	t.Setenv("DYNA_BULK_MIN_BYTES", "64")
	t.Setenv("DYNA_BULK_CHUNK_BYTES", "40")
	t.Setenv("DYNA_BULK_TEST_DROP_PCT", "30")
	killAndContinue(t, 1)
}

// Whole-cluster restart: every replica restarts (> f failures); after the grace period
// they start fresh in view 0 and serve again.
func TestE2ERecovery_WholeClusterRestart(t *testing.T) {
	t.Setenv("DYNA_FAIL_GRACE_MS", "500")
	c := New(1, 1, start, 0)
	defer c.Stop()
	workload(c, 5, 200, "x")
	for i := 0; i < 3; i++ {
		c.Kill(i)
	}
	for i := 0; i < 3; i++ {
		c.Restart(i)
	}
	c.Run(2_000_000, 2000)
	requireView(t, c, 0, 0b111)
	outs, keys := workload(c, 10, 200, "fresh")
	c.Run(500_000, 2000)
	if allDone(outs) != 10 {
		t.Fatal(allDone(outs))
	}
	requireConverged(t, c, keys)
}
