package simcluster

import (
	"testing"

	"dynago/internal/client"
	"dynago/internal/topology"
	"dynago/internal/transport"
	"dynago/internal/types"
	"dynago/internal/wire"
)

const start = 1_000_000

type outcome struct {
	done bool
	res  client.Result
}

func (o *outcome) cb() client.Callback { return func(r client.Result) { o.done, o.res = true, r } }

// executedEverywhere: the txn ran on every replica and each store holds key at t.
func executedEverywhere(t *testing.T, c *Cluster, id types.TxnId, key string, ts uint64) {
	t.Helper()
	for r := range c.Replicas {
		if !c.Replicas[r].Orderer().Executed(id) {
			st, tt, _ := c.Replicas[r].Orderer().SpecLogEntry(id)
			t.Fatalf("r%d: %v not executed (%v t=%d)", r, id, st, tt)
		}
		if v, ok := c.Stores[r].Read(key); !ok || v.T != ts {
			t.Fatalf("r%d: store %s = %+v, want t=%d", r, key, v, ts)
		}
	}
}

// sameStores: every replica's store holds the same versioned values for keys.
func sameStores(t *testing.T, c *Cluster, keys []string) {
	t.Helper()
	for _, k := range keys {
		v0, ok0 := c.Stores[0].Read(k)
		for r := 1; r < c.N; r++ {
			if v, ok := c.Stores[r].Read(k); ok != ok0 || v != v0 {
				t.Fatalf("stores diverge on %s: r0=%+v r%d=%+v", k, v0, r, v)
			}
		}
	}
}

// Fast path, single partition: the leader + one follower agree → committed; the batched
// FastCommit then moves every replica to COMMITTING at the agreed t.
func TestE2E_FastPathWrite(t *testing.T) {
	c := New(1, 1, start, 0)
	defer c.Stop()
	key := topology.KeyForPartition(0, 3)
	var o outcome
	c.Clients[0].Write([]types.KV{{Key: key, Value: "v1"}}, o.cb())
	c.Step()
	if o.done {
		t.Fatal("committed before the deadline")
	}
	c.Run(600, 100) // past t = start + 500; also past the FastCommit flush delay
	if !o.done || o.res.Slow || o.res.T != start+500 {
		t.Fatalf("%+v", o)
	}
	c.Run(400, 100)
	executedEverywhere(t, c, types.ID(1, 1), key, start+500)
	if s := c.Clients[0].Stats(); s.Fast != 1 {
		t.Fatalf("%+v", s)
	}
}

// Followers never got the request: the leader alone can't form f+1 → after the slow
// timeout the client sends SlowCommit; followers fetch the body, adopt, CommitReply.
func TestE2E_SlowPathWithFetch(t *testing.T) {
	c := New(1, 1, start, 0)
	defer c.Stop()
	key := topology.KeyForPartition(0, 3)
	dropped := 0
	c.Net.Drop = func(from, to transport.Addr, m wire.Message) bool {
		if _, ok := m.(*wire.ClientRequest); ok && to != transport.ReplicaSimAddr(0) {
			dropped++
			return true
		}
		return false
	}
	var o outcome
	c.Clients[0].Write([]types.KV{{Key: key, Value: "v"}}, o.cb())
	c.Run(3000, 100)
	if !o.done || !o.res.Slow || o.res.T != start+500 {
		t.Fatalf("%+v dropped=%d", o, dropped)
	}
	if s := c.Clients[0].Stats(); s.SlowTimeout != 1 || s.ViaCommitReply != 1 {
		t.Fatalf("%+v", s)
	}
	c.Run(1000, 100)
	executedEverywhere(t, c, types.ID(1, 1), key, start+500)
}

// Two partitions; leader 1 first misses the request, gets the same-t resend late and
// re-timestamps → the leaders disagree → SlowCommit(max) → all adopt the max.
func TestE2E_MultiPartitionLeaderDisagreement(t *testing.T) {
	c := New(1, 1, start, 0)
	defer c.Stop()
	k0, k1 := topology.KeyForPartition(0, 3), topology.KeyForPartition(1, 3)
	first := true
	c.Net.Drop = func(from, to transport.Addr, m wire.Message) bool {
		if _, ok := m.(*wire.ClientRequest); ok && to == transport.ReplicaSimAddr(1) && first {
			first = false
			return true
		}
		return false
	}
	var o outcome
	c.Clients[0].Write([]types.KV{{Key: k0, Value: "a"}, {Key: k1, Value: "b"}}, o.cb())
	c.Run(40_000, 500) // the resend fires after 30 ms
	if !o.done || !o.res.Slow || o.res.T <= start+500 {
		t.Fatalf("%+v", o)
	}
	if s := c.Clients[0].Stats(); s.SlowDisagree != 1 || s.ResendWrite != 1 {
		t.Fatalf("%+v", s)
	}
	c.Run(1000, 100)
	executedEverywhere(t, c, types.ID(1, 1), k0, o.res.T)
	executedEverywhere(t, c, types.ID(1, 1), k1, o.res.T)
}

// The leader's SpecReply is lost: the same-t resend is answered from the reply cache
// (live state) and the txn fast-commits at its original t.
func TestE2E_LostLeaderReplyResendAnsweredFromCache(t *testing.T) {
	c := New(1, 1, start, 0)
	defer c.Stop()
	key := topology.KeyForPartition(2, 3)
	lost := false
	c.Net.Drop = func(from, to transport.Addr, m wire.Message) bool {
		if _, ok := m.(*wire.SpecReply); ok && from == transport.ReplicaSimAddr(2) && !lost {
			lost = true
			return true
		}
		return false
	}
	var o outcome
	c.Clients[0].Write([]types.KV{{Key: key, Value: "v"}}, o.cb())
	c.Run(40_000, 500)
	if !o.done || o.res.Slow || o.res.T != start+500 {
		t.Fatalf("%+v", o)
	}
	c.Run(1000, 100)
	executedEverywhere(t, c, types.ID(1, 1), key, start+500)
	if c.Stores[2].ApplyCount() != 1 {
		t.Fatal("the resend must not create a second entry")
	}
}

// Many concurrent writes from several clients all commit (fast) and every replica
// holds each at the same t.
func TestE2E_ManyWritesConverge(t *testing.T) {
	c := New(1, 3, start, 0)
	defer c.Stop()
	var outs []*outcome
	for i := 0; i < 60; i++ {
		o := &outcome{}
		outs = append(outs, o)
		key := topology.KeyForPartition(uint32(i%3), 3) + "x"
		c.Clients[i%3].Write([]types.KV{{Key: key, Value: "v"}}, o.cb())
		c.Run(37, 37)
	}
	c.Run(5000, 100)
	for i, o := range outs {
		if !o.done {
			t.Fatal("not committed", i)
		}
	}
	var keys []string
	for p := uint32(0); p < 3; p++ {
		keys = append(keys, topology.KeyForPartition(p, 3)+"x")
	}
	sameStores(t, c, keys)
	for r := range c.Replicas {
		if n := c.Replicas[r].Orderer().ExecutedCount(); n != 60 {
			t.Fatalf("r%d executed %d/60", r, n)
		}
		if c.Replicas[r].Orderer().SpecLogSize() != 0 {
			t.Fatal("specLog not drained")
		}
	}
}

// No client commit message at all (every FastCommit / SlowCommit lost): leaders sync by
// TsAgree after the timeout, replicate, and every replica executes; the waiting client
// is answered by the leader's CommitReply (leader sync).
func TestE2E_LostCommitMessagesResolvedByLeaderSync(t *testing.T) {
	c := New(1, 1, start, 0)
	defer c.Stop()
	k0, k1 := topology.KeyForPartition(0, 3), topology.KeyForPartition(1, 3)
	c.Net.Drop = func(from, to transport.Addr, m wire.Message) bool {
		switch m.(type) {
		case *wire.FastCommit, *wire.FastCommitBatch, *wire.SlowCommit:
			return true
		}
		return false
	}
	var o outcome
	c.Clients[0].Write([]types.KV{{Key: k0, Value: "a"}, {Key: k1, Value: "b"}}, o.cb())
	c.Run(300_000, 1000)
	if !o.done {
		t.Fatal("client never committed")
	}
	executedEverywhere(t, c, types.ID(1, 1), k0, o.res.T)
	executedEverywhere(t, c, types.ID(1, 1), k1, o.res.T)
}

// Replication messages are lossy (30% of background batches dropped): prepare
// re-broadcast and stale re-acks still drive everything to execution, stores agree.
func TestE2E_LossyReplicationConverges(t *testing.T) {
	c := New(1, 2, start, 0)
	defer c.Stop()
	n := 0
	c.Net.Drop = func(from, to transport.Addr, m wire.Message) bool {
		switch m.(type) {
		case *wire.BgBatch, *wire.ReplicatePrepare, *wire.ReplicatePrepareReply, *wire.ReplicateCommit:
			n++
			return n%3 == 0
		}
		return false
	}
	var keys []string
	for i := 0; i < 40; i++ {
		key := topology.KeyForPartition(uint32(i%3), 3) + "y"
		if i < 3 {
			keys = append(keys, key)
		}
		c.Clients[i%2].Write([]types.KV{{Key: key, Value: "v"}}, nil)
		c.Run(50, 50)
	}
	c.Run(2_000_000, 5000)
	for r := range c.Replicas {
		if got := c.Replicas[r].Orderer().ExecutedCount(); got != 40 {
			t.Fatalf("r%d executed %d/40", r, got)
		}
	}
	sameStores(t, c, keys)
}

// Read after write: the leader serves the executed value; a two-partition read merges
// both leaders' results.
func TestE2E_ReadAfterWriteAndMultiPartitionRead(t *testing.T) {
	c := New(1, 1, start, 0)
	defer c.Stop()
	k0, k1 := topology.KeyForPartition(0, 3), topology.KeyForPartition(1, 3)
	var w outcome
	c.Clients[0].Write([]types.KV{{Key: k0, Value: "a"}, {Key: k1, Value: "b"}}, w.cb())
	c.Run(2000, 100)
	if !w.done {
		t.Fatal("write")
	}
	var r outcome
	c.Clients[0].Read([]string{k0, k1}, r.cb())
	c.Run(2000, 100)
	if !r.done || r.res.Values[k0] != "a" || r.res.Values[k1] != "b" {
		t.Fatalf("%+v", r)
	}
	var miss outcome
	c.Clients[0].Read([]string{topology.KeyForPartition(2, 3) + "never"}, miss.cb())
	c.Run(2000, 100)
	if !miss.done || len(miss.res.Values) != 1 {
		t.Fatalf("%+v", miss)
	}
}

// A read issued while a conflicting write is still un-executed returns the write's value
// (it waits for it), never the older one.
func TestE2E_ReadWaitsForConflictingWrite(t *testing.T) {
	c := New(1, 2, start, 0)
	defer c.Stop()
	k := topology.KeyForPartition(1, 3)
	var w1, w2, r outcome
	c.Clients[0].Write([]types.KV{{Key: k, Value: "old"}}, w1.cb())
	c.Run(2000, 100)
	// Hold back FastCommits so the second write stays un-executed until leader sync.
	c.Net.Drop = func(from, to transport.Addr, m wire.Message) bool {
		switch m.(type) {
		case *wire.FastCommit, *wire.FastCommitBatch:
			return true
		}
		return false
	}
	c.Clients[0].Write([]types.KV{{Key: k, Value: "new"}}, w2.cb())
	c.Run(600, 100) // w2 committed at the client, not yet at the servers
	if !w2.done {
		t.Fatal("w2")
	}
	c.Clients[1].Read([]string{k}, r.cb())
	c.Run(1000, 100)
	if r.done {
		t.Fatal("read answered before the conflicting write executed")
	}
	c.Run(300_000, 1000) // leader sync resolves w2, it executes, the read is served
	if !r.done || r.res.Values[k] != "new" {
		t.Fatalf("%+v", r)
	}
}

// The read's request to its leader is lost: the client retries as a fresh attempt
// (new request id) at a larger t and succeeds.
func TestE2E_ReadRetriesAsFreshAttempt(t *testing.T) {
	c := New(1, 1, start, 0)
	defer c.Stop()
	k := topology.KeyForPartition(2, 3)
	lost := false
	c.Net.Drop = func(from, to transport.Addr, m wire.Message) bool {
		if cr, ok := m.(*wire.ClientRequest); ok && cr.Txn.Type == types.Read &&
			to == transport.ReplicaSimAddr(2) && !lost {
			lost = true
			return true
		}
		return false
	}
	var r outcome
	id := c.Clients[0].Read([]string{k}, r.cb())
	c.Run(100_000, 500)
	if !r.done {
		t.Fatal("read never completed")
	}
	if s := c.Clients[0].Stats(); s.RetryRead != 1 {
		t.Fatalf("%+v", s)
	}
	_ = id
}
