package client

import (
	"sync"
	"testing"

	"dynago/internal/clock"
	"dynago/internal/topology"
	"dynago/internal/transport"
	"dynago/internal/types"
	"dynago/internal/wire"
)

// recorder is a Transport that keeps every message sent.
type recorder struct {
	mu   sync.Mutex
	sent []wire.Message
}

func (r *recorder) Self() transport.Addr                  { return transport.Addr{} }
func (r *recorder) Send(_ transport.Addr, m wire.Message) { r.add(m) }
func (r *recorder) SendReplica(_ int, m wire.Message)     { r.add(m) }
func (r *recorder) Broadcast(m wire.Message)              { r.add(m) }
func (r *recorder) ReplicaAddr(int) transport.Addr        { return transport.Addr{} }
func (r *recorder) NumReplicas() int                      { return 3 }
func (r *recorder) add(m wire.Message)                    { r.mu.Lock(); r.sent = append(r.sent, m); r.mu.Unlock() }
func (r *recorder) slowCommits(req uint64) (n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.sent {
		if sc, ok := m.(*wire.SlowCommit); ok && sc.RequestID == req {
			n++
		}
	}
	return n
}

// Every leader replied but the quorum is short: the slow path starts after the slow
// timeout. If the bound (hence the slow timeout) grows after that timer was armed, the
// check must re-arm at the new slow deadline — not fall through to the 30 ms resend
// deadline (the source of a ~30 ms latency tail).
func TestClient_SlowCheckRearmsWhenBoundGrows(t *testing.T) {
	t.Setenv("DYNA_INITIAL_BOUND_US", "500")
	t.Setenv("DYNA_SLOW_TIMEOUT_US", "2000")
	t.Setenv("DYNA_RESEND_TIMEOUT_MS", "30")
	t.Setenv("DYNA_EARLY_SLOW", "0") // the leader's t below differs from the request's
	clk := clock.NewMock(1000)
	tr := &recorder{}
	c := New(Options{ID: 9, F: 1, Transport: tr, Clock: clk})
	key := topology.KeyForPartition(0, 3) // led by server 0
	reqA := c.Write([]types.KV{{Key: key, Value: "a"}}, func(Result) {})
	reqB := c.Write([]types.KV{{Key: key, Value: "b"}}, func(Result) {})
	sendA := clk.Now()
	// A: the leader and one follower reply with different t → all leaders present, not
	// fast; the slow timer is armed at sendA + 2 ms.
	c.Receive(transport.Addr{}, &wire.SpecReply{ClientID: 9, RequestID: reqA, ServerID: 0, T: 5000, IsLeader: true})
	c.Receive(transport.Addr{}, &wire.SpecReply{ClientID: 9, RequestID: reqA, ServerID: 1, T: 5100})
	// A reply for B carries a large OWD estimate: bound 5 ms → slow timeout 15 ms.
	c.Receive(transport.Addr{}, &wire.SpecReply{ClientID: 9, RequestID: reqB, ServerID: 2, T: 7000, OwdEstimate: 5000})
	if got := c.slowTimeoutUS(); got != 15000 {
		t.Fatalf("slow timeout %d, want 15000", got)
	}
	clk.Set(sendA + 2000)
	c.Tick()
	if tr.slowCommits(reqA) != 0 {
		t.Fatal("slow path before the (grown) slow timeout")
	}
	clk.Set(sendA + 15000)
	c.Tick()
	if tr.slowCommits(reqA) != 1 {
		t.Fatal("no SlowCommit at the grown slow timeout (waited for the resend deadline)")
	}
}

// A late leader re-timestamps the write: its reply carries t' != the request's t. The
// followers can only reply with the request's t, so no quorum can form at t' — the
// client goes slow at once (SlowCommit(t')) instead of after the slow timeout. A leader
// reply at the request's own t, with a follower still missing, keeps waiting.
func TestClient_LateLeaderGoesSlowImmediately(t *testing.T) {
	t.Setenv("DYNA_INITIAL_BOUND_US", "500")
	t.Setenv("DYNA_SLOW_TIMEOUT_US", "2000")
	clk := clock.NewMock(1000)
	tr := &recorder{}
	c := New(Options{ID: 9, F: 1, Transport: tr, Clock: clk})
	key := topology.KeyForPartition(0, 3) // led by server 0
	var res []Result
	reqA := c.Write([]types.KV{{Key: key, Value: "a"}}, func(r Result) { res = append(res, r) })
	reqB := c.Write([]types.KV{{Key: key, Value: "b"}}, func(Result) {})
	reqT := uint64(1000 + 500)

	c.Receive(transport.Addr{}, &wire.SpecReply{ClientID: 9, RequestID: reqB, ServerID: 0, T: reqT, IsLeader: true})
	if tr.slowCommits(reqB) != 0 {
		t.Fatal("slow path for a leader reply at the request's t")
	}
	c.Receive(transport.Addr{}, &wire.SpecReply{ClientID: 9, RequestID: reqA, ServerID: 1, T: reqT})
	c.Receive(transport.Addr{}, &wire.SpecReply{ClientID: 9, RequestID: reqA, ServerID: 0, T: reqT + 300, IsLeader: true})
	if tr.slowCommits(reqA) != 1 {
		t.Fatal("no immediate SlowCommit after the leader re-timestamped")
	}
	tr.mu.Lock()
	var agreed uint64
	for _, m := range tr.sent {
		if sc, ok := m.(*wire.SlowCommit); ok && sc.RequestID == reqA {
			agreed = sc.AgreedT
		}
	}
	tr.mu.Unlock()
	if agreed != reqT+300 {
		t.Fatalf("SlowCommit agreedT %d, want the leader's %d", agreed, reqT+300)
	}
	// CommitReplies at agreedT complete it (slow).
	c.Receive(transport.Addr{}, &wire.CommitReply{ClientID: 9, RequestID: reqA, ServerID: 0, T: agreed, IsLeader: true})
	c.Receive(transport.Addr{}, &wire.CommitReply{ClientID: 9, RequestID: reqA, ServerID: 1, T: agreed})
	if len(res) != 1 || !res[0].Slow || res[0].T != agreed {
		t.Fatalf("result %+v", res)
	}
	if s := c.Stats(); s.SlowLateLeader != 1 || s.SlowTimeout != 0 {
		t.Fatalf("stats %+v", s)
	}
}
