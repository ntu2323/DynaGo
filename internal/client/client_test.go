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
