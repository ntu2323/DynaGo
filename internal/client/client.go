// Package client is the DYNA client (C++ dyna/client.{h,cc}). A txn moves
// COLLECTING → (SLOW_WAIT) → done:
//   - SpecReplies feed a multi-leader TxnQuorum; a fast quorum commits a WRITE (the
//     client then sends FastCommit, batched, off its latency path) or returns a READ's
//     merged leader results.
//   - A WRITE takes the slow path — SlowCommit(max leader t), then wait for a
//     CommitReply quorum at that t — when the leaders disagree, or when every leader
//     replied but the quorum stayed short for the slow timeout.
//   - A WRITE missing a leader reply is resent with the SAME t; a READ that doesn't
//     reach a quorum retries as a fresh attempt (new request id) at a larger t.
//
// Resends are rate-limited (token bucket, oldest first) and tracked in a deadline heap.
// Safe for concurrent use; callbacks run outside the client's lock.
package client

import (
	"container/heap"
	"log"
	"os"
	"sync"
	"time"

	"dynago/internal/clock"
	"dynago/internal/deadline"
	"dynago/internal/quorum"
	"dynago/internal/topology"
	"dynago/internal/transport"
	"dynago/internal/types"
	"dynago/internal/wire"
)

// maxResendShift caps the per-request backoff at resend × 2^5 (30 ms → ~1 s).
const maxResendShift = 5

// Op is one transaction: a pure write (Writes) or a pure read (Reads). RMW is out of
// scope.
type Op struct {
	Reads  []string
	Writes []types.KV
}

// Result of a committed txn.
type Result struct {
	Values map[string]string // READ: merged from the involved leaders
	T      uint64            // commit timestamp
	Slow   bool              // went through the slow path
}

type Callback func(Result)

type phase uint8

const (
	collecting phase = iota
	slowWait
)

type pending struct {
	reqID     uint64
	op        Op
	isRead    bool
	t         uint64 // fixed for a write across resends
	owd       uint64 // READ: grows each retry
	sendUS    uint64
	invokeUS  uint64
	slowCause uint8 // 0 fast, 1 leaders disagreed, 2 timeout
	resends   uint32
	dueGen    uint64
	restart   bool // view change: resend (same t) on its turn
	phase     phase
	q         quorum.TxnQuorum
	results   map[string]string
	cb        Callback
}

type due struct{ at, id, gen uint64 }
type dueHeap []due

func (h dueHeap) Len() int           { return len(h) }
func (h dueHeap) Less(i, j int) bool { return h[i].at < h[j].at }
func (h dueHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *dueHeap) Push(x any)        { *h = append(*h, x.(due)) }
func (h *dueHeap) Pop() any          { o := *h; x := o[len(o)-1]; *h = o[:len(o)-1]; return x }
func (h dueHeap) top() due           { return h[0] }

type Options struct {
	ID        uint64
	F         int
	Transport transport.Transport
	Clock     clock.Clock // nil: real
}

type Stats struct {
	Fast, SlowDisagree, SlowTimeout, ViaCommitReply, ResendWrite, RetryRead uint64
	ResendSlowCommit                                                        uint64 // SlowCommit re-sent: no CommitReply quorum within the resend delay
}

type Client struct {
	mu    sync.Mutex
	id    uint64
	tr    transport.Transport
	clk   clock.Clock
	debug bool

	view   uint64
	topo   topology.Topology
	quorum int

	lastReq uint64
	pend    map[uint64]*pending
	due     dueHeap // sweep deadlines (stale entries skipped by gen)
	ready   dueHeap // resends waiting for budget, oldest send first

	bound       *deadline.ClientBoundTracker
	resendUS    uint64
	resendRate  uint64
	tokens      float64
	lastSweep   uint64
	slowTimeout uint64
	fastMax     int
	fastFlushUS uint64
	fastBatch   []wire.FastCommit
	fastFirstUS uint64
	stats       Stats
	runOut      []func() // callbacks to run after unlocking
	stop        chan struct{}
	wg          sync.WaitGroup
}

func New(o Options) *Client {
	n := o.Transport.NumReplicas()
	c := &Client{
		id: o.ID, tr: o.Transport, clk: o.Clock,
		debug:       os.Getenv("DYNA_DEBUG") != "" && os.Getenv("DYNA_DEBUG") != "0",
		topo:        topology.New(uint32(n)),
		quorum:      o.F + 1,
		pend:        make(map[uint64]*pending),
		bound:       deadline.NewClientBoundTracker(deadline.FromEnv(), n),
		resendUS:    max(1, deadline.EnvU64("DYNA_RESEND_TIMEOUT_MS", 30)) * 1000,
		resendRate:  deadline.EnvU64("DYNA_RESEND_RATE", 1000),
		slowTimeout: deadline.EnvU64("DYNA_SLOW_TIMEOUT_US", 2000),
		fastMax:     int(max(1, deadline.EnvU64("DYNA_FAST_BATCH", 32))),
		fastFlushUS: max(1, deadline.EnvU64("DYNA_FAST_FLUSH_US", 200)),
	}
	if c.clk == nil {
		c.clk = clock.NewReal(false)
	}
	c.lastSweep = c.clk.Now()
	return c
}

func (c *Client) ID() uint64 { return c.id }

// Start runs the periodic sweep (resends, slow-path checks, FastCommit flush) on a
// goroutine; tests with a mock clock call Tick instead.
func (c *Client) Start() {
	c.stop = make(chan struct{})
	period := time.Duration(min(c.fastFlushUS, c.resendUS/2, max(c.slowTimeout/2, 100))) * time.Microsecond
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		tk := time.NewTicker(period)
		defer tk.Stop()
		for {
			select {
			case <-c.stop:
				return
			case <-tk.C:
				c.Tick()
			}
		}
	}()
}

func (c *Client) Stop() {
	if c.stop != nil {
		close(c.stop)
		c.wg.Wait()
		c.stop = nil
	}
}

func (c *Client) unlockAndRun() {
	out := c.runOut
	c.runOut = nil
	c.mu.Unlock()
	for _, f := range out {
		f()
	}
}

func (c *Client) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *Client) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pend)
}

// BoundUS is the current deadline bound.
func (c *Client) BoundUS() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bound.BoundUS()
}

// Write commits a blind write of every kv (one txn).
func (c *Client) Write(kvs []types.KV, cb Callback) uint64 { return c.Invoke(Op{Writes: kvs}, cb) }

// Read reads keys (one txn).
func (c *Client) Read(keys []string, cb Callback) uint64 { return c.Invoke(Op{Reads: keys}, cb) }

// Invoke starts a txn and returns its (first) request id.
func (c *Client) Invoke(op Op, cb Callback) uint64 {
	c.mu.Lock()
	c.lastReq++
	p := &pending{reqID: c.lastReq, op: op, isRead: len(op.Writes) == 0, cb: cb,
		invokeUS: c.clk.Now(), phase: collecting}
	p.owd = c.bound.BoundUS()
	p.q = quorum.New(c.leadersFor(p), c.quorum, c.view)
	c.pend[p.reqID] = p
	c.sendRequest(p)
	id := p.reqID
	c.unlockAndRun()
	return id
}

func (c *Client) txnOf(p *pending) types.Txn {
	t := types.Txn{ClientID: c.id, RequestID: p.reqID, Type: types.Write, WSet: p.op.Writes,
		T: p.t, ViewID: c.view}
	if p.isRead {
		t.Type = types.Read
		t.RSet = p.op.Reads
		t.WSet = nil
	}
	return t
}

func (c *Client) leadersFor(p *pending) types.ServerSet {
	t := c.txnOf(p)
	return c.topo.InvolvedLeaders(&t)
}

func (c *Client) slowTimeoutUS() uint64 { return max(c.slowTimeout, 3*c.bound.BoundUS()) }

func (c *Client) resendDelayUS(p *pending) uint64 {
	return c.resendUS << min(p.resends, maxResendShift)
}

func (c *Client) schedule(p *pending, at uint64) {
	p.dueGen++
	heap.Push(&c.due, due{at, p.reqID, p.dueGen})
}

// sendRequest (re)multicasts with a fresh send stamp. Writes keep their t; reads take a
// fresh, growing deadline each attempt.
func (c *Client) sendRequest(p *pending) {
	now := c.clk.Now()
	if p.isRead {
		if p.owd == 0 {
			p.owd = c.bound.BoundUS()
		}
		p.t = now + p.owd
	} else if p.t == 0 {
		p.t = now + c.bound.BoundUS()
	}
	p.sendUS = now
	c.schedule(p, now+min(c.slowTimeoutUS(), c.resendDelayUS(p)))
	txn := c.txnOf(p)
	txn.SendTime = now
	c.tr.Broadcast(&wire.ClientRequest{Txn: txn})
}

func (c *Client) sendSlowCommit(p *pending) {
	c.tr.Broadcast(&wire.SlowCommit{ClientID: c.id, RequestID: p.reqID, ViewID: c.view,
		AgreedT: p.q.SlowAgreedT()})
	if p.slowCause == 0 {
		if p.q.LeadersDisagree() {
			p.slowCause = 1
			c.stats.SlowDisagree++
		} else {
			p.slowCause = 2
			c.stats.SlowTimeout++
		}
	}
	p.phase = slowWait
	p.sendUS = c.clk.Now()
	c.schedule(p, p.sendUS+c.resendDelayUS(p))
}

// retryRead re-issues a READ as a new attempt: fresh request id (replies to the old
// attempt are ignored), reset quorum/results, larger t.
func (c *Client) retryRead(p *pending) {
	delete(c.pend, p.reqID)
	p.owd = min(max(p.owd, c.bound.BoundUS())*2, 10_000_000)
	c.lastReq++
	p.reqID = c.lastReq
	p.q = quorum.New(c.leadersFor(p), c.quorum, c.view)
	p.results = nil
	c.pend[p.reqID] = p
	c.sendRequest(p)
}

func (c *Client) commit(p *pending, t uint64) {
	delete(c.pend, p.reqID)
	if p.slowCause == 0 {
		c.stats.Fast++
	}
	res := Result{Values: p.results, T: t, Slow: p.slowCause != 0}
	if cb := p.cb; cb != nil {
		c.runOut = append(c.runOut, func() { cb(res) })
	}
}

// Tick runs the resend / slow-path sweep and flushes due FastCommits.
func (c *Client) Tick() {
	c.mu.Lock()
	c.sweep()
	if len(c.fastBatch) > 0 && c.clk.Now() >= c.fastFirstUS+c.fastFlushUS {
		c.flushFast()
	}
	c.unlockAndRun()
}

func (c *Client) sweep() {
	now := c.clk.Now()
	slowUS := c.slowTimeoutUS()
	if c.resendRate > 0 && now > c.lastSweep {
		capTokens := max(1.0, float64(c.resendRate)/10)
		c.tokens = min(capTokens, c.tokens+float64(now-c.lastSweep)*1e-6*float64(c.resendRate))
	}
	c.lastSweep = now
	for c.due.Len() > 0 && c.due.top().at <= now {
		d := heap.Pop(&c.due).(due)
		p := c.pend[d.id]
		if p == nil || p.dueGen != d.gen {
			continue // done / superseded
		}
		age := uint64(0)
		if now > p.sendUS {
			age = now - p.sendUS
		}
		switch {
		case !p.restart && !p.isRead && p.phase == collecting && p.q.AllLeadersPresent() &&
			!p.q.FastReady():
			if age >= slowUS {
				// Every leader replied but not enough matching replies → slow path.
				c.sendSlowCommit(p)
			} else {
				// The slow timeout grew with the bound since this was armed: wait for the
				// new one, not for the (much later) resend deadline.
				c.schedule(p, p.sendUS+slowUS)
			}
		case !p.restart && age < c.resendDelayUS(p):
			c.schedule(p, p.sendUS+c.resendDelayUS(p))
		default:
			heap.Push(&c.ready, due{p.sendUS, d.id, d.gen})
		}
	}
	for c.ready.Len() > 0 {
		d := c.ready.top()
		p := c.pend[d.id]
		if p == nil || p.dueGen != d.gen {
			heap.Pop(&c.ready)
			continue
		}
		if c.resendRate > 0 {
			if c.tokens < 1 {
				break // out of budget: the rest wait their turn
			}
			c.tokens--
		}
		heap.Pop(&c.ready)
		if !p.restart {
			p.resends++
		}
		p.restart = false
		switch {
		case p.phase == slowWait:
			c.stats.ResendSlowCommit++
			c.sendSlowCommit(p)
		case p.isRead:
			c.stats.RetryRead++
			c.retryRead(p)
		default:
			c.stats.ResendWrite++
			c.sendRequest(p) // same t: a leader reply is missing
		}
	}
}

// Receive handles a reply.
func (c *Client) Receive(from transport.Addr, m wire.Message) {
	c.mu.Lock()
	switch m := m.(type) {
	case *wire.SpecReply:
		c.handleSpecReply(m)
	case *wire.SpecReplyBatch:
		for i := range m.Replies {
			c.handleSpecReply(&m.Replies[i])
		}
	case *wire.CommitReply:
		if p := c.pend[m.RequestID]; p != nil && p.q.AddReply(m.ServerID, m.ViewID, m.T, m.IsLeader) &&
			p.q.FastReady() {
			c.stats.ViaCommitReply++
			c.commit(p, p.q.AgreedT())
		}
	case *wire.ViewInfo:
		c.handleViewInfo(m)
	}
	c.unlockAndRun()
}

func (c *Client) handleSpecReply(r *wire.SpecReply) {
	p := c.pend[r.RequestID]
	if p == nil {
		return
	}
	c.bound.OnOwdSample(r.ServerID, r.OwdEstimate)
	// Another view's reply doesn't count; a newer-view server answers our next resend
	// with ViewInfo and we restart there.
	if !p.q.AddReply(r.ServerID, r.ViewID, r.T, r.IsLeader) {
		return
	}
	if p.isRead && r.IsLeader {
		if p.results == nil {
			p.results = make(map[string]string, len(r.Results))
		}
		for _, kv := range r.Results {
			p.results[kv.Key] = kv.Value
		}
	}
	if c.debug {
		log.Printf("client %d RECV SpecReply s=%d r=%d t=%d leader=%v replies=%d fast=%v disagree=%v",
			c.id, r.ServerID, r.RequestID, r.T, r.IsLeader, p.q.ReplyCount(), p.q.FastReady(), p.q.LeadersDisagree())
	}
	if p.phase == collecting {
		switch {
		case p.q.FastReady():
			t := p.q.AgreedT()
			if !p.isRead {
				c.queueFast(wire.FastCommit{ClientID: c.id, RequestID: p.reqID, ViewID: c.view, AgreedT: t})
			}
			c.commit(p, t)
		case !p.isRead && p.q.LeadersDisagree():
			c.sendSlowCommit(p)
		case !p.isRead && p.q.AllLeadersPresent():
			// The slow-path timer starts once every leader replied.
			c.schedule(p, max(c.clk.Now(), p.sendUS+c.slowTimeoutUS()))
		}
	} else if p.q.FastReady() {
		// SLOW_WAIT: a straggling SpecReply at agreedT completes it like a CommitReply.
		c.commit(p, p.q.AgreedT())
	}
}

// queueFast batches FastCommits per client (off the latency path).
func (c *Client) queueFast(fc wire.FastCommit) {
	if c.fastMax <= 1 {
		c.tr.Broadcast(&fc)
		return
	}
	if len(c.fastBatch) == 0 {
		c.fastFirstUS = c.clk.Now()
	}
	c.fastBatch = append(c.fastBatch, fc)
	if len(c.fastBatch) >= c.fastMax {
		c.flushFast()
	}
}

func (c *Client) flushFast() {
	switch len(c.fastBatch) {
	case 0:
		return
	case 1:
		c.tr.Broadcast(&c.fastBatch[0])
	default:
		c.tr.Broadcast(&wire.FastCommitBatch{Commits: c.fastBatch})
	}
	c.fastBatch = nil
}

// FlushFastCommits sends any batched FastCommits now.
func (c *Client) FlushFastCommits() {
	c.mu.Lock()
	c.flushFast()
	c.mu.Unlock()
}

// handleViewInfo adopts a newer view and restarts every pending request there (writes
// at the same t via the rate-capped sweep; reads as fresh attempts).
func (c *Client) handleViewInfo(m *wire.ViewInfo) {
	if m.ViewID <= c.view {
		return
	}
	c.view = m.ViewID
	c.topo = c.topo.WithAlive(types.ServerSet(m.Alive))
	if c.debug {
		log.Printf("client %d view %d alive=%#x: restarting %d pending", c.id, c.view, m.Alive, len(c.pend))
	}
	ps := make([]*pending, 0, len(c.pend))
	for _, p := range c.pend {
		ps = append(ps, p)
	}
	for _, p := range ps {
		p.phase = collecting
		p.resends = 0
		p.q = quorum.New(c.leadersFor(p), c.quorum, c.view)
		if p.isRead {
			c.retryRead(p)
		} else {
			p.results = nil
			p.restart = true
			c.schedule(p, 0)
		}
	}
}
