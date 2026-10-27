package replica

// Pipeline mode (C++ DYNA_ORDER_THREAD=1 + lanes + senders): receive goroutines only
// decode and route; every hot message goes to its txn's lane queue, and that lane's
// ordering goroutine runs the handlers and the lane's release, so lanes never contend
// on one another's locks. SpecReplies and full background batches leave through sender
// goroutines (lane i → sender i % S), which combine replies already queued for the
// same client into one SpecReplyBatch and send each round with one sendmmsg.
//
// VR / recovery messages still run on the receive goroutine under the exclusive
// stateMu; a lane holds stateMu shared for each drained batch.

import (
	"net/netip"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"dynago/internal/clock"
	"dynago/internal/deadline"
	"dynago/internal/transport"
	"dynago/internal/types"
	"dynago/internal/wire"
)

// parker blocks a consumer goroutine until a producer has work for it, spinning first
// (busy-poll) so a busy consumer never pays for a futex wake.
type parker struct {
	parked atomic.Bool
	ch     chan struct{}
	spinUS uint64 // spin this long before parking (clock.Never = never park)
}

func newParker(spinUS uint64) parker { return parker{ch: make(chan struct{}, 1), spinUS: spinUS} }

func (p *parker) wake() {
	if p.parked.Load() {
		select {
		case p.ch <- struct{}{}:
		default:
		}
	}
}

// wait returns once ready() holds, untilUS (on clk) passes, or stop closes.
func (p *parker) wait(clk clock.Clock, untilUS uint64, ready func() bool, stop <-chan struct{}, tm *time.Timer) {
	start := clk.Now()
	for spins := 0; ; spins++ {
		if ready() {
			return
		}
		now := clk.Now()
		if now >= untilUS {
			return
		}
		if p.spinUS != clock.Never && now-start >= p.spinUS {
			break
		}
		if spins&15 == 15 {
			select {
			case <-stop:
				return
			default:
			}
			runtime.Gosched()
		}
	}
	p.parked.Store(true)
	defer p.parked.Store(false)
	if ready() {
		return
	}
	// Backstop: never sleep past 50 ms (a missed wake would otherwise stall a lane).
	d := 50 * time.Millisecond
	if untilUS != clock.Never {
		if now := clk.Now(); untilUS > now {
			d = min(d, time.Duration(untilUS-now)*time.Microsecond)
		} else {
			return
		}
	}
	tm.Reset(d)
	select {
	case <-p.ch:
	case <-tm.C:
	case <-stop:
	}
	if !tm.Stop() {
		select {
		case <-tm.C:
		default:
		}
	}
}

type inItem struct {
	from Addr
	m    wire.Message
}

// laneQ is a lane's MPSC inbox: producers append under a short lock, the lane swaps the
// whole slice out.
type laneQ struct {
	mu    sync.Mutex
	items []inItem
	n     atomic.Int32
	park  parker

	// DYNA_STATS
	busyNS, msgs atomic.Uint64
	maxQ         atomic.Int32
	maxIterNS    atomic.Uint64 // longest single drain+handle+release iteration
}

func (q *laneQ) push(from Addr, m wire.Message) {
	q.mu.Lock()
	q.items = append(q.items, inItem{from, m})
	q.n.Store(int32(len(q.items)))
	q.mu.Unlock()
	q.park.wake()
}

func (q *laneQ) swap(spare []inItem) []inItem {
	q.mu.Lock()
	out := q.items
	q.items = spare[:0]
	q.n.Store(0)
	q.mu.Unlock()
	return out
}

// outItem is one queued datagram: a SpecReply by value (spec, m == nil) or any message.
type outItem struct {
	to   Addr
	spec wire.SpecReply
	m    wire.Message
}

type sender struct {
	mu    sync.Mutex
	items []outItem
	n     atomic.Int32
	park  parker
	w     transport.Writer

	// DYNA_STATS
	busyNS, dgrams, replies, combined atomic.Uint64
}

func (s *sender) push(it outItem) {
	s.mu.Lock()
	s.items = append(s.items, it)
	s.n.Store(int32(len(s.items)))
	s.mu.Unlock()
	s.park.wake()
}

type pipeline struct {
	lanes   []*laneQ
	senders []*sender
	shedQ   int32 // drop new ClientRequests at a lane queue past this (0 = off)
	combine bool  // SpecReplyBatch per client
	stop    chan struct{}
	wg      sync.WaitGroup

	shed atomic.Uint64 // ClientRequests shed (stats)
}

// maxCombine bounds one SpecReplyBatch (~50 B per reply; well under one MTU-sized batch).
const maxCombine = 64

func (r *Replica) startPipeline(sendThreads int) {
	busy := envBool("DYNA_BUSY_POLL", true)
	spin := deadline.EnvU64("DYNA_SPIN_US", 0)
	if busy {
		spin = clock.Never
	}
	p := &pipeline{
		shedQ:   int32(deadline.EnvU64("DYNA_LANE_SHED_QLEN", 8192)),
		combine: envBool("DYNA_REPLY_COMBINE", true),
		stop:    make(chan struct{}),
	}
	for range r.lanes {
		p.lanes = append(p.lanes, &laneQ{park: newParker(spin)})
	}
	for i := 0; i < sendThreads; i++ {
		// Sender i writes on socket i+1 (socket 0 also carries the inline sends).
		p.senders = append(p.senders, &sender{park: newParker(spin), w: transport.WriterFor(r.tr, i+1)})
	}
	r.pipe = p
	r.ord.SetWakeHook(r.wakeLanes)
	for i := range p.lanes {
		p.wg.Add(1)
		go r.laneLoop(i)
	}
	for _, s := range p.senders {
		p.wg.Add(1)
		go r.senderLoop(s)
	}
}

func (r *Replica) stopPipeline() {
	if r.pipe == nil {
		return
	}
	close(r.pipe.stop)
	r.wakeLanes()
	for _, s := range r.pipe.senders {
		s.park.wake()
	}
	r.pipe.wg.Wait()
}

func (r *Replica) wakeLanes() {
	for _, q := range r.pipe.lanes {
		q.park.wake()
	}
}

func (r *Replica) stopping() bool {
	select {
	case <-r.pipe.stop:
		return true
	default:
		return false
	}
}

// route queues a hot message on its lane (pipeline mode). Client traffic is dropped
// while paused, and new ClientRequests past the lane's shed threshold (the client
// resends).
func (r *Replica) route(from Addr, m wire.Message) {
	p := r.pipe
	lanes := len(p.lanes)
	laneOf := func(id types.TxnId) *laneQ { return p.lanes[r.ord.LaneOf(id)] }
	switch m := m.(type) {
	case *wire.ClientRequest:
		if r.paused.Load() {
			return
		}
		q := laneOf(m.Txn.Id())
		if p.shedQ > 0 && q.n.Load() > p.shedQ {
			p.shed.Add(1)
			return
		}
		q.push(from, m)
	case *wire.FastCommit:
		if !r.paused.Load() {
			laneOf(m.Id()).push(from, m)
		}
	case *wire.SlowCommit:
		if !r.paused.Load() {
			laneOf(m.Id()).push(from, m)
		}
	case *wire.FastCommitBatch:
		if r.paused.Load() {
			return
		}
		if lanes == 1 {
			p.lanes[0].push(from, m)
			return
		}
		var per [64]*wire.FastCommitBatch
		for i := range m.Commits {
			l := r.ord.LaneOf(m.Commits[i].Id())
			if per[l] == nil {
				per[l] = &wire.FastCommitBatch{}
			}
			per[l].Commits = append(per[l].Commits, m.Commits[i])
		}
		for l, b := range per[:lanes] {
			if b != nil {
				p.lanes[l].push(from, b)
			}
		}
	case *wire.FetchRequest:
		laneOf(types.ID(m.ClientID, m.RequestID)).push(from, m)
	case *wire.FetchReply:
		laneOf(m.Txn.Id()).push(from, m)
	case *wire.TsAgree:
		laneOf(m.Id()).push(from, m)
	case *wire.ReplicatePrepare:
		laneOf(m.Id()).push(from, m)
	case *wire.ReplicatePrepareReply:
		laneOf(m.Id()).push(from, m)
	case *wire.ReplicateCommit:
		laneOf(m.Id()).push(from, m)
	case *wire.BgBatch:
		if lanes == 1 {
			p.lanes[0].push(from, m)
			return
		}
		var per [64]*wire.BgBatch
		get := func(id types.TxnId) *wire.BgBatch {
			l := r.ord.LaneOf(id)
			if per[l] == nil {
				per[l] = &wire.BgBatch{SenderID: m.SenderID}
			}
			return per[l]
		}
		for i := range m.Prepares {
			b := get(m.Prepares[i].Id())
			b.Prepares = append(b.Prepares, m.Prepares[i])
		}
		for i := range m.PrepareReplies {
			b := get(m.PrepareReplies[i].Id())
			b.PrepareReplies = append(b.PrepareReplies, m.PrepareReplies[i])
		}
		for i := range m.Commits {
			b := get(m.Commits[i].Id())
			b.Commits = append(b.Commits, m.Commits[i])
		}
		for i := range m.TsAgrees {
			b := get(m.TsAgrees[i].Id())
			b.TsAgrees = append(b.TsAgrees, m.TsAgrees[i])
		}
		for l, b := range per[:lanes] {
			if b != nil {
				p.lanes[l].push(from, b)
			}
		}
	default:
		r.stateMu.Lock()
		r.handleVR(from, m)
		r.stateMu.Unlock()
	}
}

// laneLoop is lane i's ordering goroutine: drain the inbox, run the handlers under one
// shared stateMu hold, release what is due, then wait for the next message or deadline.
func (r *Replica) laneLoop(i int) {
	defer r.pipe.wg.Done()
	q := r.pipe.lanes[i]
	tm := time.NewTimer(time.Hour)
	tm.Stop()
	ready := func() bool { return q.n.Load() > 0 }
	var batch []inItem
	stats := r.stats != nil
	for !r.stopping() {
		var t0 time.Time
		if stats {
			t0 = time.Now()
		}
		batch = q.swap(batch)
		if len(batch) > 0 {
			if stats {
				q.msgs.Add(uint64(len(batch)))
				if int32(len(batch)) > q.maxQ.Load() {
					q.maxQ.Store(int32(len(batch)))
				}
			}
			// The shared hold is per message, not per batch: VR messages take stateMu
			// exclusively, and a writer-preferring RWMutex would otherwise stall every
			// lane behind the longest batch of any lane (heartbeats arrive ~150 times/s).
			for j := range batch {
				r.stateMu.RLock()
				r.handleHot(batch[j].from, batch[j].m)
				r.stateMu.RUnlock()
			}
			clear(batch)
		}
		r.ord.ReleaseDueLane(i)
		if stats {
			d := uint64(time.Since(t0))
			q.busyNS.Add(d)
			if d > q.maxIterNS.Load() {
				q.maxIterNS.Store(d)
			}
		}
		if q.n.Load() > 0 {
			continue
		}
		next := r.ord.NextDueHintLane(i)
		if r.ord.Paused() {
			next = clock.Never // resume wakes us (SetPaused → wake hook)
		}
		q.park.wait(r.clk, next, ready, r.pipe.stop, tm)
	}
}

type specGroup struct {
	to   Addr
	reps []wire.SpecReply
}

// senderLoop drains its queue each round: SpecReplies queued for the same client leave
// as one SpecReplyBatch (no added wait — batches form only under backlog), and the
// round's datagrams go out together.
func (r *Replica) senderLoop(s *sender) {
	defer r.pipe.wg.Done()
	tm := time.NewTimer(time.Hour)
	tm.Stop()
	ready := func() bool { return s.n.Load() > 0 }
	var items []outItem
	var groups []specGroup
	byAddr := make(map[netip.AddrPort]int)
	for !r.stopping() || s.n.Load() > 0 {
		s.mu.Lock()
		items, s.items = s.items, items[:0]
		s.n.Store(0)
		s.mu.Unlock()
		if len(items) == 0 {
			s.park.wait(r.clk, clock.Never, ready, r.pipe.stop, tm)
			continue
		}
		var t0 time.Time
		if r.stats != nil {
			t0 = time.Now()
		}
		ng := 0
		for j := range items {
			it := &items[j]
			if it.m != nil {
				s.w.Add(it.to, it.m)
				continue
			}
			if !r.pipe.combine || len(it.spec.Results) > 0 {
				s.w.Add(it.to, &it.spec)
				continue
			}
			g, ok := byAddr[it.to]
			if !ok || len(groups[g].reps) >= maxCombine {
				if ng == len(groups) {
					groups = append(groups, specGroup{})
				}
				g = ng
				ng++
				groups[g].to = it.to
				groups[g].reps = groups[g].reps[:0]
				byAddr[it.to] = g
			}
			groups[g].reps = append(groups[g].reps, it.spec)
		}
		nd := len(items)
		for g := 0; g < ng; g++ {
			if reps := groups[g].reps; len(reps) == 1 {
				s.w.Add(groups[g].to, &reps[0])
			} else {
				s.w.Add(groups[g].to, &wire.SpecReplyBatch{Replies: reps})
				nd -= len(reps) - 1
				s.combined.Add(uint64(len(reps)))
			}
		}
		s.w.Flush()
		if r.stats != nil {
			s.dgrams.Add(uint64(nd))
			s.replies.Add(uint64(len(items)))
			s.busyNS.Add(uint64(time.Since(t0)))
		}
		clear(byAddr)
		clear(items)
	}
}

// sendSpec queues (pipeline) or sends a SpecReply for a txn of the given lane. Taking
// it by value keeps the caller's reply on the stack on the queued path.
func (r *Replica) sendSpec(lane int, to Addr, rep wire.SpecReply) {
	if r.pipe == nil || len(r.pipe.senders) == 0 {
		r.sendSpecNow(to, rep)
		return
	}
	r.pipe.senders[lane%len(r.pipe.senders)].push(outItem{to: to, spec: rep})
}

func (r *Replica) sendSpecNow(to Addr, rep wire.SpecReply) { r.tr.Send(to, &rep) }

// sendVia queues any message on lane's sender (or sends it inline).
func (r *Replica) sendVia(lane int, to Addr, m wire.Message) {
	if r.pipe == nil || len(r.pipe.senders) == 0 {
		r.tr.Send(to, m)
		return
	}
	r.pipe.senders[lane%len(r.pipe.senders)].push(outItem{to: to, m: m})
}
