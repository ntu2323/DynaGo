// Package replica is the per-server DYNA replica (C++ dyna/replica.{h,cc}). It owns
// ONE Orderer (one ordering domain per server) and drives the write path:
// ClientRequest → SubmitTxn → (release) → SpecReply; FastCommit/SlowCommit →
// ApplyCommit (with FetchRequest/FetchReply when the body is missing).
//
// Receive may be called concurrently (several receive goroutines). Per-txn replica
// state (dedup / reply cache, OWD estimator, client addresses) is sharded into the
// orderer's lanes, so every step of one txn touches one lane.
package replica

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"dynago/internal/clock"
	"dynago/internal/deadline"
	"dynago/internal/hmap"
	"dynago/internal/orderer"
	"dynago/internal/outbox"
	"dynago/internal/store"
	"dynago/internal/topology"
	"dynago/internal/transport"
	"dynago/internal/types"
	"dynago/internal/vr"
	"dynago/internal/wire"
)

type Addr = transport.Addr

type Options struct {
	Index     int
	F         int
	Transport transport.Transport
	Clock     clock.Clock // nil: real clock (busy-poll per DYNA_BUSY_POLL)
	Store     store.Store // nil: in-memory
	Lanes     int         // 0: DYNA_LANES
	// ExternalDrive: no goroutines at all — the caller drives Orderer().ReleaseDue,
	// Orderer().ExecuteReady, Tick and FlushBg (deterministic tests on a mock clock).
	ExternalDrive bool
	// VRStateDir holds dyna-vr-<idx>.state ("" = DYNA_VR_STATE_DIR, default ".").
	VRStateDir string
	Quiet      bool // no VR log lines (tests)
	// NoPipeline: handle hot messages on the receive goroutine (under the lane locks)
	// with one release goroutine, instead of per-lane ordering goroutines + senders.
	// Also DYNA_ORDER_THREAD=0. ExternalDrive implies it.
	NoPipeline bool
}

// cachedSpec: a resend is answered with a reply rebuilt from these (t and leadership
// are re-derived from live state anyway), so the release path stores no message per txn.
type cachedSpec struct {
	t, owd uint64
}

type lane struct {
	idx   int
	owdMu sync.Mutex
	owd   *deadline.ReplicaOwdEstimator

	dedupMu    sync.Mutex
	replyCache *hmap.Map[types.TxnId, cachedSpec] // at-most-once for WRITEs
	inflight   *hmap.Map[types.TxnId, struct{}]
	// client_id -> address, learned from writes: recovery M carries no addresses, so a
	// new partition leader finishing an inherited txn finds its client here.
	clientAddrs map[uint64]Addr

	bg *outbox.BgOutbox // nil = background batching off (DYNA_REPL_BATCH <= 1)
}

type pendingCommit struct {
	agreedT uint64
	slow    bool
	client  Addr
}

type Replica struct {
	idx   uint32
	n, f  int
	tr    transport.Transport
	clk   clock.Clock
	ord   *orderer.Orderer
	debug bool

	viewID atomic.Uint64
	topoMu sync.RWMutex
	topo   topology.Topology

	lanes []*lane

	pendMu         sync.Mutex
	pendingCommits map[types.TxnId]pendingCommit

	// VR layer (vr.go). stateMu: shared for hot messages, exclusive for VR / recovery.
	stateMu   sync.RWMutex
	vc        *vr.Coordinator
	paused    atomic.Bool // release paused (not NORMAL); read lock-free on receive
	hbMS      uint64
	vrDir     string
	bulk      bulkState
	recCache  map[uint32]*recReplyCache
	catchup   catchUp
	donors    map[uint32]*donorSession
	snapChunk int
	quiet     bool

	tsScanMS    uint64 // background scan cadence
	tsTimeoutUS uint64 // SPECULATIVE age before ts-agreement; prepare / re-ack throttle
	scanBudget  int    // per-tick cap on each scan's actions (smallest OrderKey first)
	bgFlushUS   uint64
	pipe        *pipeline // nil: hot messages run on the receive goroutine
	stats       *rxStats  // DYNA_STATS (nil: off)
	stop        chan struct{}
	wg          sync.WaitGroup
}

func envBool(name string, def bool) bool {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	return v != "0"
}

func New(o Options) *Replica {
	n := o.Transport.NumReplicas()
	r := &Replica{
		idx: uint32(o.Index), n: n, f: o.F, tr: o.Transport, clk: o.Clock,
		debug:          envBool("DYNA_DEBUG", false),
		topo:           topology.New(uint32(n)),
		pendingCommits: make(map[types.TxnId]pendingCommit),
	}
	if r.clk == nil {
		r.clk = clock.NewReal(envBool("DYNA_BUSY_POLL", true))
	}
	st := o.Store
	if st == nil {
		st = store.NewMem()
	}
	r.ord = orderer.New(r.idx, 0, r.clk, r.topo, r.onSpecReply, st, o.Lanes)
	rsv := int(deadline.EnvU64("DYNA_PRERESERVE", 0))
	if rsv > 0 {
		r.ord.ReserveExecuted(rsv)
	}
	r.tsScanMS = max(1, deadline.EnvU64("DYNA_TS_SCAN_MS", 50))
	r.tsTimeoutUS = deadline.EnvU64("DYNA_TS_TIMEOUT_MS", 100) * 1000
	r.scanBudget = int(deadline.EnvU64("DYNA_SCAN_BUDGET", 4096))
	r.bgFlushUS = max(1, deadline.EnvU64("DYNA_REPL_FLUSH_US", 500))
	bgBatch := int(deadline.EnvU64("DYNA_REPL_BATCH", 256))
	bgBytes := int(deadline.EnvU64("DYNA_REPL_MAX_BYTES", 8000))
	dcfg := deadline.FromEnv()
	for i := 0; i < r.ord.Lanes(); i++ {
		l := &lane{
			idx:         i,
			owd:         deadline.NewReplicaOwdEstimator(dcfg),
			replyCache:  hmap.New[types.TxnId, cachedSpec](0),
			inflight:    hmap.New[types.TxnId, struct{}](0),
			clientAddrs: make(map[uint64]Addr),
		}
		if bgBatch > 1 {
			l.bg = outbox.New(n, bgBatch, bgBytes)
		}
		if rsv > 0 { // the reply cache is never GC'd either (Tthresh out of scope)
			l.replyCache.Reserve(rsv/r.ord.Lanes() + rsv/(4*r.ord.Lanes()))
		}
		r.lanes = append(r.lanes, l)
	}
	r.initVR(o)
	if !o.ExternalDrive {
		r.ord.SetExecMinInterval(deadline.EnvU64("DYNA_EXEC_MIN_INTERVAL_US", 2000))
		if envBool("DYNA_STATS", false) {
			r.stats = &rxStats{}
		}
		pipelined := !o.NoPipeline && envBool("DYNA_ORDER_THREAD", true)
		r.ord.Start(!pipelined)
		if pipelined {
			r.startPipeline(int(deadline.EnvU64("DYNA_SEND_THREADS", 1)))
		}
		r.startTimers()
	}
	return r
}

// startTimers runs the background scan and the background-batch flush on their own
// goroutines (never on a receive loop: a heavy tick there would drop a peer's traffic).
func (r *Replica) startTimers() {
	r.stop = make(chan struct{})
	every := func(d time.Duration, f func()) {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			tk := time.NewTicker(d)
			defer tk.Stop()
			for {
				select {
				case <-r.stop:
					return
				case <-tk.C:
					f()
				}
			}
		}()
	}
	if r.stats != nil {
		r.wg.Add(1)
		go r.statsLoop()
	}
	every(time.Duration(r.tsScanMS)*time.Millisecond, r.Tick)
	every(time.Duration(r.hbMS)*time.Millisecond, r.HeartbeatTick)
	if r.lanes[0].bg != nil {
		every(time.Duration(r.bgFlushUS)*time.Microsecond, r.FlushBg)
	}
}

func (r *Replica) Orderer() *orderer.Orderer { return r.ord }
func (r *Replica) Index() int                { return int(r.idx) }
func (r *Replica) Stop() {
	if r.stop != nil {
		close(r.stop)
		r.wg.Wait()
		r.stop = nil
	}
	r.stopPipeline()
	r.ord.Stop()
}

func (r *Replica) laneFor(id types.TxnId) *lane { return r.lanes[r.ord.LaneOf(id)] }

func (r *Replica) topology() topology.Topology {
	r.topoMu.RLock()
	defer r.topoMu.RUnlock()
	return r.topo
}

// Receive dispatches one message (safe for concurrent callers). Hot messages (client
// path, replication, ts-agreement, fetch) run under the shared side of stateMu; VR /
// recovery / rejoin messages take it exclusively, so they stay serialized against
// normal processing.
func (r *Replica) Receive(from Addr, m wire.Message) {
	if r.stats != nil {
		r.stats.count(m)
	}
	if r.pipe != nil {
		r.route(from, m)
		return
	}
	switch m.(type) {
	case *wire.ClientRequest, *wire.FastCommit, *wire.FastCommitBatch, *wire.SlowCommit:
		// Paused (view change / joining): drop client traffic early so a resend flood
		// can't crowd out recovery.
		if r.paused.Load() {
			return
		}
		r.stateMu.RLock()
		r.handleHot(from, m)
		r.stateMu.RUnlock()
	case *wire.FetchRequest, *wire.FetchReply, *wire.TsAgree, *wire.ReplicatePrepare,
		*wire.ReplicatePrepareReply, *wire.ReplicateCommit, *wire.BgBatch:
		r.stateMu.RLock()
		r.handleHot(from, m)
		r.stateMu.RUnlock()
	default:
		r.stateMu.Lock()
		r.handleVR(from, m)
		r.stateMu.Unlock()
	}
}

// handleHot requires stateMu (shared).
func (r *Replica) handleHot(from Addr, m wire.Message) {
	// A JOINING server that adopted the current view applies live replication (without
	// acking: it is not a member yet), so nothing committed while it catches up falls
	// between the snapshot and the view change that re-adds it.
	if r.vc.Phase() == vr.Joining {
		if r.vc.HasView() {
			r.applyAsNonMember(m)
		}
		return
	}
	if r.vc.Phase() != vr.Normal {
		return // normal processing pauses during a view change
	}
	view := r.viewID.Load()
	switch m := m.(type) {
	case *wire.ClientRequest:
		if m.Txn.ViewID != view {
			if m.Txn.ViewID < view {
				r.sendViewInfo(from)
			}
			return
		}
		e := types.NewEntry(m.Txn)
		e.ClientAddr = from
		r.submitRequest(e)
	case *wire.FastCommit:
		if r.checkView(from, m.ViewID, view) {
			r.handleCommit(from, m.Id(), m.AgreedT, false)
		}
	case *wire.FastCommitBatch:
		told := false // one ViewInfo per batch
		for i := range m.Commits {
			c := &m.Commits[i]
			if c.ViewID != view {
				if c.ViewID < view && !told {
					r.sendViewInfo(from)
					told = true
				}
				continue
			}
			r.handleCommit(from, c.Id(), c.AgreedT, false)
		}
	case *wire.SlowCommit:
		if r.checkView(from, m.ViewID, view) {
			r.handleCommit(from, m.Id(), m.AgreedT, true)
		}
	case *wire.FetchRequest:
		if m.ViewID == view {
			r.handleFetchRequest(from, m)
		}
	case *wire.FetchReply:
		r.handleFetchReply(m)
	case *wire.TsAgree:
		if m.ViewID == view {
			r.handleTsAgree(m)
		}
	case *wire.ReplicatePrepare:
		if m.ViewID == view {
			r.handleReplicatePrepare(m)
		}
	case *wire.ReplicatePrepareReply:
		if m.ViewID == view {
			r.handlePrepareReply(m)
		}
	case *wire.ReplicateCommit:
		if m.ViewID == view {
			r.ord.MarkCommitted(m.Id())
		}
	case *wire.BgBatch:
		r.handleBgBatch(m, view)
	}
}

// applyAsNonMember: JOINING with an adopted view — apply that view's replication
// (unacked).
func (r *Replica) applyAsNonMember(m wire.Message) {
	jv := r.viewID.Load()
	prep := func(p *wire.ReplicatePrepare) {
		if p.ViewID == jv && p.Txn != nil {
			r.ord.HandleReplicatePrepare(p.Id(), p.AgreedT, p.Txn)
		}
	}
	commit := func(c *wire.ReplicateCommit) {
		if c.ViewID == jv {
			r.ord.MarkCommitted(c.Id())
		}
	}
	switch m := m.(type) {
	case *wire.ReplicatePrepare:
		prep(m)
	case *wire.ReplicateCommit:
		commit(m)
	case *wire.BgBatch:
		for i := range m.Prepares {
			prep(&m.Prepares[i])
		}
		for i := range m.Commits {
			commit(&m.Commits[i])
		}
	}
}

// checkView: an old-view commit decision is not adopted (the new view re-derives the
// txn's t); the client is told the current view.
func (r *Replica) checkView(from Addr, msgView, view uint64) bool {
	if msgView == view {
		return true
	}
	if msgView < view {
		r.sendViewInfo(from)
	}
	return false
}

func (r *Replica) sendViewInfo(to Addr) {
	r.tr.Send(to, &wire.ViewInfo{ViewID: r.viewID.Load(), Alive: uint64(r.topology().Alive)})
}

// VC exposes the VR coordinator's phase / view for tests and logs (racy outside stateMu;
// use ViewState).
func (r *Replica) ViewState() (phase vr.Phase, view, alive uint64) {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	return r.vc.Phase(), r.vc.View(), r.vc.Alive()
}

// submitRequest samples the OWD where lateness is judged, applies at-most-once for
// WRITEs, and submits to the orderer.
func (r *Replica) submitRequest(e *types.TxnEntry) {
	id := e.Key()
	l := r.laneFor(id)
	arrival := r.clk.Now()
	if e.Txn.SendTime != 0 {
		l.owdMu.Lock()
		if arrival > e.Txn.SendTime {
			l.owd.Sample(id.ClientID, arrival-e.Txn.SendTime)
		}
		e.OwdEstimateUS = l.owd.EstimateUS(id.ClientID)
		l.owdMu.Unlock()
	}
	e.ArrivalUS = arrival
	if r.debug {
		log.Printf("r%d RECV req c=%d r=%d t=%d now=%d", r.idx, id.ClientID, id.RequestID, e.T, arrival)
	}
	// At-most-once for WRITEs: answer a retry from cache; drop one still in flight so
	// the orderer never holds two entries for one id. READs skip this: a read retry is a
	// new attempt (fresh request_id, larger t).
	if e.Txn.Type == types.Write {
		l.dedupMu.Lock()
		if _, ok := l.clientAddrs[id.ClientID]; !ok && e.ClientAddr.IsValid() {
			l.clientAddrs[id.ClientID] = e.ClientAddr
		}
		if c, ok := l.replyCache.Get(id); ok {
			l.dedupMu.Unlock()
			r.answerFromCache(e, c)
			return
		}
		if l.inflight.Has(id) {
			l.dedupMu.Unlock()
			return
		}
		l.inflight.Put(id, struct{}{})
		l.dedupMu.Unlock()
	}
	r.ord.SubmitTxn(e, r.pipe == nil) // a lane goroutine releases on its next turn
}

// answerFromCache answers a write resend with the txn's CURRENT state: after a commit
// or a view change its t and our leadership may differ from the cached release.
func (r *Replica) answerFromCache(e *types.TxnEntry, c cachedSpec) {
	t := c.t
	if liveT, _, ok := r.ord.LiveState(e.Key()); ok {
		t = liveT
	}
	topo := r.topology()
	leads := false
	for _, kv := range e.Txn.WSet {
		if topo.OwnerOf(kv.Key) == r.idx {
			leads = true
		}
	}
	if e.ClientAddr.IsValid() {
		r.sendSpec(r.ord.LaneOf(e.Key()), e.ClientAddr, wire.SpecReply{ClientID: e.Txn.ClientID,
			RequestID: e.Txn.RequestID, ViewID: r.viewID.Load(), ServerID: r.idx, T: t, IsLeader: leads,
			OwdEstimate: c.owd})
	}
}

// onSpecReply fires (outside the lane lock) when an entry releases, and for a leader
// READ once its values were read.
func (r *Replica) onSpecReply(e *types.TxnEntry) {
	if r.debug {
		log.Printf("r%d SEND SpecReply c=%d r=%d t=%d leader=%v", r.idx, e.Txn.ClientID,
			e.Txn.RequestID, e.T, e.IsLeaderForThis)
	}
	id := e.Key()
	lane := r.ord.LaneOf(id)
	if e.ClientAddr.IsValid() {
		rep := wire.SpecReply{ClientID: e.Txn.ClientID, RequestID: e.Txn.RequestID,
			ViewID: r.viewID.Load(), ServerID: r.idx, T: e.T, IsLeader: e.IsLeaderForThis,
			OwdEstimate: e.OwdEstimateUS}
		for k, v := range e.Results {
			rep.Results = append(rep.Results, types.KV{Key: k, Value: v})
		}
		r.sendSpec(lane, e.ClientAddr, rep)
	}
	if e.Txn.Type == types.Read {
		return // reads aren't deduped
	}
	l := r.lanes[lane]
	l.dedupMu.Lock()
	l.replyCache.Put(id, cachedSpec{e.T, e.OwdEstimateUS})
	l.inflight.Delete(id)
	l.dedupMu.Unlock()
}

func (r *Replica) handleCommit(from Addr, id types.TxnId, agreedT uint64, slow bool) {
	res, brief := r.ord.ApplyCommit(id, agreedT)
	if res != orderer.NeedFetch {
		r.finishCommit(id, res, brief, slow, from)
		return
	}
	// Body missing: remember the commit, then ask everyone for the body.
	pc := pendingCommit{agreedT: agreedT, slow: slow}
	if slow {
		pc.client = from
	}
	r.pendMu.Lock()
	r.pendingCommits[id] = pc
	r.pendMu.Unlock()
	r.tr.Broadcast(&wire.FetchRequest{ClientID: id.ClientID, RequestID: id.RequestID,
		ViewID: r.viewID.Load(), RequesterID: r.idx})
}

func (r *Replica) finishCommit(id types.TxnId, res orderer.CommitResult, b orderer.CommitBrief, slow bool, client Addr) {
	l := r.laneFor(id)
	l.dedupMu.Lock()
	l.inflight.Delete(id)
	l.dedupMu.Unlock()
	// SlowCommit: the client re-checks the quorum at agreedT (or its final t if executed).
	if slow && client.IsValid() {
		r.sendCommitReply(client, id, b)
	}
	if res == orderer.Applied {
		r.replicateNowIfPrimary(id)
	}
}

func (r *Replica) sendCommitReply(to Addr, id types.TxnId, b orderer.CommitBrief) {
	r.sendVia(r.ord.LaneOf(id), to, &wire.CommitReply{ClientID: id.ClientID, RequestID: id.RequestID,
		ViewID: r.viewID.Load(), ServerID: r.idx, T: b.T, IsLeader: b.IsLeader})
}

func (r *Replica) handleFetchRequest(from Addr, m *wire.FetchRequest) {
	if body, ok := r.ord.TryGetBody(types.ID(m.ClientID, m.RequestID)); ok {
		r.sendVia(r.ord.LaneOf(body.Id()), from, &wire.FetchReply{Txn: body})
	}
}

func (r *Replica) handleFetchReply(m *wire.FetchReply) {
	r.ord.InstallFetched(&m.Txn)
	id := m.Txn.Id()
	r.pendMu.Lock()
	pc, ok := r.pendingCommits[id]
	delete(r.pendingCommits, id)
	r.pendMu.Unlock()
	if !ok {
		return
	}
	if res, b := r.ord.ApplyCommit(id, pc.agreedT); res != orderer.NeedFetch {
		r.finishCommit(id, res, b, pc.slow, pc.client)
	}
}
