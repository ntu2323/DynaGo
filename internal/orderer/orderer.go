// Package orderer is the per-server ordering engine (C++ dyna/server_orderer.{h,cc}).
//
// DYNA has ONE Orderer per server, not one per partition: a txn has a single OrderKey
// and lives in exactly one of earlyBuffer / lateBuffer / specLog even when it spans
// partitions.
//
// Lanes: per-txn state is sharded by TxnId into K lanes (LaneOf), each with its own
// lock, so K goroutines can order disjoint txns in parallel. Every protocol step of
// one txn touches only its lane. Cross-lane: the executor pass (all lanes, briefly),
// the late-leader re-timestamp floor (a global atomic max), and view install / reset.
//
// Callbacks (OnSpecReply) always run OUTSIDE the lane lock.
package orderer

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"dynago/internal/clock"
	"dynago/internal/hmap"
	"dynago/internal/pq"
	"dynago/internal/store"
	"dynago/internal/topology"
	"dynago/internal/types"
)

// OnSpecReply fires once per released entry (and for a leader READ once the executor
// read its values). The entry is a reply copy owned by the orderer: the callback must
// not retain the pointer past the call.
type OnSpecReply func(e *types.TxnEntry)

type CommitResult int

const (
	Applied CommitResult = iota
	AlreadyExecuted
	NeedFetch
)

func (r CommitResult) String() string {
	return [...]string{"Applied", "AlreadyExecuted", "NeedFetch"}[r]
}

// CommitBrief is what the commit-reply path needs from an adopted commit.
type CommitBrief struct {
	T        uint64
	Status   types.Status
	IsLeader bool
}

type executedInfo struct {
	t        uint64
	isLeader bool
}

type specPQ = pq.IndexedMinPQ[types.TxnId, types.TxnEntry]

func entryLess(a, b *types.TxnEntry) bool { return a.Less(b) }

// shard is one lane's per-transaction state, guarded by mu.
type shard struct {
	mu              sync.Mutex
	early           *specPQ
	late            *hmap.Map[types.TxnId, *types.TxnEntry] // late follower arrivals
	spec            *specPQ
	highestReleased types.OrderKey // max OrderKey ever appended to this lane's specLog
	// Writes executed and GC'd from specLog: id -> (final t, led by us). Guards against
	// re-admitting (and re-applying) a txn after GC. Never GC'd (Tthresh out of scope).
	executed *hmap.Map[types.TxnId, executedInfo]
	nextDue  atomic.Uint64 // earliest earlyBuffer deadline (lock-free hint)

	scratch []types.TxnEntry // reply copies for one release pass (lane-local reuse)

	st laneCounters
}

// laneCounters are DYNA_STATS arrival / release counters (one cache line set per lane).
type laneCounters struct {
	onTime, lateLeader, lateFollower, released atomic.Uint64
	slackUS, latenessUS                        atomic.Uint64 // sums: t - arrival / arrival - t
}

// Counters is a cumulative snapshot of the orderer's DYNA_STATS counters.
type Counters struct {
	OnTime, LateLeader, LateFollower, Released uint64
	SlackUS, LatenessUS                        uint64
	ExecPasses, ExecWrites, ExecPassUS         uint64
	ExecMaxSnapUS                              uint64 // since the last Counters call
	SkippedOlder                               uint64
}

func (o *Orderer) Counters() Counters {
	var c Counters
	for _, s := range o.shards {
		c.OnTime += s.st.onTime.Load()
		c.LateLeader += s.st.lateLeader.Load()
		c.LateFollower += s.st.lateFollower.Load()
		c.Released += s.st.released.Load()
		c.SlackUS += s.st.slackUS.Load()
		c.LatenessUS += s.st.latenessUS.Load()
	}
	c.ExecPasses = o.exec.passes.Load()
	c.ExecWrites = o.exec.writes.Load()
	c.ExecPassUS = o.exec.passUS.Load()
	c.ExecMaxSnapUS = o.exec.maxSnapUS.Swap(0)
	c.SkippedOlder = o.exec.skippedOlderN.Load()
	return c
}

type Orderer struct {
	serverID uint32
	viewID   uint64 // written with every lane locked; read under any lane lock
	quorumF  uint32 // f = (n-1)/2: prepare replies needed for COMMITTED
	clk      clock.Clock
	topo     topology.Topology // written with every lane locked
	onReply  OnSpecReply
	store    store.Store // executed writes (nil: no execution)

	shards []*shard
	// Server-wide floor for the late-leader re-timestamp (t of the max OrderKey released
	// in any lane).
	highestTMax atomic.Uint64

	running  atomic.Bool
	paused   atomic.Bool
	wakeHook atomic.Pointer[func()]
	wg       sync.WaitGroup

	exec executor
}

// LanesFromEnv reads DYNA_LANES (default 1), clamped to [1, 64].
func LanesFromEnv() int {
	n := 1
	if v, err := strconv.Atoi(os.Getenv("DYNA_LANES")); err == nil {
		n = v
	}
	return min(max(n, 1), 64)
}

// New builds an orderer. lanes = 0 takes DYNA_LANES from the environment, so any test
// suite can run sharded.
func New(serverID uint32, viewID uint64, clk clock.Clock, topo topology.Topology,
	onReply OnSpecReply, st store.Store, lanes int) *Orderer {
	if lanes <= 0 {
		lanes = LanesFromEnv()
	}
	lanes = min(lanes, 64)
	o := &Orderer{serverID: serverID, viewID: viewID, clk: clk, topo: topo,
		onReply: onReply, store: st}
	if topo.N > 0 {
		o.quorumF = (topo.N - 1) / 2
	}
	for i := 0; i < lanes; i++ {
		s := &shard{
			early:    pq.New[types.TxnId, types.TxnEntry](entryLess),
			late:     hmap.New[types.TxnId, *types.TxnEntry](0),
			spec:     pq.New[types.TxnId, types.TxnEntry](entryLess),
			executed: hmap.New[types.TxnId, executedInfo](0),
		}
		s.nextDue.Store(clock.Never)
		o.shards = append(o.shards, s)
	}
	o.exec.init()
	return o
}

func (o *Orderer) Lanes() int { return len(o.shards) }

// LaneOf is the lane owning a txn (stable for its lifetime).
func (o *Orderer) LaneOf(id types.TxnId) int { return LaneOf(id, len(o.shards)) }

// LaneOf with an explicit lane count (the replica routes with the same function).
func LaneOf(id types.TxnId, lanes int) int {
	if lanes <= 1 {
		return 0
	}
	g := id.Hash() * 0xD6E8FEB86659FD93
	g ^= g >> 32
	return int(((g & 0xFFFFFFFF) * uint64(lanes)) >> 32)
}

func (o *Orderer) shardOf(id types.TxnId) *shard { return o.shards[o.LaneOf(id)] }

func (o *Orderer) lockAll() {
	for _, s := range o.shards {
		s.mu.Lock()
	}
}

func (o *Orderer) unlockAll() {
	for _, s := range o.shards {
		s.mu.Unlock()
	}
}

// bumpHighestLocked: highestReleasedT := max(.., ok) for the lane and the global floor.
func (o *Orderer) bumpHighestLocked(s *shard, ok types.OrderKey) {
	if s.highestReleased.Less(ok) {
		s.highestReleased = ok
	}
	for {
		cur := o.highestTMax.Load()
		if ok.T <= cur || o.highestTMax.CompareAndSwap(cur, ok.T) {
			return
		}
	}
}

// computeLeadership fills IsLeaderForThis / LedKeys / Primary from the topology.
// Requires a lane lock (reads topo).
func (o *Orderer) computeLeadership(e *types.TxnEntry) {
	e.LedKeys = e.LedKeys[:0]
	e.Primary = types.NoServer
	note := func(k string) {
		ow := o.topo.OwnerOf(k)
		e.Primary = min(e.Primary, ow)
		if ow == o.serverID {
			e.LedKeys = append(e.LedKeys, k)
		}
	}
	for _, k := range e.Txn.RSet {
		note(k)
	}
	for _, kv := range e.Txn.WSet {
		note(kv.Key)
	}
	if len(e.LedKeys) > 1 {
		e.LedKeys = types.SortStrings(e.LedKeys)
	}
	e.IsLeaderForThis = len(e.LedKeys) > 0
}

// replyCopy: what the SpecReply callback needs — ids, type, t, leadership, OWD, client
// address — not the body or leader bookkeeping.
func replyCopy(e *types.TxnEntry) types.TxnEntry {
	return types.TxnEntry{
		Txn: types.Txn{ClientID: e.Txn.ClientID, RequestID: e.Txn.RequestID, Type: e.Txn.Type,
			T: e.Txn.T, ViewID: e.Txn.ViewID, SendTime: e.Txn.SendTime},
		T: e.T, Status: e.Status, IsLeaderForThis: e.IsLeaderForThis,
		OwdEstimateUS: e.OwdEstimateUS, ArrivalUS: e.ArrivalUS, ReleasedUS: e.ReleasedUS,
		ClientAddr: e.ClientAddr, Primary: e.Primary, Results: e.Results,
	}
}

func (o *Orderer) fire(out []types.TxnEntry) {
	if o.onReply == nil {
		return
	}
	for i := range out {
		o.onReply(&out[i])
	}
}

// SubmitTxn classifies a freshly arrived client txn (design doc § arrival); the orderer
// takes ownership of e.
//   - on time (clock <= t) → earlyBuffer;
//   - late at a leader of any key → re-timestamp t := clock() above the server-wide
//     highestReleasedT and release now (a WRITE replies at once; a READ is held for
//     the executor);
//   - late at a pure follower → a WRITE goes silently to lateBuffer, a READ is dropped.
//
// A txn already executed is never re-admitted; a WRITE resend is answered with its
// final t. poke wakes the release loop.
func (o *Orderer) SubmitTxn(e *types.TxnEntry, poke bool) {
	id := e.Key()
	isRead := e.Txn.Type == types.Read
	var released []types.TxnEntry
	kick := false
	s := o.shardOf(id)
	s.mu.Lock()
	o.computeLeadership(e)
	if x, ok := s.executed.Get(id); ok {
		if !isRead {
			e.T = x.t
			e.Status = types.Committed
			released = append(released, replyCopy(e))
		}
		s.mu.Unlock()
		o.fire(released)
		return
	}
	now := o.clk.Now()
	switch {
	case s.spec.Contains(id):
		// Already released (a resend the replica's dedup didn't catch): answer with the
		// live entry. Never hold two entries for one id.
		if !isRead {
			cur := s.spec.Find(id)
			if !cur.ClientAddr.IsValid() {
				cur.ClientAddr = e.ClientAddr
			}
			released = append(released, replyCopy(cur))
		}
	case s.early.Contains(id):
		// It will reply on release.
	default:
		// A lateBuffer copy is re-classified from the fresh arrival (we may lead it now).
		s.late.Delete(id)
		if now <= e.T {
			s.st.onTime.Add(1)
			s.st.slackUS.Add(e.T - now)
			if e.T < s.nextDue.Load() {
				s.nextDue.Store(e.T)
			}
			s.early.Push(id, e)
		} else if e.IsLeaderForThis {
			s.st.lateLeader.Add(1)
			s.st.latenessUS.Add(now - e.T)
			newT := now
			if floor := o.highestTMax.Load(); newT <= floor {
				newT = floor + 1
			}
			e.T = newT
			e.Status = types.Speculative
			e.ReleasedUS = now
			o.bumpHighestLocked(s, e.Order())
			if !isRead {
				released = append(released, replyCopy(e))
			} else {
				kick = true
			}
			s.spec.Push(id, e)
		} else {
			s.st.lateFollower.Add(1)
			s.st.latenessUS.Add(now - e.T)
			if !isRead {
				s.late.Put(id, e)
			}
		}
		// READ late at a pure follower: dropped (the client retries at a larger t).
	}
	s.mu.Unlock()
	o.fire(released)
	if kick {
		o.KickExecutor()
	}
	if poke {
		o.interrupt()
	}
}

// drainDueLocked moves due earlyBuffer entries into specLog (followers' READs are only
// endorsed, never logged) and appends reply copies to s.scratch.
func (o *Orderer) drainDueLocked(s *shard) (n int, kick bool) {
	now := o.clk.Now()
	for !s.early.Empty() {
		e := s.early.PeekMin()
		if e.T > now {
			break
		}
		id := s.early.PeekMinID()
		s.early.PopMin()
		e.Status = types.Speculative
		e.ReleasedUS = now
		// max, not assign: a protocol-decided insert may already be past this entry.
		o.bumpHighestLocked(s, e.Order())
		switch {
		case e.Txn.Type == types.Read && !e.IsLeaderForThis:
			s.scratch = append(s.scratch, replyCopy(e)) // follower endorsement; not logged
		case e.Txn.Type == types.Read:
			s.spec.Push(id, e) // leader READ: the executor serves it
			kick = true
		default:
			s.scratch = append(s.scratch, replyCopy(e))
			s.spec.Push(id, e)
		}
		n++
	}
	if n > 0 {
		s.st.released.Add(uint64(n))
	}
	s.nextDue.Store(o.nextDeadlineLocked(s))
	return n, kick
}

func (o *Orderer) nextDeadlineLocked(s *shard) uint64 {
	if s.early.Empty() {
		return clock.Never
	}
	return s.early.PeekMin().T
}

// ReleaseDue synchronously drains every lane's due entries (tests, external drivers).
func (o *Orderer) ReleaseDue() int {
	n := 0
	for lane := range o.shards {
		n += o.ReleaseDueLane(lane)
	}
	return n
}

// ReleaseDueLane drains one lane (called by that lane's ordering goroutine).
func (o *Orderer) ReleaseDueLane(lane int) int {
	if o.paused.Load() {
		return 0 // view change: entries due meanwhile release on resume
	}
	s := o.shards[lane]
	s.mu.Lock()
	n, kick := o.drainDueLocked(s)
	out := s.scratch
	s.scratch = nil
	s.mu.Unlock()
	o.fire(out)
	clear(out)
	s.mu.Lock()
	if s.scratch == nil {
		s.scratch = out[:0] // hand the buffer back for reuse
	}
	s.mu.Unlock()
	if kick {
		o.KickExecutor()
	}
	return n
}

// NextDueHint is the earliest earlyBuffer deadline as of the last change (clock.Never
// if none). Lock-free; may be early but never late for SubmitTxn'd entries.
func (o *Orderer) NextDueHint() uint64 {
	m := uint64(clock.Never)
	for _, s := range o.shards {
		m = min(m, s.nextDue.Load())
	}
	return m
}

func (o *Orderer) NextDueHintLane(lane int) uint64 { return o.shards[lane].nextDue.Load() }

func (o *Orderer) releaseLoop() {
	defer o.wg.Done()
	var out []types.TxnEntry
	for o.running.Load() {
		next := uint64(clock.Never) // parked until SetPaused(false) interrupts us
		kick := false
		if !o.paused.Load() {
			for _, s := range o.shards {
				s.mu.Lock()
				_, k := o.drainDueLocked(s)
				kick = kick || k
				out = append(out, s.scratch...)
				clear(s.scratch)
				s.scratch = s.scratch[:0]
				next = min(next, o.nextDeadlineLocked(s))
				s.mu.Unlock()
			}
		}
		o.fire(out)
		clear(out)
		out = out[:0]
		if kick {
			o.KickExecutor()
		}
		if !o.running.Load() {
			break
		}
		o.clk.SleepUntil(next)
	}
}

// Start runs the release loop (unless the caller drives ReleaseDue itself) and the
// executor.
func (o *Orderer) Start(ownReleaseLoop bool) {
	if !o.running.CompareAndSwap(false, true) {
		return
	}
	if ownReleaseLoop {
		o.wg.Add(1)
		go o.releaseLoop()
	}
	o.exec.start(o)
}

func (o *Orderer) Stop() {
	if !o.running.CompareAndSwap(true, false) {
		return
	}
	o.interrupt()
	o.exec.stop()
	o.wg.Wait()
}

// SetPaused pauses/resumes the release loop (normal processing pauses during a view
// change; entries due meanwhile release on resume).
func (o *Orderer) SetPaused(p bool) {
	o.paused.Store(p)
	o.interrupt()
}

// SetWakeHook registers f to run whenever release state changes from outside a lane's
// own goroutine (resume, view install, poke): external per-lane release loops
// (Start(false)) park on it.
func (o *Orderer) SetWakeHook(f func()) { o.wakeHook.Store(&f) }

func (o *Orderer) interrupt() {
	o.clk.Interrupt()
	if f := o.wakeHook.Load(); f != nil {
		(*f)()
	}
}

func (o *Orderer) Paused() bool { return o.paused.Load() }

// applyCommitLocked locates the txn (executed → specLog → earlyBuffer → lateBuffer),
// adopts agreedT as COMMITTING (never downgrading COMMITTED), and reports the result.
func (o *Orderer) applyCommitLocked(s *shard, id types.TxnId, agreedT uint64) (CommitResult, *types.TxnEntry, CommitBrief) {
	now := o.clk.Now()
	if x, ok := s.executed.Get(id); ok {
		return AlreadyExecuted, nil, CommitBrief{T: x.t, Status: types.Committed, IsLeader: x.isLeader}
	}
	if e := s.spec.Find(id); e != nil {
		if e.Status != types.Committed {
			e.T = agreedT
			if e.Status != types.Committing {
				e.CommittingUS = now
			}
			e.Status = types.Committing
			o.bumpHighestLocked(s, e.Order())
			s.spec.UpdateKey(id)
		}
		return Applied, e, CommitBrief{e.T, e.Status, e.IsLeaderForThis}
	}
	e := s.early.Erase(id)
	if e == nil {
		if e = s.late.At(id); e != nil {
			s.late.Delete(id)
		}
	}
	if e == nil {
		return NeedFetch, nil, CommitBrief{}
	}
	e.T = agreedT
	e.Status = types.Committing
	e.CommittingUS = now
	o.bumpHighestLocked(s, e.Order())
	s.spec.Push(id, e)
	return Applied, e, CommitBrief{e.T, e.Status, e.IsLeaderForThis}
}

// ApplyCommit adopts a Fast/SlowCommit's agreedT (protocol-decided; never vetoed).
// NeedFetch: the replica should FetchRequest the body, InstallFetched, and re-apply.
func (o *Orderer) ApplyCommit(id types.TxnId, agreedT uint64) (CommitResult, CommitBrief) {
	s := o.shardOf(id)
	s.mu.Lock()
	r, _, b := o.applyCommitLocked(s, id, agreedT)
	s.mu.Unlock()
	return r, b
}

// InstallFetched materializes a fetched body into lateBuffer (as if it arrived late)
// so a following ApplyCommit adopts it. No-op if held anywhere or already executed.
func (o *Orderer) InstallFetched(body *types.Txn) {
	id := body.Id()
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.executed.Get(id); ok || s.spec.Contains(id) || s.early.Contains(id) || s.late.At(id) != nil {
		return
	}
	e := types.NewEntry(body.Clone())
	o.computeLeadership(e)
	s.late.Put(id, e)
}

// TryGetBody copies out the body if held (servicing a FetchRequest).
func (o *Orderer) TryGetBody(id types.TxnId) (types.Txn, bool) {
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.spec.Find(id)
	if e == nil {
		e = s.early.Find(id)
	}
	if e == nil {
		e = s.late.At(id)
	}
	if e == nil {
		return types.Txn{}, false
	}
	return e.Txn.Clone(), true
}

// --- inspection (locked snapshots) ---

func (o *Orderer) HighestReleased() types.OrderKey {
	var m types.OrderKey
	for _, s := range o.shards {
		s.mu.Lock()
		if m.Less(s.highestReleased) {
			m = s.highestReleased
		}
		s.mu.Unlock()
	}
	return m
}

func (o *Orderer) sum(f func(s *shard) int) int {
	n := 0
	for _, s := range o.shards {
		s.mu.Lock()
		n += f(s)
		s.mu.Unlock()
	}
	return n
}

func (o *Orderer) EarlySize() int   { return o.sum(func(s *shard) int { return s.early.Len() }) }
func (o *Orderer) LateSize() int    { return o.sum(func(s *shard) int { return s.late.Len() }) }
func (o *Orderer) SpecLogSize() int { return o.sum(func(s *shard) int { return s.spec.Len() }) }
func (o *Orderer) ExecutedCount() int {
	return o.sum(func(s *shard) int { return s.executed.Len() })
}

func (o *Orderer) SpecLogContains(id types.TxnId) bool {
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spec.Contains(id)
}

// SpecLogEntry: status + live t of a specLog entry (ok=false if absent).
func (o *Orderer) SpecLogEntry(id types.TxnId) (st types.Status, t uint64, ok bool) {
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.spec.Find(id); e != nil {
		return e.Status, e.T, true
	}
	return 0, 0, false
}

func (o *Orderer) Executed(id types.TxnId) bool {
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.executed.Get(id)
	return ok
}

// ReserveExecuted pre-sizes the executed maps (DYNA_PRERESERVE) so they never rehash
// mid-run under a lane lock.
func (o *Orderer) ReserveExecuted(n int) {
	per := n
	if k := len(o.shards); k > 1 {
		per = n/k + n/(4*k)
	}
	for _, s := range o.shards {
		s.mu.Lock()
		if s.executed.Len() == 0 {
			s.executed.Reserve(per)
		}
		s.mu.Unlock()
	}
}

func (o *Orderer) ViewID() uint64 {
	s := o.shards[0]
	s.mu.Lock()
	defer s.mu.Unlock()
	return o.viewID
}

func (o *Orderer) Topology() topology.Topology {
	s := o.shards[0]
	s.mu.Lock()
	defer s.mu.Unlock()
	return o.topo
}

// LiveState: live (t, leadership) of a txn in specLog or already executed — answers a
// client's resend with current state rather than a cached reply. ok=false if neither.
func (o *Orderer) LiveState(id types.TxnId) (t uint64, isLeader bool, ok bool) {
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if x, found := s.executed.Get(id); found {
		return x.t, x.isLeader, true
	}
	if e := s.spec.Find(id); e != nil {
		return e.T, e.IsLeaderForThis, true
	}
	return 0, false, false
}
