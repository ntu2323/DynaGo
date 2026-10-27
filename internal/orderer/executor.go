package orderer

import (
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"dynago/internal/clock"
	"dynago/internal/store"
	"dynago/internal/types"
)

// executor is the store-owning goroutine: it applies COMMITTED writes and serves leader
// reads in dependency order (ExecuteReady), woken on commit / leader-read events
// (KickExecutor, coalesced). Only it touches the store in a running replica.
type executor struct {
	runMu sync.Mutex // serializes passes and every store access (C++ exec_run_mu_)

	mu      sync.Mutex
	cond    *sync.Cond
	pending bool
	jobs    []func()
	running bool
	kicked  atomic.Bool
	done    chan struct{}

	minIntervalUS uint64        // DYNA_EXEC_MIN_INTERVAL_US
	lastSnapUS    atomic.Uint64 // last pass's all-lanes hold

	// Store-side state (under runMu). writtenKeys: every key applied here (recovery
	// enumerates delta-p from it; the store has no usable scan). appliedOK: per key, the
	// OrderKey of the value in the store — installed by recovery ({T: t}) or, with the
	// t-guard, by execution; an older write executed later must not overwrite it.
	writtenKeys map[string]struct{}
	appliedOK   map[string]types.OrderKey
	// tGuard (DYNA_EXEC_T_GUARD, default on): execution never overwrites a key's value
	// with an older write. A replica can execute a COMMITTED write before a smaller
	// conflicting one it hasn't released yet (late at a follower → lateBuffer, or due in
	// a lane that hasn't run): the design's dependency rule only sees the specLog. Writes
	// are blind, so skipping the older value is what in-order execution would have left.
	tGuard bool

	// DYNA_STATS (skippedOlderN: writes not applied, an executed / recovered value was
	// newer)
	passes, writes, passUS, maxSnapUS, skippedOlderN atomic.Uint64

	blockedW, blockedR map[string]struct{} // per-pass scratch
	snap               []execSnap
}

type execSnap struct {
	ok         types.OrderKey
	e          *types.TxnEntry
	executable bool // COMMITTED write, or a read
	leader     bool
}

func (x *executor) init() {
	x.cond = sync.NewCond(&x.mu)
	x.writtenKeys = make(map[string]struct{})
	x.appliedOK = make(map[string]types.OrderKey)
	x.tGuard = os.Getenv("DYNA_EXEC_T_GUARD") != "0"
	x.blockedW = make(map[string]struct{})
	x.blockedR = make(map[string]struct{})
}

// SetExecMinInterval spaces executor passes (each takes every lane lock for its
// snapshot). Call before Start.
func (o *Orderer) SetExecMinInterval(us uint64) { o.exec.minIntervalUS = us }

func (x *executor) start(o *Orderer) {
	x.mu.Lock()
	x.running = true
	x.done = make(chan struct{})
	x.mu.Unlock()
	go x.loop(o)
}

func (x *executor) stop() {
	x.mu.Lock()
	if !x.running {
		x.mu.Unlock()
		return
	}
	x.running = false
	x.mu.Unlock()
	x.cond.Broadcast()
	<-x.done
}

func (x *executor) loop(o *Orderer) {
	defer close(x.done)
	var lastPass time.Time
	for {
		x.mu.Lock()
		for !x.pending && len(x.jobs) == 0 && x.running {
			x.cond.Wait()
		}
		jobs := x.jobs
		x.jobs = nil
		if !x.running && len(jobs) == 0 {
			x.mu.Unlock()
			return
		}
		x.pending = false
		// Re-arm the lock-free kick BEFORE the pass, so a commit landing during it
		// schedules one more.
		x.kicked.Store(false)
		running := x.running
		x.mu.Unlock()
		for _, j := range jobs {
			j()
		}
		if !running {
			return
		}
		if x.minIntervalUS > 0 && len(jobs) == 0 {
			// Let commits accumulate: one pass per interval at most, and keep the
			// all-lanes snapshot to ~10% of the time when the specLog is large.
			iv := time.Duration(max(x.minIntervalUS, 10*x.lastSnapUS.Load())) * time.Microsecond
			if d := time.Until(lastPass.Add(iv)); d > 0 {
				time.Sleep(d)
			}
			lastPass = time.Now()
		}
		o.ExecuteReady()
	}
}

// KickExecutor wakes the executor (coalesced; lock-free when one is already pending).
func (o *Orderer) KickExecutor() {
	x := &o.exec
	if x.kicked.Swap(true) {
		return
	}
	x.mu.Lock()
	x.pending = true
	x.mu.Unlock()
	x.cond.Signal()
}

// RunOnStoreThread runs fn on the store-owning goroutine (inline when the executor isn't
// running), serialized with ExecuteReady; blocks until it ran.
func (o *Orderer) RunOnStoreThread(fn func()) {
	x := &o.exec
	x.mu.Lock()
	if !x.running {
		x.mu.Unlock()
		x.runMu.Lock()
		fn()
		x.runMu.Unlock()
		return
	}
	done := make(chan struct{})
	x.jobs = append(x.jobs, func() {
		x.runMu.Lock()
		fn()
		x.runMu.Unlock()
		close(done)
	})
	x.mu.Unlock()
	x.cond.Signal()
	<-done
}

// ExecuteReady is one dependency-ordered execution pass: apply every COMMITTED write
// with no smaller un-executed conflicting txn (r-w / w-w / w-r) to the store with its
// commit t, serve leader reads whose conflicting smaller writes executed, and GC both
// from specLog. Returns the executed write ids in OrderKey order. Passes never
// interleave.
func (o *Orderer) ExecuteReady() []types.TxnId {
	x := &o.exec
	x.runMu.Lock()
	defer x.runMu.Unlock()

	// 1. A consistent snapshot of every lane (conflicting txns can live in different
	//    lanes): all lane locks, but only to copy each entry's order + a pointer. The
	//    pointer stays valid: only this (serialized) pass or Reset removes entries, and
	//    the body is never modified in place.
	snap := x.snap[:0]
	candidates := 0
	o.lockAll()
	t0 := clock.WallMicros()
	for _, s := range o.shards {
		s.spec.ForEach(func(_ types.TxnId, e *types.TxnEntry) {
			ex := e.Txn.Type != types.Write || e.Status == types.Committed
			if ex {
				candidates++
			}
			snap = append(snap, execSnap{e.Order(), e, ex, e.IsLeaderForThis})
		})
	}
	held := clock.WallMicros() - t0
	x.lastSnapUS.Store(held)
	o.unlockAll()
	if held > x.maxSnapUS.Load() {
		x.maxSnapUS.Store(held)
	}
	x.passes.Add(1)
	defer func() { x.passUS.Add(clock.WallMicros() - t0) }()
	defer func() { clear(snap); x.snap = snap[:0] }()
	if candidates == 0 {
		return nil
	}

	// 2. Decide with no lock held. One ascending pass = the transitive dependency rule:
	//    a non-executable txn blocks larger conflicting ones via blocked keys.
	slices.SortFunc(snap, func(a, b execSnap) int {
		if a.ok.Less(b.ok) {
			return -1
		}
		if b.ok.Less(a.ok) {
			return 1
		}
		return 0
	})
	clear(x.blockedW)
	clear(x.blockedR)
	type pick struct {
		sn  *execSnap
		run bool
	}
	var picked []pick
	for i := range snap {
		if candidates == 0 {
			break // nothing executable beyond here
		}
		sn := &snap[i]
		if sn.executable {
			candidates--
		}
		conflict := false
		for _, kv := range sn.e.Txn.WSet { // w-w or w-r
			if _, b := x.blockedW[kv.Key]; b {
				conflict = true
				break
			}
			if _, b := x.blockedR[kv.Key]; b {
				conflict = true
				break
			}
		}
		if !conflict {
			for _, k := range sn.e.Txn.RSet { // r-w
				if _, b := x.blockedW[k]; b {
					conflict = true
					break
				}
			}
		}
		switch {
		case sn.executable && !conflict && sn.e.Txn.Type == types.Write:
			picked = append(picked, pick{sn, true})
		case sn.executable && !conflict:
			// A ready READ: served if we lead it; GC'd either way so it never blocks a
			// later conflicting write.
			picked = append(picked, pick{sn, sn.leader})
		default:
			for _, kv := range sn.e.Txn.WSet {
				x.blockedW[kv.Key] = struct{}{}
			}
			for _, k := range sn.e.Txn.RSet {
				x.blockedR[k] = struct{}{}
			}
		}
	}
	if len(picked) == 0 {
		return nil
	}

	// 3. Per lane: take the picked entries out of specLog and record executed writes.
	//    A picked entry can't have changed meanwhile (a COMMITTED write is final; reads
	//    aren't re-timestamped).
	byLane := make([][]*execSnap, len(o.shards))
	for _, p := range picked {
		l := o.LaneOf(p.sn.e.Key())
		byLane[l] = append(byLane[l], p.sn)
	}
	for l, sns := range byLane {
		if len(sns) == 0 {
			continue
		}
		s := o.shards[l]
		s.mu.Lock()
		for _, sn := range sns {
			id := sn.e.Key()
			if sn.e.Txn.Type == types.Write {
				s.executed.Put(id, executedInfo{sn.ok.T, sn.leader})
			}
			s.spec.Erase(id)
		}
		s.mu.Unlock()
	}

	// 4. Apply outside the lane locks, in OrderKey order. After commit, so never on the
	//    commit path (invariant 2); no fsync (a committed txn lives on f+1 replicas).
	var executed []types.TxnId
	staged, _ := o.store.(store.Stager)
	defer func() {
		if staged != nil {
			staged.Flush()
		}
	}()
	for _, p := range picked {
		e := p.sn.e
		if e.Txn.Type == types.Write {
			executed = append(executed, e.Key())
			x.writes.Add(1)
		}
		if !p.run {
			continue // a follower's read: just GC'd
		}
		if e.Txn.Type == types.Write {
			if o.store == nil {
				continue
			}
			for _, kv := range e.Txn.WSet {
				// A value installed by recovery (or a newer executed write) may be newer
				// than this write.
				if cur, ok := x.appliedOK[kv.Key]; ok && p.sn.ok.Less(cur) {
					x.skippedOlderN.Add(1)
					continue
				}
				if staged != nil {
					staged.Stage(kv.Key, kv.Value, p.sn.ok.T)
				} else {
					o.store.Apply(kv.Key, kv.Value, p.sn.ok.T)
				}
				x.writtenKeys[kv.Key] = struct{}{}
				if x.tGuard {
					x.appliedOK[kv.Key] = p.sn.ok
				}
			}
			continue
		}
		// Leader READ: read the keys THIS server leads; the client merges results
		// across the involved leaders.
		if staged != nil {
			staged.Flush() // the read must see every smaller write of this pass
		}
		res := make(map[string]string, len(e.LedKeys))
		for _, k := range e.LedKeys {
			v := ""
			if o.store != nil {
				if vv, ok := o.store.Read(k); ok {
					v = vv.Value
				}
			}
			res[k] = v
		}
		e.Results = res
		if o.onReply != nil {
			o.onReply(e)
		}
	}
	return executed
}
