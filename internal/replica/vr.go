package replica

import (
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"dynago/internal/deadline"
	"dynago/internal/recovery"
	"dynago/internal/topology"
	"dynago/internal/types"
	"dynago/internal/vr"
	"dynago/internal/wire"
)

// --- setup ---

func (r *Replica) initVR(o Options) {
	r.hbMS = max(1, deadline.EnvU64("DYNA_HB_MS", 20))
	r.snapChunk = int(max(1, deadline.EnvU64("DYNA_SNAPSHOT_CHUNK", 512)))
	r.vrDir = o.VRStateDir
	if r.vrDir == "" {
		r.vrDir = os.Getenv("DYNA_VR_STATE_DIR")
	}
	if r.vrDir == "" {
		r.vrDir = "."
	}
	r.recCache = map[uint32]*recReplyCache{}
	r.donors = map[uint32]*donorSession{}
	r.bulk.init()
	cfg := vr.Config{
		Self: r.idx, N: uint32(r.n), F: uint32(r.f),
		FailTimeoutUS: deadline.EnvU64("DYNA_FAIL_TIMEOUT_MS", 1000) * 1000,
		GraceUS:       deadline.EnvU64("DYNA_FAIL_GRACE_MS", 10000) * 1000,
		ResendUS:      max(r.tsScanMS, r.hbMS) * 1000,
		Quiet:         o.Quiet,
	}
	r.quiet = o.Quiet
	// Durable VR state: if we were a member before, our in-memory state is gone —
	// rejoin rather than start in view 0.
	if _, err := os.Stat(r.vrStatePath()); err == nil {
		cfg.StartJoining = true
	}
	r.vc = vr.New(cfg, r.clk.Now())
	if cfg.StartJoining {
		r.logf("found %s: restarted, rejoining", r.vrStatePath())
		r.paused.Store(true)
		r.ord.SetPaused(true)
	} else {
		r.persistVrState(0, r.vc.Alive())
	}
}

func (r *Replica) logf(format string, args ...any) {
	if !r.quiet {
		log.Printf("r%d "+format, append([]any{r.idx}, args...)...)
	}
}

// HeartbeatTick (DYNA_HB_MS): heartbeat, VR failure detection + re-sends, joiner
// catch-up re-requests.
func (r *Replica) HeartbeatTick() {
	if r.stats != nil {
		t0 := time.Now()
		defer func() { maxStore(&r.stats.maxHbNS, uint64(time.Since(t0))) }()
	}
	r.tr.Broadcast(&wire.Heartbeat{ViewID: r.viewID.Load(), SenderID: r.idx})
	r.stateMu.Lock()
	defer r.stateMu.Unlock()
	now := r.clk.Now()
	var fx vr.Effects
	r.vc.Tick(now, &fx)
	r.applyEffects(&fx)
	if r.catchup.active && now >= r.catchup.lastReqUS+4*r.tsScanMS*1000 {
		r.sendSnapshotRequest() // a lost snapshot chunk
	}
}

// handleVR requires stateMu (exclusive).
func (r *Replica) handleVR(from Addr, m wire.Message) {
	now := r.clk.Now()
	var fx vr.Effects
	switch m := m.(type) {
	case *wire.Heartbeat:
		r.vc.OnHeard(m.SenderID, now)
		if m.ViewID > r.viewID.Load() && r.vc.Phase() == vr.Normal {
			// We missed a whole view change: ask (the VR leader answers with StartView).
			r.tr.Broadcast(&wire.StartViewChange{ViewID: m.ViewID, SenderID: r.idx, Alive: r.vc.Alive()})
		}
		return
	case *wire.BulkChunk:
		r.handleBulkChunk(from, m)
		return
	case *wire.BulkPoke:
		r.handleBulkPoke(from, m)
		return
	case *wire.BulkAck:
		r.handleBulkAck(m)
		return
	case *wire.StartViewChange:
		r.vc.OnStartViewChange(m, now, &fx)
	case *wire.DoViewChange:
		r.vc.OnDoViewChange(m, now, &fx)
	case *wire.StartRecovery:
		r.vc.OnStartRecovery(m, now, &fx)
	case *wire.RecoveryRequest:
		r.handleRecoveryRequest(m)
		return
	case *wire.RecoveryReply:
		r.vc.OnHeard(m.SenderID, now)
		r.vc.OnRecoveryReply(m, &fx)
	case *wire.RecoveryPrepare:
		r.vc.OnRecoveryPrepare(m, now, &fx)
	case *wire.RecoveryPrepareReply:
		r.vc.OnHeard(m.SenderID, now)
		r.vc.OnRecoveryPrepareReply(m, &fx)
	case *wire.RecoveryReport:
		r.vc.OnHeard(m.SenderID, now)
		r.vc.OnRecoveryReport(m, &fx)
	case *wire.StartView:
		r.vc.OnStartView(m, now, &fx)
	case *wire.JoinRequest:
		r.vc.OnJoinRequest(m, now, &fx)
	case *wire.JoinInfo:
		r.vc.OnJoinInfo(m, now, &fx)
	case *wire.JoinReady:
		r.vc.OnJoinReady(m, now, &fx)
	case *wire.SnapshotRequest:
		r.handleSnapshotRequest(m)
		return
	case *wire.SnapshotChunk:
		r.handleSnapshotChunk(m)
		return
	default:
		return // client-bound messages (ViewInfo, replies) aren't for a replica
	}
	r.applyEffects(&fx)
}

// applyEffects sends the coordinator's outbox and acts on its events.
func (r *Replica) applyEffects(fx *vr.Effects) {
	for _, o := range fx.Out {
		r.sendVr(o.To, o.Msg, true)
	}
	if r.vc.Phase() != vr.Normal && !r.paused.Load() {
		r.paused.Store(true)
		r.ord.SetPaused(true)
		r.logf("view change: pausing (view %d -> %d, alive %#x -> %#x)", r.viewID.Load(),
			r.vc.TargetView(), r.vc.Alive(), r.vc.TargetAlive())
	}
	if fx.Excluded {
		r.logf("left out of a started view: resetting state and rejoining")
		r.resetForRejoin()
	}
	if fx.AdoptView {
		r.adoptViewAsNonMember()
	}
	if fx.FreshStart {
		r.logf("every server is rejoining: starting fresh in view 0")
		r.catchup = catchUp{}
		topo := r.setTopology(r.vc.Alive())
		r.ord.InstallView(topo, 0, nil, nil)
		r.viewID.Store(0)
		r.persistVrState(0, r.vc.Alive())
		r.paused.Store(false)
		r.ord.SetPaused(false)
	}
	if fx.BeginRecovery {
		r.beginRecovery()
	}
	if fx.Install {
		r.installNewView()
	}
}

func (r *Replica) setTopology(alive uint64) topology.Topology {
	r.topoMu.Lock()
	r.topo = r.topo.WithAlive(types.ServerSet(alive))
	t := r.topo
	r.topoMu.Unlock()
	return t
}

// --- recovery ---

// beginRecovery: our own contribution to the M for our delta-p.
func (r *Replica) beginRecovery() {
	cur := r.topology()
	oldT := cur.WithAlive(types.ServerSet(r.vc.OldAlive()))
	newT := cur.WithAlive(types.ServerSet(r.vc.TargetAlive()))
	own := r.ord.CollectRecovery(func(k string) bool { return recovery.InDeltaP(oldT, newT, k, r.idx) })
	r.logf("recovery view %d: own delta-p txns=%d kvs=%d", r.vc.TargetView(), len(own.Txns), len(own.KVs))
	var fx vr.Effects
	r.vc.OnLocalRecoveryData(own, &fx)
	r.applyEffects(&fx)
}

// recReplyCache: a PL re-requests until it has our reply: answer with the same object
// (normal processing is paused, so the answer can't change; collecting it is costly).
type recReplyCache struct {
	view, alive, oldAlive uint64
	msg                   *wire.RecoveryReply
}

func (r *Replica) handleRecoveryRequest(m *wire.RecoveryRequest) {
	now := r.clk.Now()
	r.vc.OnHeard(m.RequesterID, now)
	var fx vr.Effects
	ok := r.vc.AcceptRecoveryView(m.ViewID, m.Alive, m.OldAlive, now, &fx)
	r.applyEffects(&fx) // may begin our own recovery (and pause) first
	if !ok || m.RequesterID == r.idx {
		return
	}
	pl := m.RequesterID
	rc := r.recCache[pl]
	if rc == nil || rc.view != m.ViewID || rc.alive != m.Alive || rc.oldAlive != m.OldAlive {
		cur := r.topology()
		oldT := cur.WithAlive(types.ServerSet(m.OldAlive))
		newT := cur.WithAlive(types.ServerSet(m.Alive))
		d := r.ord.CollectRecovery(func(k string) bool { return recovery.InDeltaP(oldT, newT, k, pl) })
		rc = &recReplyCache{m.ViewID, m.Alive, m.OldAlive,
			&wire.RecoveryReply{ViewID: m.ViewID, SenderID: r.idx, Txns: d.Txns, KVs: d.KVs}}
		r.recCache[pl] = rc
		r.logf("recovery view %d: reply to r%d txns=%d kvs=%d", m.ViewID, pl, len(d.Txns), len(d.KVs))
	}
	r.sendVr(int32(pl), rc.msg, true)
}

func (r *Replica) installNewView() {
	in := r.vc.TakeInstall(r.clk.Now())
	cur := r.topology()
	oldT := cur.WithAlive(types.ServerSet(in.OldAlive))
	newT := cur.WithAlive(types.ServerSet(in.Alive))
	st := r.ord.InstallView(newT, in.View, in.Ms, func(k string) bool { return recovery.InFailedRange(oldT, newT, k) })
	r.setTopology(in.Alive)
	r.viewID.Store(in.View)
	clear(r.recCache) // (megabytes) a later round collects afresh
	r.bulk.clearTx()
	// Resends in the new view must reach the orderer (it dedups by id itself): an id
	// stuck "in flight" here (e.g. late-buffered) would otherwise be dropped.
	for _, l := range r.lanes {
		l.dedupMu.Lock()
		l.inflight.Clear()
		l.dedupMu.Unlock()
	}
	r.pendMu.Lock()
	clear(r.pendingCommits)
	r.pendMu.Unlock()
	r.catchup = catchUp{} // a joiner is a member from here on
	r.persistVrState(in.View, in.Alive)
	r.logf("view %d installed: alive=%#x Ms=%d txns_installed=%d kvs_applied=%d leftovers=%d spec_to_sync=%d",
		in.View, in.Alive, len(in.Ms), st.TxnsInstalled, st.KVsApplied, st.Leftovers, st.SpecToSync)
	r.paused.Store(false)
	r.ord.SetPaused(false)
	// Leader-sync inherited SPECULATIVE txns now and tell their clients.
	r.runTsAgreement()
}

// --- chunked bulk transfer for large VR / recovery messages ---
//
// A message above DYNA_BULK_MIN_BYTES travels as acked BulkChunks of
// DYNA_BULK_CHUNK_BYTES: one huge datagram never arrived whole under load. Chunks are
// kept across resends (each resend round starts at a random chunk: a burst that overruns
// the receiver loses its tail). An acked xfer is re-sent as a small poke, and the
// receiver replays the answers its dispatch produced instead of re-parsing megabytes.

const (
	bulkMaxXfers       = 16
	bulkRedispatchUS   = 20000
	bulkReparseUS      = 200000
	defaultBulkMin     = 8000
	defaultBulkChunk   = 1300
	bulkTxCacheEntries = 4
)

type bulkRx struct {
	inner          wire.MsgType
	from, total    uint32
	have           uint32
	parts          [][]byte
	assembled      []byte
	done           bool
	lastDispatchUS uint64
	lastAnswerUS   uint64
	seq            uint64
	answers        []wire.Message
}

type bulkTx struct {
	msg   wire.Message // the kept object (its pointer is the key)
	id    uint64
	inner wire.MsgType
	bytes []byte
}

type bulkState struct {
	minBytes, chunk int
	dropPct         int
	rx              map[uint64]*bulkRx
	seq             uint64
	capture         *bulkRx // set while dispatching an xfer
	tx              []bulkTx
	acked           map[[2]uint64]bool // (destination, xfer) it holds
}

func (b *bulkState) init() {
	b.minBytes = int(deadline.EnvU64("DYNA_BULK_MIN_BYTES", defaultBulkMin))
	b.chunk = int(max(1, deadline.EnvU64("DYNA_BULK_CHUNK_BYTES", defaultBulkChunk)))
	b.dropPct = int(deadline.EnvU64("DYNA_BULK_TEST_DROP_PCT", 0))
	b.rx = map[uint64]*bulkRx{}
	b.acked = map[[2]uint64]bool{}
}

func (b *bulkState) clearTx() { b.tx = nil }

// sendVr sends a VR / recovery message to replica `to` (or vr.All: the other members of
// the current or target view only — a dead peer gets no megabytes). keep: the same
// object may be re-sent later, so cache its encoded chunks. Requires stateMu.
func (r *Replica) sendVr(to int32, m wire.Message, keep bool) {
	b := &r.bulk
	var tx *bulkTx
	if keep {
		for i := range b.tx {
			if b.tx[i].msg == m {
				t := b.tx[i]
				copy(b.tx[1:i+1], b.tx[:i])
				b.tx[0] = t
				tx = &b.tx[0]
				break
			}
		}
	}
	if tx == nil {
		bytes := wire.Marshal(nil, m)
		if len(bytes) <= b.minBytes {
			if c := b.capture; c != nil && (to == vr.All || to == int32(c.from)) {
				c.answers = append(c.answers, m) // replayed on a later poke
			}
			if to == vr.All {
				r.tr.Broadcast(m)
			} else {
				r.tr.SendReplica(int(to), m)
			}
			return
		}
		h := fnv.New64a()
		h.Write(bytes)
		id := h.Sum64() ^ (uint64(r.idx)+1)*0xD6E8FEB86659FD93
		t := bulkTx{msg: m, id: id, inner: m.Type(), bytes: bytes}
		if keep {
			b.tx = append([]bulkTx{t}, b.tx...)
			if len(b.tx) > bulkTxCacheEntries {
				b.tx = b.tx[:bulkTxCacheEntries]
			}
			tx = &b.tx[0]
		} else {
			tx = &t
		}
	}
	total := uint32((len(tx.bytes) + b.chunk - 1) / b.chunk)
	members := r.vc.Membership()
	for d := 0; d < r.n; d++ {
		if uint32(d) == r.idx {
			continue
		}
		if to == vr.All {
			if members&(1<<d) == 0 {
				continue
			}
		} else if d != int(to) {
			continue
		}
		if b.acked[[2]uint64{uint64(d), tx.id}] {
			r.tr.SendReplica(d, &wire.BulkPoke{SenderID: r.idx, XferID: tx.id})
			continue
		}
		start := uint32(rand.Intn(int(total)))
		for k := uint32(0); k < total; k++ {
			i := (start + k) % total
			if b.dropPct > 0 && rand.Intn(100) < b.dropPct {
				continue
			}
			off := int(i) * b.chunk
			end := min(off+b.chunk, len(tx.bytes))
			r.tr.SendReplica(d, &wire.BulkChunk{SenderID: r.idx, XferID: tx.id, Inner: tx.inner,
				Idx: i, Total: total, Data: tx.bytes[off:end]})
		}
	}
}

func (r *Replica) sendBulkAck(to uint32, id uint64, have bool) {
	r.tr.SendReplica(int(to), &wire.BulkAck{SenderID: r.idx, XferID: id, Have: have})
}

// dispatchBulk handles an assembled xfer as if it arrived whole (stateMu is held),
// capturing the small answers it sends back to the sender.
func (r *Replica) dispatchBulk(from Addr, b *bulkRx) {
	m, err := wire.Unmarshal(b.assembled)
	if err != nil {
		return
	}
	b.answers = nil
	b.lastDispatchUS = r.clk.Now()
	r.bulk.capture = b
	r.handleVR(from, m) // only VR / recovery / snapshot traffic is sent in bulk
	r.bulk.capture = nil
}

// answerBulk: a resend of an xfer we hold — the sender lost our answer. At most once
// per redispatch interval (a resend round arrives as many chunks / pokes).
func (r *Replica) answerBulk(from Addr, id uint64, b *bulkRx) {
	now := r.clk.Now()
	if now < b.lastAnswerUS+bulkRedispatchUS {
		return
	}
	b.lastAnswerUS = now
	r.sendBulkAck(b.from, id, true)
	if len(b.answers) > 0 {
		for _, m := range b.answers {
			r.tr.SendReplica(int(b.from), m)
		}
		return
	}
	// No answer last time (e.g. it arrived before we could accept it): dispatch again,
	// rationed.
	if now >= b.lastDispatchUS+bulkReparseUS {
		r.dispatchBulk(from, b)
	}
}

func (r *Replica) handleBulkChunk(from Addr, c *wire.BulkChunk) {
	if c.Total == 0 || c.Idx >= c.Total || c.Total > 1<<20 {
		return
	}
	bs := &r.bulk
	b := bs.rx[c.XferID]
	if b == nil || b.total != c.Total || b.inner != c.Inner {
		if b == nil && len(bs.rx) >= bulkMaxXfers {
			var oldest uint64
			var oseq uint64 = ^uint64(0)
			for id, x := range bs.rx {
				if x.seq < oseq {
					oldest, oseq = id, x.seq
				}
			}
			delete(bs.rx, oldest)
		}
		bs.seq++
		b = &bulkRx{inner: c.Inner, from: c.SenderID, total: c.Total, parts: make([][]byte, c.Total), seq: bs.seq}
		bs.rx[c.XferID] = b
	}
	if b.done {
		r.answerBulk(from, c.XferID, b)
		return
	}
	if b.parts[c.Idx] != nil || len(c.Data) == 0 {
		return
	}
	b.parts[c.Idx] = c.Data
	if b.have++; b.have < b.total {
		return
	}
	for _, p := range b.parts {
		b.assembled = append(b.assembled, p...)
	}
	b.parts = nil
	b.done = true
	r.sendBulkAck(b.from, c.XferID, true)
	b.lastAnswerUS = r.clk.Now()
	r.dispatchBulk(from, b)
}

func (r *Replica) handleBulkPoke(from Addr, p *wire.BulkPoke) {
	b := r.bulk.rx[p.XferID]
	if b == nil || !b.done {
		r.sendBulkAck(p.SenderID, p.XferID, false) // evicted: send the chunks again
		return
	}
	r.answerBulk(from, p.XferID, b)
}

func (r *Replica) handleBulkAck(a *wire.BulkAck) {
	k := [2]uint64{uint64(a.SenderID), a.XferID}
	if !a.Have {
		delete(r.bulk.acked, k)
		return
	}
	if len(r.bulk.acked) >= 4*bulkMaxXfers {
		clear(r.bulk.acked) // bounded; a resend is fine
	}
	r.bulk.acked[k] = true
}

// --- rejoin ---

func (r *Replica) vrStatePath() string {
	return filepath.Join(r.vrDir, fmt.Sprintf("dyna-vr-%d.state", r.idx))
}

// persistVrState: "disk-based VR" — (view, alive) written temp + fsync + rename, once
// per installed view.
func (r *Replica) persistVrState(view, alive uint64) {
	path := r.vrStatePath()
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		r.logf("cannot write VR state %s: %v", tmp, err)
		return
	}
	_, werr := fmt.Fprintf(f, "%d %d\n", view, alive)
	serr := f.Sync()
	f.Close()
	if werr != nil || serr != nil || os.Rename(tmp, path) != nil {
		r.logf("cannot persist VR state %s", path)
	}
}

type catchUp struct {
	active     bool
	donor      uint32
	view       uint64
	session    uint64
	nextOffset uint64
	lastReqUS  uint64
	kvsApplied int
}

type donorSession struct {
	session uint64
	keys    []string
}

// resetForRejoin: left out of a view — drop ordering state and per-client caches.
func (r *Replica) resetForRejoin() {
	r.ord.Reset()
	for _, l := range r.lanes {
		l.dedupMu.Lock()
		l.replyCache.Clear()
		l.inflight.Clear()
		l.dedupMu.Unlock()
	}
	r.pendMu.Lock()
	clear(r.pendingCommits)
	r.pendMu.Unlock()
	session := r.catchup.session
	r.catchup = catchUp{session: session}
	clear(r.recCache)
}

// adoptViewAsNonMember: JOINING — adopt the coordinator's (view, alive) and (re)start
// catch-up from that view's VR leader (a new session re-enumerates its keys; anything
// already applied is skipped by the t-guard).
func (r *Replica) adoptViewAsNonMember() {
	topo := r.setTopology(r.vc.Alive())
	r.viewID.Store(r.vc.View())
	r.ord.InstallView(topo, r.vc.View(), nil, nil)
	r.catchup.active = true
	r.catchup.donor = vr.LeaderOf(r.vc.Alive())
	r.catchup.view = r.vc.View()
	r.catchup.session++
	r.catchup.nextOffset = 0
	r.catchup.kvsApplied = 0
	r.logf("rejoining: adopted view %d (alive=%#x) as non-member; catching up from r%d",
		r.catchup.view, r.vc.Alive(), r.catchup.donor)
	r.sendSnapshotRequest()
}

func (r *Replica) sendSnapshotRequest() {
	r.catchup.lastReqUS = r.clk.Now()
	r.tr.SendReplica(int(r.catchup.donor), &wire.SnapshotRequest{ViewID: r.catchup.view,
		RequesterID: r.idx, Session: r.catchup.session, Offset: r.catchup.nextOffset})
}

func (r *Replica) handleSnapshotRequest(m *wire.SnapshotRequest) {
	if r.vc.Phase() != vr.Normal || m.ViewID != r.viewID.Load() {
		return // it re-syncs its view
	}
	sess := r.donors[m.RequesterID]
	if sess == nil || sess.session != m.Session {
		sess = &donorSession{session: m.Session, keys: r.ord.SnapshotKeys()}
		r.donors[m.RequesterID] = sess
	}
	off := int(m.Offset)
	c := &wire.SnapshotChunk{ViewID: m.ViewID, SenderID: r.idx, Session: m.Session, Offset: m.Offset,
		Total: uint64(len(sess.keys)), NextOffset: uint64(off + r.snapChunk)}
	c.KVs = r.ord.SnapshotRead(sess.keys, off, r.snapChunk)
	c.Done = off+r.snapChunk >= len(sess.keys)
	if c.Done {
		c.Txns = r.ord.CommittedTxns()
	}
	r.sendVr(int32(m.RequesterID), c, false)
}

func (r *Replica) handleSnapshotChunk(m *wire.SnapshotChunk) {
	cu := &r.catchup
	if !cu.active || m.Session != cu.session || m.Offset != cu.nextOffset || m.ViewID != cu.view {
		return // stale / duplicate
	}
	cu.kvsApplied += r.ord.InstallSnapshot(recovery.Data{Txns: m.Txns, KVs: m.KVs})
	cu.nextOffset = m.NextOffset
	if !m.Done {
		r.sendSnapshotRequest()
		return
	}
	cu.active = false
	r.logf("caught up in view %d: %d keys from r%d (%d applied), %d txns; asking to rejoin",
		cu.view, m.Total, cu.donor, cu.kvsApplied, len(m.Txns))
	var fx vr.Effects
	r.vc.OnCaughtUp(&fx)
	r.applyEffects(&fx)
}
