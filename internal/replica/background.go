package replica

import (
	"dynago/internal/orderer"
	"dynago/internal/types"
	"dynago/internal/vr"
	"dynago/internal/wire"
	"time"
)

// Tick is the background scan (DYNA_TS_SCAN_MS): timestamp agreement for stuck
// SPECULATIVE txns, prepare re-broadcast, stale-commit re-acks; and an executor kick as
// a backstop. Scans take one lane lock at a time.
//
// The scans (O(specLog)) run WITHOUT stateMu (the orderer locks each lane itself, and
// InstallView / Reset take every lane lock); only sending takes stateMu (shared), and
// results computed in a view that has since changed are dropped.
func (r *Replica) Tick() {
	if r.stats != nil {
		t0 := time.Now()
		defer func() { maxStore(&r.stats.maxTickNS, uint64(time.Since(t0))) }()
	}
	if r.paused.Load() {
		return
	}
	view := r.viewID.Load()
	sameView := func() bool { return r.vc.Phase() == vr.Normal && r.viewID.Load() == view }

	actions, committed := r.ord.DriveTsAgreement(r.tsTimeoutUS, r.scanBudget)
	r.stateMu.RLock()
	if sameView() {
		for i := range actions {
			r.sendTsAgree(&actions[i])
		}
		for i := range committed {
			r.leaderSyncDone(&committed[i])
			r.replicateNowIfPrimary(committed[i].Key())
		}
	}
	r.stateMu.RUnlock()

	// The first prepare normally goes out at once (replicateNowIfPrimary); this covers
	// lost prepares, throttled to once per ts timeout.
	prepares := r.ord.DriveReplication(r.tsTimeoutUS, r.scanBudget)
	r.stateMu.RLock()
	if sameView() {
		for _, p := range prepares {
			r.sendReplicatePrepare(p)
		}
	}
	r.stateMu.RUnlock()

	// COMMITTING without a ReplicateCommit: re-ack to the primary, which answers with
	// ReplicateCommit (a lost commit can't block execution).
	reacks := r.ord.CollectStaleCommitting(r.tsTimeoutUS, r.scanBudget)
	r.stateMu.RLock()
	if sameView() {
		for _, a := range reacks {
			r.bgPrepareReply(a.Primary, &wire.ReplicatePrepareReply{ClientID: a.Id.ClientID,
				RequestID: a.Id.RequestID, ViewID: view, ServerID: r.idx})
		}
	}
	r.stateMu.RUnlock()
	r.ord.KickExecutor()
}

// runTsAgreement is one ts-agreement pass (used right after a view install to
// leader-sync inherited SPECULATIVE txns). Requires stateMu.
func (r *Replica) runTsAgreement() {
	actions, committed := r.ord.DriveTsAgreement(r.tsTimeoutUS, r.scanBudget)
	for i := range actions {
		r.sendTsAgree(&actions[i])
	}
	for i := range committed {
		r.leaderSyncDone(&committed[i])
		r.replicateNowIfPrimary(committed[i].Key())
	}
}

// replicateNowIfPrimary: after a txn became COMMITTING here, the primary leader
// broadcasts its first ReplicatePrepare immediately.
func (r *Replica) replicateNowIfPrimary(id types.TxnId) {
	if a, ok := r.ord.TakeReplicateNow(id); ok {
		r.sendReplicatePrepare(a)
	}
}

func (r *Replica) sendReplicatePrepare(a orderer.ReplicateAction) {
	body := a.Body
	r.bgPrepare(&wire.ReplicatePrepare{ClientID: a.Id.ClientID, RequestID: a.Id.RequestID,
		ViewID: r.viewID.Load(), LeaderID: r.idx, AgreedT: a.AgreedT, Txn: &body})
}

func (r *Replica) handleReplicatePrepare(m *wire.ReplicatePrepare) {
	if m.Txn == nil {
		return // the leader always includes the body
	}
	if !r.ord.HandleReplicatePrepare(m.Id(), m.AgreedT, m.Txn) {
		return
	}
	r.bgPrepareReply(m.LeaderID, &wire.ReplicatePrepareReply{ClientID: m.ClientID,
		RequestID: m.RequestID, ViewID: r.viewID.Load(), ServerID: r.idx})
}

func (r *Replica) handlePrepareReply(m *wire.ReplicatePrepareReply) {
	res := r.ord.RecordPrepareReply(m.Id(), m.ServerID)
	if res == orderer.PrepNone {
		return
	}
	rc := &wire.ReplicateCommit{ClientID: m.ClientID, RequestID: m.RequestID,
		ViewID: r.viewID.Load(), LeaderID: r.idx}
	if res == orderer.PrepCommitted {
		r.bgCommit(-1, rc) // reached f acks → COMMITTED; tell everyone
	} else {
		r.bgCommit(int(m.ServerID), rc) // the acker missed our ReplicateCommit
	}
}

func (r *Replica) handleTsAgree(m *wire.TsAgree) {
	res := r.ord.RecordTsProposal(m.Id(), m.SenderID, m.ProposedT, m.Reply)
	if res.HasAction {
		r.sendTsAgree(&res.Action)
	}
	if res.Transitioned {
		r.leaderSyncDone(&res.Entry)
		r.replicateNowIfPrimary(m.Id())
	}
}

func (r *Replica) sendTsAgree(a *orderer.TsAgreeAction) {
	for _, l := range a.To.Members() {
		r.bgTsAgree(l, &wire.TsAgree{ClientID: a.Id.ClientID, RequestID: a.Id.RequestID,
			ViewID: r.viewID.Load(), SenderID: r.idx, ProposedT: a.ProposedT, Reply: a.IsReply})
	}
}

// leaderSyncDone: leader sync moved a txn SPECULATIVE → COMMITTING. The client may
// still be waiting (design doc § Recovery cases 1-3): send it a CommitReply, preceded by
// ViewInfo when the txn predates the current view.
func (r *Replica) leaderSyncDone(e *types.TxnEntry) {
	if e.Txn.Type != types.Write {
		return
	}
	addr := e.ClientAddr
	start := r.ord.LaneOf(e.Key())
	for k := 0; !addr.IsValid() && k < len(r.lanes); k++ {
		l := r.lanes[(start+k)%len(r.lanes)]
		l.dedupMu.Lock()
		addr = l.clientAddrs[e.Txn.ClientID]
		l.dedupMu.Unlock()
	}
	if !addr.IsValid() {
		return // never heard from this client: its own resend reaches the cache
	}
	if e.Txn.ViewID < r.viewID.Load() {
		r.sendViewInfo(addr)
	}
	r.sendCommitReply(addr, e.Key(), orderer.CommitBrief{T: e.T, Status: e.Status, IsLeader: e.IsLeaderForThis})
}

// --- background batching (DYNA_REPL_BATCH > 1): per lane, per destination ---

func (r *Replica) bgLane(clientID, requestID uint64) *lane {
	return r.laneFor(types.ID(clientID, requestID))
}

func (r *Replica) bgPrepare(m *wire.ReplicatePrepare) {
	l := r.bgLane(m.ClientID, m.RequestID)
	if l.bg == nil {
		mc := *m // the copy escapes, not the caller's message
		r.tr.Broadcast(&mc)
		return
	}
	for i := 0; i < r.n; i++ {
		if uint32(i) != r.idx && l.bg.AddPrepare(uint32(i), m) {
			r.flushBgTo(l, uint32(i))
		}
	}
}

func (r *Replica) bgPrepareReply(to uint32, m *wire.ReplicatePrepareReply) {
	if to == r.idx {
		return
	}
	l := r.bgLane(m.ClientID, m.RequestID)
	if l.bg == nil {
		mc := *m
		r.tr.SendReplica(int(to), &mc)
		return
	}
	if l.bg.AddPrepareReply(to, m) {
		r.flushBgTo(l, to)
	}
}

// bgCommit: to = -1 means every other replica.
func (r *Replica) bgCommit(to int, m *wire.ReplicateCommit) {
	l := r.bgLane(m.ClientID, m.RequestID)
	if l.bg == nil {
		mc := *m
		if to < 0 {
			r.tr.Broadcast(&mc)
		} else {
			r.tr.SendReplica(to, &mc)
		}
		return
	}
	for i := 0; i < r.n; i++ {
		if uint32(i) == r.idx || (to >= 0 && i != to) {
			continue
		}
		if l.bg.AddCommit(uint32(i), m) {
			r.flushBgTo(l, uint32(i))
		}
	}
}

func (r *Replica) bgTsAgree(to uint32, m *wire.TsAgree) {
	if to == r.idx {
		return
	}
	l := r.bgLane(m.ClientID, m.RequestID)
	if l.bg == nil {
		mc := *m
		r.tr.SendReplica(int(to), &mc)
		return
	}
	if l.bg.AddTsAgree(to, m) {
		r.flushBgTo(l, to)
	}
}

// flushBgTo sends a full batch now (through the lane's sender in pipeline mode).
func (r *Replica) flushBgTo(l *lane, to uint32) {
	if b, _, ok := l.bg.Take(to); ok && int(to) != int(r.idx) {
		b.SenderID = r.idx
		r.sendVia(l.idx, r.tr.ReplicaAddr(int(to)), &b)
	}
}

// FlushBg sends every non-empty background batch (timer, DYNA_REPL_FLUSH_US).
func (r *Replica) FlushBg() {
	for _, l := range r.lanes {
		if l.bg == nil {
			continue
		}
		for _, d := range l.bg.TakeAll() {
			d.Batch.SenderID = r.idx
			r.tr.SendReplica(int(d.To), &d.Batch)
		}
	}
}

func (r *Replica) handleBgBatch(m *wire.BgBatch, view uint64) {
	// Prepares before commits: a commit in this batch may be for a prepare in it.
	for i := range m.Prepares {
		if m.Prepares[i].ViewID == view {
			r.handleReplicatePrepare(&m.Prepares[i])
		}
	}
	for i := range m.PrepareReplies {
		if m.PrepareReplies[i].ViewID == view {
			r.handlePrepareReply(&m.PrepareReplies[i])
		}
	}
	for i := range m.Commits {
		if m.Commits[i].ViewID == view {
			r.ord.MarkCommitted(m.Commits[i].Id())
		}
	}
	for i := range m.TsAgrees {
		if m.TsAgrees[i].ViewID == view {
			r.handleTsAgree(&m.TsAgrees[i])
		}
	}
}
