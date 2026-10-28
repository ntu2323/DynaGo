package orderer

import (
	"slices"

	"dynago/internal/types"
)

// --- background timestamp agreement (SPECULATIVE → COMMITTING) ----------------------

// TsAgreeAction: one TsAgree to send — our live t, to each leader in To. A reply answers
// a peer's proposal and is never itself answered (no ping-pong).
type TsAgreeAction struct {
	Id        types.TxnId
	ProposedT uint64
	To        types.ServerSet
	IsReply   bool
}

// TsRecordResult is the outcome of a received TsAgree.
type TsRecordResult struct {
	Transitioned bool           // became COMMITTING
	Entry        types.TxnEntry // reply copy of the post-transition entry
	HasAction    bool           // we owe the sender our proposal
	Action       TsAgreeAction
}

// ReplicateAction: one ReplicatePrepare to broadcast (agreed order + body). Body shares
// the entry's immutable slices.
type ReplicateAction struct {
	Id      types.TxnId
	AgreedT uint64
	Body    types.Txn
}

// ReackAction: a non-primary re-acks a stale COMMITTING txn to its primary, which
// answers with ReplicateCommit.
type ReackAction struct {
	Id      types.TxnId
	Primary uint32
}

type PrepareReplyResult int

const (
	PrepNone      PrepareReplyResult = iota
	PrepCommitted                    // this ack reached f: broadcast ReplicateCommit
	PrepResend                       // already COMMITTED/executed: re-send the commit to the acker
)

// tryCompleteTsLocked moves a SPECULATIVE entry we lead to COMMITTING at the max
// proposal once every involved leader has proposed.
func (o *Orderer) tryCompleteTsLocked(s *shard, id types.TxnId, out *types.TxnEntry) bool {
	e := s.spec.Find(id)
	if e == nil || !e.IsLeaderForThis || e.Status != types.Speculative {
		return false
	}
	for _, l := range o.topo.InvolvedLeaders(&e.Txn).Members() {
		if _, ok := e.TsProposals[l]; !ok {
			return false
		}
	}
	var mx uint64
	for _, t := range e.TsProposals {
		mx = max(mx, t)
	}
	e.T = mx
	e.Status = types.Committing
	e.CommittingUS = o.clk.Now()
	o.bumpHighestLocked(s, e.Order())
	if out != nil {
		*out = replyCopy(e)
	}
	s.spec.UpdateKey(id)
	return true
}

func (o *Orderer) propose(e *types.TxnEntry, from uint32, t uint64) {
	if e.TsProposals == nil {
		e.TsProposals = make(map[uint32]uint64, 2)
	}
	e.TsProposals[from] = t
}

type scanCand struct {
	ok types.OrderKey
	id types.TxnId
}

// scanChunk bounds how many specLog entries a scan inspects per lane-lock hold. After a
// failure the specLog can hold tens of thousands of entries; walking a whole lane under
// its lock every tick (three scans per tick) stalled the lane goroutines and fed a
// spiral (fewer commits → a bigger specLog → longer scans).
const scanChunk = 512

// scanBudgeted runs act (lane lock held) on specLog entries satisfying want, at most
// budget of them (0 = all), smallest OrderKey first: execution runs in OrderKey order,
// so after a view change the oldest entries are the ones blocking it. Candidates are
// collected scanChunk entries per lane-lock hold (the heap may change between chunks,
// so an entry can be missed this tick or seen twice; duplicates are dropped) and
// re-checked under the lane lock before acting.
func (o *Orderer) scanBudgeted(budget int, want func(e *types.TxnEntry, now uint64) bool,
	act func(s *shard, id types.TxnId, e *types.TxnEntry, now uint64)) {
	var cand []scanCand
	for _, s := range o.shards {
		for i := 0; ; {
			s.mu.Lock()
			n := s.spec.Len()
			if i >= n {
				s.mu.Unlock()
				break
			}
			end := min(n, i+scanChunk)
			now := o.clk.Now()
			for ; i < end; i++ {
				id, e := s.spec.At(i)
				if want(e, now) {
					cand = append(cand, scanCand{e.Order(), id})
				}
			}
			s.mu.Unlock()
		}
	}
	if len(cand) == 0 {
		return
	}
	slices.SortFunc(cand, func(a, b scanCand) int {
		if a.ok.Less(b.ok) {
			return -1
		}
		if b.ok.Less(a.ok) {
			return 1
		}
		return 0
	})
	cand = slices.CompactFunc(cand, func(a, b scanCand) bool { return a.id == b.id })
	if budget > 0 && len(cand) > budget {
		cand = cand[:budget]
	}
	byLane := make([][]types.TxnId, len(o.shards))
	for _, c := range cand {
		l := o.LaneOf(c.id)
		byLane[l] = append(byLane[l], c.id)
	}
	for l, ids := range byLane {
		if len(ids) == 0 {
			continue
		}
		s := o.shards[l]
		s.mu.Lock()
		now := o.clk.Now()
		for _, id := range ids {
			if e := s.spec.Find(id); e != nil && want(e, now) {
				act(s, id, e, now)
			}
		}
		s.mu.Unlock()
	}
}

// DriveTsAgreement: for SPECULATIVE txns we lead that are stuck ≥ timeout (or marked
// SyncNow after a view change), record our own proposal and (re)send it to every
// involved leader we still lack — re-sent each scan until answered. A txn whose only
// involved leader is us becomes COMMITTING at once and is returned in committed.
func (o *Orderer) DriveTsAgreement(timeoutUS uint64, budget int) (actions []TsAgreeAction, committed []types.TxnEntry) {
	o.scanBudgeted(budget,
		func(e *types.TxnEntry, now uint64) bool {
			return e.Status == types.Speculative && e.IsLeaderForThis &&
				(e.SyncNow || now >= e.ReleasedUS+timeoutUS)
		},
		func(s *shard, id types.TxnId, e *types.TxnEntry, _ uint64) {
			o.propose(e, o.serverID, e.T)
			var done types.TxnEntry
			if o.tryCompleteTsLocked(s, id, &done) {
				committed = append(committed, done)
				return
			}
			var to types.ServerSet
			for _, l := range o.topo.InvolvedLeaders(&e.Txn).Members() {
				if _, ok := e.TsProposals[l]; !ok && l != o.serverID {
					to.Add(l)
				}
			}
			if to != 0 {
				actions = append(actions, TsAgreeAction{Id: id, ProposedT: e.T, To: to})
			}
		})
	return actions, committed
}

// RecordTsProposal handles a TsAgree from another involved leader: records it and
// advances our agreement. Unless it was itself a reply, we answer the sender with our
// own t whatever our status (executed included), so a peer stuck SPECULATIVE converges
// even when we already committed via the client.
func (o *Orderer) RecordTsProposal(id types.TxnId, from uint32, proposedT uint64, isReply bool) TsRecordResult {
	var r TsRecordResult
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	var ourT uint64
	have := false
	if x, ok := s.executed.Get(id); ok {
		ourT, have = x.t, true // final t is authoritative
	} else if e := s.spec.Find(id); e != nil {
		if e.IsLeaderForThis {
			o.propose(e, from, proposedT)
			ourT, have = e.T, true
			if e.Status == types.Speculative {
				o.propose(e, o.serverID, e.T)
				r.Transitioned = o.tryCompleteTsLocked(s, id, &r.Entry)
			}
		}
	} else if e := s.early.Find(id); e != nil && e.IsLeaderForThis {
		// Not released yet (our clock lags): keep the proposal on the entry (it moves to
		// specLog with it) and answer with the t it will release at.
		o.propose(e, from, proposedT)
		ourT, have = e.T, true
	}
	// Absent here: drop; the sender re-sends on its next scan.
	if have && !isReply {
		var to types.ServerSet
		to.Add(from)
		r.HasAction = true
		r.Action = TsAgreeAction{Id: id, ProposedT: ourT, To: to, IsReply: true}
	}
	return r
}

// --- replication (COMMITTING → COMMITTED) ----------------------------------------

// installCommittingLocked moves/materializes the txn into specLog at agreedT as
// COMMITTING (never downgrading COMMITTED).
func (o *Orderer) installCommittingLocked(s *shard, id types.TxnId, agreedT uint64, body *types.Txn) {
	now := o.clk.Now()
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
		return
	}
	e := s.early.Erase(id)
	if e == nil {
		if e = s.late.At(id); e != nil {
			s.late.Delete(id)
		}
	}
	if e == nil {
		e = types.NewEntry(*body) // materialize from the leader's body
		o.computeLeadership(e)
	}
	e.T = agreedT
	e.Status = types.Committing
	e.CommittingUS = now
	o.bumpHighestLocked(s, e.Order())
	s.spec.Push(id, e)
}

// DriveReplication: for COMMITTING txns we are the PRIMARY (min-id involved) leader of,
// emit a ReplicatePrepare when the last broadcast is older than resendUS (0 = always).
func (o *Orderer) DriveReplication(resendUS uint64, budget int) (prepares []ReplicateAction) {
	o.scanBudgeted(budget,
		func(e *types.TxnEntry, now uint64) bool {
			return e.Status == types.Committing &&
				(e.LastReplicateUS == 0 || now >= e.LastReplicateUS+resendUS) &&
				e.Primary == o.serverID
		},
		func(_ *shard, id types.TxnId, e *types.TxnEntry, now uint64) {
			e.LastReplicateUS = max(now, 1)
			prepares = append(prepares, ReplicateAction{id, e.T, e.Txn})
		})
	return prepares
}

// TakeReplicateNow: right after a txn became COMMITTING, the primary broadcasts its
// first prepare immediately instead of waiting for the scan.
func (o *Orderer) TakeReplicateNow(id types.TxnId) (ReplicateAction, bool) {
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.spec.Find(id)
	if e == nil || e.Status != types.Committing || e.LastReplicateUS != 0 || e.Primary != o.serverID {
		return ReplicateAction{}, false
	}
	e.LastReplicateUS = max(o.clk.Now(), 1)
	return ReplicateAction{id, e.T, e.Txn}, true
}

// RecordPrepareReply (primary): a ReplicatePrepareReply from `from`.
func (o *Orderer) RecordPrepareReply(id types.TxnId, from uint32) PrepareReplyResult {
	if from == o.serverID {
		return PrepNone
	}
	s := o.shardOf(id)
	s.mu.Lock()
	if _, ok := s.executed.Get(id); ok {
		s.mu.Unlock()
		return PrepResend // we only execute COMMITTED txns: `from` missed the commit
	}
	e := s.spec.Find(id)
	if e == nil || e.Primary != o.serverID {
		s.mu.Unlock()
		return PrepNone
	}
	if e.Status == types.Committed {
		s.mu.Unlock()
		return PrepResend
	}
	e.PrepareReplies.Add(from)
	res := PrepNone
	if e.Status == types.Committing && uint32(e.PrepareReplies.Count()) >= o.quorumF {
		e.Status = types.Committed
		res = PrepCommitted
	}
	s.mu.Unlock()
	if res == PrepCommitted {
		o.KickExecutor()
	}
	return res
}

// HandleReplicatePrepare (non-primary): adopt the txn at agreedT as COMMITTING,
// materializing it from body if absent. True if we now hold it — or executed it — (ack).
func (o *Orderer) HandleReplicatePrepare(id types.TxnId, agreedT uint64, body *types.Txn) bool {
	s := o.shardOf(id)
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.executed.Get(id); ok {
		return true // ack; never re-install
	}
	o.installCommittingLocked(s, id, agreedT, body)
	return s.spec.Contains(id)
}

// MarkCommitted on a ReplicateCommit from the primary.
func (o *Orderer) MarkCommitted(id types.TxnId) {
	s := o.shardOf(id)
	s.mu.Lock()
	kick := false
	if e := s.spec.Find(id); e != nil && e.Status != types.Committed {
		e.Status = types.Committed
		kick = true
	}
	s.mu.Unlock()
	if kick {
		o.KickExecutor()
	}
}

// CollectStaleCommitting (non-primary): COMMITTING entries whose ReplicateCommit hasn't
// arrived within timeout (each re-checked at most once per timeout).
func (o *Orderer) CollectStaleCommitting(timeoutUS uint64, budget int) (out []ReackAction) {
	o.scanBudgeted(budget,
		func(e *types.TxnEntry, now uint64) bool {
			return e.Status == types.Committing && now >= e.CommittingUS+timeoutUS &&
				(e.LastReplicateUS == 0 || now >= e.LastReplicateUS+timeoutUS) &&
				e.Primary != o.serverID
		},
		func(_ *shard, id types.TxnId, e *types.TxnEntry, now uint64) {
			e.LastReplicateUS = max(now, 1)
			if e.Primary != types.NoServer {
				out = append(out, ReackAction{id, e.Primary})
			}
		})
	return out
}
