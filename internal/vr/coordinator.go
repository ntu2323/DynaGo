// Package vr is the silent VR layer + recovery coordination (C++
// dyna/view_coordinator.{h,cc}; design doc § Recovery), as pure logic: inputs are
// messages and the current time, outputs are messages to send and events for the
// replica. No transport, no orderer, so it is testable over a fake network.
//
// A view is (view id, alive set); the VR leader of a view is its min-id alive server.
//
//	NORMAL ─(peer silent > fail timeout, or a higher StartViewChange)─► VIEW_CHANGE
//	  Broadcast StartViewChange(v', alive'); after f matching ones from others send
//	  DoViewChange to the VR leader of v'. The VR leader, with f+1 DoViewChanges
//	  (itself included), broadcasts StartRecovery.
//	VIEW_CHANGE ─(StartRecovery / RecoveryRequest / RecoveryPrepare for v')─► RECOVERING
//	  Every survivor is PL for its delta-p: it collects f+1 RecoveryReplies (own
//	  included), merges them into M, sends RecoveryPrepare(M), and after f+1
//	  RecoveryPrepareReplies (itself included) sends RecoveryReport to the VR leader,
//	  which broadcasts StartView once every alive server reported.
//	RECOVERING ─(StartView)─► NORMAL in v' (the replica installs every buffered M).
//
// Rejoin: a restarted server (Config.StartJoining) and a server left out of a started
// view are JOINING, never a voter until re-added:
//
//	JOINING ─(JoinInfo: current view without us)─► adopt it as a NON-member (AdoptView:
//	  the replica catches up from the VR leader) ─(OnCaughtUp)─► JoinReady to the VR
//	  leader, which starts a normal view change adding us; consistent hashing hands our
//	  home range back and we are its PL in that recovery.
//
// A member that gets a JoinRequest from a server it still counts alive (restarted faster
// than failure detection) first changes view without it. If every other server is
// JOINING too for GraceUS (a whole-cluster restart), start fresh in view 0.
package vr

import (
	"log"
	"math"
	"math/bits"

	"dynago/internal/recovery"
	"dynago/internal/wire"
)

type Phase int

const (
	Normal Phase = iota
	ViewChange
	Recovering
	Joining
)

func (p Phase) String() string {
	return [...]string{"NORMAL", "VIEW_CHANGE", "RECOVERING", "JOINING"}[p]
}

type Config struct {
	Self          uint32
	N, F          uint32
	FailTimeoutUS uint64 // 0 disables failure detection
	GraceUS       uint64 // allowance for a peer never heard from
	ResendUS      uint64 // re-send cadence of pending VR / recovery messages
	StartJoining  bool   // restarted: rejoin instead of view 0
	Quiet         bool   // no log lines
}

// All is the "every other replica" destination.
const All int32 = -1

type Out struct {
	To  int32 // replica id, or All
	Msg wire.Message
}

// Effects is what one input produced.
type Effects struct {
	Out           []Out
	BeginRecovery bool // entered RECOVERING: pause, then OnLocalRecoveryData
	Install       bool // StartView reached: TakeInstall, install, resume
	Excluded      bool // left out of a started view: reset state, rejoin
	AdoptView     bool // JOINING: adopt (View, Alive) as a non-member, catch up from LeaderOf
	FreshStart    bool // whole-cluster restart: NORMAL in view 0, all alive
}

func (fx *Effects) send(to int32, m wire.Message) { fx.Out = append(fx.Out, Out{to, m}) }

// Install is what to install at StartView.
type Install struct {
	View, Alive, OldAlive uint64
	Ms                    []recovery.Data // every M buffered for this view (own included)
}

type lastNormal struct{ view, alive uint64 }

type Coordinator struct {
	cfg      Config
	phase    Phase
	view     uint64 // current (last NORMAL) view
	alive    uint64
	pv, pa   uint64 // target view during VIEW_CHANGE / RECOVERING
	oldAlive uint64 // alive set of the view being replaced

	lastHeard  []uint64
	everHeard  []bool
	lastResend uint64
	lastTick   uint64
	phaseSince uint64

	// VIEW_CHANGE round.
	svcFrom map[uint32]bool
	dvcSent bool
	// VR leader only: sender -> its last NORMAL view and that view's alive set. Recovery
	// starts from the OLDEST (VR: adopt the most-behind participant's state).
	dvcFrom map[uint32]lastNormal

	// RECOVERING round.
	replies       map[uint32]recovery.Data
	mBuilt        bool
	myM           recovery.Data
	prepMsg       *wire.RecoveryPrepare // built once per round; resent as the same object
	prepAcks      map[uint32]bool
	reported      bool
	reports       map[uint32]bool
	ms            map[uint32]recovery.Data
	startViewSent bool

	// JOINING.
	haveView     bool
	caughtUp     bool
	joinFrom     map[uint32]bool
	freshStarted bool
}

func AllAlive(n uint32) uint64 {
	if n >= 64 {
		return math.MaxUint64
	}
	return 1<<n - 1
}

func New(cfg Config, nowUS uint64) *Coordinator {
	c := &Coordinator{cfg: cfg, alive: AllAlive(cfg.N), lastHeard: make([]uint64, cfg.N),
		everHeard: make([]bool, cfg.N), lastResend: nowUS, phaseSince: nowUS}
	for i := range c.lastHeard {
		c.lastHeard[i] = nowUS
	}
	c.pv, c.pa, c.oldAlive = c.view, c.alive, c.alive
	c.resetRound()
	c.joinFrom = map[uint32]bool{}
	if cfg.StartJoining {
		c.phase = Joining
	}
	return c
}

func (c *Coordinator) Phase() Phase        { return c.phase }
func (c *Coordinator) View() uint64        { return c.view }
func (c *Coordinator) Alive() uint64       { return c.alive }
func (c *Coordinator) TargetView() uint64  { return c.pv }
func (c *Coordinator) TargetAlive() uint64 { return c.pa }
func (c *Coordinator) OldAlive() uint64    { return c.oldAlive }
func (c *Coordinator) HasView() bool       { return c.phase != Joining || c.haveView }
func (c *Coordinator) Membership() uint64 {
	if c.phase == Normal {
		return c.alive
	}
	return c.pa
}

// LeaderOf is the VR leader of an alive set (its min id).
func LeaderOf(alive uint64) uint32 {
	if alive == 0 {
		return 0
	}
	return uint32(bits.TrailingZeros64(alive))
}

func inMask(mask uint64, s uint32) bool { return s < 64 && mask&(1<<s) != 0 }
func count(mask uint64) uint32          { return uint32(bits.OnesCount64(mask)) }

func (c *Coordinator) logf(format string, args ...any) {
	if !c.cfg.Quiet {
		log.Printf("r%d "+format, append([]any{c.cfg.Self}, args...)...)
	}
}

// OnHeard: any message from `from` proves it alive.
func (c *Coordinator) OnHeard(from uint32, now uint64) {
	if from < c.cfg.N {
		c.lastHeard[from] = now
		c.everHeard[from] = true
	}
}

func (c *Coordinator) excluded(alive uint64, fx *Effects) bool {
	if inMask(alive, c.cfg.Self) {
		return false
	}
	if c.phase != Joining {
		c.becomeJoining(fx)
	}
	return true
}

// becomeJoining: left out of a started view (falsely suspected, or restarted
// undetected): our state is no longer a member's; drop it and rejoin.
func (c *Coordinator) becomeJoining(fx *Effects) {
	c.phase = Joining
	c.haveView, c.caughtUp = false, false
	c.joinFrom = map[uint32]bool{}
	c.resetRound()
	fx.Excluded = true
}

func (c *Coordinator) adoptAsNonMember(v, a uint64, fx *Effects) {
	c.view, c.pv = v, v
	c.alive, c.pa, c.oldAlive = a, a, a
	c.haveView, c.caughtUp = true, false
	fx.AdoptView = true
}

func (c *Coordinator) resetRound() {
	c.svcFrom = map[uint32]bool{}
	c.dvcSent = false
	c.dvcFrom = map[uint32]lastNormal{}
	c.replies = map[uint32]recovery.Data{}
	c.mBuilt = false
	c.myM = recovery.Data{}
	c.prepMsg = nil
	c.prepAcks = map[uint32]bool{}
	c.reported = false
	c.reports = map[uint32]bool{}
	c.ms = map[uint32]recovery.Data{}
	c.startViewSent = false
}

// Tick: failure detection + re-sends of whatever this phase waits on.
func (c *Coordinator) Tick(now uint64, fx *Effects) {
	prevTick := c.lastTick
	c.lastTick = now
	if c.phase == Joining {
		if now < c.lastResend+c.cfg.ResendUS {
			return
		}
		c.lastResend = now
		if !c.haveView {
			fx.send(All, &wire.JoinRequest{SenderID: c.cfg.Self})
			// Every other server is JOINING too and nobody answered: a whole-cluster
			// restart (> f failures). Nothing survives to rejoin; start fresh.
			if uint32(len(c.joinFrom))+1 >= c.cfg.N && now >= c.phaseSince+c.cfg.GraceUS {
				c.freshStart(now, fx)
			}
		} else if c.caughtUp {
			fx.send(int32(LeaderOf(c.alive)), c.makeJoinReady())
		}
		return
	}
	// Failure detection over the servers we expect to hear from (the current view when
	// NORMAL, the target view while changing — a VR leader dying mid-change is replaced).
	if c.cfg.FailTimeoutUS > 0 {
		base := c.Membership()
		var suspects uint64
		for s := uint32(0); s < c.cfg.N; s++ {
			if s == c.cfg.Self || !inMask(base, s) {
				continue
			}
			limit := c.cfg.FailTimeoutUS
			if !c.everHeard[s] {
				limit = max(limit, c.cfg.GraceUS)
			}
			if now > c.lastHeard[s]+limit {
				suspects |= 1 << s
			}
		}
		if suspects != 0 {
			next := base &^ suspects
			if count(next) >= c.cfg.F+1 {
				gap := uint64(0)
				if prevTick != 0 {
					gap = now - prevTick
				}
				for s := uint32(0); s < c.cfg.N; s++ {
					if suspects&(1<<s) != 0 {
						c.logf("suspects r%d: silent %d us (own tick gap %d us)", s, now-c.lastHeard[s], gap)
					}
				}
				c.startViewChange(max(c.view, c.pv)+1, next, now, fx)
				return
			}
			// Fewer than f+1 left: no view can form; keep waiting.
		}
	}
	if c.phase == Normal || now < c.lastResend+c.cfg.ResendUS {
		return
	}
	c.lastResend = now
	if c.phase == ViewChange {
		fx.send(All, c.makeSvc())
		if c.dvcSent && LeaderOf(c.pa) != c.cfg.Self {
			fx.send(int32(LeaderOf(c.pa)), c.makeDvc())
		}
		return
	}
	// RECOVERING
	vrLeader := LeaderOf(c.pa) == c.cfg.Self
	if vrLeader {
		fx.send(All, c.makeStartRecovery())
	}
	switch {
	case !c.mBuilt:
		fx.send(All, c.makeRecoveryRequest())
	case !c.reported:
		fx.send(All, c.makeRecoveryPrepare())
	case !vrLeader:
		fx.send(int32(LeaderOf(c.pa)), c.makeReport())
	}
}

// --- view change (VR) ---

func (c *Coordinator) startViewChange(v, a, now uint64, fx *Effects) {
	if c.phase == Normal || c.phase == Joining {
		c.oldAlive = c.alive
	}
	c.phase = ViewChange
	c.freshStarted = false
	c.pv, c.pa = v, a
	c.phaseSince, c.lastResend = now, now
	c.resetRound()
	fx.send(All, c.makeSvc())
	c.maybeSendDoViewChange(now, fx) // f == 0
}

func (c *Coordinator) OnStartViewChange(m *wire.StartViewChange, now uint64, fx *Effects) {
	c.OnHeard(m.SenderID, now)
	if c.phase == Joining {
		// Only the change that re-adds us, once caught up in the view it replaces.
		if !c.haveView || !c.caughtUp || !inMask(m.Alive, c.cfg.Self) || m.ViewID <= c.view {
			return
		}
	}
	cur := c.pv
	if c.phase == Normal {
		cur = c.view
	}
	if m.ViewID < cur {
		return
	}
	if m.ViewID == c.view && c.phase == Normal {
		// A laggard still changing to the view we already started: the VR leader re-sends.
		if LeaderOf(c.alive) == c.cfg.Self && m.ViewID > 0 {
			fx.send(int32(m.SenderID), c.makeStartView())
		}
		return
	}
	if !inMask(m.Alive, c.cfg.Self) {
		return // others suspect us; fenced if it forms
	}
	if m.ViewID > cur || c.phase == Normal {
		c.startViewChange(m.ViewID, m.Alive, now, fx)
	}
	if c.phase != ViewChange || m.ViewID != c.pv {
		return
	}
	if m.Alive != c.pa {
		// Conflicting proposals for one view number: move on with the intersection.
		next := c.pa & m.Alive
		if count(next) >= c.cfg.F+1 && inMask(next, c.cfg.Self) {
			c.startViewChange(c.pv+1, next, now, fx)
		}
		return
	}
	if m.SenderID != c.cfg.Self {
		c.svcFrom[m.SenderID] = true
	}
	c.maybeSendDoViewChange(now, fx)
}

func (c *Coordinator) maybeSendDoViewChange(now uint64, fx *Effects) {
	if c.phase != ViewChange || c.dvcSent || uint32(len(c.svcFrom)) < c.cfg.F {
		return
	}
	c.dvcSent = true
	if leader := LeaderOf(c.pa); leader == c.cfg.Self {
		c.dvcFrom[c.cfg.Self] = lastNormal{c.view, c.oldAlive}
		c.maybeStartRecovery(now, fx)
	} else {
		fx.send(int32(leader), c.makeDvc())
	}
}

func (c *Coordinator) OnDoViewChange(m *wire.DoViewChange, now uint64, fx *Effects) {
	if c.phase == Joining {
		return
	}
	c.OnHeard(m.SenderID, now)
	if LeaderOf(m.Alive) != c.cfg.Self {
		return
	}
	cur := c.pv
	if c.phase == Normal {
		cur = c.view
	}
	if m.ViewID < cur {
		return
	}
	if c.phase == Normal && m.ViewID == c.view {
		if m.ViewID > 0 {
			fx.send(int32(m.SenderID), c.makeStartView())
		}
		return
	}
	if m.ViewID > cur || c.phase == Normal {
		c.startViewChange(m.ViewID, m.Alive, now, fx)
	}
	if m.ViewID != c.pv || m.Alive != c.pa {
		return
	}
	if c.phase == Recovering {
		fx.send(int32(m.SenderID), c.makeStartRecovery())
		return
	}
	lnv := uint64(0)
	if m.HasLastNormalView {
		lnv = m.LastNormalView
	}
	c.dvcFrom[m.SenderID] = lastNormal{lnv, m.OldAlive}
	c.maybeStartRecovery(now, fx)
}

func (c *Coordinator) maybeStartRecovery(now uint64, fx *Effects) {
	if c.phase != ViewChange || LeaderOf(c.pa) != c.cfg.Self {
		return
	}
	if _, ok := c.dvcFrom[c.cfg.Self]; !ok || uint32(len(c.dvcFrom)) < c.cfg.F+1 {
		return
	}
	// Recover from the oldest last-NORMAL view among the f+1 participants (ties: union).
	oldest, oldAlive := uint64(math.MaxUint64), uint64(0)
	for _, ln := range c.dvcFrom {
		if ln.view < oldest {
			oldest, oldAlive = ln.view, ln.alive
		} else if ln.view == oldest {
			oldAlive |= ln.alive
		}
	}
	c.enterRecovering(c.pv, c.pa, oldAlive, now, fx)
	fx.send(All, c.makeStartRecovery())
}

// --- recovery ---

func (c *Coordinator) enterRecovering(v, a, oldA, now uint64, fx *Effects) {
	c.phase = Recovering
	c.pv, c.pa, c.oldAlive = v, a, oldA
	c.phaseSince, c.lastResend = now, now
	c.resetRound()
	fx.BeginRecovery = true
	fx.send(All, c.makeRecoveryRequest())
}

// AcceptRecoveryView: before answering a RecoveryRequest, join recovery of that view if
// we haven't yet. True iff we are now RECOVERING in exactly that view.
func (c *Coordinator) AcceptRecoveryView(view, alive, oldAlive, now uint64, fx *Effects) bool {
	if c.phase == Joining {
		// A round that re-adds us (we missed its StartViewChange): join it only once caught
		// up. One that leaves us out: not ours to answer.
		if !c.haveView || !c.caughtUp || !inMask(alive, c.cfg.Self) || view <= c.view {
			return false
		}
	} else if c.excluded(alive, fx) {
		return false
	}
	cur := c.pv
	if c.phase == Normal {
		cur = c.view
	}
	if view < cur || (c.phase == Normal && view == c.view) {
		return false
	}
	if c.phase != Recovering || view != c.pv {
		c.enterRecovering(view, alive, oldAlive, now, fx)
	}
	return c.phase == Recovering && c.pv == view
}

func (c *Coordinator) OnStartRecovery(m *wire.StartRecovery, now uint64, fx *Effects) {
	c.OnHeard(m.SenderID, now)
	c.AcceptRecoveryView(m.ViewID, m.Alive, m.OldAlive, now, fx)
}

// OnLocalRecoveryData: this server's own contribution to its M.
func (c *Coordinator) OnLocalRecoveryData(d recovery.Data, fx *Effects) {
	if c.phase != Recovering {
		return
	}
	c.replies[c.cfg.Self] = d
	c.maybeBuildM(fx)
}

func (c *Coordinator) OnRecoveryReply(m *wire.RecoveryReply, fx *Effects) {
	if c.phase != Recovering || m.ViewID != c.pv || !inMask(c.pa, m.SenderID) || c.mBuilt {
		return
	}
	c.replies[m.SenderID] = recovery.Data{Txns: m.Txns, KVs: m.KVs}
	c.logf("recovery view %d: reply from r%d (txns=%d kvs=%d)", c.pv, m.SenderID, len(m.Txns), len(m.KVs))
	c.maybeBuildM(fx)
}

func (c *Coordinator) maybeBuildM(fx *Effects) {
	// f+1 servers, itself included (design doc).
	if _, own := c.replies[c.cfg.Self]; c.mBuilt || !own || uint32(len(c.replies)) < c.cfg.F+1 {
		return
	}
	rs := make([]recovery.Data, 0, len(c.replies))
	for _, d := range c.replies {
		rs = append(rs, d)
	}
	c.replies = map[uint32]recovery.Data{}
	c.myM = recovery.Merge(rs)
	c.mBuilt = true
	c.logf("recovery view %d: M built (txns=%d kvs=%d)", c.pv, len(c.myM.Txns), len(c.myM.KVs))
	c.ms[c.cfg.Self] = c.myM
	c.prepAcks[c.cfg.Self] = true
	fx.send(All, c.makeRecoveryPrepare())
	c.maybeReport(fx)
}

func (c *Coordinator) OnRecoveryPrepare(m *wire.RecoveryPrepare, now uint64, fx *Effects) {
	c.OnHeard(m.SenderID, now)
	installedAlready := c.phase == Normal && m.ViewID == c.view
	if !installedAlready && !c.AcceptRecoveryView(m.ViewID, m.Alive, m.OldAlive, now, fx) {
		return
	}
	if !installedAlready {
		if _, ok := c.ms[m.SenderID]; !ok {
			c.logf("recovery view %d: prepare from r%d (txns=%d kvs=%d)", m.ViewID, m.SenderID, len(m.Txns), len(m.KVs))
		}
		c.ms[m.SenderID] = recovery.Data{Txns: m.Txns, KVs: m.KVs}
	}
	fx.send(int32(m.SenderID), &wire.RecoveryPrepareReply{ViewID: m.ViewID, SenderID: c.cfg.Self, PLID: m.SenderID})
}

func (c *Coordinator) OnRecoveryPrepareReply(m *wire.RecoveryPrepareReply, fx *Effects) {
	if c.phase != Recovering || m.ViewID != c.pv || m.PLID != c.cfg.Self || !c.mBuilt || !inMask(c.pa, m.SenderID) {
		return
	}
	c.prepAcks[m.SenderID] = true
	c.maybeReport(fx)
}

func (c *Coordinator) maybeReport(fx *Effects) {
	if c.reported || uint32(len(c.prepAcks)) < c.cfg.F+1 {
		return
	}
	c.reported = true
	if leader := LeaderOf(c.pa); leader == c.cfg.Self {
		c.reports[c.cfg.Self] = true
		c.maybeStartView(fx)
	} else {
		fx.send(int32(leader), c.makeReport())
	}
}

func (c *Coordinator) OnRecoveryReport(m *wire.RecoveryReport, fx *Effects) {
	if c.phase == Normal && m.ViewID == c.view && LeaderOf(c.alive) == c.cfg.Self {
		fx.send(int32(m.SenderID), c.makeStartView()) // it missed our StartView
		return
	}
	if c.phase != Recovering || m.ViewID != c.pv || LeaderOf(c.pa) != c.cfg.Self || !inMask(c.pa, m.SenderID) {
		return
	}
	if !c.reports[m.SenderID] {
		c.reports[m.SenderID] = true
		c.logf("recovery view %d: report from r%d", c.pv, m.SenderID)
	}
	c.maybeStartView(fx)
}

func (c *Coordinator) maybeStartView(fx *Effects) {
	if c.startViewSent {
		return
	}
	for s := uint32(0); s < c.cfg.N; s++ {
		if inMask(c.pa, s) && !c.reports[s] {
			return // every alive PL must report
		}
	}
	c.startViewSent = true
	fx.send(All, c.makeStartView())
	fx.Install = true
}

func (c *Coordinator) OnStartView(m *wire.StartView, now uint64, fx *Effects) {
	c.OnHeard(m.SenderID, now)
	if !inMask(m.Alive, c.cfg.Self) {
		// A view without us: (re)join it as a non-member.
		if c.phase != Joining {
			c.becomeJoining(fx)
		}
		if !c.haveView || m.ViewID > c.view {
			c.adoptAsNonMember(m.ViewID, m.Alive, fx)
		}
		return
	}
	// A view with us while JOINING can only be a stale re-send (a round re-adding us
	// needs our own report first, which takes us through RECOVERING).
	if c.phase == Joining || m.ViewID <= c.view {
		return
	}
	if c.phase == Recovering && m.ViewID == c.pv {
		fx.Install = true
		return
	}
	if c.phase != Recovering || m.ViewID > c.pv {
		// Missed the whole recovery round: adopt the view with whatever we hold.
		if c.phase == Normal {
			c.oldAlive = c.alive
		}
		c.phase = Recovering
		c.pv, c.pa = m.ViewID, m.Alive
		c.ms = map[uint32]recovery.Data{}
		fx.Install = true
	}
}

// TakeInstall: after fx.Install, hand over what to install and enter NORMAL in the new
// view.
func (c *Coordinator) TakeInstall(now uint64) Install {
	in := Install{View: c.pv, Alive: c.pa, OldAlive: c.oldAlive}
	for s := uint32(0); s < c.cfg.N; s++ { // deterministic order
		if d, ok := c.ms[s]; ok {
			in.Ms = append(in.Ms, d)
		}
	}
	c.view, c.alive = c.pv, c.pa
	c.phase = Normal
	c.phaseSince = now
	c.resetRound()
	for s := range c.lastHeard {
		c.lastHeard[s] = max(c.lastHeard[s], now)
	}
	return in
}

// --- builders ---

func (c *Coordinator) makeSvc() wire.Message {
	return &wire.StartViewChange{ViewID: c.pv, SenderID: c.cfg.Self, Alive: c.pa}
}

func (c *Coordinator) makeDvc() wire.Message {
	return &wire.DoViewChange{ViewID: c.pv, SenderID: c.cfg.Self, Alive: c.pa, OldAlive: c.oldAlive,
		HasLastNormalView: true, LastNormalView: c.view}
}

func (c *Coordinator) makeStartRecovery() wire.Message {
	return &wire.StartRecovery{ViewID: c.pv, SenderID: c.cfg.Self, Alive: c.pa, OldAlive: c.oldAlive}
}

func (c *Coordinator) makeRecoveryRequest() wire.Message {
	return &wire.RecoveryRequest{ViewID: c.pv, RequesterID: c.cfg.Self, Alive: c.pa, OldAlive: c.oldAlive}
}

// makeRecoveryPrepare is built once per round and re-sent as the same object: M can be
// megabytes, and the replica caches its encoded chunks by object.
func (c *Coordinator) makeRecoveryPrepare() wire.Message {
	if c.prepMsg == nil {
		c.prepMsg = &wire.RecoveryPrepare{ViewID: c.pv, SenderID: c.cfg.Self, Alive: c.pa,
			OldAlive: c.oldAlive, Txns: c.myM.Txns, KVs: c.myM.KVs}
	}
	return c.prepMsg
}

func (c *Coordinator) makeReport() wire.Message {
	return &wire.RecoveryReport{ViewID: c.pv, SenderID: c.cfg.Self}
}

func (c *Coordinator) makeStartView() wire.Message {
	if c.phase == Normal {
		return &wire.StartView{ViewID: c.view, SenderID: c.cfg.Self, Alive: c.alive}
	}
	return &wire.StartView{ViewID: c.pv, SenderID: c.cfg.Self, Alive: c.pa}
}

func (c *Coordinator) makeJoinReady() wire.Message {
	return &wire.JoinReady{ViewID: c.view, SenderID: c.cfg.Self}
}

// --- rejoin ---

func (c *Coordinator) freshStart(now uint64, fx *Effects) {
	c.phase = Normal
	c.view, c.pv = 0, 0
	c.alive = AllAlive(c.cfg.N)
	c.pa, c.oldAlive = c.alive, c.alive
	for s := range c.lastHeard {
		c.lastHeard[s] = now
	}
	c.freshStarted = true
	fx.FreshStart = true
}

func (c *Coordinator) OnJoinRequest(m *wire.JoinRequest, now uint64, fx *Effects) {
	from := m.SenderID
	c.OnHeard(from, now)
	if from >= c.cfg.N || from == c.cfg.Self {
		return
	}
	if c.phase == Joining {
		c.joinFrom[from] = true
		return
	}
	members := c.Membership()
	if c.phase == Normal && c.freshStarted && c.view == 0 && inMask(members, from) {
		// Whole-cluster restart and this peer hasn't started fresh yet: tell it view 0
		// (it starts fresh too) rather than excluding it.
		fx.send(int32(from), &wire.JoinInfo{ViewID: 0, SenderID: c.cfg.Self, Alive: c.alive})
		return
	}
	if inMask(members, from) {
		// It restarted before we suspected it: its state is gone, so it can no longer
		// count as a member. Change view without it first (then it rejoins).
		if next := members &^ (1 << from); count(next) >= c.cfg.F+1 {
			c.startViewChange(max(c.view, c.pv)+1, next, now, fx)
		}
		return
	}
	if c.phase != Normal {
		return // it retries once the view settles
	}
	fx.send(int32(from), &wire.JoinInfo{ViewID: c.view, SenderID: c.cfg.Self, Alive: c.alive})
}

func (c *Coordinator) OnJoinInfo(m *wire.JoinInfo, now uint64, fx *Effects) {
	c.OnHeard(m.SenderID, now)
	if c.phase != Joining {
		return
	}
	if inMask(m.Alive, c.cfg.Self) {
		// Only from a fresh-started peer of a whole-cluster restart: join view 0 fresh.
		if !c.haveView && m.ViewID == 0 {
			c.freshStart(now, fx)
		}
		return
	}
	if !c.haveView || m.ViewID > c.view {
		c.adoptAsNonMember(m.ViewID, m.Alive, fx)
	}
}

// OnCaughtUp: JOINING, the replica finished catching up in the adopted view.
func (c *Coordinator) OnCaughtUp(fx *Effects) {
	if c.phase != Joining || !c.haveView || c.caughtUp {
		return
	}
	c.caughtUp = true
	fx.send(int32(LeaderOf(c.alive)), c.makeJoinReady())
}

func (c *Coordinator) OnJoinReady(m *wire.JoinReady, now uint64, fx *Effects) {
	c.OnHeard(m.SenderID, now)
	if c.phase != Normal || LeaderOf(c.alive) != c.cfg.Self {
		return
	}
	if m.ViewID != c.view || inMask(c.alive, m.SenderID) || m.SenderID >= c.cfg.N {
		return // caught up in another view: it re-syncs and asks again
	}
	c.startViewChange(c.view+1, c.alive|1<<m.SenderID, now, fx)
}
