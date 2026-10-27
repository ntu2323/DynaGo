package vr

import (
	"fmt"
	"math/rand"
	"testing"

	"dynago/internal/recovery"
	"dynago/internal/wire"
)

const kAll3 = 0b111

type msg struct {
	from, to int
	m        wire.Message
}

type installed struct {
	done                  bool
	view, alive, oldAlive uint64
	mKeys                 []string
}

// fakeNet runs N coordinators over a lossy in-memory network on simulated time.
type fakeNet struct {
	vc        []*Coordinator
	up        []bool
	q         []msg
	rng       *rand.Rand
	drop      float64
	now       uint64
	filter    func(msg) bool // false = drop this delivery
	installed []installed
	adopted   []int
	fresh     []int
}

func cfg(s, n uint32, joining bool) Config {
	return Config{Self: s, N: n, F: (n - 1) / 2, FailTimeoutUS: 100000, GraceUS: 1000000,
		ResendUS: 30000, StartJoining: joining, Quiet: true}
}

func newFakeNet(n int, allJoining bool) *fakeNet {
	f := &fakeNet{up: make([]bool, n), rng: rand.New(rand.NewSource(42)),
		installed: make([]installed, n), adopted: make([]int, n), fresh: make([]int, n)}
	for s := 0; s < n; s++ {
		f.up[s] = true
		f.vc = append(f.vc, New(cfg(uint32(s), uint32(n), allJoining), 0))
	}
	return f
}

// restart: in-memory VR state lost; the persisted file says rejoin.
func (f *fakeNet) restart(s int) {
	f.vc[s] = New(cfg(uint32(s), uint32(len(f.vc)), true), f.now)
	f.up[s] = true
	f.installed[s] = installed{}
}

// dataOf: each server's recovery contribution — one key-value naming it.
func dataOf(s int) recovery.Data {
	return recovery.Data{KVs: []wire.RecKV{{Key: fmt.Sprintf("from%d", s), Value: "v", T: 1}}}
}

func (f *fakeNet) emit(from int, fx *Effects) {
	for _, o := range fx.Out {
		if o.To == All {
			for r := range f.vc {
				if r != from {
					f.q = append(f.q, msg{from, r, o.Msg})
				}
			}
		} else {
			f.q = append(f.q, msg{from, int(o.To), o.Msg})
		}
	}
	if fx.BeginRecovery {
		var fx2 Effects
		f.vc[from].OnLocalRecoveryData(dataOf(from), &fx2)
		f.emit(from, &fx2)
	}
	if fx.AdoptView {
		f.adopted[from]++ // catch-up is the replica's job; here it completes instantly
		var fx2 Effects
		f.vc[from].OnCaughtUp(&fx2)
		f.emit(from, &fx2)
	}
	if fx.FreshStart {
		f.fresh[from]++
	}
	if fx.Install {
		in := f.vc[from].TakeInstall(f.now)
		i := installed{done: true, view: in.View, alive: in.Alive, oldAlive: in.OldAlive}
		for _, m := range in.Ms {
			for _, kv := range m.KVs {
				i.mKeys = append(i.mKeys, kv.Key)
			}
		}
		f.installed[from] = i
	}
}

func (f *fakeNet) deliver(d msg) {
	if !f.up[d.to] || !f.up[d.from] || (f.filter != nil && !f.filter(d)) ||
		(f.drop > 0 && f.rng.Float64() < f.drop) {
		return
	}
	v := f.vc[d.to]
	var fx Effects
	switch m := d.m.(type) {
	case *wire.Heartbeat:
		v.OnHeard(m.SenderID, f.now)
	case *wire.StartViewChange:
		v.OnStartViewChange(m, f.now, &fx)
	case *wire.DoViewChange:
		v.OnDoViewChange(m, f.now, &fx)
	case *wire.StartRecovery:
		v.OnStartRecovery(m, f.now, &fx)
	case *wire.RecoveryRequest:
		v.OnHeard(m.RequesterID, f.now)
		if v.AcceptRecoveryView(m.ViewID, m.Alive, m.OldAlive, f.now, &fx) {
			dd := dataOf(d.to)
			fx.send(int32(m.RequesterID), &wire.RecoveryReply{ViewID: m.ViewID, SenderID: uint32(d.to), KVs: dd.KVs})
		}
	case *wire.RecoveryReply:
		v.OnRecoveryReply(m, &fx)
	case *wire.RecoveryPrepare:
		v.OnRecoveryPrepare(m, f.now, &fx)
	case *wire.RecoveryPrepareReply:
		v.OnRecoveryPrepareReply(m, &fx)
	case *wire.RecoveryReport:
		v.OnRecoveryReport(m, &fx)
	case *wire.StartView:
		v.OnStartView(m, f.now, &fx)
	case *wire.JoinRequest:
		v.OnJoinRequest(m, f.now, &fx)
	case *wire.JoinInfo:
		v.OnJoinInfo(m, f.now, &fx)
	case *wire.JoinReady:
		v.OnJoinReady(m, f.now, &fx)
	}
	f.emit(d.to, &fx)
}

// step: 10 ms of simulated time — heartbeats + ticks, then drain the network.
func (f *fakeNet) step() {
	f.now += 10000
	for s := range f.vc {
		if !f.up[s] {
			continue
		}
		hb := &wire.Heartbeat{ViewID: f.vc[s].View(), SenderID: uint32(s)}
		for r := range f.vc {
			if r != s {
				f.q = append(f.q, msg{s, r, hb})
			}
		}
		var fx Effects
		f.vc[s].Tick(f.now, &fx)
		f.emit(s, &fx)
	}
	for guard := 0; len(f.q) > 0 && guard < 100000; guard++ {
		d := f.q[0]
		f.q = f.q[1:]
		f.deliver(d)
	}
}

func (f *fakeNet) allSurvivorsInstalled(view uint64) bool {
	for s, v := range f.vc {
		if f.up[s] && (v.Phase() != Normal || v.View() != view) {
			return false
		}
	}
	return true
}

func (f *fakeNet) allNormalIn(view, alive uint64) bool {
	for s, v := range f.vc {
		if f.up[s] && (v.Phase() != Normal || v.View() != view || v.Alive() != alive) {
			return false
		}
	}
	return true
}

func (f *fakeNet) runUntil(done func() bool, maxSteps int) {
	for i := 0; i < maxSteps && !done(); i++ {
		f.step()
	}
}

func expectRecoveredWithout(t *testing.T, dead int, drop float64) {
	t.Helper()
	net := newFakeNet(3, false)
	for i := 0; i < 20; i++ {
		net.step()
	}
	for _, v := range net.vc {
		if v.Phase() != Normal || v.View() != 0 {
			t.Fatal("not normal in view 0")
		}
	}
	net.up[dead] = false
	net.drop = drop
	steps := 0
	for ; !net.allSurvivorsInstalled(1) && steps < 500; steps++ {
		net.step()
	}
	if !net.allSurvivorsInstalled(1) {
		t.Fatalf("no convergence after %d steps", steps)
	}
	want := uint64(kAll3) &^ (1 << dead)
	for s := range net.vc {
		if s == dead {
			continue
		}
		if net.vc[s].Alive() != want || !net.installed[s].done || net.installed[s].alive != want {
			t.Fatal(s, net.installed[s])
		}
		// With f=1 and 2 survivors, both hold both Ms (each built from both servers' data).
		if drop == 0 && len(net.installed[s].mKeys) != 4 {
			t.Fatal("server", s, net.installed[s].mKeys)
		}
	}
}

func TestRecoveryView_FollowerDiesSurvivorsInstallNewView(t *testing.T) {
	expectRecoveredWithout(t, 1, 0)
}

func TestRecoveryView_VrLeaderDiesNewLeaderCoordinates(t *testing.T) {
	expectRecoveredWithout(t, 0, 0)
}

func TestRecoveryView_ConvergesUnderMessageLoss(t *testing.T) {
	expectRecoveredWithout(t, 2, 0.25)
	expectRecoveredWithout(t, 0, 0.25)
}

func TestRecoveryView_NodeLeftOutOfStartedViewResetsAndJoinsAsNonMember(t *testing.T) {
	v := New(Config{Self: 1, N: 3, F: 1, Quiet: true}, 0)
	var fx Effects
	v.OnStartView(&wire.StartView{ViewID: 1, SenderID: 0, Alive: 0b101}, 10, &fx)
	if !fx.Excluded || !fx.AdoptView || v.Phase() != Joining || v.View() != 1 || v.Alive() != 0b101 {
		t.Fatalf("%+v %v", fx, v.Phase())
	}
}

func TestRecoveryView_NoFailureDetectionWhenDisabled(t *testing.T) {
	v := New(Config{Self: 0, N: 3, F: 1, Quiet: true}, 0)
	var fx Effects
	v.Tick(100000000, &fx)
	if v.Phase() != Normal || len(fx.Out) != 0 {
		t.Fatal()
	}
}

func TestRecoveryView_FiveServersTwoSequentialFailures(t *testing.T) {
	net := newFakeNet(5, false)
	for i := 0; i < 20; i++ {
		net.step()
	}
	net.up[3] = false
	net.runUntil(func() bool { return net.allSurvivorsInstalled(1) }, 500)
	if !net.allSurvivorsInstalled(1) {
		t.Fatal("view 1")
	}
	for _, s := range []int{0, 1, 2, 4} {
		if net.vc[s].Alive() != 0b10111 {
			t.Fatal(s)
		}
	}
	net.up[0] = false // view 1's VR leader
	net.drop = 0.1
	net.runUntil(func() bool { return net.allSurvivorsInstalled(2) }, 1000)
	if !net.allSurvivorsInstalled(2) {
		t.Fatal("view 2")
	}
	for _, s := range []int{1, 2, 4} {
		if net.vc[s].Alive() != 0b10110 {
			t.Fatal(s)
		}
	}
}

// View 1's VR leader dies right after its StartView reached only server 1. Recovery of
// view 2 must start from view 0 (the oldest last-NORMAL view among the participants).
func TestRecoveryView_LeaderDiesAfterPartialStartView(t *testing.T) {
	net := newFakeNet(5, false)
	for i := 0; i < 20; i++ {
		net.step()
	}
	sent := false
	net.filter = func(d msg) bool {
		if _, ok := d.m.(*wire.StartView); ok && d.from == 0 {
			sent = true
			return d.to == 1
		}
		return true
	}
	net.up[4] = false
	net.runUntil(func() bool { return sent }, 500)
	if !sent {
		t.Fatal("no StartView")
	}
	net.up[0] = false
	if net.vc[1].Phase() != Normal || net.vc[1].View() != 1 {
		t.Fatal("server 1 should be in view 1")
	}
	for _, s := range []int{2, 3} {
		if net.vc[s].Phase() != Recovering || net.vc[s].View() != 0 {
			t.Fatal(s, net.vc[s].Phase())
		}
	}
	net.runUntil(func() bool { return net.allSurvivorsInstalled(2) }, 1000)
	if !net.allSurvivorsInstalled(2) {
		t.Fatal("view 2")
	}
	for _, s := range []int{1, 2, 3} {
		if i := net.installed[s]; i.view != 2 || i.alive != 0b01110 || i.oldAlive != 0b11111 {
			t.Fatalf("server %d: %+v", s, i)
		}
	}
}

// --- rejoin ---

func TestRejoin_RestartedServerRejoins(t *testing.T) {
	for _, dead := range []int{1, 2, 0} { // 0: the joiner is VR leader of the join view
		net := newFakeNet(3, false)
		for i := 0; i < 20; i++ {
			net.step()
		}
		net.up[dead] = false
		without := uint64(kAll3) &^ (1 << dead)
		net.runUntil(func() bool { return net.allNormalIn(1, without) }, 1000)
		if !net.allNormalIn(1, without) {
			t.Fatal("view 1, dead", dead)
		}
		net.restart(dead)
		if net.vc[dead].Phase() != Joining {
			t.Fatal()
		}
		net.runUntil(func() bool { return net.allNormalIn(2, kAll3) }, 1000)
		if !net.allNormalIn(2, kAll3) || net.adopted[dead] != 1 {
			t.Fatal("view 2, dead", dead)
		}
		for s := range net.vc {
			if net.installed[s].view != 2 || net.installed[s].oldAlive != without {
				t.Fatal(s, net.installed[s])
			}
		}
	}
}

func TestRejoin_RestartBeforeDetection(t *testing.T) {
	net := newFakeNet(3, false)
	for i := 0; i < 20; i++ {
		net.step()
	}
	net.restart(1)
	net.runUntil(func() bool { return net.allNormalIn(2, kAll3) }, 1000)
	if !net.allNormalIn(2, kAll3) || net.installed[0].oldAlive != 0b101 {
		t.Fatal(net.installed[0])
	}
}

func TestRejoin_FalselyExcludedServerRejoins(t *testing.T) {
	net := newFakeNet(3, false)
	for i := 0; i < 20; i++ {
		net.step()
	}
	mute := true
	net.filter = func(d msg) bool {
		_, hb := d.m.(*wire.Heartbeat)
		return !(mute && d.from == 2 && hb)
	}
	for i := 0; i < 50; i++ {
		net.step()
	}
	if net.adopted[2] < 1 {
		t.Fatal("not left out")
	}
	mute = false
	rejoined := func() bool { v := net.vc[0].View(); return v >= 2 && net.allNormalIn(v, kAll3) }
	net.runUntil(rejoined, 1000)
	if !rejoined() {
		t.Fatal()
	}
}

func TestRejoin_WholeClusterRestartStartsFresh(t *testing.T) {
	net := newFakeNet(3, true)
	net.runUntil(func() bool { return net.allNormalIn(0, kAll3) }, 300)
	if !net.allNormalIn(0, kAll3) {
		t.Fatal()
	}
	for s := range net.vc {
		if net.fresh[s] != 1 {
			t.Fatal(s, net.fresh)
		}
	}
}

func TestRejoin_JoinerIgnoresViewChangesUntilCaughtUp(t *testing.T) {
	v := New(cfg(2, 3, true), 0)
	var fx Effects
	v.OnJoinInfo(&wire.JoinInfo{ViewID: 1, SenderID: 0, Alive: 0b011}, 10, &fx)
	if !fx.AdoptView {
		t.Fatal()
	}
	svc := &wire.StartViewChange{ViewID: 2, SenderID: 0, Alive: 0b111}
	var fx2 Effects
	v.OnStartViewChange(svc, 20, &fx2)
	if v.Phase() != Joining || v.AcceptRecoveryView(2, 0b011, 0b011, 30, &fx2) {
		t.Fatal("must ignore until caught up")
	}
	v.OnCaughtUp(&fx2)
	if len(fx2.Out) != 1 || fx2.Out[0].To != 0 {
		t.Fatal(fx2.Out)
	}
	v.OnStartViewChange(svc, 40, &fx2)
	if v.Phase() != ViewChange {
		t.Fatal(v.Phase())
	}
}

func TestRejoin_StaggeredWholeClusterRestartNoExclusion(t *testing.T) {
	net := newFakeNet(3, true)
	for i := 0; i < 50; i++ {
		net.step()
	}
	net.restart(2)
	net.runUntil(func() bool { return net.allNormalIn(0, kAll3) }, 300)
	if !net.allNormalIn(0, kAll3) {
		t.Fatal()
	}
	for i := 0; i < 50; i++ {
		net.step()
	}
	if !net.allNormalIn(0, kAll3) {
		t.Fatal("someone got excluded afterwards")
	}
}
