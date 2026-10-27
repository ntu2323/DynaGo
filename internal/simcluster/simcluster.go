// Package simcluster wires N replicas and M clients over a SimNet with one shared mock
// clock, for deterministic end-to-end tests (the Go equivalent of the C++ recovery
// test harness).
package simcluster

import (
	"os"

	"dynago/internal/client"
	"dynago/internal/clock"
	"dynago/internal/replica"
	"dynago/internal/store"
	"dynago/internal/transport"
	"dynago/internal/wire"
)

type Cluster struct {
	N, F     int
	Clock    *clock.Mock
	Net      *transport.SimNet
	Replicas []*replica.Replica
	Stores   []*store.Mem
	Clients  []*client.Client
	lanes    int
	TickUS   uint64 // background scan cadence in Run (default 50 ms, like DYNA_TS_SCAN_MS)
	HbUS     uint64 // heartbeat / VR tick cadence in Run (default 20 ms, like DYNA_HB_MS)
	Dir      string // per-cluster VR state dir (dyna-vr-<idx>.state)
	Down     []bool // crashed replicas: not driven, cut off the network
}

// New builds n = 2f+1 replicas and `clients` clients at time `now` (µs).
func New(f, clients int, now uint64, lanes int) *Cluster {
	n := 2*f + 1
	dir, err := os.MkdirTemp("", "dyna-sim-")
	if err != nil {
		panic(err)
	}
	c := &Cluster{N: n, F: f, Clock: clock.NewMock(now), Net: transport.NewSimNet(n), lanes: lanes,
		TickUS: 50_000, HbUS: 20_000, Dir: dir, Down: make([]bool, n)}
	for i := 0; i < n; i++ {
		c.Stores = append(c.Stores, store.NewMem())
		c.Replicas = append(c.Replicas, c.newReplica(i))
	}
	for i := 0; i < clients; i++ {
		h := &handlerSlot{}
		ep := c.Net.Endpoint(transport.ClientSimAddr(i), -1, h)
		cl := client.New(client.Options{ID: uint64(i + 1), F: f, Transport: ep, Clock: c.Clock})
		h.h = cl
		c.Clients = append(c.Clients, cl)
	}
	// One heartbeat round: everyone has heard everyone (else a peer never heard from
	// gets the long DYNA_FAIL_GRACE_MS allowance instead of the failure timeout).
	c.Heartbeat()
	c.Net.Deliver(0)
	return c
}

// handlerSlot lets an endpoint exist before its handler does.
type handlerSlot struct{ h transport.Handler }

func (s *handlerSlot) Receive(from transport.Addr, m wire.Message) {
	if s.h != nil {
		s.h.Receive(from, m)
	}
}

type replicaSlot struct{ r *replica.Replica }

func (s *replicaSlot) Receive(from transport.Addr, m wire.Message) {
	if s.r != nil {
		s.r.Receive(from, m)
	}
}

func (c *Cluster) newReplica(i int) *replica.Replica {
	slot := &replicaSlot{}
	ep := c.Net.Endpoint(transport.ReplicaSimAddr(i), i, slot)
	r := replica.New(replica.Options{Index: i, F: c.F, Transport: ep, Clock: c.Clock,
		Store: c.Stores[i], Lanes: c.lanes, ExternalDrive: true, VRStateDir: c.Dir, Quiet: !Verbose})
	slot.r = r
	return r
}

// Verbose turns on replica VR logs in simulated clusters.
var Verbose = os.Getenv("SIM_VERBOSE") != ""

// live replicas (not crashed).
func (c *Cluster) live() []*replica.Replica {
	var out []*replica.Replica
	for i, r := range c.Replicas {
		if r != nil && !c.Down[i] {
			out = append(out, r)
		}
	}
	return out
}

// Release drives every replica's release (mock clock: no release goroutine).
func (c *Cluster) Release() int {
	n := 0
	for _, r := range c.live() {
		n += r.Orderer().ReleaseDue()
	}
	return n
}

// Kill crashes replica i: it stops being driven and is cut off the network.
func (c *Cluster) Kill(i int) {
	c.Down[i] = true
	c.Net.SetDown(transport.ReplicaSimAddr(i), true)
}

// Restart brings replica i back as a fresh process: empty memory and store (the store
// isn't trusted across a restart), but its VR state file survives, so it rejoins.
func (c *Cluster) Restart(i int) {
	c.Replicas[i].Stop()
	c.Stores[i] = store.NewMem()
	c.Down[i] = false
	c.Replicas[i] = c.newReplica(i)
}

// Heartbeat runs every live replica's heartbeat / VR tick.
func (c *Cluster) Heartbeat() {
	for _, r := range c.live() {
		r.HeartbeatTick()
	}
}

// Step: release due entries, execute, flush background batches, tick clients, deliver
// everything; repeated until quiet. Background scans run only in Run (on their cadence).
func (c *Cluster) Step() {
	for i := 0; i < 100; i++ {
		moved := c.Release()
		for _, r := range c.live() {
			moved += len(r.Orderer().ExecuteReady())
			r.FlushBg()
		}
		for _, cl := range c.Clients {
			cl.Tick()
		}
		moved += c.Net.Deliver(0)
		if moved == 0 {
			return
		}
	}
}

// Tick runs every replica's background scan.
func (c *Cluster) Tick() {
	for _, r := range c.live() {
		r.Tick()
	}
}

// Run advances the clock by dt µs in steps of `step`, stepping the cluster each time.
func (c *Cluster) Run(dt, step uint64) {
	for done := uint64(0); done < dt; done += step {
		c.Clock.Advance(step)
		c.Step()
		now := c.Clock.Now()
		if now/c.HbUS != (now-step)/c.HbUS {
			c.Heartbeat()
			c.Step()
		}
		if now/c.TickUS != (now-step)/c.TickUS {
			c.Tick()
			c.Step()
		}
	}
}

func (c *Cluster) Stop() {
	for _, r := range c.Replicas {
		if r != nil {
			r.Stop()
		}
	}
	os.RemoveAll(c.Dir)
}
