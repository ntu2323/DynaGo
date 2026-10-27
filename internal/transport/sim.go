package transport

import (
	"fmt"
	"net/netip"
	"sync"

	"dynago/internal/wire"
)

// SimNet is an in-process network for tests: messages are encoded with the real codec,
// queued, and delivered only when the test calls Deliver (deterministic; no goroutines).
// Drop lets a test lose messages; Partition-style faults are expressed through it.
type SimNet struct {
	mu       sync.Mutex
	nodes    map[Addr]Handler
	down     map[Addr]bool
	replicas []Addr
	queue    []simPacket
	// Drop, if set, is consulted per packet at send time (true = lost).
	Drop      func(from, to Addr, m wire.Message) bool
	Delivered int
	Dropped   int
}

type simPacket struct {
	from, to Addr
	data     []byte
}

// NewSimNet makes n replica addresses (10.0.0.1:1000+i).
func NewSimNet(n int) *SimNet {
	s := &SimNet{nodes: map[Addr]Handler{}, down: map[Addr]bool{}}
	for i := 0; i < n; i++ {
		s.replicas = append(s.replicas, ReplicaSimAddr(i))
	}
	return s
}

func ReplicaSimAddr(i int) Addr {
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, 0, 0, 1}), uint16(1000+i))
}

// ClientSimAddr is the address of simulated client i.
func ClientSimAddr(i int) Addr {
	return netip.AddrPortFrom(netip.AddrFrom4([4]byte{10, 0, 1, 1}), uint16(2000+i))
}

// Endpoint attaches h at addr and returns its Transport. selfIdx is the replica index
// (-1 for a client).
func (s *SimNet) Endpoint(addr Addr, selfIdx int, h Handler) *SimEndpoint {
	s.mu.Lock()
	s.nodes[addr] = h
	delete(s.down, addr)
	s.mu.Unlock()
	return &SimEndpoint{net: s, self: addr, idx: selfIdx}
}

// Attach rebinds addr to a new handler (a restarted node).
func (s *SimNet) Attach(addr Addr, h Handler) {
	s.mu.Lock()
	s.nodes[addr] = h
	delete(s.down, addr)
	s.mu.Unlock()
}

// SetDown makes addr drop everything it would send or receive (a crashed node).
func (s *SimNet) SetDown(addr Addr, down bool) {
	s.mu.Lock()
	s.down[addr] = down
	s.mu.Unlock()
}

func (s *SimNet) send(from, to Addr, m wire.Message) {
	data := wire.Marshal(nil, m)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.down[from] || s.down[to] || (s.Drop != nil && s.Drop(from, to, m)) {
		s.Dropped++
		return
	}
	s.queue = append(s.queue, simPacket{from, to, data})
}

// Pending is the number of queued packets.
func (s *SimNet) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.queue)
}

// DeliverOne delivers the oldest queued packet; false if none.
func (s *SimNet) DeliverOne() bool {
	s.mu.Lock()
	if len(s.queue) == 0 {
		s.mu.Unlock()
		return false
	}
	p := s.queue[0]
	s.queue = s.queue[1:]
	h := s.nodes[p.to]
	if s.down[p.to] {
		h = nil
	}
	s.mu.Unlock()
	if h == nil {
		return true
	}
	m, err := wire.Unmarshal(p.data)
	if err != nil {
		panic(fmt.Sprintf("simnet: undecodable packet: %v", err))
	}
	s.mu.Lock()
	s.Delivered++
	s.mu.Unlock()
	h.Receive(p.from, m)
	return true
}

// Deliver drains the queue, including messages sent while delivering, up to max
// packets (0 = 1e6). Returns the number delivered.
func (s *SimNet) Deliver(max int) int {
	if max <= 0 {
		max = 1_000_000
	}
	n := 0
	for n < max && s.DeliverOne() {
		n++
	}
	return n
}

// SimEndpoint is one node's Transport on a SimNet.
type SimEndpoint struct {
	net  *SimNet
	self Addr
	idx  int
}

func (e *SimEndpoint) Self() Addr                   { return e.self }
func (e *SimEndpoint) Send(to Addr, m wire.Message) { e.net.send(e.self, to, m) }
func (e *SimEndpoint) ReplicaAddr(i int) Addr       { return e.net.replicas[i] }
func (e *SimEndpoint) NumReplicas() int             { return len(e.net.replicas) }
func (e *SimEndpoint) SendReplica(i int, m wire.Message) {
	if i != e.idx && i >= 0 && i < len(e.net.replicas) {
		e.net.send(e.self, e.net.replicas[i], m)
	}
}
func (e *SimEndpoint) Broadcast(m wire.Message) {
	for i := range e.net.replicas {
		e.SendReplica(i, m)
	}
}
