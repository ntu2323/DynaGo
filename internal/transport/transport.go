// Package transport moves wire messages between DYNA nodes: one message per UDP
// datagram (udp.go), or an in-process simulated network for tests (sim.go).
package transport

import (
	"net/netip"

	"dynago/internal/wire"
)

type Addr = netip.AddrPort

// Handler receives decoded messages. It may be called concurrently (several receive
// goroutines) and must not retain m's slices beyond what it copies... (decoded
// messages own their memory, so retaining them is fine).
type Handler interface {
	Receive(from Addr, m wire.Message)
}

// Transport sends messages. Safe for concurrent use.
type Transport interface {
	Self() Addr
	// Send to one address (a client, or a replica's address).
	Send(to Addr, m wire.Message)
	// SendReplica to replica i (no-op for i == self).
	SendReplica(i int, m wire.Message)
	// Broadcast to every replica except self.
	Broadcast(m wire.Message)
	// ReplicaAddr is replica i's address.
	ReplicaAddr(i int) Addr
	NumReplicas() int
}

// Writer queues outgoing datagrams for one sending goroutine; Flush sends what is
// queued (one sendmmsg on Linux UDP). Not safe for concurrent use.
type Writer interface {
	Add(to Addr, m wire.Message)
	Flush()
}

// WriterFor returns a batching writer on socket i when t supports one, else a Writer
// that sends each message as it is added.
func WriterFor(t Transport, i int) Writer {
	if u, ok := t.(*UDP); ok {
		return u.NewBatchWriter(i)
	}
	return directWriter{t}
}

type directWriter struct{ t Transport }

func (d directWriter) Add(to Addr, m wire.Message) { d.t.Send(to, m) }
func (directWriter) Flush()                        {}
