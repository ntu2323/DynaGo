package transport

import (
	"context"
	"errors"
	"log"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"

	"golang.org/x/net/ipv4"

	"dynago/internal/wire"
)

// MaxDatagram bounds one message. Larger VR/recovery messages travel as chunked bulk
// transfers (replica layer).
const MaxDatagram = 65000

var bufPool = sync.Pool{New: func() any { b := make([]byte, 0, 2048); return &b }}

// UDPOptions shapes the socket set. Zero values mean one socket.
type UDPOptions struct {
	// Sockets bound to the same port with SO_REUSEPORT, each with its own receive
	// goroutine (the kernel may deliver to any of them: Linux spreads datagrams by
	// 4-tuple hash, macOS picks one). Each is also a separate send path: a socket's
	// writes are serialized by the runtime.
	Sockets int
	// RxBatch datagrams per receive call (recvmmsg on Linux; 1 elsewhere).
	RxBatch int
}

// UDP is a Transport over one or more UDP sockets sharing a port.
type UDP struct {
	conns    []*net.UDPConn
	pcs      []*ipv4.PacketConn
	self     Addr
	idx      int
	replicas []Addr
	repUDP   []*net.UDPAddr
	opt      UDPOptions
	next     atomic.Uint32 // round-robin socket for Send
	closed   chan struct{}
}

// ListenUDP binds bind ("host:port"; port 0 = ephemeral, for clients) with one socket.
// idx is the replica index, -1 for a client.
func ListenUDP(bind string, idx int, replicas []Addr) (*UDP, error) {
	return ListenUDPOpts(bind, idx, replicas, UDPOptions{})
}

func ListenUDPOpts(bind string, idx int, replicas []Addr, opt UDPOptions) (*UDP, error) {
	opt.Sockets = max(opt.Sockets, 1)
	opt.RxBatch = min(max(opt.RxBatch, 1), 1024)
	u := &UDP{idx: idx, replicas: replicas, opt: opt, closed: make(chan struct{})}
	lc := net.ListenConfig{}
	if opt.Sockets > 1 {
		lc.Control = reusePort
	}
	for i := 0; i < opt.Sockets; i++ {
		addr := bind
		if i > 0 { // the rest join the first socket's (possibly ephemeral) port
			addr = u.conns[0].LocalAddr().String()
		}
		pc, err := lc.ListenPacket(context.Background(), "udp4", addr)
		if err != nil {
			u.Close()
			return nil, err
		}
		c := pc.(*net.UDPConn)
		// Ask for big socket buffers, shrinking on failure: an over-limit request fails
		// outright (macOS caps at kern.ipc.maxsockbuf) and leaves the small default, and
		// bursts then overflow it.
		for sz := 32 << 20; sz >= 256<<10 && c.SetReadBuffer(sz) != nil; sz = sz * 3 / 4 {
		}
		for sz := 32 << 20; sz >= 256<<10 && c.SetWriteBuffer(sz) != nil; sz = sz * 3 / 4 {
		}
		u.conns = append(u.conns, c)
		u.pcs = append(u.pcs, ipv4.NewPacketConn(c))
	}
	u.self = unmap(u.conns[0].LocalAddr().(*net.UDPAddr).AddrPort())
	if idx >= 0 && idx < len(replicas) {
		u.self = replicas[idx]
	}
	for _, r := range replicas {
		u.repUDP = append(u.repUDP, net.UDPAddrFromAddrPort(r))
	}
	return u, nil
}

func (u *UDP) Self() Addr             { return u.self }
func (u *UDP) ReplicaAddr(i int) Addr { return u.replicas[i] }
func (u *UDP) NumReplicas() int       { return len(u.replicas) }
func (u *UDP) Sockets() int           { return len(u.conns) }

func (u *UDP) conn() *net.UDPConn {
	if len(u.conns) == 1 {
		return u.conns[0]
	}
	return u.conns[u.next.Add(1)%uint32(len(u.conns))]
}

func (u *UDP) Send(to Addr, m wire.Message) {
	bp := bufPool.Get().(*[]byte)
	b := wire.Marshal((*bp)[:0], m)
	if len(b) > MaxDatagram {
		log.Printf("udp: dropping %d-byte message type %d (too big)", len(b), m.Type())
	} else {
		// Errors (e.g. ENOBUFS) are transient: UDP is lossy anyway; the protocol re-sends.
		_, _ = u.conn().WriteToUDPAddrPort(b, to)
	}
	*bp = b[:0]
	bufPool.Put(bp)
}

func (u *UDP) SendReplica(i int, m wire.Message) {
	if i != u.idx && i >= 0 && i < len(u.replicas) {
		u.Send(u.replicas[i], m)
	}
}

type bcastScratch struct {
	buf  []byte
	msgs []ipv4.Message
	bufs [][]byte
}

var bcastPool = sync.Pool{New: func() any { return &bcastScratch{} }}

// Broadcast encodes once and sends to every replica except self in one sendmmsg (Linux).
func (u *UDP) Broadcast(m wire.Message) {
	s := bcastPool.Get().(*bcastScratch)
	s.buf = wire.Marshal(s.buf[:0], m)
	if len(s.buf) <= MaxDatagram {
		s.msgs = s.msgs[:0]
		s.bufs = s.bufs[:0]
		for i := range u.replicas {
			if i != u.idx {
				s.bufs = append(s.bufs, s.buf)
			}
		}
		j := 0
		for i := range u.replicas {
			if i != u.idx {
				s.msgs = append(s.msgs, ipv4.Message{Buffers: s.bufs[j : j+1], Addr: u.repUDP[i]})
				j++
			}
		}
		writeAll(u.pcs[u.next.Add(1)%uint32(len(u.pcs))], s.msgs)
	}
	bcastPool.Put(s)
}

// writeAll sends every message: WriteBatch is one sendmmsg on Linux but writes a single
// message elsewhere. A failed datagram is skipped (UDP is lossy; the protocol re-sends).
func writeAll(pc *ipv4.PacketConn, ms []ipv4.Message) {
	for len(ms) > 0 {
		n, err := pc.WriteBatch(ms, 0)
		if err != nil || n <= 0 {
			n = 1
		}
		ms = ms[n:]
	}
}

// BatchWriter queues datagrams and sends them with one sendmmsg (Linux) per Flush. Not
// safe for concurrent use: each sender goroutine owns one (on its own socket).
type BatchWriter struct {
	u     *UDP
	pc    *ipv4.PacketConn
	arena []byte
	ends  []int
	addrs []*net.UDPAddr
	msgs  []ipv4.Message
	bufs  [][]byte
	cache map[Addr]*net.UDPAddr
}

// NewBatchWriter sends on socket sock % Sockets.
func (u *UDP) NewBatchWriter(sock int) *BatchWriter {
	return &BatchWriter{u: u, pc: u.pcs[sock%len(u.pcs)], cache: make(map[Addr]*net.UDPAddr)}
}

func (w *BatchWriter) udpAddr(a Addr) *net.UDPAddr {
	if ua, ok := w.cache[a]; ok {
		return ua
	}
	if len(w.cache) >= 1<<14 {
		clear(w.cache)
	}
	ua := net.UDPAddrFromAddrPort(a)
	w.cache[a] = ua
	return ua
}

// Add encodes m for to. A batch flushes itself at 64 datagrams.
func (w *BatchWriter) Add(to Addr, m wire.Message) {
	start := len(w.arena)
	w.arena = wire.Marshal(w.arena, m)
	if len(w.arena)-start > MaxDatagram {
		log.Printf("udp: dropping %d-byte message type %d (too big)", len(w.arena)-start, m.Type())
		w.arena = w.arena[:start]
		return
	}
	w.ends = append(w.ends, len(w.arena))
	w.addrs = append(w.addrs, w.udpAddr(to))
	if len(w.ends) >= 64 {
		w.Flush()
	}
}

func (w *BatchWriter) Pending() int { return len(w.ends) }

func (w *BatchWriter) Flush() {
	if len(w.ends) == 0 {
		return
	}
	w.msgs = w.msgs[:0]
	w.bufs = w.bufs[:0]
	start := 0
	for _, end := range w.ends {
		w.bufs = append(w.bufs, w.arena[start:end])
		start = end
	}
	for i := range w.ends {
		w.msgs = append(w.msgs, ipv4.Message{Buffers: w.bufs[i : i+1], Addr: w.addrs[i]})
	}
	writeAll(w.pc, w.msgs)
	clear(w.addrs)
	w.arena, w.ends, w.addrs = w.arena[:0], w.ends[:0], w.addrs[:0]
}

func (u *UDP) isClosed() bool {
	select {
	case <-u.closed:
		return true
	default:
		return false
	}
}

// rxBatch receives a batch of datagrams per call (platform-specific).
type rxBatch interface {
	read() (int, error)
	msg(i int) (data []byte, from Addr, ok bool) // valid until the next read
}

// Run starts one receive goroutine per socket handing decoded messages to h
// (concurrently when Sockets > 1) and returns once all of them stop (Close).
func (u *UDP) Run(h Handler) {
	var wg sync.WaitGroup
	for i := range u.conns {
		wg.Add(1)
		go func(rx rxBatch) {
			defer wg.Done()
			u.rxLoop(rx, h)
		}(newRxBatch(u.conns[i], u.pcs[i], u.opt.RxBatch))
	}
	wg.Wait()
}

func (u *UDP) rxLoop(rx rxBatch, h Handler) {
	for {
		n, err := rx.read()
		if err != nil {
			if u.isClosed() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		for i := 0; i < n; i++ {
			b, from, ok := rx.msg(i)
			if !ok {
				continue
			}
			m, err := wire.Unmarshal(b)
			if err != nil {
				continue // garbage: drop
			}
			h.Receive(from, m)
		}
	}
}

// unmap normalizes 4-in-6 addresses: a udp4 socket can't send to [::ffff:a.b.c.d].
func unmap(a Addr) Addr { return netip.AddrPortFrom(a.Addr().Unmap(), a.Port()) }

func (u *UDP) Close() {
	select {
	case <-u.closed:
	default:
		close(u.closed)
		for _, c := range u.conns {
			c.Close()
		}
	}
}
