//go:build !(linux && (amd64 || arm64))

package transport

import (
	"net"

	"golang.org/x/net/ipv4"
)

// xnetReader: x/net ReadBatch (recvmmsg on other Linux arches, one datagram elsewhere).
type xnetReader struct {
	pc *ipv4.PacketConn
	ms []ipv4.Message
}

func newRxBatch(_ *net.UDPConn, pc *ipv4.PacketConn, n int) rxBatch {
	r := &xnetReader{pc: pc, ms: make([]ipv4.Message, n)}
	for i := range r.ms {
		r.ms[i].Buffers = [][]byte{make([]byte, 65536)}
	}
	return r
}

func (r *xnetReader) read() (int, error) { return r.pc.ReadBatch(r.ms, 0) }

func (r *xnetReader) msg(i int) ([]byte, Addr, bool) {
	ua, ok := r.ms[i].Addr.(*net.UDPAddr)
	if !ok {
		return nil, Addr{}, false
	}
	return r.ms[i].Buffers[0][:r.ms[i].N], unmap(ua.AddrPort()), true
}
