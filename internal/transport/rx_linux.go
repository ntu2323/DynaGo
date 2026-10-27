//go:build linux && (amd64 || arm64)

package transport

import (
	"encoding/binary"
	"net"
	"net/netip"
	"unsafe"

	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

// mmsghdr is struct mmsghdr (64-bit Linux).
type mmsghdr struct {
	hdr unix.Msghdr
	n   uint32
	_   [4]byte
}

// mmsgReader receives up to len(hdrs) datagrams per recvmmsg, decoding source
// addresses in place (x/net's ReadBatch allocates a *net.UDPAddr per datagram).
type mmsgReader struct {
	conn  *net.UDPConn
	hdrs  []mmsghdr
	iovs  []unix.Iovec
	names []unix.RawSockaddrInet4
	bufs  [][]byte
	n     int
}

func newRxBatch(c *net.UDPConn, _ *ipv4.PacketConn, n int) rxBatch {
	r := &mmsgReader{conn: c, hdrs: make([]mmsghdr, n), iovs: make([]unix.Iovec, n),
		names: make([]unix.RawSockaddrInet4, n), bufs: make([][]byte, n)}
	for i := range r.hdrs {
		r.bufs[i] = make([]byte, 65536)
		r.iovs[i].Base = &r.bufs[i][0]
		r.iovs[i].SetLen(len(r.bufs[i]))
		r.hdrs[i].hdr.Iov = &r.iovs[i]
		r.hdrs[i].hdr.Iovlen = 1
		r.hdrs[i].hdr.Name = (*byte)(unsafe.Pointer(&r.names[i]))
	}
	return r
}

func (r *mmsgReader) read() (int, error) {
	rc, err := r.conn.SyscallConn()
	if err != nil {
		return 0, err
	}
	for i := range r.hdrs {
		r.hdrs[i].hdr.Namelen = unix.SizeofSockaddrInet4
		r.hdrs[i].n = 0
	}
	var n int
	var serr error
	err = rc.Read(func(fd uintptr) bool {
		r1, _, e := unix.Syscall6(unix.SYS_RECVMMSG, fd, uintptr(unsafe.Pointer(&r.hdrs[0])),
			uintptr(len(r.hdrs)), 0, 0, 0)
		if e == unix.EAGAIN || e == unix.EINTR {
			return false // wait for readability (the poller re-invokes us)
		}
		if e != 0 {
			serr = e
		}
		n = int(r1)
		return true
	})
	if err != nil {
		return 0, err
	}
	if serr != nil {
		return 0, serr
	}
	r.n = n
	return n, nil
}

func (r *mmsgReader) msg(i int) ([]byte, Addr, bool) {
	sa := &r.names[i]
	if sa.Family != unix.AF_INET {
		return nil, Addr{}, false
	}
	port := binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&sa.Port))[:])
	return r.bufs[i][:r.hdrs[i].n], netip.AddrPortFrom(netip.AddrFrom4(sa.Addr), port), true
}
