//go:build unix

package transport

import (
	"syscall"

	"golang.org/x/sys/unix"
)

// reusePort lets several sockets bind one port. Linux spreads incoming datagrams over
// them by 4-tuple hash (each peer / client sticks to one socket); macOS delivers
// unicast to just one, so there the extra sockets only add send parallelism.
func reusePort(_, _ string, c syscall.RawConn) error {
	var serr error
	err := c.Control(func(fd uintptr) {
		serr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
	})
	if err != nil {
		return err
	}
	return serr
}
