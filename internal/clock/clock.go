// Package clock is the time seam for the release loop (C++ dyna/wall_clock.h).
//
// A Clock owns both "read the time" and "wait until the next deadline", so tests can
// drive the release loop deterministically with virtual time (Mock).
//   - SleepUntil(t) blocks until Now() >= t or Interrupt() is called.
//   - Interrupt() wakes a blocked (or about-to-block) SleepUntil; it is sticky.
//
// Protocol code must never read time.Now() directly: always go through a Clock.
package clock

import (
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Never is the "no deadline" value for SleepUntil.
const Never = math.MaxUint64

type Clock interface {
	Now() uint64 // microseconds since the Unix epoch (NTP-comparable across hosts)
	SleepUntil(untilUS uint64)
	Interrupt()
}

// WallMicros is the real wall clock in microseconds.
func WallMicros() uint64 { return uint64(time.Now().UnixMicro()) }

// Real is the wall clock. With busyPoll it spins to the deadline (µs-precise release
// when the transport busy-polls too); otherwise it parks on a timer.
type Real struct {
	busyPoll    bool
	interrupted atomic.Bool
	wake        chan struct{} // capacity 1: a sticky wakeup for the parked waiter
}

func NewReal(busyPoll bool) *Real {
	return &Real{busyPoll: busyPoll, wake: make(chan struct{}, 1)}
}

func (c *Real) Now() uint64 { return WallMicros() }

func (c *Real) SleepUntil(until uint64) {
	if c.busyPoll {
		for until == Never || WallMicros() < until {
			if c.interrupted.Swap(false) {
				return
			}
			runtime.Gosched()
		}
		c.interrupted.Store(false)
		return
	}
	if c.interrupted.Swap(false) {
		c.drain()
		return
	}
	if until == Never {
		<-c.wake
		c.interrupted.Store(false)
		return
	}
	now := WallMicros()
	if now >= until {
		return
	}
	t := time.NewTimer(time.Duration(until-now) * time.Microsecond)
	select {
	case <-c.wake:
	case <-t.C:
	}
	t.Stop()
	c.interrupted.Store(false)
}

func (c *Real) drain() {
	select {
	case <-c.wake:
	default:
	}
}

func (c *Real) Interrupt() {
	c.interrupted.Store(true)
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Mock is a controllable clock for tests: virtual time moves only when told to, and
// every move wakes any waiter so the release loop re-evaluates deterministically.
type Mock struct {
	mu          sync.Mutex
	cond        *sync.Cond
	now         uint64
	interrupted bool
}

func NewMock(now uint64) *Mock {
	m := &Mock{now: now}
	m.cond = sync.NewCond(&m.mu)
	return m
}

func (m *Mock) Now() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *Mock) SleepUntil(until uint64) {
	m.mu.Lock()
	for !m.interrupted && m.now < until {
		m.cond.Wait()
	}
	m.interrupted = false
	m.mu.Unlock()
}

func (m *Mock) Interrupt() {
	m.mu.Lock()
	m.interrupted = true
	m.mu.Unlock()
	m.cond.Broadcast()
}

func (m *Mock) Set(now uint64) {
	m.mu.Lock()
	m.now = now
	m.mu.Unlock()
	m.cond.Broadcast()
}

func (m *Mock) Advance(delta uint64) {
	m.mu.Lock()
	m.now += delta
	m.mu.Unlock()
	m.cond.Broadcast()
}
