package clock

import (
	"testing"
	"time"
)

func TestClock_MockClockIsControllable(t *testing.T) {
	c := NewMock(1000)
	if c.Now() != 1000 {
		t.Fatal(c.Now())
	}
	c.Advance(250)
	if c.Now() != 1250 {
		t.Fatal(c.Now())
	}
	c.Set(42)
	if c.Now() != 42 {
		t.Fatal(c.Now())
	}
}

func TestClock_MockSleepWakesOnAdvanceAndInterrupt(t *testing.T) {
	c := NewMock(0)
	done := make(chan struct{})
	go func() { c.SleepUntil(100); close(done) }()
	c.Advance(50)
	select {
	case <-done:
		t.Fatal("woke before deadline")
	case <-time.After(10 * time.Millisecond):
	}
	c.Advance(50)
	<-done

	done = make(chan struct{})
	go func() { c.SleepUntil(Never); close(done) }()
	c.Interrupt()
	<-done
}

func TestClock_RealInterruptIsSticky(t *testing.T) {
	for _, bp := range []bool{false, true} {
		c := NewReal(bp)
		c.Interrupt()
		start := time.Now()
		c.SleepUntil(Never) // must return immediately: interrupt arrived first
		if time.Since(start) > time.Second {
			t.Fatal("sticky interrupt lost")
		}
		c.SleepUntil(c.Now() + 1000) // a real deadline returns
	}
}
