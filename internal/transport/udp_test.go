package transport

import (
	"testing"
	"time"

	"dynago/internal/wire"
)

type chanHandler chan wire.Message

func (c chanHandler) Receive(from Addr, m wire.Message) { c <- m }

func TestUDP_LoopbackRoundTrip(t *testing.T) {
	a, err := ListenUDP("127.0.0.1:0", -1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := ListenUDP("127.0.0.1:0", -1, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	got := make(chanHandler, 1)
	go b.Run(got)
	a.Send(b.Self(), &wire.FastCommit{ClientID: 1, RequestID: 2, ViewID: 3, AgreedT: 4})
	select {
	case m := <-got:
		if fc, ok := m.(*wire.FastCommit); !ok || fc.AgreedT != 4 {
			t.Fatalf("%+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no datagram")
	}
}

// Multi-socket replicas: a BatchWriter batch and a Broadcast reach every peer intact,
// whichever of the receiver's SO_REUSEPORT sockets the kernel picks.
func TestUDP_MultiSocketBatchAndBroadcast(t *testing.T) {
	const n = 3
	probe := make([]*UDP, n)
	addrs := make([]Addr, n)
	for i := range probe { // reserve three free ports
		p, err := ListenUDP("127.0.0.1:0", -1, nil)
		if err != nil {
			t.Fatal(err)
		}
		addrs[i] = p.Self()
		probe[i] = p
	}
	for _, p := range probe {
		p.Close()
	}
	us := make([]*UDP, n)
	got := make([]chanHandler, n)
	for i := range us {
		u, err := ListenUDPOpts(addrs[i].String(), i, addrs, UDPOptions{Sockets: 3, RxBatch: 16})
		if err != nil {
			t.Fatal(err)
		}
		defer u.Close()
		us[i], got[i] = u, make(chanHandler, 1024)
		go u.Run(got[i])
	}
	w := us[0].NewBatchWriter(1)
	const k = 200
	for i := 0; i < k; i++ {
		w.Add(addrs[1], &wire.FastCommit{ClientID: 7, RequestID: uint64(i), AgreedT: uint64(i)})
	}
	w.Flush()
	us[2].Broadcast(&wire.ViewInfo{ViewID: 9, Alive: 7})
	seen := map[uint64]bool{}
	deadline := time.After(3 * time.Second)
	views := 0
	for len(seen) < k || views < 2 {
		select {
		case m := <-got[1]:
			switch m := m.(type) {
			case *wire.FastCommit:
				if m.AgreedT != m.RequestID {
					t.Fatalf("corrupt %+v", m)
				}
				seen[m.RequestID] = true
			case *wire.ViewInfo:
				views++
			}
		case m := <-got[0]:
			if v, ok := m.(*wire.ViewInfo); ok && v.ViewID == 9 {
				views++
			}
		case m := <-got[2]:
			t.Fatalf("broadcast reached its sender: %+v", m)
		case <-deadline:
			t.Fatalf("got %d/%d batch datagrams, %d/2 broadcasts", len(seen), k, views)
		}
	}
}
