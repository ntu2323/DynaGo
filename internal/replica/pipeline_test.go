package replica_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"dynago/internal/client"
	"dynago/internal/replica"
	"dynago/internal/store"
	"dynago/internal/transport"
	"dynago/internal/types"
)

// udpCluster runs n replicas in pipeline mode (lane goroutines + senders) over real
// loopback UDP on the real clock.
type udpCluster struct {
	addrs    []transport.Addr
	trs      []*transport.UDP
	replicas []*replica.Replica
	stores   []*store.Mem
}

func freeAddrs(t *testing.T, n int) []transport.Addr {
	var out []transport.Addr
	var probes []*transport.UDP
	for i := 0; i < n; i++ {
		p, err := transport.ListenUDP("127.0.0.1:0", -1, nil)
		if err != nil {
			t.Fatal(err)
		}
		probes = append(probes, p)
		out = append(out, p.Self())
	}
	for _, p := range probes {
		p.Close()
	}
	return out
}

func newUDPCluster(t *testing.T, f, lanes, sockets int) *udpCluster {
	n := 2*f + 1
	c := &udpCluster{addrs: freeAddrs(t, n)}
	dir := t.TempDir()
	for i := 0; i < n; i++ {
		tr, err := transport.ListenUDPOpts(c.addrs[i].String(), i, c.addrs,
			transport.UDPOptions{Sockets: sockets, RxBatch: 32})
		if err != nil {
			t.Fatal(err)
		}
		st := store.NewMem()
		r := replica.New(replica.Options{Index: i, F: f, Transport: tr, Store: st, Lanes: lanes,
			VRStateDir: dir, Quiet: true})
		go tr.Run(r)
		c.trs = append(c.trs, tr)
		c.replicas = append(c.replicas, r)
		c.stores = append(c.stores, st)
	}
	t.Cleanup(func() {
		for i := range c.replicas {
			c.trs[i].Close()
			c.replicas[i].Stop()
		}
	})
	return c
}

// Many clients with many outstanding writes each, through 4 lanes, 2 senders and
// 2 sockets per replica: every write commits, executes everywhere, and the stores agree.
func TestPipeline_UDPClusterWritesCommitAndConverge(t *testing.T) {
	t.Setenv("DYNA_BUSY_POLL", "0")
	t.Setenv("DYNA_INITIAL_BOUND_US", "3000")
	t.Setenv("DYNA_SEND_THREADS", "2")
	t.Setenv("DYNA_FAIL_TIMEOUT_MS", "0") // -race slows everything: no false view changes
	c := newUDPCluster(t, 1, 4, 2)

	const clients, perClient, keys = 6, 150, 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	committed := 0
	for ci := 0; ci < clients; ci++ {
		tr, err := transport.ListenUDP("127.0.0.1:0", -1, c.addrs)
		if err != nil {
			t.Fatal(err)
		}
		defer tr.Close()
		cl := client.New(client.Options{ID: uint64(1000 + ci), F: 1, Transport: tr})
		go tr.Run(cl)
		cl.Start()
		defer cl.Stop()
		for k := 0; k < perClient; k++ {
			wg.Add(1)
			key := fmt.Sprintf("k%02d", (ci*perClient+k)%keys)
			cl.Write([]types.KV{{Key: key, Value: fmt.Sprintf("c%d-%d", ci, k)}}, func(client.Result) {
				mu.Lock()
				committed++
				mu.Unlock()
				wg.Done()
			})
		}
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("only %d/%d writes committed", committed, clients*perClient)
	}
	want := clients * perClient
	deadline := time.Now().Add(15 * time.Second)
	for {
		ok := true
		for _, r := range c.replicas {
			if r.Orderer().ExecutedCount() != want {
				ok = false
			}
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			for i, r := range c.replicas {
				t.Logf("r%d executed %d speclog %d", i, r.Orderer().ExecutedCount(), r.Orderer().SpecLogSize())
			}
			t.Fatalf("not every write executed everywhere")
		}
		time.Sleep(20 * time.Millisecond)
	}
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("k%02d", k)
		v0 := c.stores[0].ValueOrEmpty(key)
		for i := 1; i < len(c.stores); i++ {
			if v := c.stores[i].ValueOrEmpty(key); v != v0 {
				for j, st := range c.stores {
					vv, _ := st.Read(key)
					t.Logf("r%d %s = %q t=%d", j, key, vv.Value, vv.T)
				}
				t.Fatalf("%s: r0=%q r%d=%q", key, v0, i, v)
			}
		}
	}
}
