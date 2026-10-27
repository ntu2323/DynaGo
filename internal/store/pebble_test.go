package store_test

import (
	"sync"
	"testing"
	"time"

	"dynago/internal/clock"
	"dynago/internal/orderer"
	"dynago/internal/store"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func openPebble(t *testing.T) *store.Pebble {
	p, err := store.OpenPebble(t.TempDir(), 16)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

// C++ TEST(DbBackend, AppliesAndReadsVersionedValues), on Pebble.
func TestDbBackend_AppliesAndReadsVersionedValues(t *testing.T) {
	p := openPebble(t)
	if _, ok := p.Read("k"); ok {
		t.Fatal("read of an absent key")
	}
	p.Apply("k", "v1", 100)
	if v, ok := p.Read("k"); !ok || v.Value != "v1" || v.T != 100 {
		t.Fatalf("%+v %v", v, ok)
	}
	p.Apply("k", "v2", 200) // overwrite in place
	if v, ok := p.Read("k"); !ok || v.Value != "v2" || v.T != 200 {
		t.Fatalf("%+v %v", v, ok)
	}
}

// C++ TEST(DbBackend, SplinterDbWriteThenLeaderReadThroughExecutor), on Pebble: the
// running executor applies a COMMITTED write to the disk store, then serves a leader
// read (and a clean miss) from it.
func TestDbBackend_PebbleWriteThenLeaderReadThroughExecutor(t *testing.T) {
	p := openPebble(t)
	clk := clock.NewMock(50)
	var mu sync.Mutex
	replies := map[types.TxnId]string{} // id -> value read for "K"
	o := orderer.New(0, 0, clk, topology.New(1), func(e *types.TxnEntry) {
		mu.Lock()
		replies[e.Key()] = e.Results["K"]
		mu.Unlock()
	}, p, 0)
	o.Start(true)
	defer o.Stop()
	waitFor := func(id types.TxnId) (string, bool) {
		for i := 0; i < 5000; i++ {
			mu.Lock()
			v, ok := replies[id]
			mu.Unlock()
			if ok {
				return v, true
			}
			time.Sleep(time.Millisecond)
		}
		return "", false
	}
	w := types.ID(1, 1)
	o.SubmitTxn(types.NewEntry(types.Txn{ClientID: 1, RequestID: 1, Type: types.Write,
		WSet: []types.KV{{Key: "K", Value: "v1"}}, T: 100}), true)
	clk.Set(100)
	if _, ok := waitFor(w); !ok {
		t.Fatal("no SpecReply for the write")
	}
	if res, _ := o.ApplyCommit(w, 100); res != orderer.Applied {
		t.Fatal(res)
	}
	o.MarkCommitted(w)
	for i := 0; i < 5000 && !o.Executed(w); i++ {
		time.Sleep(time.Millisecond)
	}
	if !o.Executed(w) {
		t.Fatal("write never executed")
	}
	read := func(req, at uint64, key string) string {
		o.SubmitTxn(types.NewEntry(types.Txn{ClientID: 1, RequestID: req, Type: types.Read,
			RSet: []string{key}, T: at}), true)
		clk.Set(at)
		v, ok := waitFor(types.ID(1, req))
		if !ok {
			t.Fatalf("no reply for read %d", req)
		}
		return v
	}
	if got := read(2, 200, "K"); got != "v1" {
		t.Fatalf("leader read %q", got)
	}
	if got := read(3, 300, "never-written"); got != "" {
		t.Fatalf("miss read %q", got)
	}
	if v, ok := p.Read("K"); !ok || v.Value != "v1" || v.T != 100 {
		t.Fatalf("stored %+v", v)
	}

	// A write and a larger leader read executed in ONE pass: the read waits for the
	// write's commit, and must see it although the pass stages writes in a batch.
	w2 := types.ID(1, 4)
	o.SubmitTxn(types.NewEntry(types.Txn{ClientID: 1, RequestID: 4, Type: types.Write,
		WSet: []types.KV{{Key: "K", Value: "v2"}}, T: 400}), true)
	o.SubmitTxn(types.NewEntry(types.Txn{ClientID: 1, RequestID: 5, Type: types.Read,
		RSet: []string{"K"}, T: 500}), true)
	clk.Set(500)
	if _, ok := waitFor(w2); !ok {
		t.Fatal("no SpecReply for the second write")
	}
	time.Sleep(20 * time.Millisecond) // the read is released and blocked by the write
	if res, _ := o.ApplyCommit(w2, 400); res != orderer.Applied {
		t.Fatal(res)
	}
	o.MarkCommitted(w2)
	if got, ok := waitFor(types.ID(1, 5)); !ok || got != "v2" {
		t.Fatalf("same-pass leader read %q (ok=%v), want v2", got, ok)
	}
}
