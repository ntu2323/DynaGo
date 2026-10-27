package orderer

import (
	"sync"
	"testing"
	"time"

	"dynago/internal/clock"
	"dynago/internal/topology"
	"dynago/internal/types"
)

func id(c, r uint64) types.TxnId { return types.ID(c, r) }

func mkWrite(client, req, t uint64, key string) *types.TxnEntry {
	return types.NewEntry(types.Txn{ClientID: client, RequestID: req, Type: types.Write,
		WSet: []types.KV{{Key: key, Value: "v"}}, T: t})
}

func mkRead(client, req, t uint64, key string) *types.TxnEntry {
	return types.NewEntry(types.Txn{ClientID: client, RequestID: req, Type: types.Read,
		RSet: []string{key}, T: t})
}

type released struct {
	id      types.TxnId
	t       uint64
	status  types.Status
	results map[string]string
	leader  bool
}

// collector records released entries (thread-safe).
type collector struct {
	mu   sync.Mutex
	cond *sync.Cond
	got  []released
}

func newCollector() *collector {
	c := &collector{}
	c.cond = sync.NewCond(&c.mu)
	return c
}

func (c *collector) cb() OnSpecReply {
	return func(e *types.TxnEntry) {
		c.mu.Lock()
		c.got = append(c.got, released{e.Key(), e.T, e.Status, e.Results, e.IsLeaderForThis})
		c.mu.Unlock()
		c.cond.Broadcast()
	}
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.got)
}

func (c *collector) at(i int) released {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.got[i]
}

// waitFor blocks until n entries arrived or the timeout passed.
func (c *collector) waitFor(n int, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if c.count() >= n {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return c.count() >= n
}

func newOrderer(server uint32, clk clock.Clock, n uint32, c *collector) *Orderer {
	return New(server, 0, clk, topology.New(n), c.cb(), nil, 0)
}

func mustEntry(t *testing.T, o *Orderer, i types.TxnId) (types.Status, uint64) {
	t.Helper()
	st, ts, ok := o.SpecLogEntry(i)
	if !ok {
		t.Fatalf("%v not in specLog", i)
	}
	return st, ts
}

func (c *collector) reset() {
	c.mu.Lock()
	c.got = nil
	c.mu.Unlock()
}
