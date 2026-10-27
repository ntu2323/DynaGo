// Package deadline is the OWD-feedback machinery (C++ dyna/deadline.h).
//   - ReplicaOwdEstimator: per-client sliding window; reports a percentile of the
//     observed one-way delays.
//   - ClientBoundTracker: bound = max over the latest per-replica OWD reports, clamped
//     to [initial, max]; the client's deadline is send time + bound.
package deadline

import (
	"os"
	"slices"
	"strconv"
)

type Config struct {
	InitialBoundUS uint64
	MaxOwdUS       uint64
	OwdWindowLen   uint32
	// p95: at p50 the deadline sits at the median delay, so under load a large share of
	// arrivals miss it (~30% late at 25k ops/s on CloudLab vs ~3% at p95).
	OwdPercentile uint32
}

func DefaultConfig() Config {
	return Config{InitialBoundUS: 500, MaxOwdUS: 10000, OwdWindowLen: 100, OwdPercentile: 95}
}

// EnvU64 reads a decimal env var, falling back on absence or a parse error.
func EnvU64(name string, fallback uint64) uint64 {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	p, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return fallback
	}
	return p
}

func FromEnv() Config {
	c := DefaultConfig()
	c.InitialBoundUS = EnvU64("DYNA_INITIAL_BOUND_US", c.InitialBoundUS)
	c.MaxOwdUS = EnvU64("DYNA_MAX_OWD_US", c.MaxOwdUS)
	c.OwdWindowLen = uint32(EnvU64("DYNA_OWD_WINDOW_LEN", uint64(c.OwdWindowLen)))
	c.OwdPercentile = uint32(EnvU64("DYNA_OWD_PERCENTILE", uint64(c.OwdPercentile)))
	if c.OwdWindowLen == 0 {
		c.OwdWindowLen = 1
	}
	if c.OwdPercentile > 100 {
		c.OwdPercentile = 100
	}
	return c
}

type window struct {
	samples []uint64
	next    uint64
	cache   uint64
	cached  bool
}

// ReplicaOwdEstimator is not thread-safe: each lane owns one.
type ReplicaOwdEstimator struct {
	cfg     Config
	windows map[uint64]*window
	scratch []uint64
}

func NewReplicaOwdEstimator(cfg Config) *ReplicaOwdEstimator {
	return &ReplicaOwdEstimator{cfg: cfg, windows: make(map[uint64]*window)}
}

const recomputeEvery = 16

func (e *ReplicaOwdEstimator) Sample(clientID, delayUS uint64) {
	clamped := min(delayUS, e.cfg.MaxOwdUS)
	w := e.windows[clientID]
	if w == nil {
		w = &window{samples: make([]uint64, 0, e.cfg.OwdWindowLen)}
		e.windows[clientID] = w
	}
	full := uint32(len(w.samples)) >= e.cfg.OwdWindowLen
	if !full {
		w.samples = append(w.samples, clamped)
	} else {
		w.samples[w.next%uint64(e.cfg.OwdWindowLen)] = clamped
	}
	w.next++
	if !full || w.next%recomputeEvery == 0 || !w.cached {
		w.cache = e.percentileOf(w.samples)
		w.cached = true
	}
}

func (e *ReplicaOwdEstimator) EstimateUS(clientID uint64) uint64 {
	if w := e.windows[clientID]; w != nil {
		return w.cache
	}
	return 0
}

func (e *ReplicaOwdEstimator) MaxEstimateUS() uint64 {
	var m uint64
	for _, w := range e.windows {
		m = max(m, w.cache)
	}
	return m
}

func (e *ReplicaOwdEstimator) percentileOf(w []uint64) uint64 {
	if len(w) == 0 {
		return 0
	}
	e.scratch = append(e.scratch[:0], w...)
	slices.Sort(e.scratch)
	return e.scratch[int(e.cfg.OwdPercentile)*(len(e.scratch)-1)/100]
}

// ClientBoundTracker: the client's deadline bound.
type ClientBoundTracker struct {
	cfg             Config
	lastOwd         []uint64
	boundUS         uint64
	congestionBound uint64
}

const congestionMaxUS = 20000

func NewClientBoundTracker(cfg Config, numReplicas int) *ClientBoundTracker {
	return &ClientBoundTracker{cfg: cfg, lastOwd: make([]uint64, max(1, numReplicas)), boundUS: cfg.InitialBoundUS}
}

func (c *ClientBoundTracker) BoundUS() uint64 { return c.boundUS }

func (c *ClientBoundTracker) OnOwdSample(replicaID uint32, owdUS uint64) {
	if owdUS == 0 || int(replicaID) >= len(c.lastOwd) {
		return
	}
	c.lastOwd[replicaID] = min(owdUS, c.cfg.MaxOwdUS)
	c.recompute()
}

// OnObservedHold: a reply released later than the client's bound (queueing at the
// replica) raises a decaying congestion floor.
func (c *ClientBoundTracker) OnObservedHold(sendUS, replyDeadlineUS uint64) {
	if replyDeadlineUS <= sendUS {
		return
	}
	hold := replyDeadlineUS - sendUS
	if hold > c.boundUS {
		c.congestionBound = max(c.congestionBound, min(hold, congestionMaxUS))
	} else if c.congestionBound > 0 {
		c.congestionBound -= c.congestionBound/32 + 1
	}
	c.recompute()
}

func (c *ClientBoundTracker) recompute() {
	var m uint64
	for _, o := range c.lastOwd {
		m = max(m, o)
	}
	owdBound := c.cfg.InitialBoundUS
	if m != 0 {
		owdBound = min(c.cfg.MaxOwdUS, max(c.cfg.InitialBoundUS, m))
	}
	c.boundUS = max(owdBound, c.congestionBound)
}
