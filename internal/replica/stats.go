package replica

// DYNA_STATS=1: a per-second "rX stats:" line (C++ DYNA_STATS): receive mix, shed
// requests, lane / sender busy fractions, arrival classification (on time vs late at a
// leader / follower, mean slack and lateness), releases, executor passes, buffer sizes,
// GC and process CPU.

import (
	"fmt"
	"log"
	"runtime/metrics"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"dynago/internal/orderer"
	"dynago/internal/wire"
)

type rxStats struct {
	req, commit, bg, peer, vr atomic.Uint64
	// Longest background scan / heartbeat tick since the last print (stall detectors).
	maxTickNS, maxHbNS atomic.Uint64
}

func maxStore(a *atomic.Uint64, v uint64) {
	for {
		cur := a.Load()
		if v <= cur || a.CompareAndSwap(cur, v) {
			return
		}
	}
}

func (s *rxStats) count(m wire.Message) {
	switch m.(type) {
	case *wire.ClientRequest:
		s.req.Add(1)
	case *wire.FastCommit, *wire.FastCommitBatch, *wire.SlowCommit:
		s.commit.Add(1)
	case *wire.BgBatch:
		s.bg.Add(1)
	case *wire.FetchRequest, *wire.FetchReply, *wire.TsAgree, *wire.ReplicatePrepare,
		*wire.ReplicatePrepareReply, *wire.ReplicateCommit:
		s.peer.Add(1)
	default:
		s.vr.Add(1)
	}
}

type statsPrev struct {
	at                       time.Time
	req, commit, bg, peer    uint64
	shed                     uint64
	ord                      orderer.Counters
	laneBusy, laneMsgs       []uint64
	sendBusy, dgrams, combin []uint64
	replies                  []uint64
	cpu                      time.Duration
	gcCycles, gcCPU          float64
}

func procCPU() time.Duration {
	var ru syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &ru) != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

var gcSamples = []metrics.Sample{
	{Name: "/gc/cycles/total:gc-cycles"},
	{Name: "/cpu/classes/gc/total:cpu-seconds"},
	{Name: "/gc/heap/live:bytes"},
	{Name: "/sched/pauses/total/gc:seconds"},
}

func readGC() (cycles, cpuSec float64, liveMB float64, pauses *metrics.Float64Histogram) {
	metrics.Read(gcSamples)
	f := func(s metrics.Sample) float64 {
		switch s.Value.Kind() {
		case metrics.KindUint64:
			return float64(s.Value.Uint64())
		case metrics.KindFloat64:
			return s.Value.Float64()
		}
		return 0
	}
	if gcSamples[3].Value.Kind() == metrics.KindFloat64Histogram {
		pauses = gcSamples[3].Value.Float64Histogram()
	}
	return f(gcSamples[0]), f(gcSamples[1]), f(gcSamples[2]) / (1 << 20), pauses
}

// maxNewPause: the largest bucket that gained counts since prev (an upper bound).
func maxNewPause(prev []uint64, h *metrics.Float64Histogram) time.Duration {
	if h == nil {
		return 0
	}
	for i := len(h.Counts) - 1; i >= 0; i-- {
		var before uint64
		if i < len(prev) {
			before = prev[i]
		}
		if h.Counts[i] > before {
			return time.Duration(h.Buckets[i+1] * float64(time.Second))
		}
	}
	return 0
}

func (r *Replica) statsLoop() {
	defer r.wg.Done()
	tk := time.NewTicker(time.Second)
	defer tk.Stop()
	var prev statsPrev
	var prevPauses []uint64
	snap := func() statsPrev {
		s := statsPrev{at: time.Now(), req: r.stats.req.Load(), commit: r.stats.commit.Load(),
			bg: r.stats.bg.Load(), peer: r.stats.peer.Load(), ord: r.ord.Counters(), cpu: procCPU()}
		if r.pipe != nil {
			s.shed = r.pipe.shed.Load()
			for _, q := range r.pipe.lanes {
				s.laneBusy = append(s.laneBusy, q.busyNS.Load())
				s.laneMsgs = append(s.laneMsgs, q.msgs.Load())
			}
			for _, x := range r.pipe.senders {
				s.sendBusy = append(s.sendBusy, x.busyNS.Load())
				s.dgrams = append(s.dgrams, x.dgrams.Load())
				s.combin = append(s.combin, x.combined.Load())
				s.replies = append(s.replies, x.replies.Load())
			}
		}
		s.gcCycles, s.gcCPU, _, _ = readGC()
		return s
	}
	prev = snap()
	for {
		select {
		case <-r.stop:
			return
		case <-tk.C:
		}
		cur := snap()
		dt := cur.at.Sub(prev.at).Seconds()
		rate := func(a, b uint64) float64 { return float64(a-b) / dt }
		pct := func(a, b uint64) float64 { return float64(a-b) / (dt * 1e9) * 100 }
		o, po := cur.ord, prev.ord
		var b strings.Builder
		fmt.Fprintf(&b, "r%d stats: cpu=%.0f%% rx/s req=%.0f commit=%.0f bg=%.0f peer=%.0f shed=%.0f",
			r.idx, float64(cur.cpu-prev.cpu)/(dt*1e9)*100, rate(cur.req, prev.req),
			rate(cur.commit, prev.commit), rate(cur.bg, prev.bg), rate(cur.peer, prev.peer),
			rate(cur.shed, prev.shed))
		arr := (o.OnTime - po.OnTime) + (o.LateLeader - po.LateLeader) + (o.LateFollower - po.LateFollower)
		mean := func(sum, n uint64) float64 {
			if n == 0 {
				return 0
			}
			return float64(sum) / float64(n)
		}
		fmt.Fprintf(&b, " | arrivals=%.0f/s onTime=%.1f%% lateL=%d lateF=%d slack=%.0fus late=%.0fus rel=%.0f/s",
			float64(arr)/dt, 100*mean(o.OnTime-po.OnTime, arr), o.LateLeader-po.LateLeader,
			o.LateFollower-po.LateFollower, mean(o.SlackUS-po.SlackUS, o.OnTime-po.OnTime),
			mean(o.LatenessUS-po.LatenessUS, (o.LateLeader-po.LateLeader)+(o.LateFollower-po.LateFollower)),
			rate(o.Released, po.Released))
		fmt.Fprintf(&b, " | exec passes=%.0f/s writes=%.0f/s pass=%.0fus maxSnap=%dus skippedOlder=%d",
			rate(o.ExecPasses, po.ExecPasses), rate(o.ExecWrites, po.ExecWrites),
			mean(o.ExecPassUS-po.ExecPassUS, o.ExecPasses-po.ExecPasses), o.ExecMaxSnapUS,
			o.SkippedOlder-po.SkippedOlder)
		fmt.Fprintf(&b, " | spec=%d early=%d late=%d | maxTick=%dus maxHb=%dus", r.ord.SpecLogSize(),
			r.ord.EarlySize(), r.ord.LateSize(), r.stats.maxTickNS.Swap(0)/1000, r.stats.maxHbNS.Swap(0)/1000)
		if r.pipe != nil {
			b.WriteString(" | lanes busy%/msgs/maxQ/maxIterUs")
			for i, q := range r.pipe.lanes {
				fmt.Fprintf(&b, " %.0f/%.0f/%d/%d", pct(cur.laneBusy[i], prev.laneBusy[i]),
					rate(cur.laneMsgs[i], prev.laneMsgs[i]), q.maxQ.Swap(0), q.maxIterNS.Swap(0)/1000)
			}
			b.WriteString(" | senders busy%/dgrams/combined%")
			for i := range r.pipe.senders {
				reps := cur.replies[i] - prev.replies[i]
				fmt.Fprintf(&b, " %.0f/%.0f/%.0f", pct(cur.sendBusy[i], prev.sendBusy[i]),
					rate(cur.dgrams[i], prev.dgrams[i]), 100*mean(cur.combin[i]-prev.combin[i], reps))
			}
		}
		cyc, gcCPU, live, pauses := readGC()
		fmt.Fprintf(&b, " | gc=%.0f/s gcCPU=%.0f%% maxPause<=%v live=%.0fMB", (cyc-prev.gcCycles)/dt,
			(gcCPU-prev.gcCPU)/dt*100, maxNewPause(prevPauses, pauses), live)
		if pauses != nil {
			prevPauses = append(prevPauses[:0], pauses.Counts...)
		}
		log.Print(b.String())
		prev = cur
	}
}
