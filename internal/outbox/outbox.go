// Package outbox batches BACKGROUND server-to-server traffic per destination
// (C++ dyna/bg_outbox.h): replication prepare / ack / commit and TsAgree accumulate
// per replica and leave as one wire.BgBatch on a timer flush (DYNA_REPL_FLUSH_US) or
// when full (DYNA_REPL_BATCH entries or ~maxBytes encoded). Entries are the unchanged
// per-txn messages, so receivers run the same handlers. The client-facing commit path
// never goes through here. Safe for concurrent use.
package outbox

import (
	"sync"

	"dynago/internal/wire"
)

type slot struct {
	batch   wire.BgBatch
	entries int
	bytes   int
}

type BgOutbox struct {
	maxEntries int
	maxBytes   int
	mu         sync.Mutex
	slots      []slot
	scratch    wire.Writer
}

func New(nReplicas, maxEntries, maxBytes int) *BgOutbox {
	if maxEntries <= 0 {
		maxEntries = 1
	}
	return &BgOutbox{maxEntries: maxEntries, maxBytes: maxBytes, slots: make([]slot, nReplicas)}
}

// Each Add* appends one entry for replica `to` and returns true when that destination's
// batch is full: the caller should then Take(to) and send it now.
func (o *BgOutbox) AddPrepare(to uint32, m *wire.ReplicatePrepare) bool {
	return o.add(to, m, func(b *wire.BgBatch) { b.Prepares = append(b.Prepares, *m) })
}

func (o *BgOutbox) AddPrepareReply(to uint32, m *wire.ReplicatePrepareReply) bool {
	return o.add(to, m, func(b *wire.BgBatch) { b.PrepareReplies = append(b.PrepareReplies, *m) })
}

func (o *BgOutbox) AddCommit(to uint32, m *wire.ReplicateCommit) bool {
	return o.add(to, m, func(b *wire.BgBatch) { b.Commits = append(b.Commits, *m) })
}

func (o *BgOutbox) AddTsAgree(to uint32, m *wire.TsAgree) bool {
	return o.add(to, m, func(b *wire.BgBatch) { b.TsAgrees = append(b.TsAgrees, *m) })
}

func (o *BgOutbox) add(to uint32, m wire.Message, fill func(*wire.BgBatch)) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if int(to) >= len(o.slots) {
		return false
	}
	s := &o.slots[to]
	fill(&s.batch)
	s.entries++
	if o.maxBytes > 0 {
		o.scratch.B = wire.Marshal(o.scratch.B[:0], m)
		s.bytes += len(o.scratch.B) + 1
	}
	return s.entries >= o.maxEntries || (o.maxBytes > 0 && s.bytes >= o.maxBytes)
}

// Take moves out `to`'s pending batch; ok=false if it was empty.
func (o *BgOutbox) Take(to uint32) (b wire.BgBatch, entries int, ok bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if int(to) >= len(o.slots) || o.slots[to].entries == 0 {
		return b, 0, false
	}
	s := &o.slots[to]
	b, entries = s.batch, s.entries
	*s = slot{}
	return b, entries, true
}

type Dest struct {
	To    uint32
	Batch wire.BgBatch
}

// TakeAll moves out every non-empty batch (timer flush), in destination order.
func (o *BgOutbox) TakeAll() []Dest {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []Dest
	for i := range o.slots {
		if o.slots[i].entries == 0 {
			continue
		}
		out = append(out, Dest{uint32(i), o.slots[i].batch})
		o.slots[i] = slot{}
	}
	return out
}

func (o *BgOutbox) Clear() {
	o.mu.Lock()
	defer o.mu.Unlock()
	for i := range o.slots {
		o.slots[i] = slot{}
	}
}

func (o *BgOutbox) Pending(to uint32) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	if int(to) >= len(o.slots) {
		return 0
	}
	return o.slots[to].entries
}
