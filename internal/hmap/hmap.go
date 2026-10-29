// Package hmap is an open-addressing hash map (linear probing, backward-shift deletion:
// no tombstones) for keys that carry their own hash, like types.TxnId. It stands in
// for Go maps on the per-txn hot paths (C++ uses ankerl::unordered_dense there): Go
// 1.23 maps hash a 16-byte struct key through the generic slow path, and every insert
// may evacuate buckets incrementally.
//
// Not safe for concurrent use. Pointers from Ptr are invalidated by the next Put that
// starts a resize or any Delete.
package hmap

import "unsafe"

// Key is a comparable key with a well-mixed 64-bit hash.
type Key interface {
	comparable
	Hash() uint64
}

// Slot states. dead appears only in the old table of an incremental resize (a
// migrated or deleted entry): lookups probe past it, and nothing is inserted there.
const (
	empty uint8 = iota
	live
	dead
)

type slot[K Key, V any] struct {
	k     K
	v     V
	state uint8
}

// Map grows incrementally: once the table is 3/4 full, a table twice the size takes
// new entries, and each later Put moves migrateStep old slots across (2 is the least
// that empties a table of S slots, S/2 Puts, before the new one reaches its own
// threshold, 3S/4 Puts away; the smaller the step, the thinner the work is spread). The old table is
// read until its last live entry has moved. Doubling a multi-million-entry table in one
// go would stall the caller for ~100 ms (with a lane lock held, that stalled every
// request in the lane).
type Map[K Key, V any] struct {
	slots []slot[K, V]
	n     int // live entries in both tables
	mask  uint64
	grow  int // start a resize when the new table holds this many (load factor 3/4)

	old     []slot[K, V] // table being drained; nil when no resize is in progress
	oldMask uint64
	oldLive int // live entries still in old
	cursor  int // next old slot to migrate
}

const migrateStep = 2

// New builds a map sized for n entries without growing.
func New[K Key, V any](n int) *Map[K, V] {
	m := &Map[K, V]{}
	m.Reserve(n)
	return m
}

func (m *Map[K, V]) Len() int { return m.n }

// Reserve grows the table so n entries fit without another resize.
func (m *Map[K, V]) Reserve(n int) {
	need := 8
	for need*3/4 < n {
		need *= 2
	}
	if need > len(m.slots) {
		m.finishMigration()
		m.startResize(need)
		m.finishMigration()
		prefault(m.slots)
	}
}

// prefault writes one slot per page so a reserved table is backed by memory up front:
// otherwise the first insert into each page of a fresh multi-GB table takes a kernel
// page fault, on the lane that owns it.
func prefault[T any](s []T) {
	var zero T
	step := max(1, 4096/max(1, int(unsafe.Sizeof(zero))))
	for i := 0; i < len(s); i += step {
		s[i] = zero
	}
}

// startResize makes a table of size the target for new entries and queues the current
// one for migration (the caller has finished any earlier migration).
func (m *Map[K, V]) startResize(size int) {
	if m.n > 0 {
		m.old, m.oldMask, m.oldLive, m.cursor = m.slots, m.mask, m.n, 0
	}
	m.slots = make([]slot[K, V], size)
	m.mask = uint64(size - 1)
	m.grow = size * 3 / 4
}

// migrate moves up to steps old slots into the new table.
func (m *Map[K, V]) migrate(steps int) {
	for ; steps > 0 && m.old != nil; steps-- {
		if m.cursor >= len(m.old) || m.oldLive == 0 {
			m.old = nil
			return
		}
		s := &m.old[m.cursor]
		m.cursor++
		if s.state == live {
			*m.insertNew(s.k) = s.v
			s.state = dead
			m.oldLive--
			m.n-- // insertNew counted it again
		}
	}
	if m.old != nil && m.oldLive == 0 {
		m.old = nil
	}
}

func (m *Map[K, V]) finishMigration() {
	for m.old != nil {
		m.migrate(1 << 16)
	}
}

func (m *Map[K, V]) find(k K) int {
	if m.n == 0 || len(m.slots) == 0 {
		return -1
	}
	for i := k.Hash() & m.mask; ; i = (i + 1) & m.mask {
		s := &m.slots[i]
		if s.state == empty {
			return -1
		}
		if s.k == k {
			return int(i)
		}
	}
}

// findOld is k's live slot in the table being drained, or -1.
func (m *Map[K, V]) findOld(k K) int {
	if m.old == nil {
		return -1
	}
	for i := k.Hash() & m.oldMask; ; i = (i + 1) & m.oldMask {
		s := &m.old[i]
		if s.state == empty {
			return -1
		}
		if s.state == live && s.k == k {
			return int(i)
		}
	}
}

// lookup is a pointer to k's value in either table, nil if absent. An entry still in
// the old table is moved first, so the pointer is into the new one.
func (m *Map[K, V]) lookup(k K) *V {
	if i := m.find(k); i >= 0 {
		return &m.slots[i].v
	}
	if j := m.findOld(k); j >= 0 {
		s := &m.old[j]
		v := m.insertNew(k)
		*v = s.v
		s.state = dead
		m.oldLive--
		m.n-- // insertNew counted it again
		if m.oldLive == 0 {
			m.old = nil
		}
		return v
	}
	return nil
}

// insertNew claims a slot in the new table for an absent key (no growth check) and
// returns its value.
func (m *Map[K, V]) insertNew(k K) *V {
	i := k.Hash() & m.mask
	for m.slots[i].state != empty {
		i = (i + 1) & m.mask
	}
	m.slots[i].k, m.slots[i].state = k, live
	m.n++
	return &m.slots[i].v
}

func (m *Map[K, V]) Get(k K) (V, bool) {
	if i := m.find(k); i >= 0 {
		return m.slots[i].v, true
	}
	if j := m.findOld(k); j >= 0 {
		return m.old[j].v, true
	}
	var zero V
	return zero, false
}

func (m *Map[K, V]) Has(k K) bool { return m.find(k) >= 0 || m.findOld(k) >= 0 }

// Ptr is a pointer to k's value, nil if absent.
func (m *Map[K, V]) Ptr(k K) *V { return m.lookup(k) }

// Put inserts or replaces.
func (m *Map[K, V]) Put(k K, v V) {
	if p := m.lookup(k); p != nil {
		*p = v
		return
	}
	m.migrate(migrateStep)
	if m.n-m.oldLive+1 > m.grow {
		m.finishMigration() // never in practice: a resize drains long before this
		m.startResize(max(8, len(m.slots)*2))
	}
	*m.insertNew(k) = v
}

// Delete removes k; false if absent. Later entries of the probe run shift back, so
// lookups never need tombstones.
func (m *Map[K, V]) Delete(k K) bool {
	i := m.find(k)
	if i < 0 {
		if j := m.findOld(k); j >= 0 {
			m.old[j].state = dead // the old table takes no inserts: a tombstone is fine
			m.oldLive--
			m.n--
			if m.oldLive == 0 {
				m.old = nil
			}
			return true
		}
		return false
	}
	hole := uint64(i)
	for j := (hole + 1) & m.mask; m.slots[j].state != empty; j = (j + 1) & m.mask {
		home := m.slots[j].k.Hash() & m.mask
		// Move j into the hole iff its home is not cyclically within (hole, j].
		if (j-home)&m.mask >= (j-hole)&m.mask {
			m.slots[hole] = m.slots[j]
			hole = j
		}
	}
	m.slots[hole] = slot[K, V]{}
	m.n--
	return true
}

// Clear drops every entry and keeps the capacity.
func (m *Map[K, V]) Clear() {
	clear(m.slots)
	m.n = 0
	m.old, m.oldLive = nil, 0
}

// Range calls f for every entry until it returns false; f must not modify the map.
func (m *Map[K, V]) Range(f func(k K, v *V) bool) {
	for i := range m.slots {
		if m.slots[i].state == live && !f(m.slots[i].k, &m.slots[i].v) {
			return
		}
	}
	for i := range m.old {
		if m.old[i].state == live && !f(m.old[i].k, &m.old[i].v) {
			return
		}
	}
}

// At is k's value, or the zero value if absent.
func (m *Map[K, V]) At(k K) V {
	v, _ := m.Get(k)
	return v
}
