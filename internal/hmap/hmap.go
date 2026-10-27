// Package hmap is an open-addressing hash map (linear probing, backward-shift deletion:
// no tombstones) for keys that carry their own hash, like types.TxnId. It stands in
// for Go maps on the per-txn hot paths (C++ uses ankerl::unordered_dense there): Go
// 1.23 maps hash a 16-byte struct key through the generic slow path, and every insert
// may evacuate buckets incrementally.
//
// Not safe for concurrent use. Pointers from Ptr are invalidated by the next Put that
// grows the table or any Delete.
package hmap

// Key is a comparable key with a well-mixed 64-bit hash.
type Key interface {
	comparable
	Hash() uint64
}

type slot[K Key, V any] struct {
	k    K
	v    V
	used bool
}

type Map[K Key, V any] struct {
	slots []slot[K, V]
	n     int
	mask  uint64
	grow  int // resize when n reaches this (load factor 3/4)
}

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
		m.resize(need)
	}
}

func (m *Map[K, V]) resize(size int) {
	old := m.slots
	m.slots = make([]slot[K, V], size)
	m.mask = uint64(size - 1)
	m.grow = size * 3 / 4
	m.n = 0
	for i := range old {
		if old[i].used {
			*m.insertNew(old[i].k) = old[i].v
		}
	}
}

func (m *Map[K, V]) find(k K) int {
	if m.n == 0 {
		return -1
	}
	for i := k.Hash() & m.mask; ; i = (i + 1) & m.mask {
		s := &m.slots[i]
		if !s.used {
			return -1
		}
		if s.k == k {
			return int(i)
		}
	}
}

// insertNew claims a slot for an absent key (no growth check) and returns its value.
func (m *Map[K, V]) insertNew(k K) *V {
	i := k.Hash() & m.mask
	for m.slots[i].used {
		i = (i + 1) & m.mask
	}
	m.slots[i].k, m.slots[i].used = k, true
	m.n++
	return &m.slots[i].v
}

func (m *Map[K, V]) Get(k K) (V, bool) {
	if i := m.find(k); i >= 0 {
		return m.slots[i].v, true
	}
	var zero V
	return zero, false
}

func (m *Map[K, V]) Has(k K) bool { return m.find(k) >= 0 }

// Ptr is a pointer to k's value, nil if absent.
func (m *Map[K, V]) Ptr(k K) *V {
	if i := m.find(k); i >= 0 {
		return &m.slots[i].v
	}
	return nil
}

// Put inserts or replaces.
func (m *Map[K, V]) Put(k K, v V) {
	if i := m.find(k); i >= 0 {
		m.slots[i].v = v
		return
	}
	if m.n+1 > m.grow {
		m.resize(max(8, len(m.slots)*2))
	}
	*m.insertNew(k) = v
}

// Delete removes k; false if absent. Later entries of the probe run shift back, so
// lookups never need tombstones.
func (m *Map[K, V]) Delete(k K) bool {
	i := m.find(k)
	if i < 0 {
		return false
	}
	hole := uint64(i)
	for j := (hole + 1) & m.mask; m.slots[j].used; j = (j + 1) & m.mask {
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
}

// Range calls f for every entry until it returns false; f must not modify the map.
func (m *Map[K, V]) Range(f func(k K, v *V) bool) {
	for i := range m.slots {
		if m.slots[i].used && !f(m.slots[i].k, &m.slots[i].v) {
			return
		}
	}
}

// At is k's value, or the zero value if absent.
func (m *Map[K, V]) At(k K) V {
	if i := m.find(k); i >= 0 {
		return m.slots[i].v
	}
	var zero V
	return zero
}
