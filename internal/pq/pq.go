// Package pq is a binary min-heap keyed by an id, with O(1) Contains/Find and
// O(log n) UpdateKey/Erase (C++ dyna/indexed_min_pq.h). specLog and earlyBuffer use it:
// re-timestamping changes an entry's order in place and must re-heapify.
//
// Values are pointers, so a sift swaps two words and value addresses are stable until
// the entry is removed. Each heap node carries its own index, so sifts never touch the
// id index (an hmap). Invariant: idx[id].i is id's heap position (CheckInvariants).
package pq

import "dynago/internal/hmap"

type node[ID hmap.Key, V any] struct {
	id ID
	v  *V
	i  int // heap index
}

type IndexedMinPQ[ID hmap.Key, V any] struct {
	heap []*node[ID, V]
	idx  *hmap.Map[ID, *node[ID, V]]
	free []*node[ID, V] // recycled nodes (no allocation per push in steady state)
	less func(a, b *V) bool
}

// New builds an empty queue; less(a, b) means a sorts before b.
func New[ID hmap.Key, V any](less func(a, b *V) bool) *IndexedMinPQ[ID, V] {
	return NewSized[ID, V](less, 0)
}

// NewSized pre-sizes the index (avoids multi-ms rehashes under the ordering lock).
func NewSized[ID hmap.Key, V any](less func(a, b *V) bool, n int) *IndexedMinPQ[ID, V] {
	return &IndexedMinPQ[ID, V]{heap: make([]*node[ID, V], 0, n), idx: hmap.New[ID, *node[ID, V]](n), less: less}
}

func (q *IndexedMinPQ[ID, V]) Len() int            { return len(q.heap) }
func (q *IndexedMinPQ[ID, V]) Empty() bool         { return len(q.heap) == 0 }
func (q *IndexedMinPQ[ID, V]) Contains(id ID) bool { return q.idx.Has(id) }

// Push inserts; false (no-op) if id is already present.
func (q *IndexedMinPQ[ID, V]) Push(id ID, v *V) bool {
	if q.idx.Has(id) {
		return false
	}
	var n *node[ID, V]
	if k := len(q.free); k > 0 {
		n, q.free = q.free[k-1], q.free[:k-1]
	} else {
		n = &node[ID, V]{}
	}
	n.id, n.v, n.i = id, v, len(q.heap)
	q.heap = append(q.heap, n)
	q.idx.Put(id, n)
	q.siftUp(n.i)
	return true
}

// PeekMin / PeekMinID: precondition !Empty().
func (q *IndexedMinPQ[ID, V]) PeekMin() *V   { return q.heap[0].v }
func (q *IndexedMinPQ[ID, V]) PeekMinID() ID { return q.heap[0].id }

// PopMin removes and returns the minimum; precondition !Empty().
func (q *IndexedMinPQ[ID, V]) PopMin() *V { return q.remove(q.heap[0]) }

// Find gives mutable access WITHOUT re-heapify: after changing the order the caller
// must call UpdateKey(id). nil if absent.
func (q *IndexedMinPQ[ID, V]) Find(id ID) *V {
	if n, ok := q.idx.Get(id); ok {
		return n.v
	}
	return nil
}

// UpdateKey restores heap order for id after the caller mutated its value.
func (q *IndexedMinPQ[ID, V]) UpdateKey(id ID) bool {
	n, ok := q.idx.Get(id)
	if !ok {
		return false
	}
	q.restore(n.i)
	return true
}

// Erase removes id and returns its value (nil if absent).
func (q *IndexedMinPQ[ID, V]) Erase(id ID) *V {
	n, ok := q.idx.Get(id)
	if !ok {
		return nil
	}
	return q.remove(n)
}

// ForEach visits every entry in heap (not sorted) order; f must not mutate the queue.
func (q *IndexedMinPQ[ID, V]) ForEach(f func(id ID, v *V)) {
	for _, n := range q.heap {
		f(n.id, n.v)
	}
}

// At is the entry at heap position i (0 <= i < Len()), for scans that walk the heap
// in slices and may release a lock between them.
func (q *IndexedMinPQ[ID, V]) At(i int) (ID, *V) { return q.heap[i].id, q.heap[i].v }

// Clear drops every entry.
func (q *IndexedMinPQ[ID, V]) Clear() {
	clear(q.heap)
	q.heap = q.heap[:0]
	q.idx.Clear()
}

// CheckInvariants verifies the heap property and that the index mirrors the heap
// (tests).
func (q *IndexedMinPQ[ID, V]) CheckInvariants() bool {
	if q.idx.Len() != len(q.heap) {
		return false
	}
	for i, n := range q.heap {
		if m, ok := q.idx.Get(n.id); !ok || m != n || n.i != i {
			return false
		}
		if i > 0 && q.less(n.v, q.heap[(i-1)/2].v) {
			return false
		}
	}
	return true
}

func (q *IndexedMinPQ[ID, V]) lessAt(a, b int) bool { return q.less(q.heap[a].v, q.heap[b].v) }

func (q *IndexedMinPQ[ID, V]) swap(a, b int) {
	q.heap[a], q.heap[b] = q.heap[b], q.heap[a]
	q.heap[a].i, q.heap[b].i = a, b
}

func (q *IndexedMinPQ[ID, V]) siftUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if !q.lessAt(i, p) {
			return
		}
		q.swap(i, p)
		i = p
	}
}

func (q *IndexedMinPQ[ID, V]) siftDown(i int) {
	n := len(q.heap)
	for {
		l, r, m := 2*i+1, 2*i+2, i
		if l < n && q.lessAt(l, m) {
			m = l
		}
		if r < n && q.lessAt(r, m) {
			m = r
		}
		if m == i {
			return
		}
		q.swap(i, m)
		i = m
	}
}

func (q *IndexedMinPQ[ID, V]) restore(i int) {
	if i > 0 && q.lessAt(i, (i-1)/2) {
		q.siftUp(i)
	} else {
		q.siftDown(i)
	}
}

// remove drops node n (index and heap) and recycles it.
func (q *IndexedMinPQ[ID, V]) remove(n *node[ID, V]) *V {
	i, last := n.i, len(q.heap)-1
	if i != last {
		q.swap(i, last)
	}
	q.heap[last] = nil
	q.heap = q.heap[:last]
	if i < len(q.heap) {
		q.restore(i)
	}
	q.idx.Delete(n.id)
	v := n.v
	*n = node[ID, V]{}
	q.free = append(q.free, n)
	return v
}
