package memtable

import (
	"bytes"
	"math/rand/v2"
)

// maxLevel caps the height of the skip list. With a promotion probability of
// 1/4, level n holds roughly one node in 4^(n-1), so twelve levels comfortably
// index a few million keys before the top level stops thinning out the search.
const maxLevel = 12

// node is one record in the skip list.
//
// A node is never physically unlinked once spliced in, so any pointer to it
// stays valid for the lifetime of the list. Deletes only flip tombstone and
// drop value.
type node struct {
	key   []byte
	value []byte
	// tombstone marks the key as deleted here. The node stays in the list
	// because a delete has to be a record in its own right: when this
	// memtable is flushed, the tombstone must be written out so it can
	// shadow an older value for the same key in a lower, on-disk table.
	// Physically removing the node would lose that information and the old
	// value would reappear.
	tombstone bool
	// forward holds one next-pointer per level this node participates in,
	// from level 0 (the full, sorted chain) upwards. len(forward) is the
	// node's height, chosen randomly at insertion.
	forward []*node
}

// skiplist is a sorted map from byte-slice keys to values, ordered bytewise.
//
// It is not safe for concurrent use on its own; Memtable provides the locking.
type skiplist struct {
	// head is a sentinel that owns no key. It always has maxLevel forward
	// pointers so that a node can be promoted to any height without ever
	// having to grow the entry point.
	head *node
	// level is the height currently in use, so searches can start below
	// maxLevel instead of walking empty top levels.
	level int
	// length and size are maintained incrementally by put and delete.
	// Recomputing them would mean walking the whole list, which would turn
	// the O(1) Len and ApproxSize into O(n).
	length int
	size   int
}

func newSkiplist() *skiplist {
	return &skiplist{
		head:  &node{forward: make([]*node, maxLevel)},
		level: 1,
	}
}

// randomLevel picks a height for a new node: level 1, then one extra level for
// as long as a 1-in-4 coin keeps coming up, capped at maxLevel. Deciding the
// height at random is what keeps the list balanced without any rebalancing
// work, because it reproduces the thinning-out of a balanced index in
// expectation rather than exactly.
func randomLevel() int {
	level := 1
	for level < maxLevel && rand.Uint32()&3 == 0 {
		level++
	}
	return level
}

// cloneBytes returns a private copy of b. A nil input yields an empty but
// non-nil slice, which is what lets an empty stored value be told apart from a
// missing one.
func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// search descends from the top level to level 0, at each level moving right
// while the next key is still less than key, and returns the node with exactly
// that key, or nil.
//
// When update is non-nil it also records, for each level, the last node
// visited on that level: the node that would precede key if key were inserted.
// An insertion has to rewrite one forward pointer per level of the new node,
// and those pointers live in nodes scattered across the list that the search
// has already walked past. Keeping them in update as we descend means we do
// not have to search again, once per level, to find them.
func (s *skiplist) search(key []byte, update *[maxLevel]*node) *node {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		// Strictly less than, so the walk stops on the node just before an
		// equal key and update[i] never holds the match itself.
		for x.forward[i] != nil && bytes.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		if update != nil {
			update[i] = x
		}
	}

	// Every key present at any level is present at level 0, so one step right
	// from the level-0 predecessor either is the key or the key is absent.
	if next := x.forward[0]; next != nil && bytes.Equal(next.key, key) {
		return next
	}
	return nil
}

// get returns the node for key, or nil if the list holds no record for it. A
// tombstoned key still has a node, so the caller must check the flag.
func (s *skiplist) get(key []byte) *node {
	return s.search(key, nil)
}

// insert splices a new node for key into the list, taking value and tombstone
// as given. update must come from a search for key that found no match.
func (s *skiplist) insert(key, value []byte, tombstone bool, update *[maxLevel]*node) {
	level := randomLevel()

	// search only filled update up to the levels currently in use. Any level
	// above that is empty, so the sentinel is the predecessor there.
	for i := s.level; i < level; i++ {
		update[i] = s.head
	}

	n := &node{
		key:       cloneBytes(key),
		value:     value,
		tombstone: tombstone,
		forward:   make([]*node, level),
	}
	for i := range level {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}

	if level > s.level {
		s.level = level
	}
	s.length++
	s.size += len(n.key) + len(n.value)
}

// put stores value under key, overwriting any previous value or tombstone.
func (s *skiplist) put(key, value []byte) {
	var update [maxLevel]*node
	if existing := s.search(key, &update); existing != nil {
		// Overwrite in place: the node keeps its position and height, so no
		// pointer has to change. Only the value and the accounting do, and
		// the old value's bytes stop counting towards the size.
		s.size -= len(existing.value)
		existing.value = cloneBytes(value)
		existing.tombstone = false
		s.size += len(existing.value)
		return
	}
	s.insert(key, cloneBytes(value), false, &update)
}

// delete records a tombstone for key.
func (s *skiplist) delete(key []byte) {
	var update [maxLevel]*node
	if existing := s.search(key, &update); existing != nil {
		if existing.tombstone {
			// Already deleted; nothing about the record or its size changes.
			return
		}
		// The node stays linked. Only its value is released, which is why the
		// key still counts towards the length and towards the size.
		s.size -= len(existing.value)
		existing.value = nil
		existing.tombstone = true
		return
	}

	// The key is not in this memtable, but an older on-disk table may still
	// hold a value for it, so the delete has to be recorded as its own node
	// rather than discarded as a no-op.
	s.insert(key, nil, true, &update)
}

// entries walks level 0, which is the complete chain in ascending key order,
// and returns a snapshot of copies.
func (s *skiplist) entries() []Entry {
	out := make([]Entry, 0, s.length)
	for x := s.head.forward[0]; x != nil; x = x.forward[0] {
		e := Entry{Key: cloneBytes(x.key), Tombstone: x.tombstone}
		if !x.tombstone {
			e.Value = cloneBytes(x.value)
		}
		out = append(out, e)
	}
	return out
}
