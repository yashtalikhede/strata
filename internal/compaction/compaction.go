package compaction

import (
	"bytes"
	"container/heap"
)

// Source is one stream of entries in ascending key order, with no duplicate
// keys of its own. An *sstable.Iterator satisfies it.
//
// The slices Key and Value return are only guaranteed valid until the next
// call to Next on that same source, which is what shapes the merge: a winning
// entry has to be handed to the caller before anything is advanced.
type Source interface {
	// Next advances to the next entry and reports whether there is one. It
	// returns false at the end of the stream and on the first error.
	Next() bool
	// Key returns the current key.
	Key() []byte
	// Value returns the current value, nil for a tombstone.
	Value() []byte
	// Tombstone reports whether the current entry is a delete.
	Tombstone() bool
	// Err returns the first error the stream hit, or nil if it simply ran
	// out of entries.
	Err() error
}

// Stats counts what a merge did.
type Stats struct {
	// EntriesRead is the total number of entries pulled from all sources.
	EntriesRead uint64
	// EntriesWritten is the number of entries handed to emit, which is the
	// number of distinct keys that survived.
	EntriesWritten uint64
	// Shadowed is the number of entries discarded because a newer source
	// held the same key.
	Shadowed uint64
	// TombstonesDropped is the number of tombstones discarded rather than
	// emitted, which only happens when the caller allows it.
	TombstonesDropped uint64
}

// Merge performs a k-way merge of sources into a single ascending stream of
// distinct keys, calling emit once per surviving key.
//
// sources are ordered oldest to newest, and that order alone decides which
// version of a key wins: on equal keys the newest source wins and the older
// versions are discarded. Nothing about the sources themselves, file numbers
// included, is consulted.
//
// dropTombstones says whether a winning tombstone may be thrown away instead
// of emitted. It is only safe to pass true when the sources include the oldest
// table in the database, because a tombstone's whole job is to shadow an older
// value: drop one while an older table outside the merge still holds that
// value, and the deleted key comes back. See docs/DESIGN.md.
//
// The slices handed to emit are valid for the duration of the call and no
// longer, because emit runs before the source they come from is advanced. A
// caller that keeps them must copy them. An error from a source or from emit
// stops the merge at once and comes back with the statistics gathered so far.
// No sources, or sources that are all empty, is not an error and emits
// nothing.
func Merge(sources []Source, dropTombstones bool, emit func(key, value []byte, tombstone bool) error) (Stats, error) {
	var stats Stats

	h := make(sourceHeap, 0, len(sources))
	for i, s := range sources {
		if s.Next() {
			stats.EntriesRead++
			h = append(h, sourceEntry{src: s, age: i})
			continue
		}
		if err := s.Err(); err != nil {
			return stats, err
		}
	}
	heap.Init(&h)

	// key holds a copy of the key being settled. It has to be a copy: the
	// sources below are advanced while it is still needed for comparison,
	// and a source is free to overwrite the slice it handed out.
	var key []byte

	for len(h) > 0 {
		winner := h[0].src
		key = append(key[:0], winner.Key()...)
		tombstone := winner.Tombstone()

		if dropTombstones && tombstone {
			stats.TombstonesDropped++
		} else {
			// Before any Next, so the slices are still the winner's own.
			if err := emit(winner.Key(), winner.Value(), tombstone); err != nil {
				return stats, err
			}
			stats.EntriesWritten++
		}

		// Advance the winner, then every other source sitting on the same
		// key: those are the versions this one shadowed.
		if err := advance(&h, &stats); err != nil {
			return stats, err
		}
		for len(h) > 0 && bytes.Equal(h[0].src.Key(), key) {
			stats.Shadowed++
			if err := advance(&h, &stats); err != nil {
				return stats, err
			}
		}
	}
	return stats, nil
}

// advance moves the source at the top of the heap forward, restoring the heap
// order, or removes it if it is exhausted.
func advance(h *sourceHeap, stats *Stats) error {
	top := (*h)[0]
	if top.src.Next() {
		stats.EntriesRead++
		heap.Fix(h, 0)
		return nil
	}
	if err := top.src.Err(); err != nil {
		return err
	}
	heap.Pop(h)
	return nil
}

// sourceEntry is one source in the heap, together with its age: its index in
// the caller's slice, where a larger number means newer.
type sourceEntry struct {
	src Source
	age int
}

// sourceHeap orders sources by current key ascending, and for equal keys by
// age with the newest first, so the top of the heap is always the entry that
// wins.
type sourceHeap []sourceEntry

func (h sourceHeap) Len() int { return len(h) }

func (h sourceHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].src.Key(), h[j].src.Key()); c != 0 {
		return c < 0
	}
	return h[i].age > h[j].age
}

func (h sourceHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *sourceHeap) Push(x any) { *h = append(*h, x.(sourceEntry)) }

func (h *sourceHeap) Pop() any {
	old := *h
	n := len(old)
	last := old[n-1]
	*h = old[:n-1]
	return last
}
