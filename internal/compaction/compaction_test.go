package compaction

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
)

// entry is one entry in a test source.
type entry struct {
	key       string
	value     string
	tombstone bool
}

func put(key, value string) entry { return entry{key: key, value: value} }
func tomb(key string) entry       { return entry{key: key, tombstone: true} }

// sliceSource serves entries from a slice. It deliberately hands out slices
// into one reused buffer per source, so every test exercises the aliasing
// contract rather than only the one test named for it.
type sliceSource struct {
	entries []entry
	pos     int
	keyBuf  []byte
	valBuf  []byte
	// failAt makes Next stop and Err report a failure once pos reaches it,
	// standing in for a corrupt table partway through.
	failAt int
	err    error
}

func newSource(entries ...entry) *sliceSource {
	return &sliceSource{entries: entries, failAt: -1}
}

func (s *sliceSource) failAfter(n int) *sliceSource {
	s.failAt = n
	return s
}

func (s *sliceSource) Next() bool {
	if s.err != nil {
		return false
	}
	if s.failAt >= 0 && s.pos >= s.failAt {
		s.err = fmt.Errorf("source failed at entry %d", s.pos)
		return false
	}
	if s.pos >= len(s.entries) {
		return false
	}
	e := s.entries[s.pos]
	s.pos++
	// Overwrite the buffers in place: anything the merge kept from the last
	// call is now wrong, which is exactly what a real iterator does.
	s.keyBuf = append(s.keyBuf[:0], e.key...)
	s.valBuf = append(s.valBuf[:0], e.value...)
	return true
}

func (s *sliceSource) Key() []byte { return s.keyBuf }

func (s *sliceSource) Value() []byte {
	if s.entries[s.pos-1].tombstone {
		return nil
	}
	return s.valBuf
}

func (s *sliceSource) Tombstone() bool { return s.entries[s.pos-1].tombstone }

func (s *sliceSource) Err() error { return s.err }

// collector gathers emitted entries, copying them, which is what a caller has
// to do given that the slices expire.
type collector struct {
	got []entry
	// failAt makes emit fail on that call, standing in for a writer error.
	failAt int
	err    error
}

func newCollector() *collector { return &collector{failAt: -1} }

func (c *collector) emit(key, value []byte, tombstone bool) error {
	if c.failAt >= 0 && len(c.got) >= c.failAt {
		c.err = fmt.Errorf("emit failed on entry %d", len(c.got))
		return c.err
	}
	e := entry{key: string(key), tombstone: tombstone}
	if !tombstone {
		if value == nil {
			return fmt.Errorf("emit got a nil value for the live key %q", key)
		}
		e.value = string(value)
	} else if value != nil {
		return fmt.Errorf("emit got a non-nil value %q for tombstone %q", value, key)
	}
	c.got = append(c.got, e)
	return nil
}

// sources converts test sources to the interface slice Merge takes.
func sources(ss ...*sliceSource) []Source {
	out := make([]Source, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

func formatEntries(es []entry) string {
	parts := make([]string, len(es))
	for i, e := range es {
		if e.tombstone {
			parts[i] = fmt.Sprintf("%s=<tomb>", e.key)
		} else {
			parts[i] = fmt.Sprintf("%s=%s", e.key, e.value)
		}
	}
	return "[" + fmt.Sprint(parts) + "]"
}

func expectEntries(t *testing.T, got, want []entry) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Fatalf("output = %s, want %s", formatEntries(got), formatEntries(want))
	}
}

func TestMergeNewestWins(t *testing.T) {
	// Oldest to newest. "b" and "d" appear in several, "c" only in the
	// middle one, and "e" only in the oldest.
	oldest := newSource(put("a", "a-old"), put("b", "b-old"), put("d", "d-old"), put("e", "e-old"))
	middle := newSource(put("b", "b-mid"), put("c", "c-mid"), put("d", "d-mid"))
	newest := newSource(put("b", "b-new"), put("d", "d-new"), put("f", "f-new"))

	c := newCollector()
	stats, err := Merge(sources(oldest, middle, newest), false, c.emit)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}

	expectEntries(t, c.got, []entry{
		put("a", "a-old"),
		put("b", "b-new"),
		put("c", "c-mid"),
		put("d", "d-new"),
		put("e", "e-old"),
		put("f", "f-new"),
	})

	want := Stats{EntriesRead: 10, EntriesWritten: 6, Shadowed: 4}
	if stats != want {
		t.Errorf("stats = %+v, want %+v", stats, want)
	}
	// The counts have to add up: everything read either survived or was
	// shadowed or was a dropped tombstone.
	if total := stats.EntriesWritten + stats.Shadowed + stats.TombstonesDropped; total != stats.EntriesRead {
		t.Errorf("written+shadowed+dropped = %d, want EntriesRead = %d", total, stats.EntriesRead)
	}
}

func TestTombstoneKeptVsDropped(t *testing.T) {
	// "gone" was written in the oldest and deleted in the newest; "stays" is
	// only ever written; "ghost" is only ever a tombstone.
	build := func() []Source {
		return sources(
			newSource(put("gone", "value"), put("stays", "kept")),
			newSource(tomb("ghost"), tomb("gone")),
		)
	}

	t.Run("kept", func(t *testing.T) {
		c := newCollector()
		stats, err := Merge(build(), false, c.emit)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		expectEntries(t, c.got, []entry{
			tomb("ghost"),
			tomb("gone"),
			put("stays", "kept"),
		})
		want := Stats{EntriesRead: 4, EntriesWritten: 3, Shadowed: 1}
		if stats != want {
			t.Errorf("stats = %+v, want %+v", stats, want)
		}
	})

	t.Run("dropped", func(t *testing.T) {
		c := newCollector()
		stats, err := Merge(build(), true, c.emit)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		// Both tombstones are gone, and so is the older value "gone" held.
		expectEntries(t, c.got, []entry{put("stays", "kept")})
		want := Stats{EntriesRead: 4, EntriesWritten: 1, Shadowed: 1, TombstonesDropped: 2}
		if stats != want {
			t.Errorf("stats = %+v, want %+v", stats, want)
		}
	})

	// A tombstone in the middle of three, with a newer value on top, must
	// lose to the newer value whichever way dropTombstones is set.
	t.Run("newer value beats a tombstone", func(t *testing.T) {
		for _, drop := range []bool{false, true} {
			c := newCollector()
			_, err := Merge(sources(
				newSource(put("k", "old")),
				newSource(tomb("k")),
				newSource(put("k", "new")),
			), drop, c.emit)
			if err != nil {
				t.Fatalf("Merge(drop=%v): %v", drop, err)
			}
			expectEntries(t, c.got, []entry{put("k", "new")})
		}
	})
}

func TestEdgeCases(t *testing.T) {
	t.Run("no sources", func(t *testing.T) {
		c := newCollector()
		stats, err := Merge(nil, false, c.emit)
		if err != nil {
			t.Fatalf("Merge(nil) = %v, want no error", err)
		}
		if stats != (Stats{}) {
			t.Errorf("stats = %+v, want the zero value", stats)
		}
		if len(c.got) != 0 {
			t.Errorf("emitted %d entries, want none", len(c.got))
		}
	})

	t.Run("empty slice of sources", func(t *testing.T) {
		c := newCollector()
		if _, err := Merge([]Source{}, true, c.emit); err != nil {
			t.Fatalf("Merge: %v", err)
		}
		if len(c.got) != 0 {
			t.Errorf("emitted %d entries, want none", len(c.got))
		}
	})

	t.Run("one source", func(t *testing.T) {
		c := newCollector()
		stats, err := Merge(sources(newSource(put("a", "1"), tomb("b"), put("c", "3"))), false, c.emit)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		expectEntries(t, c.got, []entry{put("a", "1"), tomb("b"), put("c", "3")})
		if stats.Shadowed != 0 {
			t.Errorf("Shadowed = %d, want 0 with a single source", stats.Shadowed)
		}
	})

	t.Run("one empty source", func(t *testing.T) {
		c := newCollector()
		stats, err := Merge(sources(newSource()), false, c.emit)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		if stats != (Stats{}) || len(c.got) != 0 {
			t.Errorf("stats = %+v and %d entries, want nothing", stats, len(c.got))
		}
	})

	t.Run("all sources empty", func(t *testing.T) {
		c := newCollector()
		stats, err := Merge(sources(newSource(), newSource(), newSource()), true, c.emit)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		if stats != (Stats{}) || len(c.got) != 0 {
			t.Errorf("stats = %+v and %d entries, want nothing", stats, len(c.got))
		}
	})

	t.Run("empty source among full ones", func(t *testing.T) {
		c := newCollector()
		_, err := Merge(sources(
			newSource(put("a", "1")),
			newSource(),
			newSource(put("a", "2"), put("b", "3")),
		), false, c.emit)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		expectEntries(t, c.got, []entry{put("a", "2"), put("b", "3")})
	})

	t.Run("empty key and empty value", func(t *testing.T) {
		c := newCollector()
		_, err := Merge(sources(
			newSource(put("", "old"), put("k", "")),
			newSource(put("", "new")),
		), false, c.emit)
		if err != nil {
			t.Fatalf("Merge: %v", err)
		}
		expectEntries(t, c.got, []entry{put("", "new"), put("k", "")})
	})
}

// TestBufferAliasing uses sources that overwrite their key and value buffers
// on every Next, which is what sstable.Iterator does. The merge has to copy
// the key it is settling before it advances anything, and has to hand the
// winner to emit before advancing it.
func TestBufferAliasing(t *testing.T) {
	// Keys chosen so that a stale comparison buffer would give a different
	// answer: every source holds the same first key, and the keys that follow
	// differ in their last byte only.
	a := newSource(put("kaa", "a1"), put("kab", "a2"), put("kac", "a3"))
	b := newSource(put("kaa", "b1"), put("kab", "b2"), put("kad", "b4"))
	cc := newSource(put("kaa", "c1"), put("kac", "c3"), put("kae", "c5"))

	c := newCollector()
	stats, err := Merge(sources(a, b, cc), false, c.emit)
	if err != nil {
		t.Fatalf("Merge: %v", err)
	}
	expectEntries(t, c.got, []entry{
		put("kaa", "c1"),
		put("kab", "b2"),
		put("kac", "c3"),
		put("kad", "b4"),
		put("kae", "c5"),
	})
	if stats.Shadowed != 4 {
		t.Errorf("Shadowed = %d, want 4", stats.Shadowed)
	}

	// The emitted slices must have been copied out by the collector, not
	// retained: if the merge had handed out slices that later changed, the
	// strings above would already be wrong. Check the buffers really are
	// shared, so the test is not vacuous.
	s := newSource(put("x", "1"), put("y", "2"))
	s.Next()
	first := s.Key()
	s.Next()
	if !bytes.Equal(first, s.Key()) {
		t.Fatal("the test source does not reuse its key buffer, so this test proves nothing")
	}
}

func TestErrorPropagation(t *testing.T) {
	t.Run("source fails mid-stream", func(t *testing.T) {
		// The second source gives up after two entries.
		bad := newSource(put("a", "1"), put("b", "2"), put("c", "3")).failAfter(2)
		good := newSource(put("a", "9"), put("d", "4"))

		c := newCollector()
		_, err := Merge(sources(bad, good), false, c.emit)
		if err == nil {
			t.Fatal("Merge = nil, want the source's error")
		}
		if !errors.Is(err, bad.err) {
			t.Fatalf("Merge = %v, want %v", err, bad.err)
		}
	})

	t.Run("source fails immediately", func(t *testing.T) {
		bad := newSource(put("a", "1")).failAfter(0)
		c := newCollector()
		if _, err := Merge(sources(bad), false, c.emit); err == nil {
			t.Fatal("Merge = nil, want the source's error")
		}
		if len(c.got) != 0 {
			t.Errorf("emitted %d entries despite the source failing at once", len(c.got))
		}
	})

	t.Run("emit fails", func(t *testing.T) {
		c := newCollector()
		c.failAt = 2
		_, err := Merge(sources(
			newSource(put("a", "1"), put("b", "2"), put("c", "3"), put("d", "4")),
		), false, c.emit)
		if err == nil {
			t.Fatal("Merge = nil, want emit's error")
		}
		if !errors.Is(err, c.err) {
			t.Fatalf("Merge = %v, want %v", err, c.err)
		}
		if len(c.got) != 2 {
			t.Errorf("emitted %d entries, want the merge to stop at the failing one", len(c.got))
		}
	})

	t.Run("emit fails on the first entry", func(t *testing.T) {
		c := newCollector()
		c.failAt = 0
		if _, err := Merge(sources(newSource(put("a", "1"))), false, c.emit); err == nil {
			t.Fatal("Merge = nil, want emit's error")
		}
	})
}

func TestModelRandomized(t *testing.T) {
	for _, seed := range []uint64{5, 17} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed))

			for round := range 200 {
				n := 1 + rng.IntN(8)
				// A small key space so the sources overlap heavily.
				keyspace := 5 + rng.IntN(25)

				built := make([]*sliceSource, n)
				for i := range built {
					var es []entry
					for k := range keyspace {
						if rng.IntN(3) == 0 {
							continue // This source says nothing about the key.
						}
						key := fmt.Sprintf("k%03d", k)
						if rng.IntN(4) == 0 {
							es = append(es, tomb(key))
						} else {
							es = append(es, put(key, fmt.Sprintf("s%d-k%d-r%d", i, k, round)))
						}
					}
					built[i] = newSource(es...)
				}

				// The model: apply the sources oldest to newest, last write
				// winning, which is what the merge has to reproduce.
				model := map[string]entry{}
				for _, s := range built {
					for _, e := range s.entries {
						model[e.key] = e
					}
				}
				keys := make([]string, 0, len(model))
				for k := range model {
					keys = append(keys, k)
				}
				slices.Sort(keys)

				for _, drop := range []bool{false, true} {
					// Fresh sources, since the previous merge consumed them.
					fresh := make([]Source, n)
					for i, s := range built {
						fresh[i] = newSource(s.entries...)
					}

					var want []entry
					for _, k := range keys {
						if drop && model[k].tombstone {
							continue
						}
						want = append(want, model[k])
					}

					c := newCollector()
					stats, err := Merge(fresh, drop, c.emit)
					if err != nil {
						t.Fatalf("round %d drop=%v: Merge: %v", round, drop, err)
					}
					if !slices.Equal(c.got, want) {
						t.Fatalf("round %d drop=%v: output = %s, want %s",
							round, drop, formatEntries(c.got), formatEntries(want))
					}

					// Strictly increasing, with no duplicates.
					for i := 1; i < len(c.got); i++ {
						if c.got[i-1].key >= c.got[i].key {
							t.Fatalf("round %d drop=%v: output is not strictly increasing at %d: %q then %q",
								round, drop, i, c.got[i-1].key, c.got[i].key)
						}
					}

					if stats.EntriesWritten != uint64(len(want)) {
						t.Fatalf("round %d drop=%v: EntriesWritten = %d, want %d",
							round, drop, stats.EntriesWritten, len(want))
					}
					if total := stats.EntriesWritten + stats.Shadowed + stats.TombstonesDropped; total != stats.EntriesRead {
						t.Fatalf("round %d drop=%v: written+shadowed+dropped = %d, want EntriesRead = %d",
							round, drop, total, stats.EntriesRead)
					}
				}
			}
		})
	}
}

func BenchmarkMerge(b *testing.B) {
	const (
		numSources = 8
		perSource  = 50_000
	)
	// Interleaved keys, so each source covers the whole range and every step
	// of the merge has to consult the heap.
	built := make([][]entry, numSources)
	for i := range built {
		es := make([]entry, 0, perSource)
		for j := range perSource {
			key := fmt.Sprintf("key-%09d", j*numSources+i)
			if j%7 == 0 {
				es = append(es, tomb(key))
			} else {
				es = append(es, put(key, "a hundred bytes of value padding........................................................"))
			}
		}
		built[i] = es
	}

	b.SetBytes(int64(numSources * perSource))
	b.ReportAllocs()
	for b.Loop() {
		ss := make([]Source, numSources)
		for i := range ss {
			ss[i] = newSource(built[i]...)
		}
		stats, err := Merge(ss, true, func(key, value []byte, tombstone bool) error { return nil })
		if err != nil {
			b.Fatalf("Merge: %v", err)
		}
		if stats.EntriesRead != numSources*perSource {
			b.Fatalf("EntriesRead = %d, want %d", stats.EntriesRead, numSources*perSource)
		}
	}
}
