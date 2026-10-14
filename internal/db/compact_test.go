package db

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/yashtalikhede/strata/internal/wal"
)

// sstBytes returns the total size of the .sst files in dir.
func sstBytes(t *testing.T, dir string) int64 {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var total int64
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), sstSuffix) {
			continue
		}
		fi, err := e.Info()
		if err != nil {
			t.Fatalf("Info(%s): %v", e.Name(), err)
		}
		total += fi.Size()
	}
	return total
}

func TestBucketOf(t *testing.T) {
	const base = 1 << 20
	cases := []struct {
		size int64
		want int
	}{
		{0, 0},
		{1, 0},
		{base - 1, 0},
		{base, 1},
		{base + 1, 1},
		{4*base - 1, 1},
		{4 * base, 2},
		{16*base - 1, 2},
		{16 * base, 3},
		{64 * base, 4},
	}
	for _, tc := range cases {
		if got := bucketOf(tc.size, base); got != tc.want {
			t.Errorf("bucketOf(%d, %d) = %d, want %d", tc.size, base, got, tc.want)
		}
	}
	// A nonsensical base must not divide by zero or loop forever.
	if got := bucketOf(1<<30, 0); got < 1 {
		t.Errorf("bucketOf with a zero base = %d, want the default sizing to apply", got)
	}
}

func TestPickCompaction(t *testing.T) {
	const base int64 = 1 << 20
	small := base - 1       // bucket 0
	medium := base          // bucket 1
	large := 4 * base       // bucket 2
	huge := 16 * base       // bucket 3
	justUnder := 4*base - 1 // bucket 1, the boundary below large

	cases := []struct {
		name               string
		sizes              []int64
		trigger            int
		wantStart, wantEnd int
		wantOK             bool
	}{
		{
			name:    "no tables",
			sizes:   nil,
			trigger: 4,
		},
		{
			name:    "too few tables",
			sizes:   []int64{small, small, small},
			trigger: 4,
		},
		{
			name:      "exactly trigger tables in one bucket",
			sizes:     []int64{small, small, small, small},
			trigger:   4,
			wantStart: 0, wantEnd: 4, wantOK: true,
		},
		{
			name:      "a bigger table splits the run",
			sizes:     []int64{small, small, huge, small, small},
			trigger:   4,
			wantStart: 0, wantEnd: 0,
		},
		{
			name: "run after a splitter",
			// Two bucket-0 runs: one of 2, then one of 4. Only the second
			// qualifies, so the pick is not at index 0.
			sizes:     []int64{small, small, huge, small, small, small, small},
			trigger:   4,
			wantStart: 3, wantEnd: 7, wantOK: true,
		},
		{
			name: "lowest eligible bucket wins",
			// Bucket 1 has a qualifying run first in list order, but bucket
			// 0's run is cheaper, so it is chosen.
			sizes:     []int64{medium, medium, medium, medium, small, small, small, small},
			trigger:   4,
			wantStart: 4, wantEnd: 8, wantOK: true,
		},
		{
			name: "oldest qualifying run in the bucket",
			sizes: []int64{
				small, small, small, small, // run of 4 at 0
				huge,
				small, small, small, small, small, // run of 5 at 5
			},
			trigger:   4,
			wantStart: 0, wantEnd: 4, wantOK: true,
		},
		{
			name: "run longer than maxCompactionInputs takes the oldest ten",
			sizes: []int64{
				small, small, small, small, small, small,
				small, small, small, small, small, small,
			},
			trigger:   4,
			wantStart: 0, wantEnd: maxCompactionInputs, wantOK: true,
		},
		{
			name:      "trigger of two",
			sizes:     []int64{large, small, small, huge},
			trigger:   2,
			wantStart: 1, wantEnd: 3, wantOK: true,
		},
		{
			name:    "trigger below the minimum is raised",
			sizes:   []int64{small},
			trigger: 1,
			wantOK:  false,
		},
		{
			name:      "size exactly at baseSize groups together",
			sizes:     []int64{medium, medium},
			trigger:   2,
			wantStart: 0, wantEnd: 2, wantOK: true,
		},
		{
			name: "baseSize boundary separates buckets",
			// base-1 is bucket 0 and base is bucket 1, so these do not form
			// a run even though their sizes are one byte apart.
			sizes:   []int64{small, medium},
			trigger: 2,
			wantOK:  false,
		},
		{
			name: "4*baseSize boundary separates buckets",
			// 4*base-1 is bucket 1, 4*base is bucket 2.
			sizes:   []int64{justUnder, large},
			trigger: 2,
			wantOK:  false,
		},
		{
			name:      "just under 4*baseSize groups with baseSize",
			sizes:     []int64{medium, justUnder},
			trigger:   2,
			wantStart: 0, wantEnd: 2, wantOK: true,
		},
		{
			name: "highest bucket is eligible too",
			sizes: []int64{
				small, medium, large,
				huge, huge, huge, huge,
			},
			trigger:   4,
			wantStart: 3, wantEnd: 7, wantOK: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			start, end, ok := pickCompaction(tc.sizes, tc.trigger, base)
			if ok != tc.wantOK {
				t.Fatalf("pickCompaction(%v, trigger %d) ok = %v, want %v (got [%d,%d))",
					tc.sizes, tc.trigger, ok, tc.wantOK, start, end)
			}
			if !ok {
				return
			}
			if start != tc.wantStart || end != tc.wantEnd {
				t.Fatalf("pickCompaction(%v, trigger %d) = [%d,%d), want [%d,%d)",
					tc.sizes, tc.trigger, start, end, tc.wantStart, tc.wantEnd)
			}
			// Whatever it picks must be usable: contiguous, inside the list,
			// and at least as long as the trigger.
			if end-start < max(tc.trigger, MinCompactionTrigger) {
				t.Errorf("picked %d tables, fewer than the trigger %d", end-start, tc.trigger)
			}
			if end-start > maxCompactionInputs {
				t.Errorf("picked %d tables, more than the cap %d", end-start, maxCompactionInputs)
			}
		})
	}
}

func TestAutoCompactionBoundsTableCount(t *testing.T) {
	const n = 100_000
	value := bytes.Repeat([]byte("v"), 40)
	write := func(t *testing.T, d *DB) {
		t.Helper()
		for i := range n {
			if err := d.Put([]byte(fmt.Sprintf("key-%08d", i)), value); err != nil {
				t.Fatalf("Put(%d): %v", i, err)
			}
		}
	}

	var withTables, withoutTables int

	t.Run("compaction on", func(t *testing.T) {
		dir := t.TempDir()
		d := openDB(t, dir, &Options{
			MemtableSize:       8 << 10,
			CompactionBaseSize: 64 << 10,
			Sync:               wal.SyncNone,
		})
		write(t, d)

		stats := d.Stats()
		withTables = stats.SSTables
		if stats.SSTables > 20 {
			t.Errorf("SSTables = %d, want at most 20", stats.SSTables)
		}
		if stats.Compactions == 0 {
			t.Error("Compactions = 0; the workload did not provoke any")
		}
		if stats.CompactionErrors != 0 {
			t.Errorf("CompactionErrors = %d, want 0 (last: %v)", stats.CompactionErrors, d.LastCompactionError())
		}

		// Stats, the manifest and the directory must all agree.
		m, err := readManifest(dir)
		if err != nil {
			t.Fatalf("readManifest: %v", err)
		}
		if len(m.sstables) != stats.SSTables {
			t.Errorf("manifest lists %d tables, Stats reports %d", len(m.sstables), stats.SSTables)
		}
		if onDisk := countSuffix(t, dir, sstSuffix); onDisk != stats.SSTables {
			t.Errorf("%d .sst files on disk, Stats reports %d", onDisk, stats.SSTables)
		}

		for i := range n {
			expectValue(t, d, []byte(fmt.Sprintf("key-%08d", i)), value)
		}
		t.Logf("compaction on: %d tables, %d flushes, %d compactions, write-amp %.2f",
			stats.SSTables, stats.Flushes, stats.Compactions, stats.WriteAmplification())
	})

	t.Run("compaction off", func(t *testing.T) {
		dir := t.TempDir()
		d := openDB(t, dir, &Options{
			MemtableSize:          8 << 10,
			CompactionBaseSize:    64 << 10,
			Sync:                  wal.SyncNone,
			DisableAutoCompaction: true,
		})
		write(t, d)

		stats := d.Stats()
		withoutTables = stats.SSTables
		if stats.Compactions != 0 {
			t.Errorf("Compactions = %d with auto compaction disabled, want 0", stats.Compactions)
		}
		for i := 0; i < n; i += 997 {
			expectValue(t, d, []byte(fmt.Sprintf("key-%08d", i)), value)
		}
		t.Logf("compaction off: %d tables, %d flushes, write-amp %.2f",
			stats.SSTables, stats.Flushes, stats.WriteAmplification())
	})

	if withTables == 0 || withoutTables == 0 {
		t.Fatal("one of the subtests did not run")
	}
	if withoutTables <= withTables*5 {
		t.Errorf("tables without compaction = %d, with = %d; want the uncompacted count to be much larger",
			withoutTables, withTables)
	}
	t.Logf("table count: %d with compaction, %d without, a factor of %.1f",
		withTables, withoutTables, float64(withoutTables)/float64(withTables))
}

// TestTombstoneNotDroppedOutsideOldestRun is the correctness core of the
// phase: a tombstone may only be discarded by a merge that includes the oldest
// table, because otherwise an older table outside the run may still hold the
// value it exists to shadow.
func TestTombstoneNotDroppedOutsideOldestRun(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{Sync: wal.SyncNone, DisableAutoCompaction: true}
	d := openDB(t, dir, opts)
	k := []byte("buried")

	// Table 0 holds the value the tombstone has to keep shadowing.
	if err := d.Put(k, []byte("the old value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Tables 1 to 4: the first carries the tombstone, all carry other keys.
	for i := range 4 {
		if i == 0 {
			if err := d.Delete(k); err != nil {
				t.Fatalf("Delete: %v", err)
			}
		}
		if err := d.Put([]byte(fmt.Sprintf("other-%02d", i)), []byte("filler")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		if err := d.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	if got := d.Stats().SSTables; got != 5 {
		t.Fatalf("SSTables = %d, want 5", got)
	}
	expectMissing(t, d, k)

	// Merging tables 1 to 4 must keep the tombstone: table 0 is older and
	// still holds the value.
	d.mu.Lock()
	err := d.compactRun(1, 5)
	d.mu.Unlock()
	if err != nil {
		t.Fatalf("compactRun(1,5): %v", err)
	}
	if got := d.Stats().TombstonesDropped; got != 0 {
		t.Fatalf("TombstonesDropped = %d after a merge that excludes the oldest table, want 0", got)
	}
	if got := d.Stats().SSTables; got != 2 {
		t.Fatalf("SSTables = %d, want 2", got)
	}
	expectMissing(t, d, k)

	// Now a merge that does include table 0 may drop it, and the key must
	// still be absent afterwards: the value it shadowed went with it.
	d.mu.Lock()
	err = d.compactRun(0, 2)
	d.mu.Unlock()
	if err != nil {
		t.Fatalf("compactRun(0,2): %v", err)
	}
	if got := d.Stats().TombstonesDropped; got != 1 {
		t.Errorf("TombstonesDropped = %d after a merge from the oldest table, want 1", got)
	}
	if got := d.Stats().SSTables; got != 1 {
		t.Fatalf("SSTables = %d, want 1", got)
	}
	expectMissing(t, d, k)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d2 := openDB(t, dir, opts)
	expectMissing(t, d2, k)
	for i := range 4 {
		expectValue(t, d2, []byte(fmt.Sprintf("other-%02d", i)), []byte("filler"))
	}
}

func TestCompactReclaimsSpace(t *testing.T) {
	const (
		keys        = 1_000
		generations = 50
	)
	dir := t.TempDir()
	opts := &Options{MemtableSize: 16 << 10, Sync: wal.SyncNone, DisableAutoCompaction: true}
	d := openDB(t, dir, opts)

	value := func(gen int) []byte {
		return []byte(fmt.Sprintf("gen-%02d-%s", gen, strings.Repeat("p", 80)))
	}
	for gen := range generations {
		for i := range keys {
			if err := d.Put([]byte(fmt.Sprintf("key-%05d", i)), value(gen)); err != nil {
				t.Fatalf("Put: %v", err)
			}
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	before := sstBytes(t, dir)
	beforeTables := d.Stats().SSTables
	if err := d.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after := sstBytes(t, dir)
	stats := d.Stats()

	if stats.SSTables != 1 {
		t.Errorf("SSTables after Compact = %d, want 1", stats.SSTables)
	}
	// One generation's worth of data is the floor: everything else was an
	// overwrite of the same keys.
	oneGeneration := before / int64(generations)
	if limit := oneGeneration * 3 / 2; after >= limit {
		t.Errorf("after Compact the tables total %d bytes, want below %d (1.5x one generation of %d)",
			after, limit, oneGeneration)
	}
	if stats.ShadowedDropped == 0 {
		t.Error("ShadowedDropped = 0, want the overwritten versions counted")
	}
	t.Logf("space: %d bytes in %d tables before, %d bytes in 1 table after, a factor of %.1f (one generation is about %d)",
		before, beforeTables, after, float64(before)/float64(after), oneGeneration)

	for i := range keys {
		expectValue(t, d, []byte(fmt.Sprintf("key-%05d", i)), value(generations-1))
	}

	// Deleting half the keys and compacting must reclaim them for good.
	for i := 0; i < keys; i += 2 {
		if err := d.Delete([]byte(fmt.Sprintf("key-%05d", i))); err != nil {
			t.Fatalf("Delete: %v", err)
		}
	}
	droppedBefore := d.Stats().TombstonesDropped
	if err := d.Compact(); err != nil {
		t.Fatalf("Compact after deletes: %v", err)
	}
	dropped := d.Stats().TombstonesDropped - droppedBefore
	if dropped == 0 {
		t.Error("TombstonesDropped did not increase, want the tombstones reclaimed")
	}
	t.Logf("deleted %d keys, %d tombstones dropped, tables now %d bytes", keys/2, dropped, sstBytes(t, dir))

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d2 := openDB(t, dir, opts)
	for i := range keys {
		k := []byte(fmt.Sprintf("key-%05d", i))
		if i%2 == 0 {
			expectMissing(t, d2, k)
			continue
		}
		expectValue(t, d2, k, value(generations-1))
	}
}

// buildTables fills a database with several tables and returns the keys it
// wrote, with compaction off so the layout is predictable.
//
// It closes the database before returning, so the caller is free to open the
// directory again; openDB would have held the lock until the test ended.
func buildTables(t *testing.T, dir string, opts *Options, tables, perTable int) map[string]string {
	t.Helper()
	d, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	want := make(map[string]string, tables*perTable)
	for tbl := range tables {
		for i := range perTable {
			k := fmt.Sprintf("t%02d-key-%04d", tbl, i)
			v := fmt.Sprintf("t%02d-value-%04d", tbl, i)
			if err := d.Put([]byte(k), []byte(v)); err != nil {
				t.Fatalf("Put: %v", err)
			}
			want[k] = v
		}
		if err := d.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
	if got := d.Stats().SSTables; got != tables {
		t.Fatalf("SSTables = %d, want %d", got, tables)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return want
}

// expectCleanDirectory asserts the directory holds only what the manifest
// accounts for, plus the lock and exactly one log.
func expectCleanDirectory(t *testing.T, dir string) manifest {
	t.Helper()
	m, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	want := []string{lockFileName, manifestFileName, walName(m.logNumber)}
	for _, num := range m.sstables {
		want = append(want, sstName(num))
	}
	slices.Sort(want)
	if got := listDir(t, dir); !slices.Equal(got, want) {
		t.Errorf("directory holds %v, want %v", got, want)
	}
	return m
}

func TestCompactionCrashEveryStage(t *testing.T) {
	stages := []struct {
		name string
		set  func(d *DB, fail func() error)
	}{
		{"afterCompactionWritten", func(d *DB, f func() error) { d.hooks.afterCompactionWritten = f }},
		{"afterCompactionManifest", func(d *DB, f func() error) { d.hooks.afterCompactionManifest = f }},
	}

	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := &Options{Sync: wal.SyncNone, DisableAutoCompaction: true}
			want := buildTables(t, dir, opts, 6, 100)

			// Reopen to get a handle we can set hooks on and abandon.
			d, err := Open(dir, opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			if got := d.Stats().SSTables; got != 6 {
				t.Fatalf("SSTables after reopen = %d, want 6", got)
			}

			boom := errors.New("simulated crash")
			stage.set(d, func() error { return boom })

			d.mu.Lock()
			err = d.compactRun(1, 5)
			d.mu.Unlock()
			if !errors.Is(err, boom) {
				t.Fatalf("compactRun with the %s hook failing = %v, want the hook's error", stage.name, err)
			}
			d.abandon()

			d2 := openDB(t, dir, opts)
			for k, v := range want {
				expectValue(t, d2, []byte(k), []byte(v))
			}
			expectMissing(t, d2, []byte("never-written"))
			m := expectCleanDirectory(t, dir)
			t.Logf("recovered %d keys from %d tables", len(want), len(m.sstables))
		})
	}
}

func TestCompactionAbortLeavesStateIntact(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{Sync: wal.SyncNone, DisableAutoCompaction: true}
	want := buildTables(t, dir, opts, 5, 100)

	d, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	before, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	beforeFiles := listDir(t, dir)

	// Fail partway through the merge, after some entries have been written.
	boom := errors.New("simulated writer failure")
	calls := 0
	d.hooks.beforeCompactionEmit = func() error {
		calls++
		if calls > 50 {
			return boom
		}
		return nil
	}

	d.mu.Lock()
	err = d.compactRun(0, 5)
	d.mu.Unlock()
	if !errors.Is(err, boom) {
		t.Fatalf("compactRun = %v, want the emit error", err)
	}

	// Nothing may have changed: no output file, the same manifest, the same
	// tables, and the same answers.
	if got := listDir(t, dir); !slices.Equal(got, beforeFiles) {
		t.Errorf("directory holds %v, want it unchanged at %v", got, beforeFiles)
	}
	after, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if !slices.Equal(after.sstables, before.sstables) || after.logNumber != before.logNumber {
		t.Errorf("manifest changed: %+v, want %+v", after, before)
	}
	if got := d.Stats().SSTables; got != 5 {
		t.Errorf("SSTables = %d, want the original 5", got)
	}
	if got := d.Stats().Compactions; got != 0 {
		t.Errorf("Compactions = %d, want 0 after a failed one", got)
	}
	for k, v := range want {
		expectValue(t, d, []byte(k), []byte(v))
	}

	// And a compaction must still work once the fault is removed.
	d.hooks.beforeCompactionEmit = nil
	if err := d.Compact(); err != nil {
		t.Fatalf("Compact after the failure: %v", err)
	}
	if got := d.Stats().SSTables; got != 1 {
		t.Errorf("SSTables after Compact = %d, want 1", got)
	}
	for k, v := range want {
		expectValue(t, d, []byte(k), []byte(v))
	}
}

func TestCompactionErrorDoesNotFailPut(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, &Options{
		MemtableSize:       2 << 10,
		CompactionBaseSize: 16 << 10,
		Sync:               wal.SyncNone,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.Close()

	boom := errors.New("simulated compaction failure")
	d.hooks.afterCompactionWritten = func() error { return boom }

	// Enough writes to flush many times, and so to attempt compaction.
	want := make(map[string]string, 2000)
	value := bytes.Repeat([]byte("v"), 60)
	for i := range 2000 {
		k := fmt.Sprintf("key-%05d", i)
		if err := d.Put([]byte(k), value); err != nil {
			t.Fatalf("Put(%d) = %v; a compaction failure must not fail the write", i, err)
		}
		want[k] = string(value)
	}

	stats := d.Stats()
	if stats.CompactionErrors == 0 {
		t.Fatal("CompactionErrors = 0, want the failures recorded")
	}
	if stats.Compactions != 0 {
		t.Errorf("Compactions = %d, want 0 since every one failed", stats.Compactions)
	}
	if err := d.LastCompactionError(); !errors.Is(err, boom) {
		t.Errorf("LastCompactionError = %v, want the hook's error", err)
	}
	t.Logf("%d compaction attempts failed across %d flushes, leaving %d tables",
		stats.CompactionErrors, stats.Flushes, stats.SSTables)

	// The data is all there despite the failures.
	for k := range want {
		expectValue(t, d, []byte(k), value)
	}

	// Once the fault is gone, the next flush compacts normally.
	d.hooks.afterCompactionWritten = nil
	if err := d.Put([]byte("trigger"), value); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := d.Stats().Compactions; got == 0 {
		t.Error("Compactions = 0 after the fault was removed, want compaction to resume")
	}
	for k := range want {
		expectValue(t, d, []byte(k), value)
	}
}

func TestConcurrentWithCompaction(t *testing.T) {
	const (
		writers = 8
		readers = 4
		each    = 500
	)
	dir := t.TempDir()
	// Small memtable and base size, so flushes and compactions run
	// constantly while the readers are working.
	opts := &Options{MemtableSize: 4 << 10, CompactionBaseSize: 32 << 10, Sync: wal.SyncNone}
	d := openDB(t, dir, opts)

	keyOf := func(w, i int) []byte { return []byte(fmt.Sprintf("w%d-key-%04d", w, i)) }
	value := func(w, i int) []byte {
		return []byte(fmt.Sprintf("w%d-i%04d-%s", w, i, strings.Repeat("v", 50)))
	}

	var wgWriters, wgReaders sync.WaitGroup
	errs := make(chan error, writers+readers)
	stop := make(chan struct{})

	for w := range writers {
		wgWriters.Add(1)
		go func(w int) {
			defer wgWriters.Done()
			for i := range each {
				if err := d.Put(keyOf(w, i), value(w, i)); err != nil {
					errs <- fmt.Errorf("Put(w%d i%d): %w", w, i, err)
					return
				}
			}
		}(w)
	}
	for r := range readers {
		wgReaders.Add(1)
		go func(r int) {
			defer wgReaders.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 31))
			for {
				select {
				case <-stop:
					return
				default:
				}
				w, i := rng.IntN(writers), rng.IntN(each)
				got, err := d.Get(keyOf(w, i))
				switch {
				case errors.Is(err, ErrNotFound):
				case err != nil:
					errs <- fmt.Errorf("Get(w%d i%d): %w", w, i, err)
					return
				case !bytes.Equal(got, value(w, i)):
					errs <- fmt.Errorf("Get(w%d i%d) = %q, want %q", w, i, got, value(w, i))
					return
				}
			}
		}(r)
	}

	wgWriters.Wait()
	close(stop)
	wgReaders.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent access failed: %v", err)
	}

	stats := d.Stats()
	if stats.Compactions == 0 {
		t.Error("Compactions = 0; the run did not exercise compaction under concurrency")
	}
	if stats.CompactionErrors != 0 {
		t.Errorf("CompactionErrors = %d (last: %v)", stats.CompactionErrors, d.LastCompactionError())
	}
	verify := func(t *testing.T, d *DB) {
		t.Helper()
		for w := range writers {
			for i := range each {
				expectValue(t, d, keyOf(w, i), value(w, i))
			}
		}
	}
	verify(t, d)
	t.Logf("%d flushes, %d compactions, %d tables at the end", stats.Flushes, stats.Compactions, stats.SSTables)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	verify(t, openDB(t, dir, opts))
}

func TestManifestAcceptsOutOfOrderTableNumbers(t *testing.T) {
	// A compaction's output carries a high number but sits where its inputs
	// were, so the list is not sorted. Version 2 has to accept that.
	m := manifest{nextFileNum: 20, logNumber: 19, sstables: []uint64{11, 15, 3, 7}}
	got, err := decodeManifest(m.encode())
	if err != nil {
		t.Fatalf("decodeManifest = %v, want out-of-order numbers accepted", err)
	}
	if !slices.Equal(got.sstables, m.sstables) {
		t.Errorf("sstables = %v, want %v in the original order", got.sstables, m.sstables)
	}

	t.Run("duplicates rejected", func(t *testing.T) {
		dup := manifest{nextFileNum: 20, logNumber: 19, sstables: []uint64{11, 15, 11}}
		if _, err := decodeManifest(dup.encode()); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("decodeManifest = %v, want ErrCorrupt for a repeated table number", err)
		}
	})

	t.Run("version 1 rejected", func(t *testing.T) {
		raw := m.encode()
		// Rewrite the version field and fix the checksum, so the only thing
		// wrong with the file is the version itself.
		binary.LittleEndian.PutUint32(raw[8:12], 1)
		binary.LittleEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(raw[:len(raw)-4], castagnoli))
		if _, err := decodeManifest(raw); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("decodeManifest = %v, want ErrCorrupt for a version 1 manifest", err)
		}
	})

	t.Run("number at or above nextFileNum rejected", func(t *testing.T) {
		bad := manifest{nextFileNum: 10, logNumber: 9, sstables: []uint64{3, 10}}
		if _, err := decodeManifest(bad.encode()); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("decodeManifest = %v, want ErrCorrupt", err)
		}
	})
}

func TestCompactionOrphanCleanup(t *testing.T) {
	dir := t.TempDir()
	opts := &Options{Sync: wal.SyncNone, DisableAutoCompaction: true}
	want := buildTables(t, dir, opts, 5, 100)

	d, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	m, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if len(m.sstables) != 1 {
		t.Fatalf("manifest lists %d tables after Compact, want 1", len(m.sstables))
	}
	d.abandon()

	// Debris of both compaction shapes: an output that was written but never
	// committed, and inputs that the manifest has already let go of.
	orphanOut := m.sstables[0] + 7
	strays := []string{sstName(orphanOut), "000003" + sstSuffix, sstName(orphanOut) + tmpSuffix}
	for _, name := range strays {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("debris"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}

	d2 := openDB(t, dir, opts)
	for _, name := range strays {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("Stat(%s) after Open = %v, want not-exist", name, err)
		}
	}
	for k, v := range want {
		expectValue(t, d2, []byte(k), []byte(v))
	}
	expectCleanDirectory(t, dir)
}

// benchKeys returns n distinct 16-byte keys in a shuffled order, so a load
// inserts them in no useful order.
func benchKeys(n int, seed uint64) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%016d", i))
	}
	rng := rand.New(rand.NewPCG(seed, seed))
	rng.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })
	return keys
}

// benchLoadSize is the number of entries the load benchmarks insert.
const benchLoadSize = 200_000

// benchOptions returns the options the load benchmarks share.
func benchOptions(disableCompaction bool) *Options {
	return &Options{
		MemtableSize:          1 << 20,
		Sync:                  wal.SyncNone,
		DisableAutoCompaction: disableCompaction,
	}
}

func benchmarkLoad(b *testing.B, disableCompaction bool) {
	keys := benchKeys(benchLoadSize, 42)
	value := bytes.Repeat([]byte("v"), 100)

	var last Stats
	b.SetBytes(int64(benchLoadSize) * 116)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		dir := b.TempDir()
		b.StartTimer()

		d, err := Open(dir, benchOptions(disableCompaction))
		if err != nil {
			b.Fatalf("Open: %v", err)
		}
		for _, k := range keys {
			if err := d.Put(k, value); err != nil {
				b.Fatalf("Put: %v", err)
			}
		}
		last = d.Stats()
		if err := d.Close(); err != nil {
			b.Fatalf("Close: %v", err)
		}
	}
	b.ReportMetric(last.WriteAmplification(), "write-amp")
	b.ReportMetric(float64(last.SSTables), "tables")
	b.ReportMetric(float64(last.Compactions), "compactions")
}

// BenchmarkLoad inserts benchLoadSize entries in random key order, with and
// without compaction, and reports the write amplification and table count each
// leaves behind.
func BenchmarkLoad(b *testing.B) {
	b.Run("compaction-on", func(b *testing.B) { benchmarkLoad(b, false) })
	b.Run("compaction-off", func(b *testing.B) { benchmarkLoad(b, true) })
}

// loadForGet builds the fixture the Get benchmarks share: one database holding
// benchLoadSize entries, loaded the same way BenchmarkLoad does.
func loadForGet(b *testing.B, disableCompaction bool) *DB {
	b.Helper()
	d, err := Open(b.TempDir(), benchOptions(disableCompaction))
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	value := bytes.Repeat([]byte("v"), 100)
	for _, k := range benchKeys(benchLoadSize, 42) {
		if err := d.Put(k, value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := d.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	b.Cleanup(func() { d.Close() })
	return d
}

// reportGetMetrics reports the per-lookup reader work between two snapshots.
func reportGetMetrics(b *testing.B, before, after Stats, ops int) {
	b.Helper()
	b.ReportMetric(float64(after.BlockReads-before.BlockReads)/float64(ops), "blockreads/op")
	b.ReportMetric(float64(after.BloomRejects-before.BloomRejects)/float64(ops), "bloomrejects/op")
	b.ReportMetric(float64(after.SSTables), "tables")
}

func benchmarkGetHit(b *testing.B, disableCompaction bool) {
	d := loadForGet(b, disableCompaction)

	b.ReportAllocs()
	before := d.Stats()
	ops, i := 0, 0
	for b.Loop() {
		// Strided, so successive lookups land in different blocks.
		key := []byte(fmt.Sprintf("%016d", (i*7919)%benchLoadSize))
		if _, err := d.Get(key); err != nil {
			b.Fatalf("Get: %v", err)
		}
		i++
		ops++
	}
	reportGetMetrics(b, before, d.Stats(), ops)
}

func benchmarkGetMissing(b *testing.B, disableCompaction bool) {
	d := loadForGet(b, disableCompaction)

	// Keys of the same shape but outside the written range, so every lookup
	// has to be ruled out by every table.
	misses := make([][]byte, 1024)
	for i := range misses {
		misses[i] = []byte(fmt.Sprintf("%016d", benchLoadSize+i))
	}

	b.ReportAllocs()
	before := d.Stats()
	ops, i := 0, 0
	for b.Loop() {
		if _, err := d.Get(misses[i%len(misses)]); !errors.Is(err, ErrNotFound) {
			b.Fatalf("Get(absent) = %v, want ErrNotFound", err)
		}
		i++
		ops++
	}
	reportGetMetrics(b, before, d.Stats(), ops)
}

// BenchmarkGetHit measures lookups that find their key, with and without
// compaction, which differ in how many tables the lookup walks through first.
func BenchmarkGetHit(b *testing.B) {
	b.Run("compaction-on", func(b *testing.B) { benchmarkGetHit(b, false) })
	b.Run("compaction-off", func(b *testing.B) { benchmarkGetHit(b, true) })
}

// BenchmarkGetMissing measures lookups that find nothing, which is the case
// that pays a Bloom check in every table and so shows what compaction is
// worth.
func BenchmarkGetMissing(b *testing.B) {
	b.Run("compaction-on", func(b *testing.B) { benchmarkGetMissing(b, false) })
	b.Run("compaction-off", func(b *testing.B) { benchmarkGetMissing(b, true) })
}
