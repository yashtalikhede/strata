package db

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yashtalikhede/strata/internal/compaction"
	"github.com/yashtalikhede/strata/internal/memtable"
	"github.com/yashtalikhede/strata/internal/sstable"
)

// bucketOf returns the size class a table of the given size belongs to:
// 0 for anything below baseSize, and otherwise floor(log4(size/baseSize)) + 1.
//
// Four is the ratio because the compaction trigger is four: merging four
// tables of one class produces one table of roughly four times the size, which
// lands in the next class up and cannot immediately provoke another merge of
// the same tables. The arithmetic is integer throughout, so the classes are
// decided the same way on every platform and no rounding of a float can put
// the same table in two classes on two runs.
func bucketOf(size, baseSize int64) int {
	if baseSize <= 0 {
		baseSize = DefaultCompactionBaseSize
	}
	if size < baseSize {
		return 0
	}
	bucket := 1
	for n := size / baseSize; n >= 4; n /= 4 {
		bucket++
	}
	return bucket
}

// pickCompaction chooses the next run of tables to merge, given their sizes in
// manifest order, and reports the half-open range [start, end) to take.
//
// The policy is size-tiered: tables are sorted into size classes, and a class
// holding enough of them has them merged. It scans classes from smallest to
// largest, which keeps the cheap work ahead of the expensive work: merging
// four small tables costs little and removes four tables from every lookup,
// while merging the large ones rewrites a lot of bytes to remove the same
// count.
//
// Within a class it takes the oldest maximal run of adjacent tables, where
// adjacent means adjacent in the manifest, because a compaction's inputs have
// to be contiguous in that list for its output to take their place without
// disturbing which version of a key wins. A table of a different class in the
// middle therefore breaks a run rather than being merged through. At most
// maxCompactionInputs tables are taken, from the run's oldest end.
//
// It reports false when nothing is worth merging. It is a pure function of its
// arguments so that the policy can be tested without a database.
func pickCompaction(sizes []int64, trigger int, baseSize int64) (start, end int, ok bool) {
	if trigger < MinCompactionTrigger {
		trigger = MinCompactionTrigger
	}
	if len(sizes) < trigger {
		return 0, 0, false
	}

	buckets := make([]int, len(sizes))
	highest := 0
	for i, size := range sizes {
		buckets[i] = bucketOf(size, baseSize)
		highest = max(highest, buckets[i])
	}

	for bucket := 0; bucket <= highest; bucket++ {
		for i := 0; i < len(buckets); {
			if buckets[i] != bucket {
				i++
				continue
			}
			// The maximal run starting here: extend while the class holds.
			j := i
			for j < len(buckets) && buckets[j] == bucket {
				j++
			}
			if j-i >= trigger {
				return i, i + min(j-i, maxCompactionInputs), true
			}
			i = j
		}
	}
	return 0, 0, false
}

// tableSizes returns the live tables' sizes in manifest order.
func (d *DB) tableSizes() []int64 {
	sizes := make([]int64, len(d.tables))
	for i, t := range d.tables {
		sizes[i] = t.size
	}
	return sizes
}

// maybeCompactLocked runs compactions for as long as the policy asks for them.
// The caller must hold the write lock.
//
// A failure stops the loop and is recorded rather than returned, because the
// only callers are a flush and a write that has already succeeded. The next
// flush will ask the policy again and retry.
func (d *DB) maybeCompactLocked() {
	if d.opts.DisableAutoCompaction {
		return
	}
	for range maxCompactionsPerFlush {
		start, end, ok := pickCompaction(d.tableSizes(), d.opts.CompactionTrigger, d.opts.CompactionBaseSize)
		if !ok {
			return
		}
		if err := d.compactRun(start, end); err != nil {
			d.compactionErrors++
			d.lastCompactionErr = err
			return
		}
	}
}

// compactRun merges the tables at [start, end) into one and replaces them with
// it. The caller must hold the write lock.
//
// The range has to be contiguous in the manifest, since the output takes its
// inputs' place in that list and the list's order is what decides which
// version of a key wins. Tombstones are discarded only when the run starts at
// index 0: a tombstone exists to shadow an older value, and if any table older
// than the run survives, that value may be in it, so dropping the tombstone
// would bring a deleted key back.
//
// The steps mirror a flush, and for the same reason: everything before the
// manifest rename can be discarded, the rename commits, and nothing after it
// is allowed to matter.
//
//  1. Merge the inputs into a new table nothing references yet.
//  2. Write the manifest with the inputs replaced by the output. This atomic
//     rename is the commit point.
//  3. Close and delete the inputs, and swap the output into the live list.
//
// A failure before the rename leaves every byte of state as it was and removes
// the half-built output. A failure to delete an input afterwards is ignored,
// because the manifest no longer refers to it and the next open sweeps it up.
func (d *DB) compactRun(start, end int) error {
	if start < 0 || end > len(d.tables) || end-start < 1 {
		return fmt.Errorf("db: compaction range [%d,%d) is not within the %d live tables", start, end, len(d.tables))
	}

	inputs := d.tables[start:end]
	sources := make([]compaction.Source, len(inputs))
	for i, t := range inputs {
		sources[i] = t.reader.NewIterator()
	}
	// Only a run reaching the oldest table may discard tombstones.
	dropTombstones := start == 0

	outNum := d.nextFileNum
	outPath := filepath.Join(d.dir, sstName(outNum))
	w, err := sstable.NewWriter(outPath)
	if err != nil {
		return fmt.Errorf("db: creating compaction output %06d: %w", outNum, err)
	}

	emit := func(key, value []byte, tombstone bool) error {
		if err := fire(d.hooks.beforeCompactionEmit); err != nil {
			return err
		}
		e := memtable.Entry{Key: key, Tombstone: tombstone}
		if !tombstone {
			e.Value = value
		}
		return w.Add(e)
	}
	stats, err := compaction.Merge(sources, dropTombstones, emit)
	if err != nil {
		_ = w.Abort()
		return fmt.Errorf("db: merging tables %d to %d into %06d: %w", start, end-1, outNum, err)
	}

	// Everything may have been shadowed or dropped, in which case the run
	// collapses to nothing and the manifest simply loses those tables.
	empty := stats.EntriesWritten == 0
	var reader *sstable.Reader
	var outSize int64
	if empty {
		if err := w.Abort(); err != nil {
			return fmt.Errorf("db: discarding the empty compaction output %06d: %w", outNum, err)
		}
	} else {
		if err := w.Finish(); err != nil {
			_ = w.Abort()
			return fmt.Errorf("db: finishing compaction output %06d: %w", outNum, err)
		}
		if reader, outSize, err = openTable(d.dir, outNum); err != nil {
			os.Remove(outPath)
			return fmt.Errorf("db: opening compaction output %06d: %w", outNum, err)
		}
	}

	if err := fire(d.hooks.afterCompactionWritten); err != nil {
		return err
	}

	nums := make([]uint64, 0, len(d.tables))
	for _, t := range d.tables[:start] {
		nums = append(nums, t.num)
	}
	if !empty {
		nums = append(nums, outNum)
	}
	for _, t := range d.tables[end:] {
		nums = append(nums, t.num)
	}

	next := manifest{nextFileNum: outNum + 1, logNumber: d.logNum, sstables: nums}
	if err := writeManifest(d.dir, next); err != nil {
		if reader != nil {
			reader.Close()
			os.Remove(outPath)
		}
		return fmt.Errorf("db: committing the compaction of tables %d to %d: %w", start, end-1, err)
	}

	if err := fire(d.hooks.afterCompactionManifest); err != nil {
		return err
	}

	// Past the commit point. Nothing below may fail in a way that matters.
	var bytesRead int64
	for _, t := range inputs {
		bytesRead += t.size
		_ = t.reader.Close()
		_ = os.Remove(filepath.Join(d.dir, sstName(t.num)))
	}

	live := make([]*table, 0, len(d.tables))
	live = append(live, d.tables[:start]...)
	if !empty {
		live = append(live, &table{num: outNum, reader: reader, size: outSize})
	}
	live = append(live, d.tables[end:]...)
	d.tables = live

	d.nextFileNum = outNum + 1
	d.compactions++
	d.compactionBytesRead += uint64(bytesRead)
	d.compactionBytesWritten += uint64(outSize)
	d.tombstonesDropped += stats.TombstonesDropped
	d.shadowedDropped += stats.Shadowed
	return nil
}

// Compact merges every table into one, flushing the memtable first if it holds
// anything.
//
// Because the merge then covers the oldest table, it is the one moment at
// which every tombstone can be discarded: nothing older survives for them to
// shadow. That makes Compact the way to actually reclaim the space taken by
// deleted and overwritten data, which the automatic policy only does for runs
// that happen to reach the oldest table.
//
// A single table is still rewritten, which is not pointless: it is what drops
// that table's tombstones. The cost is one full rewrite of the data.
//
// Unlike the compactions a flush triggers, Compact reports its errors.
func (d *DB) Compact() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	if err := d.flushLocked(); err != nil {
		return err
	}
	if len(d.tables) == 0 {
		return nil
	}
	return d.compactRun(0, len(d.tables))
}

// openTable opens the table numbered num in dir and reports its size.
func openTable(dir string, num uint64) (*sstable.Reader, int64, error) {
	path := filepath.Join(dir, sstName(num))
	r, err := sstable.Open(path)
	if err != nil {
		return nil, 0, err
	}
	fi, err := os.Stat(path)
	if err != nil {
		r.Close()
		return nil, 0, err
	}
	return r, fi.Size(), nil
}
