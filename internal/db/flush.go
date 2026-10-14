package db

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/yashtalikhede/strata/internal/memtable"
	"github.com/yashtalikhede/strata/internal/sstable"
	"github.com/yashtalikhede/strata/internal/wal"
)

// fire calls a test hook if one is set. A hook that returns an error aborts
// the flush with no cleanup, standing in for the process dying at that point.
func fire(h func() error) error {
	if h == nil {
		return nil
	}
	return h()
}

// flushLocked moves the memtable into a new table. The caller must hold the
// write lock.
//
// The steps are ordered so that exactly one of them is the commit point, and
// everything before it can be thrown away without consequence:
//
//  1. Snapshot the memtable. An empty one means there is nothing to do.
//  2. Write the table and open a reader on it. The file is not referenced by
//     any manifest yet, so it is invisible to a reopen and removable.
//  3. Create the log the next writes will go to, also unreferenced so far.
//  4. Write the manifest naming the new table and the new log. This single
//     atomic rename is the commit point: before it the old state is whole and
//     the new files are orphans the next Open would delete, and after it the
//     new state is whole and the old log is the orphan.
//  5. Only now change anything in memory, and delete the old log.
//  6. Count the flush.
//
// A failure before step 4 leaves the in-memory state untouched and removes the
// files this flush created, best effort; the next Open would remove them
// anyway. A failure to delete the old log in step 5 is ignored, because the
// manifest has already moved past it and the next Open will clean it up.
func (d *DB) flushLocked() error {
	entries := d.mem.Entries()
	if len(entries) == 0 {
		return nil
	}

	// Numbers are only reserved locally; d.nextFileNum does not move until
	// the manifest makes the flush real.
	sstNum := d.nextFileNum
	newLogNum := d.nextFileNum + 1
	sstPath := filepath.Join(d.dir, sstName(sstNum))
	newLogPath := filepath.Join(d.dir, walName(newLogNum))

	if err := sstable.WriteEntries(sstPath, entries); err != nil {
		return fmt.Errorf("db: writing table %06d: %w", sstNum, err)
	}
	if err := fire(d.hooks.afterSSTWritten); err != nil {
		return err
	}

	reader, outSize, err := openTable(d.dir, sstNum)
	if err != nil {
		os.Remove(sstPath)
		return fmt.Errorf("db: opening the table just written, %06d: %w", sstNum, err)
	}

	newLog, err := wal.Open(newLogPath, d.opts.Sync)
	if err != nil {
		reader.Close()
		os.Remove(sstPath)
		return fmt.Errorf("db: creating log %06d: %w", newLogNum, err)
	}
	if err := fire(d.hooks.afterNewWAL); err != nil {
		return err
	}

	next := manifest{
		nextFileNum: newLogNum + 1,
		logNumber:   newLogNum,
		sstables:    append(d.tableNumbers(), sstNum),
	}
	if err := writeManifest(d.dir, next); err != nil {
		newLog.Close()
		os.Remove(newLogPath)
		reader.Close()
		os.Remove(sstPath)
		return fmt.Errorf("db: committing the flush of table %06d: %w", sstNum, err)
	}
	if err := fire(d.hooks.afterManifest); err != nil {
		return err
	}

	// Past the commit point. Nothing below may fail in a way that matters.
	oldLogPath := filepath.Join(d.dir, walName(d.logNum))
	if d.log != nil {
		_ = d.log.Close()
	}
	_ = os.Remove(oldLogPath)

	d.log, d.logNum = newLog, newLogNum
	d.mem = memtable.New()
	d.tables = append(d.tables, &table{num: sstNum, reader: reader, size: outSize})
	d.nextFileNum = newLogNum + 1
	d.flushes++
	d.flushBytesWritten += uint64(outSize)
	return nil
}

// tableNumbers returns the live table numbers, oldest first, as a fresh slice
// the caller may append to.
func (d *DB) tableNumbers() []uint64 {
	nums := make([]uint64, 0, len(d.tables)+1)
	for _, t := range d.tables {
		nums = append(nums, t.num)
	}
	return nums
}

// rotateLog creates a brand new, empty log, commits a manifest naming it, and
// makes it current. The caller must hold the write lock.
//
// The log file is created before the manifest names it, so the manifest never
// points at a log that does not exist. Recovery uses this when there was
// nothing to flush, which is how an open always ends with exactly one live and
// empty log.
func (d *DB) rotateLog() error {
	num := d.nextFileNum
	path := filepath.Join(d.dir, walName(num))

	log, err := wal.Open(path, d.opts.Sync)
	if err != nil {
		return fmt.Errorf("db: creating log %06d: %w", num, err)
	}
	next := manifest{
		nextFileNum: num + 1,
		logNumber:   num,
		sstables:    d.tableNumbers(),
	}
	if err := writeManifest(d.dir, next); err != nil {
		log.Close()
		os.Remove(path)
		return fmt.Errorf("db: recording log %06d in the manifest: %w", num, err)
	}

	if d.log != nil {
		_ = d.log.Close()
	}
	d.log, d.logNum = log, num
	d.nextFileNum = num + 1
	return nil
}
