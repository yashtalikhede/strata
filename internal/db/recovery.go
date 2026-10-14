package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/yashtalikhede/strata/internal/wal"
)

// File name suffixes used in a database directory.
const (
	sstSuffix = ".sst"
	walSuffix = ".wal"
	tmpSuffix = ".tmp"
)

// sstName and walName render a file number as a name. Six zero-padded digits
// sort the same way lexically as numerically for any database that stays below
// a million files, which makes a directory listing readable.
func sstName(n uint64) string { return fmt.Sprintf("%06d%s", n, sstSuffix) }

// walName returns the file name of log number n.
func walName(n uint64) string { return fmt.Sprintf("%06d%s", n, walSuffix) }

// parseFileNum splits a numbered file name into its number and suffix. It
// reports false for any name that is not digits followed by a known suffix,
// which is how LOCK, MANIFEST and anything a user left behind are passed over.
func parseFileNum(name string) (num uint64, suffix string, ok bool) {
	ext := filepath.Ext(name)
	if ext != sstSuffix && ext != walSuffix {
		return 0, "", false
	}
	stem := strings.TrimSuffix(name, ext)
	if stem == "" {
		return 0, "", false
	}
	n, err := strconv.ParseUint(stem, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return n, ext, true
}

// dirContents is what a scan of the database directory found.
type dirContents struct {
	ssts []uint64
	wals []uint64
	tmps []string
	// highest is the largest file number seen, so that a new number can be
	// chosen above anything already on disk even if the manifest disagrees.
	highest uint64
}

// scanDir lists the numbered files and temporary files in dir.
func scanDir(dir string) (dirContents, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return dirContents{}, err
	}
	var c dirContents
	for _, e := range ents {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Temporary files are recognised by suffix alone, since a half
		// written one may not have a numbered name at all.
		if strings.HasSuffix(name, tmpSuffix) {
			c.tmps = append(c.tmps, name)
			continue
		}
		num, suffix, ok := parseFileNum(name)
		if !ok {
			continue
		}
		c.highest = max(c.highest, num)
		switch suffix {
		case sstSuffix:
			c.ssts = append(c.ssts, num)
		case walSuffix:
			c.wals = append(c.wals, num)
		}
	}
	slices.Sort(c.ssts)
	slices.Sort(c.wals)
	return c, nil
}

// recover brings a freshly locked database up to date. The caller holds no
// lock, because nothing else can reach the DB until Open returns.
//
// The order is forced by what each step needs from the last. The manifest comes
// first because it is the only thing that says which files matter. Leftovers go
// next, so that nothing unreferenced is mistaken for state later. Readers open
// before the logs are replayed, so that a missing table is reported as such
// rather than after the work of a replay. The replay then rebuilds the memtable
// the crash lost. Finally the recovered memtable is flushed rather than kept,
// which is the step worth dwelling on: a replayed memtable is already durable
// in the logs it came from, so keeping it would mean the next open had to
// replay the same records again, and a crash during a long run of opens would
// keep replaying an ever-growing set. Flushing it turns those records into a
// table once and lets the logs be deleted, so every open starts from one empty
// log whatever happened before it.
func (d *DB) recover() error {
	// Scanned once, up front: a directory with many tables in it is not
	// cheap to list, and everything below can be answered from this one
	// snapshot. Files recovery creates afterwards are all numbered at or
	// above the new log number, so they never need to appear in it.
	contents, err := scanDir(d.dir)
	if err != nil {
		return err
	}

	m, err := readManifest(d.dir)
	if errors.Is(err, os.ErrNotExist) {
		if m, err = d.initManifest(contents); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	if err := d.removeOrphans(m, contents); err != nil {
		return err
	}
	if err := d.openTables(m); err != nil {
		return err
	}

	live := liveLogs(contents.wals, m.logNumber)
	if err := d.replayLogs(live); err != nil {
		return err
	}

	// A new file number must be above both what the manifest promised and
	// anything already on disk, so that recovery cannot hand out a number
	// that names an existing file.
	d.nextFileNum = max(m.nextFileNum, contents.highest+1)
	d.logNum = m.logNumber

	if d.mem.Len() > 0 {
		// A non-empty memtable can only have come from a replayed log, so
		// there is always one to adopt; the check is here so that a future
		// change which breaks that invariant fails loudly instead of
		// indexing an empty slice.
		if len(live) == 0 {
			return fmt.Errorf("%w: %d entries were replayed from no logs at all", ErrCorrupt, d.mem.Len())
		}
		// Adopt the newest live log as the current one, so the flush routine
		// retires it exactly as it retires any other.
		if err := d.adoptLog(live[len(live)-1]); err != nil {
			return err
		}
		if err := d.flushLocked(); err != nil {
			return err
		}
	} else if err := d.rotateLog(); err != nil {
		return err
	}

	// The log number has moved on, so every log the replay consumed is now
	// obsolete. Re-running the sweep here is what guarantees an open ends
	// with exactly one log, even if a crash had left several behind. Only
	// the logs found in the initial scan can be below the new number, since
	// anything created since is the new log itself.
	return d.removeLogsBelow(contents.wals, d.logNum)
}

// initManifest writes the manifest for a brand new database.
//
// A directory with tables or logs in it but no manifest is not a new database,
// it is one whose manifest was lost, and there is no way to tell which tables
// were live or which logs still mattered. Starting empty would silently discard
// all of it, so that case is corruption instead.
func (d *DB) initManifest(contents dirContents) (manifest, error) {
	if len(contents.ssts) > 0 || len(contents.wals) > 0 {
		return manifest{}, fmt.Errorf("%w: %s has no manifest but holds %d tables and %d logs",
			ErrCorrupt, d.dir, len(contents.ssts), len(contents.wals))
	}
	m := manifest{nextFileNum: 1, logNumber: 1}
	if err := writeManifest(d.dir, m); err != nil {
		return manifest{}, err
	}
	return m, nil
}

// removeOrphans deletes what the manifest does not account for: temporary
// files from an interrupted write, tables no manifest references, and logs
// below the log number. Each of those is debris from a crash, and each is
// safe to remove precisely because the manifest does not point at it.
func (d *DB) removeOrphans(m manifest, c dirContents) error {
	for _, name := range c.tmps {
		if err := os.Remove(filepath.Join(d.dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	for _, num := range c.ssts {
		if slices.Contains(m.sstables, num) {
			continue
		}
		if err := os.Remove(filepath.Join(d.dir, sstName(num))); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return d.removeLogsBelow(c.wals, m.logNumber)
}

// removeLogsBelow deletes those of wals numbered below logNumber. A log that
// is already gone is not an error, since the flush routine removes the one it
// retires itself.
func (d *DB) removeLogsBelow(wals []uint64, logNumber uint64) error {
	for _, num := range wals {
		if num >= logNumber {
			continue
		}
		if err := os.Remove(filepath.Join(d.dir, walName(num))); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// openTables opens a reader for every table the manifest lists. A table that
// is missing or unreadable fails the open: the manifest says it holds live
// data, so carrying on without it would serve stale or missing answers.
func (d *DB) openTables(m manifest) error {
	for _, num := range m.sstables {
		reader, size, err := openTable(d.dir, num)
		if err != nil {
			return fmt.Errorf("%w: opening table %06d: %w", ErrCorrupt, num, err)
		}
		d.tables = append(d.tables, &table{num: num, reader: reader, size: size})
	}
	return nil
}

// liveLogs returns the log numbers at or above logNumber, ascending. Those are
// the logs whose records are not yet in any table.
func liveLogs(wals []uint64, logNumber uint64) []uint64 {
	var live []uint64
	for _, num := range wals {
		if num >= logNumber {
			live = append(live, num)
		}
	}
	return live
}

// replayLogs rebuilds the memtable from the live logs, oldest first so that
// later records overwrite earlier ones exactly as they did before the crash.
func (d *DB) replayLogs(live []uint64) error {
	for _, num := range live {
		path := filepath.Join(d.dir, walName(num))
		if _, err := wal.Replay(path, func(r wal.Record) error {
			apply(d.mem, r)
			return nil
		}); err != nil {
			return fmt.Errorf("%w: replaying log %06d: %w", ErrCorrupt, num, err)
		}
	}
	return nil
}

// adoptLog opens log num for appending and makes it the current one, so that a
// recovery flush can retire it.
func (d *DB) adoptLog(num uint64) error {
	log, err := wal.Open(filepath.Join(d.dir, walName(num)), d.opts.Sync)
	if err != nil {
		return fmt.Errorf("db: reopening log %06d: %w", num, err)
	}
	if d.log != nil {
		_ = d.log.Close()
	}
	d.log, d.logNum = log, num
	return nil
}
