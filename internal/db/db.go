package db

import (
	"fmt"
	"os"
	"sync"

	"github.com/yashtalikhede/strata/internal/memtable"
	"github.com/yashtalikhede/strata/internal/sstable"
	"github.com/yashtalikhede/strata/internal/wal"
)

// DefaultMemtableSize is the memtable size Options uses when none is given:
// four mebibytes of keys and values, after which the next write flushes.
const DefaultMemtableSize = 4 << 20

// Options configures a database. A nil *Options means all defaults.
type Options struct {
	// MemtableSize is the approximate size in bytes the memtable may reach
	// before a write flushes it to a table. A value of zero or less means
	// DefaultMemtableSize.
	MemtableSize int
	// Sync is the durability policy for the write-ahead log. Its zero value
	// is wal.SyncAlways, so a caller that does not think about it gets a
	// database in which a returned Put has reached stable storage.
	Sync wal.SyncPolicy
}

// withDefaults returns o with absent values filled in.
func (o *Options) withDefaults() Options {
	out := Options{MemtableSize: DefaultMemtableSize}
	if o != nil {
		out = *o
		if out.MemtableSize <= 0 {
			out.MemtableSize = DefaultMemtableSize
		}
	}
	return out
}

// Stats reports what the database holds and what it has had to do.
//
// The reader counters are summed over the tables currently live. Readers that
// have been closed are not counted, so the numbers fall when a database is
// closed and do not carry across a reopen.
type Stats struct {
	// MemtableBytes is the memtable's approximate size, the same measure
	// MemtableSize is compared against.
	MemtableBytes int
	// SSTables is the number of live tables.
	SSTables int
	// Flushes is how many flushes have completed since this DB was opened.
	Flushes uint64
	// BlockReads is the number of table blocks read from disk by lookups.
	BlockReads uint64
	// BloomRejects is the number of lookups a table's Bloom filter answered
	// without touching the file.
	BloomRejects uint64
}

// table pairs a live table's file number with its open reader.
type table struct {
	num uint64
	r   *sstable.Reader
}

// DB is a key-value store over one directory.
//
// All methods are safe for concurrent use. A single reader-writer mutex guards
// everything: Put, Delete and Flush take it exclusively, while Get and Stats
// share it, so any number of reads run in parallel with each other but none
// runs during a write. Flushes happen inside the write lock, which means a
// writer that trips the memtable threshold stalls every other writer until the
// table has been written. See the limitations section of docs/DESIGN.md.
type DB struct {
	// mu guards every field below it, and is held across a flush.
	mu   sync.RWMutex
	dir  string
	opts Options

	lock   *fileLock
	closed bool

	mem *memtable.Memtable
	// log is the one live write-ahead log, and logNum its file number.
	log    *wal.Log
	logNum uint64

	// tables holds the live tables oldest first, which is the order a read
	// walks in reverse.
	tables []*table

	nextFileNum uint64
	flushes     uint64

	// hooks are test-only interception points inside the flush routine. A
	// hook that returns an error aborts the flush with it and performs no
	// cleanup, which is how a test simulates the process dying at that
	// exact step. They are nil in normal use and cost one nil check.
	hooks struct {
		afterSSTWritten func() error
		afterNewWAL     func() error
		afterManifest   func() error
	}
}

// Open opens or creates the database in dir, creating the directory if it does
// not exist.
//
// It takes the directory's process lock, reads the manifest, removes files that
// no longer belong, opens a reader for every live table, replays the live
// write-ahead logs into a fresh memtable, and leaves the database with exactly
// one live and empty log. A directory already open elsewhere returns ErrLocked;
// on-disk state that does not hold together returns an error wrapping
// ErrCorrupt. See docs/DESIGN.md for the full sequence and why it is ordered
// that way.
func Open(dir string, opts *Options) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := acquireLock(dir)
	if err != nil {
		return nil, err
	}
	d := &DB{
		dir:  dir,
		opts: opts.withDefaults(),
		lock: lock,
		mem:  memtable.New(),
	}
	if err := d.recover(); err != nil {
		// Give back the lock and any readers already opened, so a failed
		// Open leaves nothing held.
		d.abandon()
		return nil, err
	}
	return d, nil
}

// Close syncs and closes the write-ahead log, closes every table reader, and
// releases the directory lock.
//
// It does not flush. The memtable's contents are already in the log, which is
// what the next Open replays, so flushing on close would only move work that
// recovery does anyway. Close is idempotent; calling it again returns nil.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true

	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if d.log != nil {
		keep(d.log.Sync())
		keep(d.log.Close())
		d.log = nil
	}
	for _, t := range d.tables {
		keep(t.r.Close())
	}
	d.tables = nil
	// The memtable's contents are already in the log, so dropping it loses
	// nothing and releases what can be several megabytes. It is also what
	// makes Stats report zeros rather than a size nothing can be read from.
	d.mem = nil
	if d.lock != nil {
		keep(d.lock.release())
		d.lock = nil
	}
	return firstErr
}

// abandon drops the database's file handles and releases the directory lock
// without flushing or syncing anything.
//
// It is test-only, and exists so that a test can simulate the process being
// killed without leaving the directory locked against the reopen that follows.
// Unsynced log records survive it exactly as they survive a kill, because the
// operating system still owns them.
func (d *DB) abandon() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return
	}
	d.closed = true
	if d.log != nil {
		_ = d.log.Close()
		d.log = nil
	}
	for _, t := range d.tables {
		_ = t.r.Close()
	}
	d.tables = nil
	if d.lock != nil {
		_ = d.lock.release()
		d.lock = nil
	}
}

// Put stores value under key, replacing any previous value or tombstone.
//
// The record reaches the write-ahead log before the memtable, so a write that
// returns has been logged and will come back after a crash. Both slices are
// copied.
//
// If the write pushes the memtable past its size limit, Put flushes before
// returning. A flush that fails returns an error wrapping ErrFlushFailed: the
// write itself is durable and visible to Get in that case, and only the move
// to a table did not happen, so the right response is to look at why the disk
// refused rather than to retry the write. An error from the log wraps the log's
// own sentinel, wal.ErrRecordTooLarge among them, and leaves the memtable
// untouched.
func (d *DB) Put(key, value []byte) error {
	return d.write(wal.Record{Op: wal.OpPut, Key: key, Value: value})
}

// Delete records a tombstone for key.
//
// Deleting a key that is not there is not an error: a tombstone has to be
// written either way, because an older table may hold a value that this delete
// has to shadow. The errors are the same as Put's.
func (d *DB) Delete(key []byte) error {
	return d.write(wal.Record{Op: wal.OpDelete, Key: key})
}

// write logs r, applies it, and flushes if the memtable has grown enough.
func (d *DB) write(r wal.Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}

	// Log first. Until this returns there is nothing to recover, and if it
	// fails the memtable must look as though the write never happened.
	if err := d.log.Append(r); err != nil {
		return fmt.Errorf("db: appending to log %06d: %w", d.logNum, err)
	}
	apply(d.mem, r)

	if d.mem.ApproxSize() >= d.opts.MemtableSize {
		if err := d.flushLocked(); err != nil {
			// Two sentinels, so a caller can ask both whether the flush
			// failed and what went wrong underneath.
			return fmt.Errorf("%w: %w", ErrFlushFailed, err)
		}
	}
	return nil
}

// apply puts one log record into a memtable.
func apply(m *memtable.Memtable, r wal.Record) {
	if r.Op == wal.OpDelete {
		m.Delete(r.Key)
		return
	}
	m.Put(r.Key, r.Value)
}

// Get returns a copy of the value stored under key.
//
// It returns ErrNotFound if the key was never written or its newest record is
// a tombstone. Sources are consulted newest first, the memtable and then the
// tables from newest to oldest, and the first one holding any record for the
// key decides the answer: that is what makes a tombstone shadow an older
// value instead of the older value showing through. A table that cannot be
// read returns a wrapped error rather than being skipped, because skipping it
// would turn corruption into a wrong answer.
func (d *DB) Get(key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}

	switch value, status := d.mem.Get(key); status {
	case memtable.Found:
		return value, nil
	case memtable.Deleted:
		return nil, ErrNotFound
	}

	for i := len(d.tables) - 1; i >= 0; i-- {
		value, status, err := d.tables[i].r.Get(key)
		if err != nil {
			return nil, fmt.Errorf("db: reading table %06d: %w", d.tables[i].num, err)
		}
		switch status {
		case memtable.Found:
			return value, nil
		case memtable.Deleted:
			return nil, ErrNotFound
		}
	}
	return nil, ErrNotFound
}

// Flush writes the memtable out as a new table and starts a fresh one. It is a
// no-op, and returns nil, if the memtable is empty.
//
// Unlike the flush a write triggers, this one does not wrap ErrFlushFailed,
// since there is no write whose fate the caller needs to be told apart from
// the flush's: the returned error is the underlying one.
func (d *DB) Flush() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	return d.flushLocked()
}

// Stats returns a snapshot of the database's size and counters. On a closed
// database it reports zeros.
func (d *DB) Stats() Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	s := Stats{SSTables: len(d.tables), Flushes: d.flushes}
	if d.mem != nil {
		s.MemtableBytes = d.mem.ApproxSize()
	}
	for _, t := range d.tables {
		rs := t.r.Stats()
		s.BlockReads += rs.BlockReads
		s.BloomRejects += rs.BloomRejects
	}
	return s
}
