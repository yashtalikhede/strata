// Package strata is an embedded, ordered key-value store built as a
// log-structured merge tree on the Go standard library alone.
//
// A database is a directory. Writes go to a write-ahead log and an in-memory
// table, which is flushed to an immutable sorted table once it grows past a
// threshold. Those tables are later merged with one another so that reads stay
// fast and overwritten data is reclaimed, though that merging happens inside a
// write rather than in the background; see the limitations below. Keys and values are
// opaque byte slices, keys are ordered bytewise, and the empty key is valid.
//
// Usage:
//
//	store, err := strata.Open("/tmp/mydb", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer store.Close()
//
//	if err := store.Put([]byte("colour"), []byte("blue")); err != nil {
//		log.Fatal(err)
//	}
//
//	value, err := store.Get([]byte("colour"))
//	switch {
//	case errors.Is(err, strata.ErrNotFound):
//		log.Print("no such key")
//	case err != nil:
//		log.Fatal(err)
//	default:
//		log.Printf("colour is %s", value)
//	}
//
//	if err := store.Delete([]byte("colour")); err != nil {
//		log.Fatal(err)
//	}
//
// # Guarantees
//
// A write that returns without error has been logged. Under the default
// [SyncAlways] policy it has reached stable storage and survives a power loss;
// under [SyncNone] it survives the process dying but not the machine losing
// power until [DB.Flush] or a later write forces it out. Either way, a crash
// at any moment leaves the database openable, and recovery restores exactly
// the writes that were acknowledged. Every byte on disk is checksummed, and a
// damaged table or log is reported rather than served as data.
//
// Reads see one consistent answer per call: the newest write for a key, or
// [ErrNotFound] if it was never written or has been deleted.
//
// All methods on [DB] are safe to call concurrently.
//
// # Limitations
//
// One process at a time may open a directory; a second [Open] returns
// [ErrLocked]. The lock relies on flock and so is effective only on unix.
//
// Flushes and compactions run synchronously inside the write lock, so the
// write that triggers one waits for it and so does every other writer and
// reader. There is no background compaction yet.
//
// There are no range scans, iterators or snapshots: a caller can look one key
// up and nothing else. There is no block cache, no compression, and no
// transactions.
//
// See docs/DESIGN.md in the repository for how all of this works and what it
// costs.
package strata

import (
	"github.com/yashtalikhede/strata/internal/db"
	"github.com/yashtalikhede/strata/internal/wal"
)

// DB is a key-value store over one directory. Open returns one, and all of its
// methods are safe for concurrent use.
type DB = db.DB

// Options configures a database. A nil *Options means every default.
type Options = db.Options

// Stats reports what a database holds and what it has had to do. The counters
// cover the period since the database was opened.
type Stats = db.Stats

// SyncPolicy selects how hard a write works to reach stable storage.
type SyncPolicy = wal.SyncPolicy

const (
	// SyncAlways fsyncs after every write, so a write that returns survives
	// a power loss. It is the zero value, so a policy nobody chose is the
	// safe one.
	SyncAlways = wal.SyncAlways
	// SyncNone leaves durability to the operating system. A write survives
	// the process dying, because the kernel still owns the data, but not
	// the machine losing power.
	SyncNone = wal.SyncNone
)

// Defaults applied to the zero value of each Options field.
const (
	// DefaultMemtableSize is how large the in-memory table may grow before
	// a write flushes it to disk.
	DefaultMemtableSize = db.DefaultMemtableSize
	// DefaultCompactionTrigger is how many tables of one size class it
	// takes to provoke a compaction.
	DefaultCompactionTrigger = db.DefaultCompactionTrigger
	// DefaultCompactionBaseSize is the size below which all tables count as
	// the smallest class.
	DefaultCompactionBaseSize = db.DefaultCompactionBaseSize
)

// Errors returned by this package. They are the same values the engine uses
// internally, so errors.Is works on anything a DB method returns.
var (
	// ErrNotFound reports that a key has no live value: it was never
	// written, or the newest record for it is a delete.
	ErrNotFound = db.ErrNotFound
	// ErrClosed reports that the database has been closed.
	ErrClosed = db.ErrClosed
	// ErrLocked reports that another process already has the directory
	// open. Only one may at a time.
	ErrLocked = db.ErrLocked
	// ErrCorrupt reports that the on-disk state does not hold together: a
	// bad checksum, or a manifest describing files that are missing.
	ErrCorrupt = db.ErrCorrupt
	// ErrFlushFailed reports that a write succeeded but the flush it
	// triggered did not. The write itself is durable and visible.
	ErrFlushFailed = db.ErrFlushFailed
)

// Open opens or creates the database in dir, creating the directory if it does
// not exist.
//
// It takes the directory's lock, replays whatever the last run left behind,
// and returns a database ready for use. A directory already open elsewhere
// returns an error wrapping [ErrLocked]; on-disk state that does not hold
// together returns one wrapping [ErrCorrupt]. A nil opts means every default.
//
// The caller must Close the result to release the lock.
func Open(dir string, opts *Options) (*DB, error) {
	return db.Open(dir, opts)
}
