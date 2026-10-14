package db

import "errors"

// Errors returned by this package. They are wrapped with detail, so callers
// should test for them with errors.Is rather than comparing directly.
var (
	// ErrNotFound reports that the key has no live value: either it was
	// never written, or the newest record for it is a tombstone.
	ErrNotFound = errors.New("db: key not found")
	// ErrClosed reports that the database has been closed.
	ErrClosed = errors.New("db: database is closed")
	// ErrLocked reports that another process already holds the directory's
	// lock. Only one process may have a database open at a time.
	ErrLocked = errors.New("db: directory is locked by another process")
	// ErrCorrupt reports that the on-disk state does not hold together: a
	// bad manifest, or a manifest describing files that are missing or
	// unreadable.
	ErrCorrupt = errors.New("db: corrupt database")
	// ErrFlushFailed reports that a write succeeded but the flush it
	// triggered did not. See Put for what that means for the write.
	ErrFlushFailed = errors.New("db: flush failed")
)
