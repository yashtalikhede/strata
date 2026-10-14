//go:build unix

package db

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// fileLock is an advisory lock on a database directory, held for as long as
// the file descriptor stays open.
type fileLock struct {
	f *os.File
}

// acquireLock takes an exclusive, non-blocking flock on dir's LOCK file.
//
// flock is tied to the open descriptor rather than to the path, so the lock
// goes away by itself if the process dies, however it dies: there is no stale
// lock to clean up after a crash, which is exactly the property a lock guarding
// recovery needs. It returns ErrLocked if another process holds it.
func acquireLock(dir string) (*fileLock, error) {
	path := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		// Both spellings mean "someone else has it"; they are the same value
		// on Linux and darwin but are not guaranteed to be everywhere.
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrLocked, path)
		}
		return nil, err
	}
	return &fileLock{f: f}, nil
}

// release drops the lock by closing the descriptor it is attached to. The LOCK
// file itself is left in place, since removing it would let a second process
// create and lock a new one while this one still believes it holds the
// directory.
func (l *fileLock) release() error {
	return l.f.Close()
}
