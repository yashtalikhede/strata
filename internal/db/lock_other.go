//go:build !unix

package db

// fileLock is a placeholder on platforms without flock.
//
// The directory lock is unix-only. Everywhere else acquireLock succeeds
// unconditionally and nothing stops two processes from opening the same
// database at once, which would let both recover and both write. A caller on
// such a platform has to arrange exclusion itself.
type fileLock struct{}

// acquireLock does nothing and always succeeds. See fileLock for why.
func acquireLock(dir string) (*fileLock, error) {
	_ = dir
	return &fileLock{}, nil
}

// release does nothing, because nothing was locked.
func (l *fileLock) release() error { return nil }
