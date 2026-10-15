package strata_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/yashtalikhede/strata"
)

// openTemp opens a database in a temporary directory, closing it when the
// test ends.
func openTemp(t *testing.T, opts *strata.Options) (*strata.DB, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := strata.Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store, dir
}

func TestRoundTripThroughFacade(t *testing.T) {
	store, dir := openTemp(t, &strata.Options{Sync: strata.SyncNone})

	const n = 200
	for i := range n {
		key := []byte(fmt.Sprintf("key-%04d", i))
		if err := store.Put(key, []byte(fmt.Sprintf("value-%04d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	// Enough to force a table, so the read path goes through disk as well as
	// the memtable.
	if err := store.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for i := range n {
		key := []byte(fmt.Sprintf("key-%04d", i))
		got, err := store.Get(key)
		if err != nil {
			t.Fatalf("Get(%s): %v", key, err)
		}
		if want := fmt.Sprintf("value-%04d", i); !bytes.Equal(got, []byte(want)) {
			t.Fatalf("Get(%s) = %q, want %q", key, got, want)
		}
	}

	// Delete, compact, reopen: the facade exposes the whole lifecycle.
	if err := store.Delete([]byte("key-0007")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := store.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if got := store.Stats().SSTables; got != 1 {
		t.Errorf("SSTables after Compact = %d, want 1", got)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := strata.Open(dir, &strata.Options{Sync: strata.SyncNone})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Get([]byte("key-0007")); !errors.Is(err, strata.ErrNotFound) {
		t.Errorf("Get(deleted) = %v, want ErrNotFound", err)
	}
	if _, err := reopened.Get([]byte("key-0008")); err != nil {
		t.Errorf("Get(key-0008) = %v, want it to survive the reopen", err)
	}
}

func TestErrorsAreComparableAcrossPackages(t *testing.T) {
	store, _ := openTemp(t, &strata.Options{Sync: strata.SyncNone})

	_, err := store.Get([]byte("never-written"))
	if !errors.Is(err, strata.ErrNotFound) {
		t.Fatalf("Get(absent) = %v, want ErrNotFound", err)
	}

	// The facade must re-export the engine's own error values rather than
	// copies, or errors.Is would be false for anything a DB method returns.
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := store.Get([]byte("anything")); !errors.Is(err, strata.ErrClosed) {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
	if err := store.Put([]byte("a"), []byte("b")); !errors.Is(err, strata.ErrClosed) {
		t.Errorf("Put after Close = %v, want ErrClosed", err)
	}
}

func TestOpenLockedDirectory(t *testing.T) {
	switch runtime.GOOS {
	case "windows", "plan9", "js", "wasip1":
		t.Skip("the directory lock is unix-only")
	}
	_, dir := openTemp(t, &strata.Options{Sync: strata.SyncNone})

	second, err := strata.Open(dir, nil)
	if err == nil {
		second.Close()
		t.Fatal("a second Open of the same directory succeeded, want ErrLocked")
	}
	if !errors.Is(err, strata.ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
}

func TestOpenCorruptDirectory(t *testing.T) {
	dir := t.TempDir()
	// A table with no manifest is a database whose manifest was lost, which
	// the engine refuses rather than silently starting empty.
	if err := os.WriteFile(filepath.Join(dir, "000001.sst"), []byte("not a table"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	store, err := strata.Open(dir, nil)
	if err == nil {
		store.Close()
		t.Fatal("Open succeeded on a directory holding a table but no manifest")
	}
	if !errors.Is(err, strata.ErrCorrupt) {
		t.Fatalf("Open = %v, want ErrCorrupt", err)
	}
}

func TestSyncPolicyConstants(t *testing.T) {
	// The zero value has to be the safe one, so a caller who sets only
	// MemtableSize still gets durable writes.
	var zero strata.SyncPolicy
	if zero != strata.SyncAlways {
		t.Errorf("the zero SyncPolicy is %v, want SyncAlways", zero)
	}
	if strata.SyncAlways == strata.SyncNone {
		t.Fatal("SyncAlways and SyncNone are the same value")
	}

	var opts strata.Options
	if opts.Sync != strata.SyncAlways {
		t.Errorf("the zero Options uses %v, want SyncAlways", opts.Sync)
	}

	// Both policies have to work end to end, whatever they cost.
	for name, policy := range map[string]strata.SyncPolicy{
		"SyncAlways": strata.SyncAlways,
		"SyncNone":   strata.SyncNone,
	} {
		t.Run(name, func(t *testing.T) {
			store, dir := openTemp(t, &strata.Options{Sync: policy})
			if err := store.Put([]byte("k"), []byte("v")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			// Close syncs the log, so the write survives either way.
			reopened, err := strata.Open(dir, &strata.Options{Sync: policy})
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer reopened.Close()
			got, err := reopened.Get([]byte("k"))
			if err != nil {
				t.Fatalf("Get after reopen: %v", err)
			}
			if !bytes.Equal(got, []byte("v")) {
				t.Errorf("Get = %q, want %q", got, "v")
			}
		})
	}
}

func TestDefaultsAreExported(t *testing.T) {
	// The defaults are part of the documented surface, so a caller can say
	// "twice the default" without guessing.
	if strata.DefaultMemtableSize != 4<<20 {
		t.Errorf("DefaultMemtableSize = %d, want 4 MiB", strata.DefaultMemtableSize)
	}
	if strata.DefaultCompactionTrigger != 4 {
		t.Errorf("DefaultCompactionTrigger = %d, want 4", strata.DefaultCompactionTrigger)
	}
	if strata.DefaultCompactionBaseSize != 1<<20 {
		t.Errorf("DefaultCompactionBaseSize = %d, want 1 MiB", strata.DefaultCompactionBaseSize)
	}
}
