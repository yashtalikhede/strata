package db

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yashtalikhede/strata/internal/wal"
)

// logicalOpts returns the options for a test about logical correctness rather
// than durability.
//
// SyncNone keeps the write-ahead log out of fsync, which on macOS is
// F_FULLFSYNC and costs milliseconds a call. Nothing is given up: Close syncs
// the log, so everything these tests check still survives a close and reopen.
// Durability against a process dying mid-write is covered by
// TestCrashRecoveryProcessKill and the hook-based crash tests, which keep
// SyncAlways precisely because that is what they are about.
func logicalOpts() *Options { return &Options{Sync: wal.SyncNone} }

// openDB opens a database in dir with opts, closing it when the test ends.
func openDB(t *testing.T, dir string, opts *Options) *DB {
	t.Helper()
	d, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open(%s): %v", dir, err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// expectValue asserts that key reads back as want.
func expectValue(t *testing.T, d *DB, key, want []byte) {
	t.Helper()
	got, err := d.Get(key)
	if err != nil {
		t.Fatalf("Get(%q) = %v, want %q", key, err, want)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

// expectMissing asserts that key is absent or deleted.
func expectMissing(t *testing.T, d *DB, key []byte) {
	t.Helper()
	got, err := d.Get(key)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) = (%q, %v), want ErrNotFound", key, got, err)
	}
}

// listDir returns the names in dir, sorted.
func listDir(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		names = append(names, e.Name())
	}
	slices.Sort(names)
	return names
}

// countSuffix returns how many names in dir end with suffix.
func countSuffix(t *testing.T, dir, suffix string) int {
	t.Helper()
	n := 0
	for _, name := range listDir(t, dir) {
		if strings.HasSuffix(name, suffix) {
			n++
		}
	}
	return n
}

func TestBasic(t *testing.T) {
	d := openDB(t, t.TempDir(), logicalOpts())

	if err := d.Put([]byte("a"), []byte("one")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	expectValue(t, d, []byte("a"), []byte("one"))

	// Overwrite.
	if err := d.Put([]byte("a"), []byte("two")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	expectValue(t, d, []byte("a"), []byte("two"))

	// Delete, then the key is gone.
	if err := d.Delete([]byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	expectMissing(t, d, []byte("a"))

	// A key never written.
	expectMissing(t, d, []byte("never"))

	// Deleting an absent key is not an error: the tombstone has to be
	// written anyway, in case an older table holds a value.
	if err := d.Delete([]byte("absent")); err != nil {
		t.Errorf("Delete(absent) = %v, want nil", err)
	}

	// The empty key and the empty value are both real.
	if err := d.Put([]byte{}, []byte("empty key")); err != nil {
		t.Fatalf("Put(empty key): %v", err)
	}
	expectValue(t, d, []byte{}, []byte("empty key"))
	if err := d.Put([]byte("empty value"), []byte{}); err != nil {
		t.Fatalf("Put(empty value): %v", err)
	}
	got, err := d.Get([]byte("empty value"))
	if err != nil {
		t.Fatalf("Get(empty value): %v", err)
	}
	if got == nil {
		t.Error("Get of an empty value returned nil; it must be distinguishable from a missing key")
	}
	if len(got) != 0 {
		t.Errorf("Get(empty value) = %q, want empty", got)
	}

	// Binary keys with zero bytes.
	bin := []byte{0x00, 0x01, 0x00, 0xff, 0x00}
	if err := d.Put(bin, []byte{0xff, 0x00, 0xfe}); err != nil {
		t.Fatalf("Put(binary): %v", err)
	}
	expectValue(t, d, bin, []byte{0xff, 0x00, 0xfe})
	expectMissing(t, d, []byte{0x00, 0x01, 0x00, 0xff})

	// Get must return a copy: mutating it cannot reach the store.
	v, err := d.Get([]byte{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	for i := range v {
		v[i] = 'X'
	}
	expectValue(t, d, []byte{}, []byte("empty key"))
}

func TestPersistenceViaWAL(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, logicalOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	const n = 500
	for i := range n {
		if err := d.Put([]byte(fmt.Sprintf("k%04d", i)), []byte(fmt.Sprintf("v%04d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := d.Delete([]byte("k0007")); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// No flush: everything is in the memtable and the log.
	if got := d.Stats().SSTables; got != 0 {
		t.Fatalf("SSTables = %d before any flush, want 0", got)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	d2 := openDB(t, dir, logicalOpts())
	for i := range n {
		k := []byte(fmt.Sprintf("k%04d", i))
		if i == 7 {
			expectMissing(t, d2, k)
			continue
		}
		expectValue(t, d2, k, []byte(fmt.Sprintf("v%04d", i)))
	}
	// Recovery flushes what it replayed, so the records are now in a table.
	if got := d2.Stats().SSTables; got != 1 {
		t.Errorf("SSTables after recovery = %d, want 1", got)
	}
	if got := countSuffix(t, dir, walSuffix); got != 1 {
		t.Errorf("%d logs on disk after recovery, want exactly 1", got)
	}
}

func TestFlushAndReadAcrossSSTables(t *testing.T) {
	dir := t.TempDir()
	// Compaction is off here on purpose: this test is about reading across
	// many tables, and the table count is how it checks that it is actually
	// doing so. With compaction on there is no way to keep ten tables around,
	// and the merged read path gets its own tests.
	d := openDB(t, dir, &Options{
		MemtableSize:          4 << 10,
		Sync:                  wal.SyncNone,
		DisableAutoCompaction: true,
	})

	// Values big enough that a 4 KiB memtable fills every few writes.
	const n = 600
	value := func(i int) []byte {
		return append([]byte(fmt.Sprintf("value-%04d-", i)), bytes.Repeat([]byte("p"), 100)...)
	}
	for i := range n {
		if err := d.Put([]byte(fmt.Sprintf("key-%06d", i)), value(i)); err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
	}

	stats := d.Stats()
	if stats.SSTables < 10 {
		t.Fatalf("SSTables = %d, want at least 10", stats.SSTables)
	}
	if stats.Flushes != uint64(stats.SSTables) {
		t.Errorf("Flushes = %d but SSTables = %d, want them equal", stats.Flushes, stats.SSTables)
	}
	if onDisk := countSuffix(t, dir, sstSuffix); onDisk != stats.SSTables {
		t.Errorf("%d .sst files on disk, but Stats reports %d tables", onDisk, stats.SSTables)
	}

	for i := range n {
		expectValue(t, d, []byte(fmt.Sprintf("key-%06d", i)), value(i))
	}
	expectMissing(t, d, []byte("key-999999"))
	t.Logf("%d keys across %d tables", n, stats.SSTables)
}

func TestTombstoneShadowsOlderSSTable(t *testing.T) {
	dir := t.TempDir()
	d := openDB(t, dir, &Options{Sync: wal.SyncNone})
	k := []byte("shadowed")

	if err := d.Put(k, []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	expectValue(t, d, k, []byte("value"))

	// Tombstone in the memtable, over a value in a table.
	if err := d.Delete(k); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	expectMissing(t, d, k)

	// Tombstone in a newer table, over a value in an older one.
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if got := d.Stats().SSTables; got != 2 {
		t.Fatalf("SSTables = %d, want 2", got)
	}
	expectMissing(t, d, k)

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d2 := openDB(t, dir, &Options{Sync: wal.SyncNone})
	expectMissing(t, d2, k)
}

func TestNewestWins(t *testing.T) {
	k := []byte("contested")

	// Compaction off, so the five versions really do sit in five tables and
	// the lookup has to pick between them.
	t.Run("across tables", func(t *testing.T) {
		dir := t.TempDir()
		opts := &Options{Sync: wal.SyncNone, DisableAutoCompaction: true}
		d := openDB(t, dir, opts)

		for round := range 5 {
			if err := d.Put(k, []byte(fmt.Sprintf("round-%d", round))); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := d.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			expectValue(t, d, k, []byte(fmt.Sprintf("round-%d", round)))
		}
		if got := d.Stats().SSTables; got != 5 {
			t.Fatalf("SSTables = %d, want 5", got)
		}
		expectValue(t, d, k, []byte("round-4"))

		if err := d.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		d2 := openDB(t, dir, opts)
		expectValue(t, d2, k, []byte("round-4"))
	})

	// And the answer must survive the five tables being merged into one,
	// which is the same property asked of the compactor instead of the
	// lookup.
	t.Run("after compaction", func(t *testing.T) {
		dir := t.TempDir()
		opts := &Options{Sync: wal.SyncNone, DisableAutoCompaction: true}
		d := openDB(t, dir, opts)

		for round := range 5 {
			if err := d.Put(k, []byte(fmt.Sprintf("round-%d", round))); err != nil {
				t.Fatalf("Put: %v", err)
			}
			if err := d.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
		}
		if err := d.Compact(); err != nil {
			t.Fatalf("Compact: %v", err)
		}
		if got := d.Stats().SSTables; got != 1 {
			t.Fatalf("SSTables after Compact = %d, want 1", got)
		}
		if got := d.Stats().ShadowedDropped; got != 4 {
			t.Errorf("ShadowedDropped = %d, want 4 older versions discarded", got)
		}
		expectValue(t, d, k, []byte("round-4"))

		if err := d.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		d2 := openDB(t, dir, opts)
		expectValue(t, d2, k, []byte("round-4"))
	})
}

func TestModelRandomized(t *testing.T) {
	variants := []struct {
		name string
		opts Options
	}{
		{
			// Small sizes so flushes and compactions both happen often.
			name: "compaction on",
			opts: Options{MemtableSize: 8 << 10, CompactionBaseSize: 32 << 10, Sync: wal.SyncNone},
		},
		{
			name: "compaction off",
			opts: Options{MemtableSize: 8 << 10, Sync: wal.SyncNone, DisableAutoCompaction: true},
		},
	}

	for _, variant := range variants {
		for _, seed := range []uint64{3, 11} {
			t.Run(fmt.Sprintf("%s/seed-%d", strings.ReplaceAll(variant.name, " ", "-"), seed), func(t *testing.T) {
				t.Parallel()
				const keyspace = 400
				// Enough operations to shake out ordering bugs, but the
				// count is what makes this the slowest test in the repo, so
				// -short takes a sixth of it.
				ops := 30_000
				if testing.Short() {
					ops = 5_000
				}
				rng := rand.New(rand.NewPCG(seed, seed))
				dir := t.TempDir()
				opts := variant.opts

				d, err := Open(dir, &opts)
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				defer func() {
					if d != nil {
						d.Close()
					}
				}()

				// model holds the value for each live key; absence means
				// either never written or deleted, which Get reports the
				// same way.
				model := make(map[string][]byte, keyspace)
				poolKey := func(i int) string { return fmt.Sprintf("key-%04d", i) }

				// Counters reset when a database is closed, and this
				// workload reopens hundreds of times, so the totals have to
				// be gathered before each Close rather than read at the end.
				var reopens, flushes, compacts int
				var totalCompactions, totalCompactionErrors uint64
				for range ops {
					k := poolKey(rng.IntN(keyspace))
					switch roll := rng.IntN(100); {
					case roll < 50: // Put
						v := []byte(fmt.Sprintf("v-%d-%s", rng.Uint64(), strings.Repeat("x", rng.IntN(80))))
						if err := d.Put([]byte(k), v); err != nil {
							t.Fatalf("Put(%s): %v", k, err)
						}
						model[k] = v
					case roll < 70: // Delete
						if err := d.Delete([]byte(k)); err != nil {
							t.Fatalf("Delete(%s): %v", k, err)
						}
						delete(model, k)
					case roll < 94: // Get
						got, err := d.Get([]byte(k))
						want, live := model[k]
						switch {
						case !live:
							if !errors.Is(err, ErrNotFound) {
								t.Fatalf("Get(%s) = (%q, %v), want ErrNotFound", k, got, err)
							}
						case err != nil:
							t.Fatalf("Get(%s) = %v, want %q", k, err, want)
						case !bytes.Equal(got, want):
							t.Fatalf("Get(%s) = %q, want %q", k, got, want)
						}
					case roll < 97: // Flush
						if err := d.Flush(); err != nil {
							t.Fatalf("Flush: %v", err)
						}
						flushes++
					case roll < 98: // Compact
						if err := d.Compact(); err != nil {
							t.Fatalf("Compact: %v", err)
						}
						compacts++
					default: // Close and reopen
						s := d.Stats()
						totalCompactions += s.Compactions
						totalCompactionErrors += s.CompactionErrors
						if err := d.Close(); err != nil {
							t.Fatalf("Close: %v", err)
						}
						if d, err = Open(dir, &opts); err != nil {
							t.Fatalf("reopen: %v", err)
						}
						reopens++
					}
				}

				// Every key in the pool, live or not.
				for i := range keyspace {
					k := poolKey(i)
					got, err := d.Get([]byte(k))
					want, live := model[k]
					switch {
					case !live:
						if !errors.Is(err, ErrNotFound) {
							t.Fatalf("final Get(%s) = (%q, %v), want ErrNotFound", k, got, err)
						}
					case err != nil:
						t.Fatalf("final Get(%s) = %v, want %q", k, err, want)
					case !bytes.Equal(got, want):
						t.Fatalf("final Get(%s) = %q, want %q", k, got, want)
					}
				}

				stats := d.Stats()
				totalCompactions += stats.Compactions
				totalCompactionErrors += stats.CompactionErrors
				if totalCompactionErrors != 0 {
					t.Errorf("CompactionErrors = %d (last: %v)", totalCompactionErrors, d.LastCompactionError())
				}
				if totalCompactions == 0 {
					t.Error("no compaction ran at all; the workload did not provoke any")
				}
				t.Logf("%d ops, %d live keys, %d flushes, %d explicit compacts, %d reopens, %d tables, %d compactions in all, write-amp %.2f since the last open",
					ops, len(model), flushes, compacts, reopens, stats.SSTables, totalCompactions, stats.WriteAmplification())
			})
		}
	}
}

func TestReopenAfterAbandonAtEveryFlushStage(t *testing.T) {
	stages := []struct {
		name string
		set  func(d *DB, fail func() error)
	}{
		{"afterSSTWritten", func(d *DB, f func() error) { d.hooks.afterSSTWritten = f }},
		{"afterNewWAL", func(d *DB, f func() error) { d.hooks.afterNewWAL = f }},
		{"afterManifest", func(d *DB, f func() error) { d.hooks.afterManifest = f }},
	}

	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := &Options{Sync: wal.SyncNone}

			d, err := Open(dir, opts)
			if err != nil {
				t.Fatalf("Open: %v", err)
			}

			// Acknowledged writes, which must all survive.
			const n = 300
			want := make(map[string]string, n)
			for i := range n {
				k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("value-%04d", i)
				if err := d.Put([]byte(k), []byte(v)); err != nil {
					t.Fatalf("Put: %v", err)
				}
				want[k] = v
			}
			// A flush before the crash, so recovery has an older table to
			// keep as well as logs to replay.
			if err := d.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			for i := n; i < 2*n; i++ {
				k, v := fmt.Sprintf("key-%04d", i), fmt.Sprintf("value-%04d", i)
				if err := d.Put([]byte(k), []byte(v)); err != nil {
					t.Fatalf("Put: %v", err)
				}
				want[k] = v
			}

			// Now die partway through a flush.
			boom := errors.New("simulated crash")
			stage.set(d, func() error { return boom })
			if err := d.Flush(); !errors.Is(err, boom) {
				t.Fatalf("Flush with the %s hook failing = %v, want the hook's error", stage.name, err)
			}
			d.abandon()

			d2 := openDB(t, dir, opts)
			for k, v := range want {
				expectValue(t, d2, []byte(k), []byte(v))
			}
			// Nothing extra, and nothing duplicated: a full sweep of the
			// key space finds exactly the keys that were written.
			for i := 2 * n; i < 3*n; i++ {
				expectMissing(t, d2, []byte(fmt.Sprintf("key-%04d", i)))
			}

			// The directory must hold only what the manifest accounts for.
			m, err := readManifest(dir)
			if err != nil {
				t.Fatalf("readManifest: %v", err)
			}
			wantNames := []string{lockFileName, manifestFileName}
			for _, num := range m.sstables {
				wantNames = append(wantNames, sstName(num))
			}
			wantNames = append(wantNames, walName(m.logNumber))
			slices.Sort(wantNames)

			if got := listDir(t, dir); !slices.Equal(got, wantNames) {
				t.Errorf("directory holds %v, want %v", got, wantNames)
			}
			t.Logf("recovered %d keys into %d tables", len(want), len(m.sstables))
		})
	}
}

func TestOrphanCleanup(t *testing.T) {
	dir := t.TempDir()
	d := openDB(t, dir, &Options{Sync: wal.SyncNone})
	for i := range 50 {
		if err := d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i))); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if m.logNumber < 2 {
		t.Fatalf("log number is %d; the test needs room for a log below it", m.logNumber)
	}

	// Debris of every kind a crash can leave.
	strays := []string{
		"000099" + sstSuffix, // a table no manifest references
		"000098" + walSuffix, // a log below the log number
		"x" + tmpSuffix,      // a half-written temporary file
		manifestTmpName,
		"000097.sst.tmp",
	}
	for _, name := range strays {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("garbage"), 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", name, err)
		}
	}

	d2 := openDB(t, dir, &Options{Sync: wal.SyncNone})
	for _, name := range strays {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("Stat(%s) after Open = %v, want not-exist", name, err)
		}
	}
	for i := range 50 {
		expectValue(t, d2, []byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("v%03d", i)))
	}
}

func TestManifestCorruption(t *testing.T) {
	dir := t.TempDir()
	d := openDB(t, dir, &Options{Sync: wal.SyncNone})
	for i := range 100 {
		if err := d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	manifestPath := filepath.Join(dir, manifestFileName)
	good, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if len(good) <= manifestMinSize {
		t.Fatalf("manifest is %d bytes; the test needs one listing tables", len(good))
	}

	t.Run("bit flips", func(t *testing.T) {
		for i := range good {
			corrupt := append([]byte(nil), good...)
			corrupt[i] ^= 0x01
			if err := os.WriteFile(manifestPath, corrupt, 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			d2, err := Open(dir, logicalOpts())
			if err == nil {
				d2.Close()
				t.Fatalf("Open succeeded with manifest byte %d flipped", i)
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open with manifest byte %d flipped = %v, want ErrCorrupt", i, err)
			}
		}
		// Restore, so the fixture is sound for the next subtest.
		if err := os.WriteFile(manifestPath, good, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		d2, err := Open(dir, logicalOpts())
		if err != nil {
			t.Fatalf("Open(restored) = %v, want no error", err)
		}
		d2.Close()
	})

	t.Run("missing manifest with tables present", func(t *testing.T) {
		if err := os.Remove(manifestPath); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		if n := countSuffix(t, dir, sstSuffix); n == 0 {
			t.Fatal("test is wrong: no tables left to make this corruption")
		}
		d2, err := Open(dir, logicalOpts())
		if err == nil {
			d2.Close()
			t.Fatal("Open succeeded with no manifest but tables on disk; it must never silently start empty")
		}
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open = %v, want ErrCorrupt", err)
		}
	})

	t.Run("missing manifest in an empty directory is a fresh database", func(t *testing.T) {
		fresh := t.TempDir()
		d2 := openDB(t, fresh, logicalOpts())
		if got := d2.Stats().SSTables; got != 0 {
			t.Errorf("SSTables = %d, want 0", got)
		}
		if err := d2.Put([]byte("a"), []byte("1")); err != nil {
			t.Fatalf("Put: %v", err)
		}
		expectValue(t, d2, []byte("a"), []byte("1"))
		if _, err := os.Stat(filepath.Join(fresh, manifestFileName)); err != nil {
			t.Errorf("a fresh database left no manifest: %v", err)
		}
	})
}

func TestMissingSSTableFailsOpen(t *testing.T) {
	dir := t.TempDir()
	d := openDB(t, dir, &Options{Sync: wal.SyncNone})
	for i := range 100 {
		if err := d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	m, err := readManifest(dir)
	if err != nil {
		t.Fatalf("readManifest: %v", err)
	}
	if len(m.sstables) == 0 {
		t.Fatal("test is wrong: the manifest lists no tables")
	}
	victim := filepath.Join(dir, sstName(m.sstables[0]))
	saved, err := os.ReadFile(victim)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if err := os.Remove(victim); err != nil {
		t.Fatalf("Remove: %v", err)
	}

	d2, err := Open(dir, logicalOpts())
	if err == nil {
		d2.Close()
		t.Fatal("Open succeeded with a live table missing")
	}
	if !errors.Is(err, ErrCorrupt) {
		t.Errorf("Open = %v, want ErrCorrupt", err)
	}

	// The failed Open must have let go of the lock, so restoring the file
	// and trying again works.
	if err := os.WriteFile(victim, saved, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	d3, err := Open(dir, logicalOpts())
	if err != nil {
		t.Fatalf("Open after restoring the table = %v; the failed Open left the directory locked", err)
	}
	defer d3.Close()
	for i := range 100 {
		expectValue(t, d3, []byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}
}

func TestConcurrent(t *testing.T) {
	const (
		writers = 8
		readers = 4
		each    = 400
	)
	dir := t.TempDir()
	d := openDB(t, dir, &Options{MemtableSize: 8 << 10, Sync: wal.SyncNone})

	value := func(w, i int) []byte {
		return []byte(fmt.Sprintf("w%d-i%04d-%s", w, i, strings.Repeat("v", 60)))
	}
	keyOf := func(w, i int) []byte { return []byte(fmt.Sprintf("w%d-key-%04d", w, i)) }

	// Writers and readers need separate groups: the readers run until the
	// writers are done and stop is closed, so waiting on one group for both
	// would deadlock.
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

	// Readers chase the writers: a key may not be there yet, which is fine,
	// but if it is there it must be right.
	for r := range readers {
		wgReaders.Add(1)
		go func(r int) {
			defer wgReaders.Done()
			rng := rand.New(rand.NewPCG(uint64(r), 7))
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

	if got := d.Stats().Flushes; got == 0 {
		t.Error("no flushes happened during the run; the test is not exercising flush under concurrency")
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

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d2 := openDB(t, dir, &Options{MemtableSize: 8 << 10, Sync: wal.SyncNone})
	verify(t, d2)
}

func TestProcessLock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("the directory lock is unix-only")
	}
	dir := t.TempDir()
	d, err := Open(dir, logicalOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// flock is per descriptor rather than per process, so a second Open in
	// this same process is blocked exactly as another process would be.
	d2, err := Open(dir, logicalOpts())
	if !errors.Is(err, ErrLocked) {
		if err == nil {
			d2.Close()
		}
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d3, err := Open(dir, logicalOpts())
	if err != nil {
		t.Fatalf("Open after Close = %v, want no error", err)
	}
	defer d3.Close()

	// The LOCK file itself stays put.
	if _, err := os.Stat(filepath.Join(dir, lockFileName)); err != nil {
		t.Errorf("Stat(LOCK) = %v, want the file to remain", err)
	}
}

func TestClosed(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, logicalOpts())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := d.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}

	if err := d.Put([]byte("b"), []byte("2")); !errors.Is(err, ErrClosed) {
		t.Errorf("Put after Close = %v, want ErrClosed", err)
	}
	if err := d.Delete([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Errorf("Delete after Close = %v, want ErrClosed", err)
	}
	if _, err := d.Get([]byte("a")); !errors.Is(err, ErrClosed) {
		t.Errorf("Get after Close = %v, want ErrClosed", err)
	}
	if err := d.Flush(); !errors.Is(err, ErrClosed) {
		t.Errorf("Flush after Close = %v, want ErrClosed", err)
	}
	if got := d.Stats(); got != (Stats{}) {
		t.Errorf("Stats after Close = %+v, want the zero value", got)
	}
}

func TestWriteErrorLeavesStateUnchanged(t *testing.T) {
	dir := t.TempDir()
	d := openDB(t, dir, &Options{Sync: wal.SyncNone})

	if err := d.Put([]byte("before"), []byte("kept")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// A value the log refuses outright.
	huge := make([]byte, wal.MaxRecordSize+1)
	err := d.Put([]byte("huge"), huge)
	if !errors.Is(err, wal.ErrRecordTooLarge) {
		t.Fatalf("Put(oversized) = %v, want an error wrapping wal.ErrRecordTooLarge", err)
	}

	// The failed write must not be visible, and must not have reached the
	// memtable either.
	expectMissing(t, d, []byte("huge"))
	expectValue(t, d, []byte("before"), []byte("kept"))

	// And the database still works.
	if err := d.Put([]byte("after"), []byte("fine")); err != nil {
		t.Fatalf("Put after the rejected write: %v", err)
	}
	expectValue(t, d, []byte("after"), []byte("fine"))
	if err := d.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	expectMissing(t, d, []byte("huge"))

	if err := d.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	d2 := openDB(t, dir, &Options{Sync: wal.SyncNone})
	expectMissing(t, d2, []byte("huge"))
	expectValue(t, d2, []byte("before"), []byte("kept"))
	expectValue(t, d2, []byte("after"), []byte("fine"))
}

func TestFlushFailureReportsErrFlushFailed(t *testing.T) {
	dir := t.TempDir()
	d, err := Open(dir, &Options{MemtableSize: 1 << 10, Sync: wal.SyncNone})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer d.abandon()

	boom := errors.New("simulated flush failure")
	d.hooks.afterSSTWritten = func() error { return boom }

	// Write until one of them trips the flush.
	var failed error
	var lastKey, lastValue []byte
	for i := range 200 {
		k := []byte(fmt.Sprintf("key-%04d", i))
		v := bytes.Repeat([]byte("v"), 100)
		if err := d.Put(k, v); err != nil {
			failed, lastKey, lastValue = err, k, v
			break
		}
	}
	if failed == nil {
		t.Fatal("no Put triggered a flush; the memtable limit was never reached")
	}
	if !errors.Is(failed, ErrFlushFailed) {
		t.Fatalf("Put = %v, want an error wrapping ErrFlushFailed", failed)
	}
	if !errors.Is(failed, boom) {
		t.Errorf("Put = %v, want it to also wrap the underlying cause", failed)
	}

	// The write itself is durable and visible, which is what ErrFlushFailed
	// promises.
	expectValue(t, d, lastKey, lastValue)
	expectValue(t, d, []byte("key-0000"), bytes.Repeat([]byte("v"), 100))
}

func FuzzManifestDecode(f *testing.F) {
	f.Add([]byte(nil))
	f.Add(manifest{nextFileNum: 1, logNumber: 1}.encode())
	f.Add(manifest{nextFileNum: 10, logNumber: 7, sstables: []uint64{2, 4, 6}}.encode())
	f.Add(make([]byte, manifestMinSize))
	f.Add(bytes.Repeat([]byte{0xff}, manifestMinSize+16))

	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := decodeManifest(data)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("decodeManifest = %v, want an error wrapping ErrCorrupt", err)
			}
			return
		}

		// A manifest that parsed must hold every invariant the engine
		// depends on, and must not have allocated beyond its own input.
		if 8*len(m.sstables) > len(data) {
			t.Fatalf("decoded %d tables from only %d bytes", len(m.sstables), len(data))
		}
		if m.logNumber > m.nextFileNum {
			t.Fatalf("log number %d is above the next file number %d", m.logNumber, m.nextFileNum)
		}
		for i, n := range m.sstables {
			if n >= m.nextFileNum {
				t.Fatalf("table %d has number %d, at or above the next file number %d", i, n, m.nextFileNum)
			}
			if i > 0 && n <= m.sstables[i-1] {
				t.Fatalf("table numbers are not strictly increasing: %d then %d", m.sstables[i-1], n)
			}
		}
		// Re-encoding must reproduce the bytes exactly.
		if got := m.encode(); !bytes.Equal(got, data) {
			t.Fatalf("re-encoding gave %d bytes, want the original %d", len(got), len(data))
		}
	})
}

// Environment variables that turn the test binary into the crash helper.
const (
	helperEnv     = "STRATA_DB_HELPER"
	helperDirEnv  = "STRATA_DB_HELPER_DIR"
	helperFromEnv = "STRATA_DB_HELPER_FROM"
)

// TestHelperProcess is the child half of TestCrashRecoveryProcessKill. It does
// nothing unless the parent set helperEnv, so it is inert in a normal run.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		return
	}
	dir := os.Getenv(helperDirEnv)
	from, err := strconv.Atoi(os.Getenv(helperFromEnv))
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: bad start sequence: %v\n", err)
		os.Exit(2)
	}

	d, err := Open(dir, &Options{
		MemtableSize:       4 << 10,
		CompactionBaseSize: 32 << 10,
		Sync:               wal.SyncAlways,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: Open: %v\n", err)
		os.Exit(2)
	}

	// Write until killed, reporting each acknowledged operation together with
	// the number of compactions done so far. os.Stdout is unbuffered, so a
	// line reaching the parent means the Put or Delete, and the fsync inside
	// it, had already returned; the compaction count on that line is one the
	// child really reached before it was killed, which is how the parent
	// knows compaction was active in the process it killed.
	for seq := from; ; seq++ {
		if err := d.Put(helperKey(seq), helperValue(seq)); err != nil {
			fmt.Fprintf(os.Stderr, "helper: Put(%d): %v\n", seq, err)
			os.Exit(2)
		}
		// Every seventh operation also deletes an earlier key, so recovery
		// has tombstones to get right as well as values.
		if seq%7 == 0 && seq >= from+7 {
			if err := d.Delete(helperKey(seq - 7)); err != nil {
				fmt.Fprintf(os.Stderr, "helper: Delete(%d): %v\n", seq-7, err)
				os.Exit(2)
			}
		}
		fmt.Fprintf(os.Stdout, "%d %d\n", seq, d.Stats().Compactions)
	}
}

func helperKey(seq int) []byte   { return []byte(fmt.Sprintf("seq-%08d", seq)) }
func helperValue(seq int) []byte { return []byte(fmt.Sprintf("value-%08d-%d", seq, seq*7919)) }

func TestCrashRecoveryProcessKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL is not available on Windows")
	}
	dir := t.TempDir()

	// Four rounds on the same directory, each continuing the sequence and
	// killed at a different point, so the crash lands at a different place in
	// the flush cycle each time. Each round is a fresh process writing with
	// SyncAlways, so -short settles for one.
	targets := []int{300, 450, 777, 1000}
	if testing.Short() {
		targets = targets[:1]
	}
	opts := &Options{
		MemtableSize:       4 << 10,
		CompactionBaseSize: 32 << 10,
		Sync:               wal.SyncAlways,
	}
	next := 0
	var childCompactions uint64

	for round, target := range targets {
		acked := runHelperUntilKilled(t, dir, next, target)
		if len(acked) < target {
			t.Fatalf("round %d: saw %d acknowledgements, want at least %d", round, len(acked), target)
		}
		// The highest count the child reported on a line the parent actually
		// read, so it was reached before the kill.
		roundCompactions := acked[len(acked)-1].compactions
		childCompactions += roundCompactions

		d, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("round %d: Open after kill: %v", round, err)
		}

		// Every acknowledged operation must have taken effect. A key is
		// deleted if some acknowledged seq deleted it and no later
		// acknowledged seq rewrote it; here each key is written once and
		// possibly deleted once, seven operations later.
		highest := acked[len(acked)-1].seq
		deleted := make(map[int]bool)
		for _, a := range acked {
			if a.seq%7 == 0 && a.seq-7 >= 0 {
				deleted[a.seq-7] = true
			}
		}
		for _, a := range acked {
			got, err := d.Get(helperKey(a.seq))
			if deleted[a.seq] {
				if !errors.Is(err, ErrNotFound) {
					t.Fatalf("round %d: Get(seq %d) = (%q, %v), want ErrNotFound after its delete was acknowledged",
						round, a.seq, got, err)
				}
				continue
			}
			if err != nil {
				t.Fatalf("round %d: Get(seq %d) = %v, want %q", round, a.seq, err, helperValue(a.seq))
			}
			if !bytes.Equal(got, helperValue(a.seq)) {
				t.Fatalf("round %d: Get(seq %d) = %q, want %q", round, a.seq, got, helperValue(a.seq))
			}
		}
		// Nothing beyond what the child could have written.
		for seq := highest + 50; seq < highest+60; seq++ {
			if _, err := d.Get(helperKey(seq)); !errors.Is(err, ErrNotFound) {
				t.Fatalf("round %d: Get(seq %d) found a key the child never wrote", round, seq)
			}
		}

		stats := d.Stats()
		if err := d.Close(); err != nil {
			t.Fatalf("round %d: Close: %v", round, err)
		}
		if n := countSuffix(t, dir, walSuffix); n != 1 {
			t.Errorf("round %d: %d logs on disk after recovery, want exactly 1", round, n)
		}
		expectCleanDirectory(t, dir)
		t.Logf("round %d: killed after %d acks (seq %d..%d), child had completed %d compactions, recovered into %d tables",
			round, len(acked), acked[0].seq, highest, roundCompactions, stats.SSTables)
		next = highest + 1
	}

	// The point of wiring compaction into this test is that the crashes land
	// in a process that is compacting, so a run where none happened would
	// prove much less. The first round starts from an empty database and
	// reaches its kill point before enough tables pile up to merge, so this
	// only holds once later rounds have run; -short keeps the correctness
	// checks above and gives up this one.
	if childCompactions == 0 && !testing.Short() {
		t.Error("no round's child reported a completed compaction; the crashes did not exercise compaction")
	}
}

// runHelperUntilKilled starts the helper on dir beginning at sequence from,
// waits for at least target acknowledgements, SIGKILLs it, and returns the
// acknowledgements it saw.
func runHelperUntilKilled(t *testing.T, dir string, from, target int) []ack {
	t.Helper()

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(),
		helperEnv+"=1",
		helperDirEnv+"="+dir,
		helperFromEnv+"="+strconv.Itoa(from),
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start helper: %v", err)
	}

	acks := make(chan ack, 1<<16)
	go func() {
		defer close(acks)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			fields := strings.Fields(sc.Text())
			if len(fields) != 2 {
				continue // Test-framework chatter, not an acknowledgement.
			}
			seq, err := strconv.Atoi(fields[0])
			if err != nil {
				continue
			}
			compactions, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				continue
			}
			acks <- ack{seq: seq, compactions: compactions}
		}
	}()

	// fsync is slow, especially F_FULLFSYNC on macOS, so allow plenty of
	// time for the first target operations.
	deadline := time.After(5 * time.Minute)
	var seen []ack
	for len(seen) < target {
		select {
		case a, ok := <-acks:
			if !ok {
				_ = cmd.Wait()
				t.Fatalf("helper exited after %d acknowledgements, want at least %d; stderr: %s",
					len(seen), target, stderr.String())
			}
			seen = append(seen, a)
		case <-deadline:
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("helper produced only %d acknowledgements in 5 minutes, want %d; stderr: %s",
				len(seen), target, stderr.String())
		}
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Kill helper: %v", err)
	}
	_ = cmd.Wait() // SIGKILL always reports an error; nothing to learn from it.

	for i, a := range seen {
		if want := from + i; a.seq != want {
			t.Fatalf("acknowledgement %d reported sequence %d, want %d; the helper is not counting up", i, a.seq, want)
		}
	}
	return seen
}

// ack is one acknowledged operation as the child reported it, with the number
// of compactions the child had completed by that point.
type ack struct {
	seq         int
	compactions uint64
}

func BenchmarkPutSyncNone(b *testing.B) {
	d, err := Open(b.TempDir(), &Options{Sync: wal.SyncNone})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	value := bytes.Repeat([]byte("v"), 100)
	i := 0
	b.SetBytes(116)
	b.ReportAllocs()
	for b.Loop() {
		if err := d.Put([]byte(fmt.Sprintf("%016d", i)), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
		i++
	}
}

func BenchmarkGetMemtable(b *testing.B) {
	const n = 10_000
	d, err := Open(b.TempDir(), &Options{MemtableSize: 1 << 30, Sync: wal.SyncNone})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer d.Close()

	value := bytes.Repeat([]byte("v"), 100)
	for i := range n {
		if err := d.Put([]byte(fmt.Sprintf("%016d", i)), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if got := d.Stats().SSTables; got != 0 {
		b.Fatalf("SSTables = %d, want everything still in the memtable", got)
	}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, err := d.Get([]byte(fmt.Sprintf("%016d", (i*7919)%n))); err != nil {
			b.Fatalf("Get: %v", err)
		}
		i++
	}
}

// preloadTables fills a database with n keys spread over many tables.
func preloadTables(b *testing.B, n int) *DB {
	b.Helper()
	d, err := Open(b.TempDir(), &Options{MemtableSize: 256 << 10, Sync: wal.SyncNone})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	value := bytes.Repeat([]byte("v"), 100)
	for i := range n {
		if err := d.Put([]byte(fmt.Sprintf("%016d", i)), value); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	if err := d.Flush(); err != nil {
		b.Fatalf("Flush: %v", err)
	}
	if got := d.Stats().SSTables; got < 20 {
		b.Fatalf("SSTables = %d, want at least 20", got)
	}
	b.Cleanup(func() { d.Close() })
	return d
}

func BenchmarkGetSSTables(b *testing.B) {
	const n = 100_000
	d := preloadTables(b, n)
	b.Logf("%d keys across %d tables", n, d.Stats().SSTables)

	b.ReportAllocs()
	before := d.Stats()
	i := 0
	for b.Loop() {
		if _, err := d.Get([]byte(fmt.Sprintf("%016d", (i*7919)%n))); err != nil {
			b.Fatalf("Get: %v", err)
		}
		i++
	}
	after := d.Stats()
	b.ReportMetric(float64(after.BlockReads-before.BlockReads)/float64(b.N), "blockreads/op")
}
