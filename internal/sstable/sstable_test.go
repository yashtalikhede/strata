package sstable

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/yashtalikhede/strata/internal/memtable"
)

// statusName makes failure messages readable, since Status is a bare int.
func statusName(s memtable.Status) string {
	switch s {
	case memtable.NotFound:
		return "NotFound"
	case memtable.Found:
		return "Found"
	case memtable.Deleted:
		return "Deleted"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

func key(i int) []byte { return []byte(fmt.Sprintf("key-%08d", i)) }

func put(k, v []byte) memtable.Entry {
	return memtable.Entry{Key: k, Value: v}
}

func tomb(k []byte) memtable.Entry {
	return memtable.Entry{Key: k, Tombstone: true}
}

// writeAndOpen writes entries to a fresh table and opens it, closing the
// Reader when the test ends.
func writeAndOpen(t *testing.T, entries []memtable.Entry) (*Reader, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "table.sst")
	if err := WriteEntries(path, entries); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r, path
}

// expectGet asserts the status and value Get returns for key.
func expectGet(t *testing.T, r *Reader, k []byte, wantStatus memtable.Status, wantValue []byte) {
	t.Helper()
	got, status, err := r.Get(k)
	if err != nil {
		t.Fatalf("Get(%q): %v", k, err)
	}
	if status != wantStatus {
		t.Fatalf("Get(%q) status = %s, want %s", k, statusName(status), statusName(wantStatus))
	}
	switch wantStatus {
	case memtable.Found:
		if got == nil {
			t.Fatalf("Get(%q) returned a nil value with status Found", k)
		}
		if !bytes.Equal(got, wantValue) {
			t.Fatalf("Get(%q) value = %q, want %q", k, got, wantValue)
		}
	default:
		if got != nil {
			t.Fatalf("Get(%q) value = %q, want nil for status %s", k, got, statusName(wantStatus))
		}
	}
}

// collectAll walks the whole table and returns what the iterator yielded.
func collectAll(t *testing.T, r *Reader) []memtable.Entry {
	t.Helper()
	var out []memtable.Entry
	it := r.NewIterator()
	for it.Next() {
		// Key and Value expire on the next Next, so copy them here.
		e := memtable.Entry{Key: cloneBytes(it.Key()), Tombstone: it.Tombstone()}
		if !it.Tombstone() {
			e.Value = cloneBytes(it.Value())
		}
		out = append(out, e)
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iteration: %v", err)
	}
	return out
}

// mixedEntries builds n entries in key order, every seventh a tombstone and
// every eleventh a put of an empty value, with values padded to valueSize so
// the table spans many blocks.
func mixedEntries(n, valueSize int) []memtable.Entry {
	entries := make([]memtable.Entry, 0, n)
	for i := range n {
		k := key(i)
		switch {
		case i%7 == 3:
			entries = append(entries, tomb(k))
		case i%11 == 5:
			entries = append(entries, put(k, []byte{}))
		default:
			v := bytes.Repeat([]byte{byte('a' + i%26)}, valueSize)
			entries = append(entries, put(k, append([]byte(fmt.Sprintf("v%d-", i)), v...)))
		}
	}
	return entries
}

func TestRoundTrip(t *testing.T) {
	const n = 1000
	entries := mixedEntries(n, 100)
	r, _ := writeAndOpen(t, entries)

	if len(r.index) < 20 {
		t.Fatalf("table has %d blocks, want at least 20; the test is not exercising multi-block reads", len(r.index))
	}
	if r.Len() != n {
		t.Errorf("Len() = %d, want %d", r.Len(), n)
	}

	for _, e := range entries {
		if e.Tombstone {
			expectGet(t, r, e.Key, memtable.Deleted, nil)
		} else {
			expectGet(t, r, e.Key, memtable.Found, e.Value)
		}
	}

	got := collectAll(t, r)
	if len(got) != len(entries) {
		t.Fatalf("iteration yielded %d entries, want %d", len(got), len(entries))
	}
	for i := range entries {
		want := entries[i]
		if !bytes.Equal(got[i].Key, want.Key) {
			t.Fatalf("entry %d key = %q, want %q", i, got[i].Key, want.Key)
		}
		if got[i].Tombstone != want.Tombstone {
			t.Fatalf("entry %d tombstone = %v, want %v", i, got[i].Tombstone, want.Tombstone)
		}
		if want.Tombstone {
			if got[i].Value != nil {
				t.Fatalf("entry %d is a tombstone with value %q, want nil", i, got[i].Value)
			}
			continue
		}
		if got[i].Value == nil {
			t.Fatalf("entry %d has a nil value; an empty put must stay distinct from a tombstone", i)
		}
		if !bytes.Equal(got[i].Value, want.Value) {
			t.Fatalf("entry %d value = %q, want %q", i, got[i].Value, want.Value)
		}
	}
	t.Logf("%d entries in %d blocks", r.Len(), len(r.index))
}

func TestMissingKeysBeforeBetweenAfter(t *testing.T) {
	// Even-numbered keys only, so every odd key is a gap.
	var entries []memtable.Entry
	for i := 0; i < 2000; i += 2 {
		entries = append(entries, put(key(i), bytes.Repeat([]byte("x"), 100)))
	}
	r, _ := writeAndOpen(t, entries)
	if len(r.index) < 20 {
		t.Fatalf("table has %d blocks, want at least 20", len(r.index))
	}

	// Before the first key.
	for _, k := range [][]byte{nil, {}, []byte("a"), []byte("key-"), []byte("key-00000000")[:11]} {
		expectGet(t, r, k, memtable.NotFound, nil)
	}
	if got := r.Stats().BlockReads; got != 0 {
		t.Errorf("keys below the first block caused %d block reads, want 0", got)
	}

	// Between two blocks: the odd key immediately below each block's first
	// key sorts after the previous block's last key and before this one's.
	for b := 1; b < len(r.index); b++ {
		first := r.index[b].firstKey
		var n int
		if _, err := fmt.Sscanf(string(first), "key-%08d", &n); err != nil {
			t.Fatalf("could not parse block %d first key %q: %v", b, first, err)
		}
		gap := key(n - 1)
		if bytes.Compare(gap, first) >= 0 {
			t.Fatalf("gap key %q is not below block first key %q", gap, first)
		}
		expectGet(t, r, gap, memtable.NotFound, nil)
	}

	// Inside a block but absent. The step is even, so every probe stays odd
	// and therefore absent; an odd step would alternate parity and hit real
	// keys.
	for i := 1; i < 2000; i += 200 {
		expectGet(t, r, key(i), memtable.NotFound, nil)
	}

	// After the last key.
	for _, k := range [][]byte{key(2000), key(999999), []byte("zzzz"), {0xff}} {
		expectGet(t, r, k, memtable.NotFound, nil)
	}
}

func TestEmptyTable(t *testing.T) {
	for _, tc := range []struct {
		name    string
		entries []memtable.Entry
	}{{"nil", nil}, {"empty slice", []memtable.Entry{}}} {
		t.Run(tc.name, func(t *testing.T) {
			r, _ := writeAndOpen(t, tc.entries)
			if r.Len() != 0 {
				t.Errorf("Len() = %d, want 0", r.Len())
			}
			if len(r.index) != 0 {
				t.Errorf("index has %d blocks, want 0", len(r.index))
			}
			for _, k := range [][]byte{nil, {}, []byte("anything"), {0xff}} {
				expectGet(t, r, k, memtable.NotFound, nil)
			}
			if got := collectAll(t, r); len(got) != 0 {
				t.Errorf("iteration yielded %d entries, want 0", len(got))
			}
			if got := r.Stats().BlockReads; got != 0 {
				t.Errorf("BlockReads = %d, want 0 for an empty table", got)
			}
		})
	}
}

func TestSingleEntry(t *testing.T) {
	t.Run("put", func(t *testing.T) {
		r, _ := writeAndOpen(t, []memtable.Entry{put([]byte("only"), []byte("value"))})
		if r.Len() != 1 || len(r.index) != 1 {
			t.Fatalf("Len() = %d, blocks = %d, want 1 and 1", r.Len(), len(r.index))
		}
		expectGet(t, r, []byte("only"), memtable.Found, []byte("value"))
		expectGet(t, r, []byte("onlx"), memtable.NotFound, nil)
		expectGet(t, r, []byte("onlz"), memtable.NotFound, nil)
		if got := collectAll(t, r); len(got) != 1 {
			t.Fatalf("iteration yielded %d entries, want 1", len(got))
		}
	})
	t.Run("empty key and empty value", func(t *testing.T) {
		r, _ := writeAndOpen(t, []memtable.Entry{put([]byte{}, []byte{})})
		expectGet(t, r, []byte{}, memtable.Found, []byte{})
		expectGet(t, r, []byte("a"), memtable.NotFound, nil)
	})
	t.Run("tombstone", func(t *testing.T) {
		r, _ := writeAndOpen(t, []memtable.Entry{tomb([]byte("gone"))})
		expectGet(t, r, []byte("gone"), memtable.Deleted, nil)
		got := collectAll(t, r)
		if len(got) != 1 || !got[0].Tombstone {
			t.Fatalf("iteration yielded %v, want one tombstone", got)
		}
	})
}

func TestHugeValueOwnBlock(t *testing.T) {
	huge := bytes.Repeat([]byte("H"), 1<<20)
	entries := []memtable.Entry{
		put(key(1), []byte("small before")),
		put(key(2), huge),
		put(key(3), []byte("small after")),
	}
	r, _ := writeAndOpen(t, entries)

	expectGet(t, r, key(2), memtable.Found, huge)
	expectGet(t, r, key(1), memtable.Found, []byte("small before"))
	expectGet(t, r, key(3), memtable.Found, []byte("small after"))

	// Find the block holding the huge entry and confirm it holds only that.
	b := r.findBlock(key(2))
	if b < 0 {
		t.Fatal("findBlock did not locate the huge key")
	}
	if !bytes.Equal(r.index[b].firstKey, key(2)) {
		t.Fatalf("block %d starts at %q, want the huge entry to begin its own block", b, r.index[b].firstKey)
	}
	body, err := r.readBlock(b)
	if err != nil {
		t.Fatalf("readBlock: %v", err)
	}
	count := 0
	for pos := 0; pos < len(body); {
		_, _, _, n, ok := decodeEntry(body[pos:])
		if !ok {
			t.Fatalf("malformed entry at %d", pos)
		}
		count++
		pos += n
	}
	if count != 1 {
		t.Errorf("the huge entry's block holds %d entries, want 1", count)
	}
	if uint64(r.index[b].length) < 1<<20 {
		t.Errorf("block length %d, want at least the 1 MiB value", r.index[b].length)
	}

	if got := collectAll(t, r); len(got) != 3 {
		t.Fatalf("iteration yielded %d entries, want 3", len(got))
	}
}

func TestWriterRejectsUnsortedAndDuplicate(t *testing.T) {
	cases := []struct {
		name   string
		second memtable.Entry
	}{
		{"decreasing", put([]byte("aaa"), []byte("v"))},
		{"equal", put([]byte("bbb"), []byte("v"))},
		{"equal tombstone", tomb([]byte("bbb"))},
		{"prefix of previous", put([]byte("bb"), []byte("v"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "table.sst")
			w, err := NewWriter(path)
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			defer w.Abort()

			if err := w.Add(put([]byte("bbb"), []byte("first"))); err != nil {
				t.Fatalf("Add: %v", err)
			}
			if err := w.Add(tc.second); !errors.Is(err, ErrUnsorted) {
				t.Fatalf("Add(%q) error = %v, want ErrUnsorted", tc.second.Key, err)
			}
		})
	}

	t.Run("tombstone with value", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "table.sst")
		w, err := NewWriter(path)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		defer w.Abort()
		bad := memtable.Entry{Key: []byte("k"), Value: []byte("v"), Tombstone: true}
		if err := w.Add(bad); err == nil {
			t.Fatal("Add(tombstone carrying a value) = nil, want an error")
		}
	})
}

func TestBloomAvoidsDiskReads(t *testing.T) {
	// Present keys are the even numbers and the probes are the odd ones, so
	// the absent keys are interleaved with real ones rather than sorting off
	// one end of the table. That matters: probes that sort below the first key
	// are turned away by the index whatever the filter says, so a test using
	// those would report zero block reads without the filter doing anything.
	// Interleaved, every false positive costs a real block read.
	const n = 10_000
	entries := make([]memtable.Entry, 0, n)
	for i := range n {
		entries = append(entries, put([]byte(fmt.Sprintf("k-%08d", 2*i)), []byte("v")))
	}
	r, _ := writeAndOpen(t, entries)

	for i := range n {
		if _, _, err := r.Get([]byte(fmt.Sprintf("k-%08d", 2*i+1))); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}

	stats := r.Stats()
	blockPct := float64(stats.BlockReads) / float64(n) * 100
	rejectPct := float64(stats.BloomRejects) / float64(n) * 100
	t.Logf("%d absent probes: %d block reads (%.2f%%), %d bloom rejects (%.2f%%)",
		n, stats.BlockReads, blockPct, stats.BloomRejects, rejectPct)

	if blockPct >= 3 {
		t.Errorf("block reads = %.2f%% of probes, want below 3%%", blockPct)
	}
	if rejectPct <= 97 {
		t.Errorf("bloom rejects = %.2f%% of probes, want above 97%%", rejectPct)
	}

	// Present keys must still be found, so the filter is not just saying no.
	for i := 0; i < n; i += 97 {
		expectGet(t, r, []byte(fmt.Sprintf("k-%08d", 2*i)), memtable.Found, []byte("v"))
	}
}

// corruptCopy writes data with one bit flipped to a fresh file and returns its
// path.
func corruptCopy(t *testing.T, data []byte, pos, bit int) string {
	t.Helper()
	out := append([]byte(nil), data...)
	out[pos] ^= 1 << bit
	path := filepath.Join(t.TempDir(), "corrupt.sst")
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return path
}

func TestCorruptionDetected(t *testing.T) {
	const n = 200
	entries := mixedEntries(n, 60)
	_, path := writeAndOpen(t, entries)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	foot, err := decodeFooter(data[len(data)-footerSize:], uint64(len(data)))
	if err != nil {
		t.Fatalf("decodeFooter on a table we just wrote: %v", err)
	}
	dataEnd := int(foot.indexOffset)
	if dataEnd < 2*TargetBlockSize {
		t.Fatalf("data section is only %d bytes; the test needs several blocks", dataEnd)
	}

	// A flipped bit anywhere in the data section falls inside some block, and
	// every byte of a block is covered by that block's checksum, so a full
	// iteration must report it. No Get may ever answer with wrong data.
	t.Run("data section", func(t *testing.T) {
		step := max(dataEnd/40, 1)
		for pos := 0; pos < dataEnd; pos += step {
			bit := pos % 8
			cp := corruptCopy(t, data, pos, bit)

			r, err := Open(cp)
			if err != nil {
				t.Fatalf("Open with data byte %d bit %d flipped = %v; damage to the data section must not stop Open", pos, bit, err)
			}

			it := r.NewIterator()
			for it.Next() {
			}
			if !errors.Is(it.Err(), ErrCorrupt) {
				r.Close()
				t.Fatalf("iteration with data byte %d bit %d flipped gave err = %v, want ErrCorrupt", pos, bit, it.Err())
			}

			for _, e := range entries {
				got, status, err := r.Get(e.Key)
				if err != nil {
					if !errors.Is(err, ErrCorrupt) {
						r.Close()
						t.Fatalf("Get(%q) with byte %d flipped = %v, want ErrCorrupt", e.Key, pos, err)
					}
					continue
				}
				// No error means this block was undamaged, so the answer has
				// to be exactly right.
				wantStatus := memtable.Found
				if e.Tombstone {
					wantStatus = memtable.Deleted
				}
				if status != wantStatus {
					r.Close()
					t.Fatalf("Get(%q) with byte %d flipped returned %s silently, want %s or ErrCorrupt",
						e.Key, pos, statusName(status), statusName(wantStatus))
				}
				if status == memtable.Found && !bytes.Equal(got, e.Value) {
					r.Close()
					t.Fatalf("Get(%q) with byte %d flipped returned %q silently, want %q or ErrCorrupt",
						e.Key, pos, got, e.Value)
				}
			}
			r.Close()
		}
	})

	// Every byte of the footer is covered by the footer checksum, so all 56
	// must stop Open.
	t.Run("footer", func(t *testing.T) {
		for i := range footerSize {
			pos := len(data) - footerSize + i
			for _, bit := range []int{0, 7} {
				cp := corruptCopy(t, data, pos, bit)
				r, err := Open(cp)
				if err == nil {
					r.Close()
					t.Fatalf("Open succeeded with footer byte %d bit %d flipped", i, bit)
				}
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("Open with footer byte %d bit %d flipped = %v, want ErrCorrupt", i, bit, err)
				}
			}
		}
	})

	for _, sec := range []struct {
		name       string
		from, size int
	}{
		{"index", int(foot.indexOffset), int(foot.indexLen)},
		{"bloom", int(foot.bloomOffset), int(foot.bloomLen)},
	} {
		t.Run(sec.name, func(t *testing.T) {
			step := max(sec.size/40, 1)
			for off := 0; off < sec.size; off += step {
				pos, bit := sec.from+off, off%8
				cp := corruptCopy(t, data, pos, bit)
				r, err := Open(cp)
				if err == nil {
					r.Close()
					t.Fatalf("Open succeeded with %s byte %d bit %d flipped", sec.name, off, bit)
				}
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("Open with %s byte %d bit %d flipped = %v, want ErrCorrupt", sec.name, off, bit, err)
				}
			}
		})
	}
}

func TestTruncatedFileRejected(t *testing.T) {
	entries := mixedEntries(300, 60)
	_, path := writeAndOpen(t, entries)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	lengths := make([]int, 0, footerSize+13)
	for n := 0; n <= footerSize+10; n++ {
		lengths = append(lengths, n)
	}
	lengths = append(lengths, len(data)-1, len(data)-100)

	dir := t.TempDir()
	for _, n := range lengths {
		if n < 0 || n > len(data) {
			t.Fatalf("test is wrong: truncation length %d is outside the %d byte file", n, len(data))
		}
		cut := filepath.Join(dir, "cut.sst")
		if err := os.WriteFile(cut, data[:n], 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		r, err := Open(cut)
		if err == nil {
			r.Close()
			t.Fatalf("Open succeeded on a file truncated to %d of %d bytes", n, len(data))
		}
	}

	// The untruncated file must still open, so the failures above are about
	// the truncation and not about the fixture.
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open(untruncated) = %v, want no error", err)
	}
	r.Close()
}

func TestAtomicFinish(t *testing.T) {
	t.Run("finish", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "table.sst")
		tmp := path + ".tmp"

		w, err := NewWriter(path)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		for i := range 500 {
			if err := w.Add(put(key(i), bytes.Repeat([]byte("v"), 100))); err != nil {
				t.Fatalf("Add: %v", err)
			}
		}

		// Nothing under the final name until Finish says so.
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("Stat(final) before Finish = %v, want not-exist", err)
		}
		if _, err := os.Stat(tmp); err != nil {
			t.Fatalf("Stat(tmp) before Finish = %v, want the temporary file to exist", err)
		}

		if err := w.Finish(); err != nil {
			t.Fatalf("Finish: %v", err)
		}
		if _, err := os.Stat(tmp); !os.IsNotExist(err) {
			t.Fatalf("Stat(tmp) after Finish = %v, want not-exist", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("Stat(final) after Finish = %v, want the table to exist", err)
		}

		// Abort after a successful Finish must not remove the table.
		if err := w.Abort(); err != nil {
			t.Errorf("Abort after Finish = %v, want nil", err)
		}
		if err := w.Abort(); err != nil {
			t.Errorf("second Abort = %v, want nil", err)
		}
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("the table disappeared after Abort: %v", err)
		}
		r, err := Open(path)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer r.Close()
		if r.Len() != 500 {
			t.Errorf("Len() = %d, want 500", r.Len())
		}

		if err := w.Add(put([]byte("zzz"), nil)); err == nil {
			t.Error("Add after Finish = nil, want an error")
		}
		if err := w.Finish(); err == nil {
			t.Error("second Finish = nil, want an error")
		}
	})

	t.Run("abort", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "table.sst")
		tmp := path + ".tmp"

		w, err := NewWriter(path)
		if err != nil {
			t.Fatalf("NewWriter: %v", err)
		}
		for i := range 500 {
			if err := w.Add(put(key(i), bytes.Repeat([]byte("v"), 100))); err != nil {
				t.Fatalf("Add: %v", err)
			}
		}
		if err := w.Abort(); err != nil {
			t.Fatalf("Abort: %v", err)
		}
		if _, err := os.Stat(tmp); !os.IsNotExist(err) {
			t.Errorf("Stat(tmp) after Abort = %v, want not-exist", err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("Stat(final) after Abort = %v, want not-exist", err)
		}

		// The directory must hold nothing at all.
		names, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(names) != 0 {
			t.Errorf("Abort left %d files behind", len(names))
		}
		if err := w.Finish(); err == nil {
			t.Error("Finish after Abort = nil, want an error")
		}
	})

	t.Run("WriteEntries aborts on unsorted input", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "table.sst")
		err := WriteEntries(path, []memtable.Entry{
			put([]byte("b"), []byte("1")),
			put([]byte("a"), []byte("2")),
		})
		if !errors.Is(err, ErrUnsorted) {
			t.Fatalf("WriteEntries error = %v, want ErrUnsorted", err)
		}
		names, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("ReadDir: %v", err)
		}
		if len(names) != 0 {
			t.Errorf("WriteEntries left %d files behind after failing", len(names))
		}
	})
}

// modelEntry is what the randomized test believes about one key.
type modelEntry struct {
	value     []byte
	tombstone bool
}

func TestModelRandomized(t *testing.T) {
	for _, seed := range []uint64{7, 13} {
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			const (
				ops      = 20_000
				keyspace = 5_000
				probes   = 20_000
			)
			r := rand.New(rand.NewPCG(seed, seed))

			// Apply random puts and deletes to a model, last write winning,
			// exactly as a memtable would.
			model := make(map[string]modelEntry, keyspace)
			for range ops {
				k := fmt.Sprintf("k-%06d", r.IntN(keyspace))
				if r.IntN(4) == 0 {
					model[k] = modelEntry{tombstone: true}
					continue
				}
				v := make([]byte, r.IntN(200))
				for i := range v {
					v[i] = byte(r.UintN(256))
				}
				model[k] = modelEntry{value: v}
			}

			keys := make([]string, 0, len(model))
			for k := range model {
				keys = append(keys, k)
			}
			slices.Sort(keys)

			entries := make([]memtable.Entry, 0, len(keys))
			for _, k := range keys {
				m := model[k]
				if m.tombstone {
					entries = append(entries, tomb([]byte(k)))
				} else {
					entries = append(entries, put([]byte(k), m.value))
				}
			}

			reader, _ := writeAndOpen(t, entries)
			if reader.Len() != len(entries) {
				t.Fatalf("Len() = %d, want %d", reader.Len(), len(entries))
			}

			for _, k := range keys {
				m := model[k]
				if m.tombstone {
					expectGet(t, reader, []byte(k), memtable.Deleted, nil)
				} else {
					expectGet(t, reader, []byte(k), memtable.Found, m.value)
				}
			}

			// Probes over a wider keyspace, so most of them are absent.
			for range probes {
				k := fmt.Sprintf("k-%06d", r.IntN(keyspace*2))
				m, ok := model[k]
				switch {
				case !ok:
					expectGet(t, reader, []byte(k), memtable.NotFound, nil)
				case m.tombstone:
					expectGet(t, reader, []byte(k), memtable.Deleted, nil)
				default:
					expectGet(t, reader, []byte(k), memtable.Found, m.value)
				}
			}

			// Iteration must agree with the sorted model exactly.
			got := collectAll(t, reader)
			if len(got) != len(entries) {
				t.Fatalf("iteration yielded %d entries, want %d", len(got), len(entries))
			}
			for i := range entries {
				if !bytes.Equal(got[i].Key, entries[i].Key) || got[i].Tombstone != entries[i].Tombstone {
					t.Fatalf("entry %d = (%q, tombstone %v), want (%q, tombstone %v)",
						i, got[i].Key, got[i].Tombstone, entries[i].Key, entries[i].Tombstone)
				}
				if !entries[i].Tombstone && !bytes.Equal(got[i].Value, entries[i].Value) {
					t.Fatalf("entry %d value mismatch for key %q", i, entries[i].Key)
				}
			}
			t.Logf("%d keys in %d blocks", reader.Len(), len(reader.index))
		})
	}
}

func TestConcurrentReaders(t *testing.T) {
	const n = 5_000
	entries := mixedEntries(n, 80)
	r, _ := writeAndOpen(t, entries)

	var wg sync.WaitGroup
	errs := make(chan error, 16)

	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(uint64(g), 99))
			for range 2_000 {
				i := rng.IntN(n)
				e := entries[i]
				got, status, err := r.Get(e.Key)
				if err != nil {
					errs <- fmt.Errorf("Get(%q): %w", e.Key, err)
					return
				}
				if e.Tombstone {
					if status != memtable.Deleted {
						errs <- fmt.Errorf("Get(%q) = %s, want Deleted", e.Key, statusName(status))
						return
					}
					continue
				}
				if status != memtable.Found || !bytes.Equal(got, e.Value) {
					errs <- fmt.Errorf("Get(%q) = (%s, %q), want (Found, %q)", e.Key, statusName(status), got, e.Value)
					return
				}
			}
		}(g)
	}

	// One goroutine iterating the whole table while the others look keys up.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 3 {
			count := 0
			it := r.NewIterator()
			for it.Next() {
				count++
			}
			if err := it.Err(); err != nil {
				errs <- fmt.Errorf("iteration: %w", err)
				return
			}
			if count != n {
				errs <- fmt.Errorf("iteration saw %d entries, want %d", count, n)
				return
			}
		}
	}()

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent access failed: %v", err)
	}
}

func TestFlushFromMemtable(t *testing.T) {
	m := memtable.New()
	r := rand.New(rand.NewPCG(21, 22))

	keys := make([]string, 0, 3000)
	for i := range 3000 {
		k := fmt.Sprintf("mem-%06d", r.IntN(2000))
		keys = append(keys, k)
		switch {
		case i%5 == 0:
			m.Delete([]byte(k))
		case i%7 == 0:
			m.Put([]byte(k), []byte{})
		default:
			m.Put([]byte(k), []byte(fmt.Sprintf("value-%d", i)))
		}
	}

	path := filepath.Join(t.TempDir(), "flushed.sst")
	if err := WriteEntries(path, m.Entries()); err != nil {
		t.Fatalf("WriteEntries: %v", err)
	}
	table, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer table.Close()

	if table.Len() != m.Len() {
		t.Errorf("table holds %d entries, memtable holds %d", table.Len(), m.Len())
	}

	// Every key must behave the same through both, including keys that were
	// never written at all.
	probes := append(append([]string(nil), keys...), "absent-1", "zzz", "")
	for _, k := range probes {
		wantValue, wantStatus := m.Get([]byte(k))
		got, status, err := table.Get([]byte(k))
		if err != nil {
			t.Fatalf("table Get(%q): %v", k, err)
		}
		if status != wantStatus {
			t.Fatalf("Get(%q): table says %s, memtable says %s", k, statusName(status), statusName(wantStatus))
		}
		if status == memtable.Found && !bytes.Equal(got, wantValue) {
			t.Fatalf("Get(%q): table says %q, memtable says %q", k, got, wantValue)
		}
		if status != memtable.Found && got != nil {
			t.Fatalf("Get(%q): table returned %q with status %s, want nil", k, got, statusName(status))
		}
	}

	// And the flushed order must match the memtable snapshot exactly.
	want := m.Entries()
	got := collectAll(t, table)
	if len(got) != len(want) {
		t.Fatalf("iteration yielded %d entries, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i].Key, want[i].Key) || got[i].Tombstone != want[i].Tombstone {
			t.Fatalf("entry %d = (%q, %v), want (%q, %v)", i, got[i].Key, got[i].Tombstone, want[i].Key, want[i].Tombstone)
		}
	}
}

func FuzzOpen(f *testing.F) {
	// A real table, so the fuzzer has something structured to mutate.
	dir := f.TempDir()
	for i, entries := range [][]memtable.Entry{
		nil,
		{put([]byte("a"), []byte("1"))},
		{put([]byte("a"), []byte("1")), tomb([]byte("b")), put([]byte("c"), []byte{})},
		mixedEntries(200, 40),
	} {
		path := filepath.Join(dir, fmt.Sprintf("seed-%d.sst", i))
		if err := WriteEntries(path, entries); err != nil {
			f.Fatalf("WriteEntries: %v", err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			f.Fatalf("ReadFile: %v", err)
		}
		f.Add(raw)
		if len(raw) > footerSize {
			f.Add(raw[:len(raw)-1])
			f.Add(raw[:footerSize])
		}
	}
	f.Add([]byte(nil))
	f.Add(make([]byte, footerSize))
	f.Add([]byte("not a table"))

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "fuzz.sst")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		r, err := Open(path)
		if err != nil {
			return
		}
		defer r.Close()

		// A table that opened must answer without panicking.
		for _, k := range [][]byte{nil, {}, []byte("a"), []byte("key-00000001"), {0xff, 0xff}} {
			if _, _, err := r.Get(k); err != nil && !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Get(%q) = %v, want nil or ErrCorrupt", k, err)
			}
		}

		// A bounded loop, so a bug that failed to advance shows up as a
		// failure here instead of hanging the fuzzer.
		const limit = 1 << 20
		it := r.NewIterator()
		seen := 0
		for it.Next() {
			seen++
			if seen > limit {
				t.Fatalf("iterator produced more than %d entries from a %d byte file", limit, len(data))
			}
		}
		if err := it.Err(); err != nil && !errors.Is(err, ErrCorrupt) {
			t.Fatalf("iteration error = %v, want nil or ErrCorrupt", err)
		}
	})
}

// benchEntries builds n entries with 16-byte keys and 100-byte values.
func benchEntries(n int) []memtable.Entry {
	entries := make([]memtable.Entry, n)
	value := bytes.Repeat([]byte("v"), 100)
	for i := range entries {
		entries[i] = memtable.Entry{Key: []byte(fmt.Sprintf("%016d", i)), Value: value}
	}
	return entries
}

func BenchmarkWrite(b *testing.B) {
	const n = 100_000
	entries := benchEntries(n)
	path := filepath.Join(b.TempDir(), "bench.sst")

	b.SetBytes(int64(n) * 116)
	b.ReportAllocs()
	for b.Loop() {
		if err := WriteEntries(path, entries); err != nil {
			b.Fatalf("WriteEntries: %v", err)
		}
	}
}

// openBenchTable writes a table of n entries and opens it for reading.
func openBenchTable(b *testing.B, n int) (*Reader, []memtable.Entry) {
	b.Helper()
	entries := benchEntries(n)
	path := filepath.Join(b.TempDir(), "bench.sst")
	if err := WriteEntries(path, entries); err != nil {
		b.Fatalf("WriteEntries: %v", err)
	}
	r, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { r.Close() })
	return r, entries
}

func BenchmarkGetHit(b *testing.B) {
	const n = 100_000
	r, entries := openBenchTable(b, n)

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		// Strided so successive lookups land in different blocks.
		e := entries[(i*7919)%n]
		_, status, err := r.Get(e.Key)
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		if status != memtable.Found {
			b.Fatalf("Get(%q) = %s, want Found", e.Key, statusName(status))
		}
		i++
	}
}

func BenchmarkGetMiss(b *testing.B) {
	const n = 100_000
	r, _ := openBenchTable(b, n)

	// Keys of the same shape but outside the written range, so almost every
	// lookup is rejected by the Bloom filter with no disk read at all.
	misses := make([][]byte, 1024)
	for i := range misses {
		misses[i] = []byte(fmt.Sprintf("%016d", n+i))
	}

	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_, status, err := r.Get(misses[i%len(misses)])
		if err != nil {
			b.Fatalf("Get: %v", err)
		}
		if status != memtable.NotFound {
			b.Fatalf("Get(miss) = %s, want NotFound", statusName(status))
		}
		i++
	}
	b.ReportMetric(float64(r.Stats().BlockReads)/float64(b.N), "blockreads/op")
}
