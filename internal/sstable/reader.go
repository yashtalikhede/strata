package sstable

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"sync/atomic"

	"github.com/yashtalikhede/strata/internal/bloom"
	"github.com/yashtalikhede/strata/internal/memtable"
)

// Stats counts what a Reader has had to do, so a caller can see whether the
// Bloom filter is earning its space.
type Stats struct {
	// BlockReads is the number of data blocks read from disk by Get.
	BlockReads uint64
	// BloomRejects is the number of Gets the Bloom filter answered on its
	// own, without touching the file.
	BloomRejects uint64
}

// Reader reads one immutable table.
//
// Open loads the index and the Bloom filter into memory and leaves them
// read-only for the Reader's lifetime, so Get and iteration need no locking
// beyond what the operating system already gives ReadAt. A Reader is therefore
// safe for any number of concurrent Gets and iterations; a single Iterator is
// not, and each goroutine should take its own.
type Reader struct {
	f    *os.File
	size int64

	// index and bloom are built once in Open and never written again.
	index []blockHandle
	bloom *bloom.Filter
	count uint64

	blockReads   atomic.Uint64
	bloomRejects atomic.Uint64
}

// Open opens the table at path and loads its index and Bloom filter.
//
// Everything read from the file is checked before it is believed: the footer's
// own checksum, its magic and version, the way its offsets tile the file, the
// index and Bloom checksums, and the shape of the index. A file that fails any
// of these returns an error wrapping ErrCorrupt rather than a Reader that
// would go on to read from arbitrary offsets.
//
// Both sections are held in memory for the Reader's lifetime because both are
// consulted on the way to every lookup, and together they are small: the index
// holds one key per 4 KiB block and the filter about ten bits per key, so a
// gigabyte of table needs a few megabytes of them. Re-reading either from disk
// per lookup would cost two extra reads to save memory the table has already
// paid for.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	r := &Reader{f: f, size: fi.Size()}
	if err := r.load(); err != nil {
		f.Close()
		return nil, err
	}
	return r, nil
}

// load reads and validates the footer, index and Bloom filter.
func (r *Reader) load() error {
	if r.size < footerSize {
		return fmt.Errorf("%w: file is %d bytes, shorter than the %d byte footer", ErrCorrupt, r.size, footerSize)
	}

	raw := make([]byte, footerSize)
	if _, err := r.f.ReadAt(raw, r.size-footerSize); err != nil {
		return err
	}
	foot, err := decodeFooter(raw, uint64(r.size))
	if err != nil {
		return err
	}
	r.count = foot.entryCount

	// Safe to allocate: decodeFooter has already established that both
	// lengths lie inside the file.
	indexBlock := make([]byte, foot.indexLen)
	if _, err := r.f.ReadAt(indexBlock, int64(foot.indexOffset)); err != nil {
		return err
	}
	indexBytes, err := verifyBlock(indexBlock, "index block")
	if err != nil {
		return err
	}
	if r.index, err = decodeIndex(indexBytes, foot.indexOffset); err != nil {
		return err
	}

	bloomBlock := make([]byte, foot.bloomLen)
	if _, err := r.f.ReadAt(bloomBlock, int64(foot.bloomOffset)); err != nil {
		return err
	}
	bloomBytes, err := verifyBlock(bloomBlock, "bloom block")
	if err != nil {
		return err
	}
	if r.bloom, err = bloom.Unmarshal(bloomBytes); err != nil {
		return fmt.Errorf("%w: %w", ErrCorrupt, err)
	}
	return nil
}

// Len returns the number of entries in the table, tombstones included, as
// recorded in the footer.
func (r *Reader) Len() int { return int(r.count) }

// Stats returns a snapshot of the Reader's counters.
func (r *Reader) Stats() Stats {
	return Stats{
		BlockReads:   r.blockReads.Load(),
		BloomRejects: r.bloomRejects.Load(),
	}
}

// Close closes the underlying file. Gets and iterations in flight will start
// failing, so a caller must stop using the Reader first.
func (r *Reader) Close() error { return r.f.Close() }

// Get looks up key.
//
// It returns memtable.Found with a copy of the value, memtable.Deleted if the
// table holds a tombstone for the key, or memtable.NotFound if this table has
// nothing to say about it, in which case an older table may still have a
// value.
//
// A lookup can stop early in three places, cheapest first. The Bloom filter
// rejects most absent keys outright, with no disk read at all. Failing that,
// the index settles which single block could hold the key, or that no block
// could, again without reading. Only then is one block read and scanned, and
// that scan stops as soon as it passes where the key would have been.
func (r *Reader) Get(key []byte) ([]byte, memtable.Status, error) {
	if !r.bloom.MayContain(key) {
		r.bloomRejects.Add(1)
		return nil, memtable.NotFound, nil
	}

	i := r.findBlock(key)
	if i < 0 {
		return nil, memtable.NotFound, nil
	}

	entries, err := r.readBlock(i)
	if err != nil {
		return nil, memtable.NotFound, err
	}
	return scanBlock(entries, key)
}

// findBlock returns the index of the last block whose first key is at or below
// key, or -1 if key sorts before every block. Because the blocks partition the
// key space in order, that block is the only one that can hold the key.
func (r *Reader) findBlock(key []byte) int {
	return sort.Search(len(r.index), func(i int) bool {
		return bytes.Compare(r.index[i].firstKey, key) > 0
	}) - 1
}

// readBlock reads data block i and returns its entry bytes with the checksum
// verified and stripped.
func (r *Reader) readBlock(i int) ([]byte, error) {
	h := r.index[i]
	buf := make([]byte, h.length)
	if _, err := r.f.ReadAt(buf, int64(h.offset)); err != nil {
		return nil, err
	}
	r.blockReads.Add(1)
	return verifyBlock(buf, fmt.Sprintf("data block %d", i))
}

// scanBlock walks a block's entries looking for key.
func scanBlock(entries []byte, key []byte) ([]byte, memtable.Status, error) {
	for pos := 0; pos < len(entries); {
		k, v, tombstone, n, ok := decodeEntry(entries[pos:])
		if !ok {
			return nil, memtable.NotFound, fmt.Errorf("%w: malformed entry at offset %d of a block", ErrCorrupt, pos)
		}
		switch cmp := bytes.Compare(k, key); {
		case cmp == 0:
			if tombstone {
				return nil, memtable.Deleted, nil
			}
			// A copy, so the caller does not hold the whole block alive and
			// cannot be surprised by a buffer that gets reused.
			return cloneBytes(v), memtable.Found, nil
		case cmp > 0:
			// Entries are sorted, so the key cannot appear later.
			return nil, memtable.NotFound, nil
		}
		pos += n
	}
	return nil, memtable.NotFound, nil
}

// cloneBytes returns a copy of b that is non-nil even when b is empty, so that
// a found empty value stays distinguishable from a missing one.
func cloneBytes(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// Iterator walks every entry in a table in key order.
//
// The usual shape is a Next loop followed by a check of Err:
//
//	it := r.NewIterator()
//	for it.Next() {
//		use(it.Key(), it.Value())
//	}
//	if err := it.Err(); err != nil { ... }
//
// The slices Key and Value return point into a block buffer the iterator
// reuses, so they are only valid until the next call to Next. A caller that
// keeps them must copy them. An Iterator is not safe for concurrent use,
// though separate iterators over one Reader are.
type Iterator struct {
	r *Reader
	// block is the next data block to load.
	block int
	// buf holds the current block's entry bytes, reused from block to block,
	// which is what makes Key and Value expire on the next Next.
	buf []byte
	pos int

	key       []byte
	value     []byte
	tombstone bool
	err       error
	done      bool
}

// NewIterator returns an iterator positioned before the first entry. Nothing
// is read from disk until the first call to Next.
func (r *Reader) NewIterator() *Iterator { return &Iterator{r: r} }

// Next advances to the next entry and reports whether there is one. It returns
// false at the end of the table and also on the first error, which Err then
// reports.
func (it *Iterator) Next() bool {
	if it.done {
		return false
	}
	for it.pos >= len(it.buf) {
		if it.block >= len(it.r.index) {
			it.done = true
			return false
		}
		if !it.loadBlock(it.block) {
			return false
		}
		it.block++
	}

	k, v, tombstone, n, ok := decodeEntry(it.buf[it.pos:])
	if !ok {
		it.fail(fmt.Errorf("%w: malformed entry at offset %d of block %d", ErrCorrupt, it.pos, it.block-1))
		return false
	}
	it.key, it.value, it.tombstone = k, v, tombstone
	it.pos += n
	return true
}

// loadBlock reads block i into the iterator's buffer, reporting success.
func (it *Iterator) loadBlock(i int) bool {
	h := it.r.index[i]
	if cap(it.buf) < int(h.length) {
		it.buf = make([]byte, h.length)
	}
	buf := it.buf[:h.length]
	if _, err := it.r.f.ReadAt(buf, int64(h.offset)); err != nil {
		it.fail(err)
		return false
	}
	it.r.blockReads.Add(1)

	entries, err := verifyBlock(buf, fmt.Sprintf("data block %d", i))
	if err != nil {
		it.fail(err)
		return false
	}
	it.buf, it.pos = entries, 0
	return true
}

// fail records the first error and stops the iteration.
func (it *Iterator) fail(err error) {
	it.err = err
	it.done = true
	it.key, it.value = nil, nil
}

// Key returns the current entry's key. It is valid only until the next call to
// Next.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current entry's value, nil for a tombstone and non-nil but
// empty for a put of an empty value. It is valid only until the next call to
// Next.
func (it *Iterator) Value() []byte { return it.value }

// Tombstone reports whether the current entry is a delete.
func (it *Iterator) Tombstone() bool { return it.tombstone }

// Err returns the first error the iteration hit, or nil if it simply ran out
// of entries.
func (it *Iterator) Err() error { return it.err }
