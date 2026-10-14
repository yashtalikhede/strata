package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"

	"github.com/yashtalikhede/strata/internal/bloom"
	"github.com/yashtalikhede/strata/internal/memtable"
)

// Writer builds one immutable table from entries handed to it in ascending key
// order.
//
// The table is assembled in a sibling file named with a ".tmp" suffix and only
// takes its final name in Finish, so a reader can never see a partly written
// table. A Writer is not safe for concurrent use.
type Writer struct {
	path    string
	tmpPath string
	f       *os.File

	// offset is how many bytes have been written, which is where the next
	// block will start.
	offset uint64

	// block holds the current block's entry bytes. It is reused between
	// blocks, so it settles at the size of the largest block written.
	block []byte
	// blockFirstKey is a copy of the first key in the current block, kept
	// because the index needs it after the entry bytes have been flushed.
	blockFirstKey []byte

	// index accumulates encoded index entries, one per finished block. It is
	// the only part of the table that has to be held in memory while
	// writing, and it costs one key per TargetBlockSize of table.
	index []byte

	// hashes holds one 64-bit hash per key, so that Finish can size the
	// Bloom filter for the number of keys actually written. Keeping hashes
	// rather than keys makes this eight bytes per entry whatever the keys
	// look like, instead of growing with key length.
	hashes []uint64

	// lastKey is a copy of the previous key, for the ordering check. The
	// buffer is reused, so the check costs no allocation per entry.
	lastKey []byte

	count    uint64
	finished bool
	aborted  bool
}

// NewWriter creates a table builder for path. The bytes go to path+".tmp"
// until Finish renames them into place.
//
// An existing temporary file is truncated, on the grounds that it can only be
// debris from a writer that died before it finished.
func NewWriter(path string) (*Writer, error) {
	tmpPath := path + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{path: path, tmpPath: tmpPath, f: f}, nil
}

// Add appends one entry to the table.
//
// Keys must arrive in strictly increasing bytewise order; a key equal to or
// below its predecessor returns ErrUnsorted, because a table with a repeated
// or out-of-order key would make the index binary search in Get unsound. A
// tombstone must carry no value.
//
// Blocks are cut around the entry, never through it: the current block is
// closed once its entries reach TargetBlockSize, and an entry that is itself
// that large or larger is given a block of its own.
//
// Add does not retain e, e.Key or e.Value.
func (w *Writer) Add(e memtable.Entry) error {
	if w.finished {
		return fmt.Errorf("sstable: Add after Finish")
	}
	if w.aborted {
		return fmt.Errorf("sstable: Add after Abort")
	}
	if e.Tombstone && len(e.Value) != 0 {
		return fmt.Errorf("sstable: tombstone for key %q carries a %d byte value", e.Key, len(e.Value))
	}
	if w.count > 0 && bytes.Compare(e.Key, w.lastKey) <= 0 {
		return fmt.Errorf("%w: key %q is not above the previous key %q", ErrUnsorted, e.Key, w.lastKey)
	}
	// A block's length is stored as a uint32 in the index, so one entry can
	// never be allowed to grow a block past that.
	size := entrySize(e)
	if uint64(len(w.block))+uint64(size)+checksumSize > math.MaxUint32 {
		return fmt.Errorf("sstable: entry for key %q is too large to fit a block", e.Key)
	}

	// An entry that is itself at or above the target gets a block to itself,
	// so flush whatever is pending before it. Otherwise a megabyte-sized value
	// would drag the small entries written just before it into its block, and
	// a reader wanting one of those would have to pull the whole megabyte to
	// reach it.
	if size >= TargetBlockSize && len(w.block) > 0 {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	if len(w.block) == 0 {
		w.blockFirstKey = append(w.blockFirstKey[:0], e.Key...)
	}
	w.block = encodeEntry(w.block, e)
	w.lastKey = append(w.lastKey[:0], e.Key...)
	w.hashes = append(w.hashes, bloom.Hash(e.Key))
	w.count++

	// Closing on "reached the target" rather than "would exceed it" keeps
	// entries whole: a block overshoots by at most the last entry added, and
	// no entry is ever split across two blocks.
	if len(w.block) >= TargetBlockSize {
		return w.flushBlock()
	}
	return nil
}

// flushBlock writes the current block, appends its index entry, and resets the
// buffer. A block with no entries is not written at all.
func (w *Writer) flushBlock() error {
	if len(w.block) == 0 {
		return nil
	}
	body := len(w.block)
	// The checksum is appended to the same buffer so the block reaches the
	// file in one write.
	w.block = binary.LittleEndian.AppendUint32(w.block, crc32.Checksum(w.block, castagnoli))
	if _, err := w.f.Write(w.block); err != nil {
		return err
	}

	w.index = appendIndexEntry(w.index, blockHandle{
		firstKey: w.blockFirstKey,
		offset:   w.offset,
		length:   uint32(body + checksumSize),
	})
	w.offset += uint64(body + checksumSize)
	w.block = w.block[:0]
	w.blockFirstKey = w.blockFirstKey[:0]
	return nil
}

// writeChecksummed writes b followed by its CRC and returns how many bytes
// that came to, which is what the footer records as the section's length.
func (w *Writer) writeChecksummed(b []byte) (uint64, error) {
	out := binary.LittleEndian.AppendUint32(append([]byte(nil), b...), crc32.Checksum(b, castagnoli))
	if _, err := w.f.Write(out); err != nil {
		return 0, err
	}
	w.offset += uint64(len(out))
	return uint64(len(out)), nil
}

// Finish completes the table: it flushes the last block, writes the index, the
// Bloom filter and the footer, fsyncs the file, closes it, renames it into
// place, and fsyncs the parent directory.
//
// That last pair of steps is what makes a flush crash-safe. The rename is
// atomic, so the final name either does not exist or names a complete table,
// never a half-written one. Renaming alone is not enough, though: the rename
// is a change to the directory, and until the directory itself is fsynced a
// power loss can leave the entry pointing at nothing. Fsyncing the file first
// and the directory second is what makes the table and its name durable
// together.
//
// A table with no entries is valid, and Finish writes one happily. On error
// the temporary file is left behind for the caller to Abort.
func (w *Writer) Finish() error {
	if w.finished {
		return fmt.Errorf("sstable: Finish called twice")
	}
	if w.aborted {
		return fmt.Errorf("sstable: Finish after Abort")
	}
	if err := w.flushBlock(); err != nil {
		return err
	}

	indexOffset := w.offset
	indexLen, err := w.writeChecksummed(w.index)
	if err != nil {
		return err
	}

	filter := bloom.New(len(w.hashes), 0)
	for _, h := range w.hashes {
		filter.AddHash(h)
	}
	bloomOffset := w.offset
	bloomLen, err := w.writeChecksummed(filter.Marshal())
	if err != nil {
		return err
	}

	foot := footer{
		indexOffset: indexOffset,
		indexLen:    indexLen,
		bloomOffset: bloomOffset,
		bloomLen:    bloomLen,
		entryCount:  w.count,
	}
	if _, err := w.f.Write(foot.encode()); err != nil {
		return err
	}

	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	if err := os.Rename(w.tmpPath, w.path); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(w.path)); err != nil {
		return err
	}

	w.finished = true
	// The buffers are large and the table is complete; let them go.
	w.block, w.index, w.hashes, w.lastKey, w.blockFirstKey = nil, nil, nil, nil, nil
	return nil
}

// Abort gives up on the table and removes the temporary file.
//
// It is a no-op once Finish has succeeded, so a caller can defer it without
// having to know which way the write went, and it is safe to call twice.
func (w *Writer) Abort() error {
	if w.finished || w.aborted {
		return nil
	}
	w.aborted = true
	w.block, w.index, w.hashes, w.lastKey, w.blockFirstKey = nil, nil, nil, nil, nil
	// The close error is of no interest: the file is about to be removed, and
	// the removal is the operation that can actually fail in a way the caller
	// needs to hear about.
	_ = w.f.Close()
	if err := os.Remove(w.tmpPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// syncDir fsyncs a directory, which is what makes a rename into it durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// WriteEntries writes entries to a new table at path in one call, aborting and
// removing the temporary file if anything goes wrong. The entries must already
// be sorted by key, which is what memtable.Entries returns, so this is what a
// flush calls.
func WriteEntries(path string, entries []memtable.Entry) error {
	w, err := NewWriter(path)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			_ = w.Abort()
			return err
		}
	}
	if err := w.Finish(); err != nil {
		_ = w.Abort()
		return err
	}
	return nil
}
