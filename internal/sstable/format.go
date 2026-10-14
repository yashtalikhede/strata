package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/yashtalikhede/strata/internal/memtable"
)

// Errors returned by this package. They are wrapped with detail, so callers
// should test for them with errors.Is.
var (
	// ErrCorrupt reports that a table does not hold what its own structure
	// claims: a checksum did not match, a length or offset pointed outside
	// the file, the magic or footer checksum was wrong, or an entry could
	// not be parsed.
	ErrCorrupt = errors.New("sstable: corrupt table")
	// ErrUnsorted reports that Add was given a key that is not strictly
	// greater than the previous one.
	ErrUnsorted = errors.New("sstable: keys out of order")
)

// TargetBlockSize is the size in bytes a data block's entries aim for. A block
// is closed once its entries reach this much, so most blocks land a little
// over it and a single entry larger than it occupies a block by itself.
//
// Four kilobytes matches the usual filesystem page, which means a block read
// is normally one trip to the device, and it keeps the index small enough to
// hold in memory: one index entry per block is roughly one key per 4 KiB of
// table.
const TargetBlockSize = 4096

// Magic identifies a strata table. It spells "strata1" in ASCII, and because
// the footer stores it little endian those bytes appear reversed in a hex dump
// of the file.
const Magic uint64 = 0x73747261746131

// Version is the format version written into the footer. A reader refuses a
// version it does not know rather than guessing at the layout.
const Version uint32 = 1

// Sizes of the fixed parts of the format.
const (
	// footerSize is the fixed width of the trailing footer:
	//
	//	[ 0:8 ]  indexOffset uint64
	//	[ 8:16]  indexLen    uint64
	//	[16:24]  bloomOffset uint64
	//	[24:32]  bloomLen    uint64
	//	[32:40]  entryCount  uint64
	//	[40:48]  magic       uint64
	//	[48:52]  version     uint32
	//	[52:56]  crc32c      uint32, over bytes 0 to 52
	footerSize = 56
	// checksumSize is the width of the CRC that ends every block.
	checksumSize = 4
)

// flagTombstone is bit 0 of an entry's flags byte. It is the only flag
// defined, and a reader rejects an entry that sets any other bit, so that a
// future flag cannot be silently misread as a plain put.
const flagTombstone byte = 1 << 0

// castagnoli is the CRC-32C table, the same polynomial the write-ahead log
// uses, so the whole engine checksums its bytes one way.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// entrySize returns the number of bytes encodeEntry will append for e.
func entrySize(e memtable.Entry) int {
	valLen := len(e.Value)
	if e.Tombstone {
		valLen = 0
	}
	return 1 + // flags
		uvarintLen(uint64(len(e.Key))) +
		uvarintLen(uint64(valLen)) +
		len(e.Key) +
		valLen
}

// uvarintLen returns the number of bytes binary.AppendUvarint spends on v.
func uvarintLen(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

// encodeEntry appends e to dst as [flags][keyLen][valLen][key][value] and
// returns the extended slice. A tombstone is written with a value length of
// zero and no value bytes; what distinguishes it from a put of an empty value
// is the flags byte, not the length.
func encodeEntry(dst []byte, e memtable.Entry) []byte {
	var flags byte
	value := e.Value
	if e.Tombstone {
		flags |= flagTombstone
		value = nil
	}
	dst = append(dst, flags)
	dst = binary.AppendUvarint(dst, uint64(len(e.Key)))
	dst = binary.AppendUvarint(dst, uint64(len(value)))
	dst = append(dst, e.Key...)
	return append(dst, value...)
}

// decodeEntry parses the entry at the front of b and reports how many bytes it
// occupied. ok is false if b does not begin with a well-formed entry: an
// unknown flag bit, a truncated or malformed varint, a length reaching past
// the end of b, or a tombstone claiming a value.
//
// key and value alias b. value is nil for a tombstone and non-nil but empty
// for a put of an empty value, which is how the two stay distinguishable.
func decodeEntry(b []byte) (key, value []byte, tombstone bool, n int, ok bool) {
	if len(b) < 1 {
		return nil, nil, false, 0, false
	}
	flags := b[0]
	if flags&^flagTombstone != 0 {
		return nil, nil, false, 0, false
	}
	tombstone = flags&flagTombstone != 0

	p := 1
	keyLen, m := binary.Uvarint(b[p:])
	if m <= 0 {
		return nil, nil, false, 0, false
	}
	p += m
	valLen, m := binary.Uvarint(b[p:])
	if m <= 0 {
		return nil, nil, false, 0, false
	}
	p += m

	// Compared as uint64 against the bytes that remain, so a huge length
	// cannot wrap into a small one or index out of range.
	if keyLen > uint64(len(b)-p) {
		return nil, nil, false, 0, false
	}
	key = b[p : p+int(keyLen)]
	p += int(keyLen)

	if valLen > uint64(len(b)-p) {
		return nil, nil, false, 0, false
	}
	if tombstone {
		if valLen != 0 {
			return nil, nil, false, 0, false
		}
		return key, nil, true, p, true
	}
	value = b[p : p+int(valLen)]
	return key, value, false, p + int(valLen), true
}

// blockHandle locates one data block and records the first key in it, which is
// what the index binary search compares against.
type blockHandle struct {
	firstKey []byte
	offset   uint64
	// length covers the block's entries and its trailing CRC.
	length uint32
}

// appendIndexEntry appends one index record, [keyLen][firstKey][offset][length].
func appendIndexEntry(dst []byte, h blockHandle) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(h.firstKey)))
	dst = append(dst, h.firstKey...)
	dst = binary.LittleEndian.AppendUint64(dst, h.offset)
	return binary.LittleEndian.AppendUint32(dst, h.length)
}

// footer is the decoded trailing record of a table.
type footer struct {
	indexOffset uint64
	indexLen    uint64
	bloomOffset uint64
	bloomLen    uint64
	entryCount  uint64
}

// encode returns the footer's 56 bytes, checksum included.
func (f footer) encode() []byte {
	b := make([]byte, 0, footerSize)
	b = binary.LittleEndian.AppendUint64(b, f.indexOffset)
	b = binary.LittleEndian.AppendUint64(b, f.indexLen)
	b = binary.LittleEndian.AppendUint64(b, f.bloomOffset)
	b = binary.LittleEndian.AppendUint64(b, f.bloomLen)
	b = binary.LittleEndian.AppendUint64(b, f.entryCount)
	b = binary.LittleEndian.AppendUint64(b, Magic)
	b = binary.LittleEndian.AppendUint32(b, Version)
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// decodeFooter parses and validates a footer against the size of the file it
// came from.
//
// The checks run in the order that lets each one trust the last: the checksum
// first, because every other field is meaningless if the bytes are damaged,
// then the magic and version, then the offsets. The offsets have to tile the
// file exactly, data then index then bloom then footer with no gap and no
// overlap, which is a far tighter constraint than "inside the file" and is
// what stops a crafted length from being used to allocate or read anywhere it
// should not.
func decodeFooter(b []byte, fileSize uint64) (footer, error) {
	if len(b) != footerSize {
		return footer{}, fmt.Errorf("%w: footer is %d bytes, want %d", ErrCorrupt, len(b), footerSize)
	}
	want := crc32.Checksum(b[:footerSize-checksumSize], castagnoli)
	if got := binary.LittleEndian.Uint32(b[footerSize-checksumSize:]); got != want {
		return footer{}, fmt.Errorf("%w: footer checksum %#08x, want %#08x", ErrCorrupt, got, want)
	}
	if magic := binary.LittleEndian.Uint64(b[40:48]); magic != Magic {
		return footer{}, fmt.Errorf("%w: magic %#016x, want %#016x", ErrCorrupt, magic, Magic)
	}
	if v := binary.LittleEndian.Uint32(b[48:52]); v != Version {
		return footer{}, fmt.Errorf("%w: format version %d, want %d", ErrCorrupt, v, Version)
	}

	f := footer{
		indexOffset: binary.LittleEndian.Uint64(b[0:8]),
		indexLen:    binary.LittleEndian.Uint64(b[8:16]),
		bloomOffset: binary.LittleEndian.Uint64(b[16:24]),
		bloomLen:    binary.LittleEndian.Uint64(b[24:32]),
		entryCount:  binary.LittleEndian.Uint64(b[32:40]),
	}
	footerStart := fileSize - footerSize

	// Each section must start where the previous one ended, and the last must
	// end where the footer begins. Subtraction is only ever done after the
	// bound that makes it safe has been checked.
	if f.indexOffset > footerStart {
		return footer{}, fmt.Errorf("%w: index starts at %d, past the footer at %d", ErrCorrupt, f.indexOffset, footerStart)
	}
	if f.indexLen < checksumSize || f.indexLen > footerStart-f.indexOffset {
		return footer{}, fmt.Errorf("%w: index length %d does not fit between %d and %d", ErrCorrupt, f.indexLen, f.indexOffset, footerStart)
	}
	if f.bloomOffset != f.indexOffset+f.indexLen {
		return footer{}, fmt.Errorf("%w: bloom starts at %d, want %d just after the index", ErrCorrupt, f.bloomOffset, f.indexOffset+f.indexLen)
	}
	if f.bloomLen < checksumSize || f.bloomLen != footerStart-f.bloomOffset {
		return footer{}, fmt.Errorf("%w: bloom length %d does not reach the footer from %d", ErrCorrupt, f.bloomLen, f.bloomOffset)
	}
	// Every entry costs at least three bytes, so a count above the size of
	// the data section is impossible and the footer is not to be trusted.
	if f.entryCount > f.indexOffset {
		return footer{}, fmt.Errorf("%w: entry count %d exceeds the %d byte data section", ErrCorrupt, f.entryCount, f.indexOffset)
	}
	return f, nil
}

// decodeIndex parses the index block's entries into block handles, validating
// as it goes that the blocks tile the data section from offset zero with no
// gaps and that their first keys strictly increase, which is what the binary
// search in Get relies on.
func decodeIndex(b []byte, dataEnd uint64) ([]blockHandle, error) {
	var handles []blockHandle
	var next uint64

	for len(b) > 0 {
		keyLen, m := binary.Uvarint(b)
		if m <= 0 {
			return nil, fmt.Errorf("%w: malformed key length in index entry %d", ErrCorrupt, len(handles))
		}
		b = b[m:]
		if keyLen > uint64(len(b)) {
			return nil, fmt.Errorf("%w: index entry %d claims a %d byte key with %d bytes left", ErrCorrupt, len(handles), keyLen, len(b))
		}
		firstKey := b[:keyLen]
		b = b[keyLen:]

		if len(b) < 8+4 {
			return nil, fmt.Errorf("%w: index entry %d is missing its offset and length", ErrCorrupt, len(handles))
		}
		h := blockHandle{
			firstKey: firstKey,
			offset:   binary.LittleEndian.Uint64(b[0:8]),
			length:   binary.LittleEndian.Uint32(b[8:12]),
		}
		b = b[12:]

		if h.offset != next {
			return nil, fmt.Errorf("%w: block %d starts at %d, want %d", ErrCorrupt, len(handles), h.offset, next)
		}
		// Strictly greater than the checksum, so a block always holds at
		// least one entry; an empty block is never written.
		if uint64(h.length) <= checksumSize || uint64(h.length) > dataEnd-h.offset {
			return nil, fmt.Errorf("%w: block %d has length %d at offset %d, with the data section ending at %d", ErrCorrupt, len(handles), h.length, h.offset, dataEnd)
		}
		if n := len(handles); n > 0 && bytes.Compare(handles[n-1].firstKey, h.firstKey) >= 0 {
			return nil, fmt.Errorf("%w: block %d first key is not above block %d's", ErrCorrupt, n, n-1)
		}

		next = h.offset + uint64(h.length)
		handles = append(handles, h)
	}

	if next != dataEnd {
		return nil, fmt.Errorf("%w: blocks cover %d bytes, but the data section is %d", ErrCorrupt, next, dataEnd)
	}
	return handles, nil
}

// verifyBlock splits a block into its entry bytes and its trailing CRC, and
// checks the one against the other.
func verifyBlock(block []byte, what string) ([]byte, error) {
	if len(block) < checksumSize {
		return nil, fmt.Errorf("%w: %s is %d bytes, too short for a checksum", ErrCorrupt, what, len(block))
	}
	body := block[:len(block)-checksumSize]
	want := crc32.Checksum(body, castagnoli)
	if got := binary.LittleEndian.Uint32(block[len(block)-checksumSize:]); got != want {
		return nil, fmt.Errorf("%w: %s checksum %#08x, want %#08x", ErrCorrupt, what, got, want)
	}
	return body, nil
}
