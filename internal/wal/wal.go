package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// MaxRecordSize is the largest payload, in bytes, that a single record may
// carry: 64 MiB. It bounds the value the on-disk length field is allowed to
// hold, which is what makes it a safety limit and not just a policy. Replay
// reads the length before it has any way to know whether the record is
// trustworthy, so a garbage length has to be rejected against this constant
// *before* a buffer is allocated for it; otherwise a few corrupt bytes could
// make recovery ask the allocator for gigabytes.
const MaxRecordSize = 64 << 20

// headerSize is the fixed width of a record header: a 4-byte CRC followed by
// a 4-byte payload length, both little endian.
const headerSize = 8

// castagnoli is the CRC-32C table. Castagnoli has better error-detection
// properties than the IEEE polynomial at these record sizes, and Go's
// implementation of it is hardware accelerated on amd64 and arm64.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Errors returned by this package. Callers that need to tell these apart from
// I/O failures should use errors.Is, because they are wrapped with detail.
var (
	// ErrRecordTooLarge reports that a record's payload would exceed
	// MaxRecordSize, so Append refused to write it.
	ErrRecordTooLarge = errors.New("wal: record too large")
	// ErrClosed reports that the log has already been closed.
	ErrClosed = errors.New("wal: log is closed")
)

// Op identifies what a record does to a key.
//
// Zero is deliberately not a valid Op, so a run of zero bytes left behind by a
// sparse file or a partly written block can never decode as a real record.
type Op byte

const (
	// OpPut records that a key was given a value. The value may be empty,
	// which is a real record and not a delete.
	OpPut Op = 1
	// OpDelete records a tombstone: the key is gone as far as the caller is
	// concerned, and any older on-disk value for it must be shadowed.
	OpDelete Op = 2
)

// Record is one logged operation.
//
// For OpDelete, Value is nil and must be empty on the way in. For OpPut,
// Value is non-nil after a replay even when it is zero length, which is how an
// empty-value put stays distinguishable from a delete.
type Record struct {
	// Op is the operation this record carries.
	Op Op
	// Key is the key written. The empty key is valid.
	Key []byte
	// Value is the value written, for OpPut only.
	Value []byte
}

// SyncPolicy says how hard Append works to get a record onto stable storage.
type SyncPolicy int

const (
	// SyncAlways fsyncs after every Append, so a successful Append means the
	// record will survive a power loss. It is the zero value, so a
	// SyncPolicy nobody set is the safe one.
	SyncAlways SyncPolicy = iota
	// SyncNone leaves durability to the operating system. A successful
	// Append survives the process dying, because the bytes are in the page
	// cache, but not the machine losing power. The caller may call Sync to
	// create a durability point of its own.
	SyncNone
)

// Log is an append-only, checksummed record log.
//
// All methods are safe for concurrent use. Append serialises on a mutex, which
// is what lets it promise that each record reaches the file as one write.
type Log struct {
	// mu guards f, closed and buf. It is held across the write so that two
	// appends can never interleave halves of their records in the file.
	mu     sync.Mutex
	f      *os.File
	policy SyncPolicy
	closed bool
	// buf is reused between appends to keep the steady state free of
	// allocations. It grows to the largest record written and stays there.
	buf []byte
}

// Open opens the log at path for appending, creating it if it does not exist.
//
// Open does not inspect what is already in the file; a log that may hold a
// torn tail from an earlier crash should be opened with Recover instead.
func Open(path string, policy SyncPolicy) (*Log, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Log{f: f, policy: policy}, nil
}

// Append writes r to the end of the log, fsyncing it if the policy says to.
//
// The whole record, header included, is built in one buffer and handed to a
// single Write, which keeps the window in which a crash can tear a record as
// narrow as the platform allows. Append returns ErrRecordTooLarge if the
// encoded payload would exceed MaxRecordSize, ErrClosed if the log is closed,
// and an error if r is malformed: an unknown Op, or an OpDelete carrying a
// value, which would otherwise be silently dropped.
//
// Append does not retain r, r.Key or r.Value.
func (l *Log) Append(r Record) error {
	if err := validateRecord(r); err != nil {
		return err
	}
	size := payloadLen(r)
	if size > MaxRecordSize {
		return fmt.Errorf("%w: payload of %d bytes exceeds the limit of %d", ErrRecordTooLarge, size, MaxRecordSize)
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}

	l.buf = encode(l.buf[:0], r, size)
	if _, err := l.f.Write(l.buf); err != nil {
		return err
	}
	if l.policy == SyncAlways {
		return l.f.Sync()
	}
	return nil
}

// Sync flushes everything appended so far to stable storage. It is how a
// SyncNone caller creates a durability point of its own.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.f.Sync()
}

// Close closes the underlying file. It returns ErrClosed if the log was
// already closed.
//
// Close deliberately does not fsync: under SyncAlways there is nothing left to
// flush, and under SyncNone the caller asked not to pay for one. A SyncNone
// caller that wants its writes to survive a power loss must call Sync first.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.closed = true
	l.buf = nil
	return l.f.Close()
}

// validateRecord rejects records that cannot be encoded faithfully.
func validateRecord(r Record) error {
	switch r.Op {
	case OpPut:
		return nil
	case OpDelete:
		if len(r.Value) != 0 {
			return fmt.Errorf("wal: delete for key %q carries a %d byte value", r.Key, len(r.Value))
		}
		return nil
	default:
		return fmt.Errorf("wal: unknown op %d", byte(r.Op))
	}
}

// payloadLen returns the encoded payload length for r, which is what the
// header's length field will hold. It is computed without encoding anything,
// so that Append can reject an oversized record before building a buffer for
// it.
func payloadLen(r Record) int {
	return 1 + // op
		uvarintLen(uint64(len(r.Key))) +
		uvarintLen(uint64(len(r.Value))) +
		len(r.Key) +
		len(r.Value)
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

// encode appends the wire form of r to dst and returns the extended slice.
// size must equal payloadLen(r).
//
// The CRC is written last, once the bytes it covers exist, and it covers the
// length field as well as the payload: a replay has to trust the length before
// it can read the payload, so the length needs to be checksummed by something
// that is already in hand.
func encode(dst []byte, r Record, size int) []byte {
	start := len(dst)
	dst = append(dst, 0, 0, 0, 0) // CRC, filled in below.
	dst = binary.LittleEndian.AppendUint32(dst, uint32(size))
	dst = append(dst, byte(r.Op))
	dst = binary.AppendUvarint(dst, uint64(len(r.Key)))
	dst = binary.AppendUvarint(dst, uint64(len(r.Value)))
	dst = append(dst, r.Key...)
	dst = append(dst, r.Value...)
	crc := crc32.Checksum(dst[start+4:], castagnoli)
	binary.LittleEndian.PutUint32(dst[start:start+4], crc)
	return dst
}

// decode parses a payload into a Record, reporting false if the payload is
// malformed: too short, an unknown op, a truncated or oversized varint, byte
// counts that do not add up to exactly the payload length, or a delete that
// claims a value.
//
// The returned Key and Value alias payload, so callers must hand decode a
// buffer they do not reuse.
func decode(payload []byte) (Record, bool) {
	if len(payload) < 1 {
		return Record{}, false
	}
	op := Op(payload[0])
	if op != OpPut && op != OpDelete {
		return Record{}, false
	}

	rest := payload[1:]
	keyLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return Record{}, false
	}
	rest = rest[n:]
	valLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return Record{}, false
	}
	rest = rest[n:]

	if keyLen > uint64(len(rest)) {
		return Record{}, false
	}
	key := rest[:keyLen]
	rest = rest[keyLen:]
	// Exact, not "at least": trailing bytes inside a record mean the lengths
	// disagree with the header, and a record we cannot read unambiguously is
	// corrupt even if its CRC happens to match.
	if valLen != uint64(len(rest)) {
		return Record{}, false
	}

	if op == OpDelete {
		if valLen != 0 {
			return Record{}, false
		}
		return Record{Op: OpDelete, Key: key}, true
	}
	// rest is a non-nil zero-length slice for an empty value, which is what
	// keeps an empty-value put distinguishable from a delete.
	return Record{Op: OpPut, Key: key, Value: rest}, true
}

// Replay reads the log at path from the start and calls fn for each valid
// record, in the order they were appended. A nil fn validates the log without
// visiting anything.
//
// Replay stops cleanly, without an error, at the first record it cannot trust:
// a short header, a short payload, a length above MaxRecordSize, a bad CRC, an
// unknown op, or malformed varints. validBytes is the offset just past the last
// record it accepted, which is therefore always a record boundary and always
// at most the file size. A missing file is not an error and replays as an empty
// log.
//
// Only a genuine I/O failure, or an error from fn, comes back as err. If fn
// returns an error, Replay stops and returns it along with the offset past the
// record fn rejected, since that record was itself valid.
//
// Each record handed to fn owns its memory: Key and Value never alias a buffer
// that a later record will reuse.
func Replay(path string, fn func(Record) error) (validBytes int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()
	return replay(bufio.NewReader(f), fn)
}

// replay is the reader-level half of Replay, split out so the scanning logic
// can be read on its own.
func replay(r io.Reader, fn func(Record) error) (int64, error) {
	var offset int64
	var header [headerSize]byte

	for {
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Clean end of file, or a header torn by a crash.
				return offset, nil
			}
			return offset, err
		}
		crc := binary.LittleEndian.Uint32(header[0:4])
		length := binary.LittleEndian.Uint32(header[4:8])

		// Checked before the allocation below, so a corrupt length can never
		// turn into a huge allocation.
		if length > MaxRecordSize {
			return offset, nil
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return offset, nil
			}
			return offset, err
		}

		// The CRC covers the length field followed by the payload.
		want := crc32.Update(crc32.Checksum(header[4:8], castagnoli), castagnoli, payload)
		if want != crc {
			return offset, nil
		}

		rec, ok := decode(payload)
		if !ok {
			return offset, nil
		}

		offset += headerSize + int64(length)
		if fn != nil {
			if err := fn(rec); err != nil {
				return offset, err
			}
		}
	}
}

// Recover prepares the log at path for use after a crash: it replays the
// records through fn, cuts the file back to the end of the last valid record,
// fsyncs that truncation, and returns the log open for appending.
//
// Truncating is what keeps the log append-only in the face of a torn tail. If
// the garbage were left in place, the next Append would sit behind it, and the
// record after the damage could never be reached by a replay. Cutting it off
// instead costs only writes that were never acknowledged.
//
// This is what a database calls on startup. If fn returns an error, Recover
// returns it and changes nothing on disk.
func Recover(path string, policy SyncPolicy, fn func(Record) error) (*Log, error) {
	validBytes, err := Replay(path, fn)
	if err != nil {
		return nil, err
	}
	if err := truncateAndSync(path, validBytes); err != nil {
		return nil, err
	}
	return Open(path, policy)
}

// truncateAndSync cuts path down to size and flushes the new size to stable
// storage, so that the torn tail does not come back after a second crash. A
// missing file has nothing to truncate.
func truncateAndSync(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if err := f.Truncate(size); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
