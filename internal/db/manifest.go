package db

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

// Names of the two fixed files in a database directory.
const (
	lockFileName     = "LOCK"
	manifestFileName = "MANIFEST"
	manifestTmpName  = "MANIFEST.tmp"
)

// manifestMagic identifies a strata manifest. It spells "stratam" in ASCII and
// is stored little endian, so those bytes appear reversed in a hex dump.
const manifestMagic uint64 = 0x7374726174616d

// manifestVersion is the layout version written into every manifest. A reader
// refuses a version it does not know rather than guessing at the layout.
//
// Version 2 differs from version 1 only in what it permits, not in its bytes:
// table numbers no longer have to increase along the list, because a
// compaction's output takes the position of its inputs while carrying a higher
// number than the tables after it. A version 1 file is rejected outright
// rather than migrated, which costs nothing because no data was ever deployed
// under it.
const manifestVersion uint32 = 2

// Sizes of the manifest's fixed parts. The header is the magic, version,
// nextFileNum, logNumber and table count; the trailer is the checksum.
const (
	manifestHeaderSize = 8 + 4 + 8 + 8 + 4
	manifestCRCSize    = 4
	manifestMinSize    = manifestHeaderSize + manifestCRCSize
)

// castagnoli is the CRC-32C table, the same polynomial the log and the tables
// use, so the whole engine checksums its bytes one way.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// manifest is the authoritative description of a database's durable state:
// which tables are live, which write-ahead logs still matter, and where the
// next file number comes from.
//
// It is small and rewritten whole, which is what lets a single atomic rename
// serve as the commit point for both a flush and a compaction.
type manifest struct {
	// nextFileNum is the lowest file number not yet handed out. Every file
	// in the directory, log or table, draws its number from this one
	// counter, so a number is never reused for a different kind of file.
	nextFileNum uint64
	// logNumber is the first log that still matters. Logs numbered at or
	// above it must be replayed on open; those below it are obsolete and
	// are deleted.
	logNumber uint64
	// sstables lists the live tables, oldest first, which is also the order
	// a read consults them in reverse.
	//
	// This order, and nothing else, decides which version of a key wins. In
	// particular it is not the same as sorting by file number: a compaction
	// writes its output with a fresh, high number but puts it where its
	// inputs were, so a table late in the list can have a lower number than
	// one before it.
	sstables []uint64
}

// encode returns the manifest's bytes, checksum included.
func (m manifest) encode() []byte {
	b := make([]byte, 0, manifestMinSize+8*len(m.sstables))
	b = binary.LittleEndian.AppendUint64(b, manifestMagic)
	b = binary.LittleEndian.AppendUint32(b, manifestVersion)
	b = binary.LittleEndian.AppendUint64(b, m.nextFileNum)
	b = binary.LittleEndian.AppendUint64(b, m.logNumber)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(m.sstables)))
	for _, n := range m.sstables {
		b = binary.LittleEndian.AppendUint64(b, n)
	}
	return binary.LittleEndian.AppendUint32(b, crc32.Checksum(b, castagnoli))
}

// decodeManifest parses and validates a manifest.
//
// The checks run in the order that lets each one trust the last: the length,
// then the checksum, because no field means anything if the bytes are damaged,
// then the magic and version, then the table count, which must account for
// exactly the bytes that follow. Only after the count has been pinned to the
// input's own length is any memory allocated for it, so a corrupt count can
// never turn into a huge allocation. The numbers are then checked for the
// invariants the rest of the engine relies on: tables all distinct, every
// number already handed out, and a log number that is not in the future.
//
// Distinct rather than increasing: the list order carries the age of each
// table, so the numbers along it need only name different files.
func decodeManifest(b []byte) (manifest, error) {
	if len(b) < manifestMinSize {
		return manifest{}, fmt.Errorf("%w: manifest is %d bytes, shorter than the %d byte minimum", ErrCorrupt, len(b), manifestMinSize)
	}
	body := b[:len(b)-manifestCRCSize]
	want := crc32.Checksum(body, castagnoli)
	if got := binary.LittleEndian.Uint32(b[len(b)-manifestCRCSize:]); got != want {
		return manifest{}, fmt.Errorf("%w: manifest checksum %#08x, want %#08x", ErrCorrupt, got, want)
	}
	if magic := binary.LittleEndian.Uint64(b[0:8]); magic != manifestMagic {
		return manifest{}, fmt.Errorf("%w: manifest magic %#016x, want %#016x", ErrCorrupt, magic, manifestMagic)
	}
	if v := binary.LittleEndian.Uint32(b[8:12]); v != manifestVersion {
		return manifest{}, fmt.Errorf("%w: manifest version %d, want %d", ErrCorrupt, v, manifestVersion)
	}

	m := manifest{
		nextFileNum: binary.LittleEndian.Uint64(b[12:20]),
		logNumber:   binary.LittleEndian.Uint64(b[20:28]),
	}
	count := binary.LittleEndian.Uint32(b[28:32])

	// The count has to account for exactly the bytes between the header and
	// the checksum. Computed in 64 bits, since count is read from disk and
	// 8*count would wrap at the top of the uint32 range.
	if got, want := uint64(len(body)-manifestHeaderSize), 8*uint64(count); got != want {
		return manifest{}, fmt.Errorf("%w: manifest lists %d tables needing %d bytes, but holds %d", ErrCorrupt, count, want, got)
	}

	m.sstables = make([]uint64, 0, count)
	// Bounded by count, which has already been pinned to the input length.
	seen := make(map[uint64]int, count)
	for i := range int(count) {
		off := manifestHeaderSize + 8*i
		n := binary.LittleEndian.Uint64(body[off : off+8])
		if first, dup := seen[n]; dup {
			return manifest{}, fmt.Errorf("%w: manifest lists table %d at positions %d and %d", ErrCorrupt, n, first, i)
		}
		if n >= m.nextFileNum {
			return manifest{}, fmt.Errorf("%w: manifest table %d has number %d, which was never handed out (next is %d)", ErrCorrupt, i, n, m.nextFileNum)
		}
		seen[n] = i
		m.sstables = append(m.sstables, n)
	}
	if m.logNumber > m.nextFileNum {
		return manifest{}, fmt.Errorf("%w: manifest log number %d is above the next file number %d", ErrCorrupt, m.logNumber, m.nextFileNum)
	}
	return m, nil
}

// readManifest reads the manifest from dir. A missing manifest comes back as an
// error wrapping os.ErrNotExist, which the caller tells apart from corruption.
func readManifest(dir string) (manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, manifestFileName))
	if err != nil {
		return manifest{}, err
	}
	return decodeManifest(b)
}

// writeManifest replaces dir's manifest with m, atomically.
//
// The bytes go to a temporary file which is fsynced and then renamed over the
// real name, and the directory is fsynced afterwards. The rename is atomic, so
// a reader sees either the old manifest or the new one and never a partial
// write; the directory fsync is what makes the rename itself durable, since a
// rename changes the directory rather than the file. That pair of properties is
// what allows a flush to treat this one call as its commit point.
func writeManifest(dir string, m manifest) error {
	tmp := filepath.Join(dir, manifestTmpName)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(m.encode()); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, manifestFileName)); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

// syncDir fsyncs a directory, which is what makes a rename into it durable.
// It mirrors the helper in internal/sstable, including opening the directory
// read-only, which is what unix requires and what Windows does not support.
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
