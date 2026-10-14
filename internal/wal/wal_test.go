package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// opName makes failure messages readable, since Op is a bare byte.
func opName(op Op) string {
	switch op {
	case OpPut:
		return "OpPut"
	case OpDelete:
		return "OpDelete"
	default:
		return fmt.Sprintf("Op(%d)", byte(op))
	}
}

// equalRecord compares two records exactly. A nil Value and an empty non-nil
// Value count as different, because that is precisely the difference between a
// delete and a put of an empty value. Keys are compared by content only, since
// a nil key and an empty key mean the same thing.
func equalRecord(a, b Record) bool {
	if a.Op != b.Op || !bytes.Equal(a.Key, b.Key) {
		return false
	}
	if (a.Value == nil) != (b.Value == nil) {
		return false
	}
	return bytes.Equal(a.Value, b.Value)
}

func formatRecord(r Record) string {
	if r.Value == nil {
		return fmt.Sprintf("{%s %q <nil>}", opName(r.Op), r.Key)
	}
	// Values can be a megabyte long; summarise rather than print them.
	if len(r.Value) > 32 {
		return fmt.Sprintf("{%s %q %d bytes}", opName(r.Op), r.Key, len(r.Value))
	}
	return fmt.Sprintf("{%s %q %q}", opName(r.Op), r.Key, r.Value)
}

func formatRecords(recs []Record) string {
	parts := make([]string, len(recs))
	for i, r := range recs {
		parts[i] = formatRecord(r)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// writeRecords appends recs to a fresh log at path and returns the file size
// after each append. Because Append writes a record with one Write, those
// sizes are exactly the record boundaries in the file, and bounds[0] is 0.
func writeRecords(t *testing.T, path string, recs []Record) []int64 {
	t.Helper()
	l, err := Open(path, SyncNone)
	if err != nil {
		t.Fatalf("Open(%s): %v", path, err)
	}
	bounds := make([]int64, 1, len(recs)+1)
	for i, r := range recs {
		if err := l.Append(r); err != nil {
			t.Fatalf("Append(record %d, %s): %v", i, formatRecord(r), err)
		}
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatalf("Stat after record %d: %v", i, err)
		}
		bounds = append(bounds, fi.Size())
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return bounds
}

// replayFile replays path and gathers the records it yields.
func replayFile(path string) ([]Record, int64, error) {
	var got []Record
	n, err := Replay(path, func(r Record) error {
		got = append(got, r)
		return nil
	})
	return got, n, err
}

// encodeRecords produces the on-disk bytes for recs without going through a
// file, for building fuzz seeds and hand-crafted logs.
func encodeRecords(recs ...Record) []byte {
	var buf []byte
	for _, r := range recs {
		buf = encode(buf, r, payloadLen(r))
	}
	return buf
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	oneMiB := bytes.Repeat([]byte("x"), 1<<20)
	recs := []Record{
		{Op: OpPut, Key: []byte("alpha"), Value: []byte("one")},
		{Op: OpDelete, Key: []byte("alpha")},
		{Op: OpPut, Key: []byte{}, Value: []byte("value under the empty key")},
		{Op: OpDelete, Key: []byte{}},
		{Op: OpPut, Key: []byte("empty-value"), Value: []byte{}},
		{Op: OpPut, Key: []byte{0x00, 0x01, 0x00, 0xff, 0x00}, Value: []byte{0xff, 0x00, 0xfe, 0x00}},
		{Op: OpDelete, Key: []byte{0x00, 0x00, 0x00}},
		// A 300-byte key and a 1 MiB value both need multi-byte varints for
		// their lengths, which a single-byte-varint bug would get wrong.
		{Op: OpPut, Key: bytes.Repeat([]byte{0x00}, 300), Value: []byte("long key")},
		{Op: OpPut, Key: []byte("one-mib"), Value: oneMiB},
		{Op: OpPut, Key: []byte("last"), Value: []byte("tail")},
	}

	writeRecords(t, path, recs)

	got, validBytes, err := replayFile(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if validBytes != fi.Size() {
		t.Errorf("Replay validBytes = %d, want the whole file, %d", validBytes, fi.Size())
	}
	if len(got) != len(recs) {
		t.Fatalf("Replay returned %d records, want %d: %s", len(got), len(recs), formatRecords(got))
	}
	for i := range recs {
		want := recs[i]
		if want.Op == OpPut && want.Value == nil {
			// A put of a nil value is stored as an empty value, and reads
			// back non-nil; normalise before comparing.
			want.Value = []byte{}
		}
		if !equalRecord(got[i], want) {
			t.Errorf("record %d = %s, want %s", i, formatRecord(got[i]), formatRecord(want))
		}
	}
}

func TestEmptyValueVersusDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	key := []byte("k")

	writeRecords(t, path, []Record{
		{Op: OpPut, Key: key, Value: []byte{}},
		{Op: OpDelete, Key: key},
	})

	got, _, err := replayFile(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("Replay returned %d records, want 2: %s", len(got), formatRecords(got))
	}

	if got[0].Op != OpPut {
		t.Errorf("empty-value put replayed as %s, want OpPut", opName(got[0].Op))
	}
	if got[0].Value == nil {
		t.Error("empty-value put replayed with a nil Value; it must stay distinguishable from a delete")
	} else if len(got[0].Value) != 0 {
		t.Errorf("empty-value put replayed with Value = %q, want empty", got[0].Value)
	}

	if got[1].Op != OpDelete {
		t.Errorf("delete replayed as %s, want OpDelete", opName(got[1].Op))
	}
	if got[1].Value != nil {
		t.Errorf("delete replayed with Value = %q, want nil", got[1].Value)
	}
}

// TestRecordFormat pins the documented wire format down byte for byte, so that
// a change to the encoding cannot slip through just because the round trip
// still agrees with itself.
func TestRecordFormat(t *testing.T) {
	raw := encodeRecords(Record{Op: OpPut, Key: []byte("ab"), Value: []byte("xyz")})

	// 8-byte header, then op, keyLen, valLen, key, value.
	wantPayload := []byte{byte(OpPut), 2, 3, 'a', 'b', 'x', 'y', 'z'}
	if len(raw) != headerSize+len(wantPayload) {
		t.Fatalf("encoded length = %d, want %d", len(raw), headerSize+len(wantPayload))
	}
	if length := binary.LittleEndian.Uint32(raw[4:8]); int(length) != len(wantPayload) {
		t.Errorf("length field = %d, want %d", length, len(wantPayload))
	}
	if !bytes.Equal(raw[headerSize:], wantPayload) {
		t.Errorf("payload = % x, want % x", raw[headerSize:], wantPayload)
	}
	// The CRC must cover the length field as well as the payload.
	wantCRC := crc32.Checksum(raw[4:], crc32.MakeTable(crc32.Castagnoli))
	if got := binary.LittleEndian.Uint32(raw[0:4]); got != wantCRC {
		t.Errorf("crc = %#08x, want %#08x over the length field and payload", got, wantCRC)
	}
}

// TestTornTailEveryOffset truncates the log at every possible length and
// checks that a replay always yields some prefix of the records, stops on a
// record boundary, and recovers as much as was fully written.
func TestTornTailEveryOffset(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "wal.log")

	recs := []Record{
		{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		{Op: OpDelete, Key: []byte("b")},
		{Op: OpPut, Key: []byte(""), Value: []byte("")},
		{Op: OpPut, Key: []byte("ccc"), Value: []byte("three")},
		{Op: OpPut, Key: []byte{0, 0}, Value: []byte{0, 0, 0}},
		{Op: OpDelete, Key: []byte("")},
		{Op: OpPut, Key: []byte("medium"), Value: bytes.Repeat([]byte("m"), 200)},
		{Op: OpPut, Key: []byte("x"), Value: []byte("y")},
		{Op: OpDelete, Key: []byte("zzzz")},
		{Op: OpPut, Key: []byte("last"), Value: []byte("tail")},
	}
	bounds := writeRecords(t, full, recs)

	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if int64(len(data)) != bounds[len(bounds)-1] {
		t.Fatalf("file is %d bytes, want %d", len(data), bounds[len(bounds)-1])
	}

	// boundaryAt maps an offset to the number of whole records that end there,
	// or -1 if the offset is inside a record.
	boundaryAt := make(map[int64]int, len(bounds))
	for i, b := range bounds {
		boundaryAt[b] = i
	}

	torn := filepath.Join(dir, "torn.log")
	for cut := 0; cut <= len(data); cut++ {
		if err := os.WriteFile(torn, data[:cut], 0o644); err != nil {
			t.Fatalf("WriteFile(cut=%d): %v", cut, err)
		}

		got, validBytes, err := replayFile(torn)
		if err != nil {
			t.Fatalf("Replay(cut=%d): unexpected error %v", cut, err)
		}
		if validBytes < 0 || validBytes > int64(cut) {
			t.Fatalf("Replay(cut=%d) validBytes = %d, want within [0, %d]", cut, validBytes, cut)
		}

		k, ok := boundaryAt[validBytes]
		if !ok {
			t.Fatalf("Replay(cut=%d) validBytes = %d, which is not a record boundary (%v)", cut, validBytes, bounds)
		}

		// Everything that was written in full must come back: the largest
		// boundary at or below the cut.
		wantK := 0
		for i, b := range bounds {
			if b <= int64(cut) {
				wantK = i
			}
		}
		if k != wantK {
			t.Fatalf("Replay(cut=%d) recovered %d records (validBytes %d), want %d (validBytes %d)",
				cut, k, validBytes, wantK, bounds[wantK])
		}
		if len(got) != k {
			t.Fatalf("Replay(cut=%d) yielded %d records but validBytes says %d", cut, len(got), k)
		}
		for i := 0; i < k; i++ {
			want := recs[i]
			if want.Op == OpPut && want.Value == nil {
				want.Value = []byte{}
			}
			if !equalRecord(got[i], want) {
				t.Fatalf("Replay(cut=%d) record %d = %s, want %s", cut, i, formatRecord(got[i]), formatRecord(want))
			}
		}
	}
}

// TestBitFlipDetected flips every single bit of the last record and checks
// that the damaged record is never accepted while the earlier ones survive.
func TestBitFlipDetected(t *testing.T) {
	dir := t.TempDir()
	full := filepath.Join(dir, "wal.log")

	recs := []Record{
		{Op: OpPut, Key: []byte("a"), Value: []byte("one")},
		{Op: OpDelete, Key: []byte("b")},
		{Op: OpPut, Key: []byte("c"), Value: []byte("three")},
		{Op: OpPut, Key: []byte("last"), Value: []byte("payload")},
	}
	bounds := writeRecords(t, full, recs)
	lastStart := bounds[len(bounds)-2]

	data, err := os.ReadFile(full)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}

	flipped := filepath.Join(dir, "flipped.log")
	for i := int(lastStart); i < len(data); i++ {
		for bit := 0; bit < 8; bit++ {
			corrupt := append([]byte(nil), data...)
			corrupt[i] ^= 1 << bit
			if err := os.WriteFile(flipped, corrupt, 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}

			got, validBytes, err := replayFile(flipped)
			if err != nil {
				t.Fatalf("Replay with byte %d bit %d flipped: unexpected error %v", i, bit, err)
			}
			if validBytes != lastStart {
				t.Fatalf("Replay with byte %d bit %d flipped: validBytes = %d, want %d (the corrupt last record must be rejected)",
					i, bit, validBytes, lastStart)
			}
			if len(got) != len(recs)-1 {
				t.Fatalf("Replay with byte %d bit %d flipped: %d records, want %d: %s",
					i, bit, len(got), len(recs)-1, formatRecords(got))
			}
			for j := range got {
				if !equalRecord(got[j], recs[j]) {
					t.Fatalf("Replay with byte %d bit %d flipped: record %d = %s, want %s",
						i, bit, j, formatRecord(got[j]), formatRecord(recs[j]))
				}
			}
		}
	}
}

// TestOversizedLengthNoAlloc feeds Replay a header whose length field is
// absurd but whose CRC matches it, which is the one case where a naive reader
// would trust the length and try to allocate 4 GiB.
func TestOversizedLengthNoAlloc(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[4:8], 0xFFFFFFFF)
	// A CRC that is correct for the length field and an empty payload, so the
	// only thing standing between Replay and a huge allocation is the
	// MaxRecordSize check.
	binary.LittleEndian.PutUint32(header[0:4], crc32.Checksum(header[4:8], castagnoli))
	if err := os.WriteFile(path, header, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	got, validBytes, err := replayFile(path)

	runtime.ReadMemStats(&after)

	if err != nil {
		t.Fatalf("Replay: unexpected error %v", err)
	}
	if validBytes != 0 {
		t.Errorf("Replay validBytes = %d, want 0", validBytes)
	}
	if len(got) != 0 {
		t.Errorf("Replay yielded %d records, want none: %s", len(got), formatRecords(got))
	}

	// The bogus length asks for 4 GiB. Anything in the low megabytes proves
	// the length was rejected before the allocation.
	const limit = 8 << 20
	if grew := after.TotalAlloc - before.TotalAlloc; grew > limit {
		t.Errorf("Replay allocated %d bytes for a record claiming %d; want under %d",
			grew, uint32(0xFFFFFFFF), limit)
	}
}

func TestAppendRejectsTooLarge(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l, err := Open(path, SyncNone)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer l.Close()

	// The key and the length varints push the payload past the limit even
	// though the value alone is exactly MaxRecordSize.
	huge := Record{Op: OpPut, Key: []byte("k"), Value: make([]byte, MaxRecordSize)}
	if n := payloadLen(huge); n <= MaxRecordSize {
		t.Fatalf("test is wrong: payload is %d bytes, which is not over the %d limit", n, MaxRecordSize)
	}
	if err := l.Append(huge); !errors.Is(err, ErrRecordTooLarge) {
		t.Fatalf("Append(oversized) error = %v, want ErrRecordTooLarge", err)
	}

	// The rejection must leave nothing behind.
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != 0 {
		t.Errorf("log is %d bytes after a rejected append, want 0", fi.Size())
	}
}

func TestRecoverTruncatesAndAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	recs := []Record{
		{Op: OpPut, Key: []byte("a"), Value: []byte("one")},
		{Op: OpDelete, Key: []byte("b")},
		{Op: OpPut, Key: []byte("c"), Value: []byte("three")},
	}
	bounds := writeRecords(t, path, recs)
	goodBytes := bounds[len(bounds)-1]

	// A torn tail: half a header, then a plausible-looking but bogus record.
	garbage := []byte{0xde, 0xad, 0xbe, 0xef, 0x10, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if _, err := f.Write(garbage); err != nil {
		t.Fatalf("Write garbage: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	var recovered []Record
	l, err := Recover(path, SyncNone, func(r Record) error {
		recovered = append(recovered, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if len(recovered) != len(recs) {
		t.Fatalf("Recover replayed %d records, want %d: %s", len(recovered), len(recs), formatRecords(recovered))
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if fi.Size() != goodBytes {
		t.Errorf("log is %d bytes after Recover, want %d (the torn tail must be gone)", fi.Size(), goodBytes)
	}

	fresh := Record{Op: OpPut, Key: []byte("d"), Value: []byte("four")}
	if err := l.Append(fresh); err != nil {
		t.Fatalf("Append after Recover: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, validBytes, err := replayFile(path)
	if err != nil {
		t.Fatalf("Replay after Recover: %v", err)
	}
	fi, err = os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if validBytes != fi.Size() {
		t.Errorf("validBytes = %d, want the whole file, %d", validBytes, fi.Size())
	}

	want := append(append([]Record(nil), recs...), fresh)
	if len(got) != len(want) {
		t.Fatalf("Replay returned %d records, want %d: %s", len(got), len(want), formatRecords(got))
	}
	for i := range want {
		if !equalRecord(got[i], want[i]) {
			t.Errorf("record %d = %s, want %s", i, formatRecord(got[i]), formatRecord(want[i]))
		}
	}
}

func TestConcurrentAppend(t *testing.T) {
	const (
		writers = 8
		each    = 500
	)
	path := filepath.Join(t.TempDir(), "wal.log")
	l, err := Open(path, SyncNone)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				key := fmt.Sprintf("w%d-k%04d", w, i)
				r := Record{Op: OpPut, Key: []byte(key), Value: []byte("val-" + key)}
				if err := l.Append(r); err != nil {
					errs <- fmt.Errorf("Append(%s): %w", key, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent append failed: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	got, validBytes, err := replayFile(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if validBytes != fi.Size() {
		t.Fatalf("validBytes = %d, want the whole file, %d; a record was torn", validBytes, fi.Size())
	}
	if len(got) != writers*each {
		t.Fatalf("Replay returned %d records, want %d", len(got), writers*each)
	}

	// Order across goroutines is not defined, so check the multiset of keys
	// and that every value still matches its own key.
	seen := make(map[string]int, writers*each)
	for _, r := range got {
		if r.Op != OpPut {
			t.Fatalf("replayed %s, want OpPut", formatRecord(r))
		}
		key := string(r.Key)
		if want := "val-" + key; string(r.Value) != want {
			t.Fatalf("value for %q = %q, want %q; records interleaved", key, r.Value, want)
		}
		seen[key]++
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < each; i++ {
			key := fmt.Sprintf("w%d-k%04d", w, i)
			if seen[key] != 1 {
				t.Fatalf("key %q appeared %d times, want exactly 1", key, seen[key])
			}
		}
	}
}

func TestClosedLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	l, err := Open(path, SyncNone)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := l.Append(Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")}); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := l.Append(Record{Op: OpPut, Key: []byte("b"), Value: []byte("2")}); !errors.Is(err, ErrClosed) {
		t.Errorf("Append after Close = %v, want ErrClosed", err)
	}
	if err := l.Sync(); !errors.Is(err, ErrClosed) {
		t.Errorf("Sync after Close = %v, want ErrClosed", err)
	}
	if err := l.Close(); !errors.Is(err, ErrClosed) {
		t.Errorf("second Close = %v, want ErrClosed", err)
	}

	// The record written before Close must still be there.
	got, _, err := replayFile(path)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("Replay returned %d records, want 1: %s", len(got), formatRecords(got))
	}
}

func TestReplayMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "absent.log")
	got, validBytes, err := replayFile(path)
	if err != nil {
		t.Fatalf("Replay of a missing file = %v, want no error", err)
	}
	if validBytes != 0 || len(got) != 0 {
		t.Errorf("Replay of a missing file = (%d records, %d bytes), want (0, 0)", len(got), validBytes)
	}
}

// Environment variables that turn the test binary into the crash helper.
const (
	helperEnv     = "STRATA_WAL_HELPER"
	helperPathEnv = "STRATA_WAL_HELPER_PATH"
)

// TestHelperProcess is the child half of TestCrashRecoveryProcessKill. It does
// nothing unless the parent set helperEnv, so it is inert in a normal run.
func TestHelperProcess(t *testing.T) {
	if os.Getenv(helperEnv) != "1" {
		return
	}
	path := os.Getenv(helperPathEnv)
	if path == "" {
		fmt.Fprintln(os.Stderr, "helper: no log path given")
		os.Exit(2)
	}

	l, err := Open(path, SyncAlways)
	if err != nil {
		fmt.Fprintf(os.Stderr, "helper: Open: %v\n", err)
		os.Exit(2)
	}

	// Append until the parent kills us, reporting each acknowledged write.
	// os.Stdout is unbuffered, so a sequence number reaching the parent means
	// Append, and therefore the fsync inside it, had already returned.
	for seq := 0; ; seq++ {
		r := Record{
			Op:    OpPut,
			Key:   []byte(fmt.Sprintf("key-%06d", seq)),
			Value: []byte(fmt.Sprintf("value-%06d", seq)),
		}
		if err := l.Append(r); err != nil {
			fmt.Fprintf(os.Stderr, "helper: Append(%d): %v\n", seq, err)
			os.Exit(2)
		}
		fmt.Fprintln(os.Stdout, seq)
	}
}

// TestCrashRecoveryProcessKill kills a child mid-write and checks that every
// write the child had acknowledged survives, which is the whole point of
// SyncAlways.
func TestCrashRecoveryProcessKill(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL is not available on Windows")
	}

	const wantAcks = 200
	dir := t.TempDir()
	path := filepath.Join(dir, "wal.log")

	cmd := exec.Command(os.Args[0], "-test.run=^TestHelperProcess$")
	cmd.Env = append(os.Environ(), helperEnv+"=1", helperPathEnv+"="+path)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start helper: %v", err)
	}

	acks := make(chan int, 1<<16)
	go func() {
		defer close(acks)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			n, err := strconv.Atoi(strings.TrimSpace(sc.Text()))
			if err != nil {
				continue // Test-framework chatter, not an acknowledgement.
			}
			acks <- n
		}
	}()

	// fsync can be slow, especially F_FULLFSYNC on macOS, so allow plenty of
	// time for the first wantAcks writes.
	deadline := time.After(2 * time.Minute)
	var seen []int
	for len(seen) < wantAcks {
		select {
		case n, ok := <-acks:
			if !ok {
				_ = cmd.Wait()
				t.Fatalf("helper exited after %d acknowledgements, want at least %d; stderr: %s",
					len(seen), wantAcks, stderr.String())
			}
			seen = append(seen, n)
		case <-deadline:
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("helper produced only %d acknowledgements in 2 minutes, want %d", len(seen), wantAcks)
		}
	}

	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("Kill helper: %v", err)
	}
	_ = cmd.Wait() // SIGKILL always reports an error; nothing to learn from it.

	for i, n := range seen {
		if n != i {
			t.Fatalf("acknowledgement %d reported sequence %d; the helper is not counting up", i, n)
		}
	}

	var got []Record
	l, err := Recover(path, SyncAlways, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover after kill: %v", err)
	}
	defer l.Close()

	if len(got) < len(seen) {
		t.Fatalf("Recover replayed %d records, but %d writes were acknowledged", len(got), len(seen))
	}
	// Writes the helper completed after the parent last read stdout may also
	// be present; what must hold is that the log is a gapless run from zero
	// that covers every acknowledgement.
	for i, r := range got {
		if r.Op != OpPut {
			t.Fatalf("record %d is %s, want OpPut", i, formatRecord(r))
		}
		wantKey := fmt.Sprintf("key-%06d", i)
		if string(r.Key) != wantKey {
			t.Fatalf("record %d has key %q, want %q: the log has a gap or is out of order", i, r.Key, wantKey)
		}
		wantValue := fmt.Sprintf("value-%06d", i)
		if string(r.Value) != wantValue {
			t.Fatalf("record %d has value %q, want %q", i, r.Value, wantValue)
		}
	}

	// The recovered log must be usable, not just readable.
	if err := l.Append(Record{Op: OpPut, Key: []byte("after-recovery"), Value: []byte("ok")}); err != nil {
		t.Fatalf("Append after Recover: %v", err)
	}
	t.Logf("saw %d acknowledgements, recovered %d records", len(seen), len(got))
}

// FuzzReplay checks the two invariants that matter for recovery code reading
// bytes it cannot trust: it never panics, and it never claims more valid bytes
// than the file holds.
func FuzzReplay(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{})
	f.Add(encodeRecords(Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")}))
	f.Add(encodeRecords(
		Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")},
		Record{Op: OpDelete, Key: []byte("b")},
		Record{Op: OpPut, Key: []byte(""), Value: []byte("")},
	))
	// A valid record followed by a torn one.
	valid := encodeRecords(Record{Op: OpPut, Key: []byte("a"), Value: []byte("1")})
	f.Add(append(append([]byte(nil), valid...), valid[:len(valid)-1]...))
	f.Add(valid[:3])
	f.Add(make([]byte, headerSize))
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	f.Add([]byte("not a log at all, just text"))

	f.Fuzz(func(t *testing.T, data []byte) {
		path := filepath.Join(t.TempDir(), "fuzz.log")
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}

		validBytes, err := Replay(path, func(r Record) error {
			if r.Op != OpPut && r.Op != OpDelete {
				t.Fatalf("Replay yielded %s", formatRecord(r))
			}
			if r.Op == OpDelete && r.Value != nil {
				t.Fatalf("Replay yielded a delete carrying a value: %s", formatRecord(r))
			}
			// Touch the bytes so a bad slice bound would be caught here.
			_ = len(r.Key) + len(r.Value)
			return nil
		})
		if err != nil {
			t.Fatalf("Replay of %d arbitrary bytes returned %v, want no error", len(data), err)
		}
		if validBytes < 0 || validBytes > int64(len(data)) {
			t.Fatalf("Replay returned validBytes = %d for a %d byte file", validBytes, len(data))
		}
	})
}

func benchmarkAppend(b *testing.B, policy SyncPolicy) {
	path := filepath.Join(b.TempDir(), "bench.log")
	l, err := Open(path, policy)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer l.Close()

	r := Record{
		Op:    OpPut,
		Key:   []byte("benchmark-key"),
		Value: bytes.Repeat([]byte("v"), 100),
	}
	b.SetBytes(int64(headerSize + payloadLen(r)))
	b.ReportAllocs()

	for b.Loop() {
		if err := l.Append(r); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

func BenchmarkAppendSyncNone(b *testing.B)   { benchmarkAppend(b, SyncNone) }
func BenchmarkAppendSyncAlways(b *testing.B) { benchmarkAppend(b, SyncAlways) }
