package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"testing"
)

// statusName makes failure messages readable, since Status is a bare int.
func statusName(s Status) string {
	switch s {
	case NotFound:
		return "NotFound"
	case Found:
		return "Found"
	case Deleted:
		return "Deleted"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// expectFound asserts that key reads back as a live value equal to want. The
// returned slice must be non-nil even when want is empty, so that callers can
// tell an empty value apart from a missing one without consulting the status.
func expectFound(t *testing.T, m *Memtable, key, want []byte) {
	t.Helper()
	got, status := m.Get(key)
	if status != Found {
		t.Fatalf("Get(%q) status = %s, want Found", key, statusName(status))
	}
	if got == nil {
		t.Fatalf("Get(%q) returned a nil value with status Found", key)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) value = %q, want %q", key, got, want)
	}
}

func expectStatus(t *testing.T, m *Memtable, key []byte, want Status) {
	t.Helper()
	got, status := m.Get(key)
	if status != want {
		t.Fatalf("Get(%q) status = %s, want %s", key, statusName(status), statusName(want))
	}
	if got != nil {
		t.Fatalf("Get(%q) value = %q, want nil for status %s", key, got, statusName(want))
	}
}

func TestPutGetBasic(t *testing.T) {
	m := New()

	m.Put([]byte("alpha"), []byte("one"))
	m.Put([]byte("beta"), []byte("two"))
	expectFound(t, m, []byte("alpha"), []byte("one"))
	expectFound(t, m, []byte("beta"), []byte("two"))

	// Last write wins: a memtable keeps only the newest write per key.
	m.Put([]byte("alpha"), []byte("three"))
	expectFound(t, m, []byte("alpha"), []byte("three"))
	if got := m.Len(); got != 2 {
		t.Fatalf("Len after overwrite = %d, want 2", got)
	}

	expectStatus(t, m, []byte("gamma"), NotFound)
}

func TestDelete(t *testing.T) {
	m := New()

	m.Put([]byte("present"), []byte("v"))
	m.Delete([]byte("present"))
	expectStatus(t, m, []byte("present"), Deleted)
	if got := m.Len(); got != 1 {
		t.Fatalf("Len after deleting a present key = %d, want 1", got)
	}

	// Deleting a key this memtable never saw still has to leave a tombstone,
	// because the key may have an older value in an on-disk table to shadow.
	m.Delete([]byte("absent"))
	expectStatus(t, m, []byte("absent"), Deleted)
	if got := m.Len(); got != 2 {
		t.Fatalf("Len after deleting an unseen key = %d, want 2", got)
	}

	// A later Put resurrects the key.
	m.Put([]byte("present"), []byte("again"))
	expectFound(t, m, []byte("present"), []byte("again"))
	if got := m.Len(); got != 2 {
		t.Fatalf("Len after re-putting a deleted key = %d, want 2", got)
	}
}

func TestEmptyValueVersusTombstone(t *testing.T) {
	m := New()

	// A nil value is a real, empty record, not a delete.
	m.Put([]byte("k"), nil)
	got, status := m.Get([]byte("k"))
	if status != Found {
		t.Fatalf("Get after Put(nil) status = %s, want Found", statusName(status))
	}
	if got == nil {
		t.Fatalf("Get after Put(nil) returned a nil value, want non-nil and empty")
	}
	if len(got) != 0 {
		t.Fatalf("Get after Put(nil) value = %q, want empty", got)
	}

	m.Put([]byte("k"), []byte{})
	got, status = m.Get([]byte("k"))
	if status != Found {
		t.Fatalf("Get after Put(empty) status = %s, want Found", statusName(status))
	}
	if got == nil {
		t.Fatalf("Get after Put(empty) returned a nil value, want non-nil and empty")
	}
	if len(got) != 0 {
		t.Fatalf("Get after Put(empty) value = %q, want empty", got)
	}

	m.Delete([]byte("k"))
	expectStatus(t, m, []byte("k"), Deleted)
}

func TestEmptyKey(t *testing.T) {
	m := New()

	m.Put([]byte(""), []byte("root"))
	expectFound(t, m, []byte(""), []byte("root"))
	if got := m.Len(); got != 1 {
		t.Fatalf("Len with only the empty key = %d, want 1", got)
	}

	m.Delete([]byte(""))
	expectStatus(t, m, []byte(""), Deleted)

	// A nil key is the same key as an empty one.
	m.Put(nil, []byte("back"))
	expectFound(t, m, nil, []byte("back"))
	if got := m.Len(); got != 1 {
		t.Fatalf("Len after nil-key Put = %d, want 1", got)
	}
}

func TestCopySemantics(t *testing.T) {
	m := New()

	key := []byte("key")
	value := []byte("value")
	m.Put(key, value)

	// The memtable must not alias the caller's slices.
	key[0] = 'X'
	value[0] = 'X'
	expectFound(t, m, []byte("key"), []byte("value"))
	expectStatus(t, m, []byte("Xey"), NotFound)

	// A value handed back by Get belongs to the caller.
	got, status := m.Get([]byte("key"))
	if status != Found {
		t.Fatalf("Get status = %s, want Found", statusName(status))
	}
	got[0] = 'Z'
	expectFound(t, m, []byte("key"), []byte("value"))

	// So does everything inside an Entries snapshot.
	m.Put([]byte("other"), []byte("data"))
	entries := m.Entries()
	for i := range entries {
		for j := range entries[i].Key {
			entries[i].Key[j] = 'Q'
		}
		for j := range entries[i].Value {
			entries[i].Value[j] = 'Q'
		}
	}
	for _, e := range m.Entries() {
		if bytes.Contains(e.Key, []byte("Q")) {
			t.Fatalf("Entries key %q was corrupted by mutating an earlier snapshot", e.Key)
		}
		if bytes.Contains(e.Value, []byte("Q")) {
			t.Fatalf("Entries value %q was corrupted by mutating an earlier snapshot", e.Value)
		}
	}
	expectFound(t, m, []byte("key"), []byte("value"))
	expectFound(t, m, []byte("other"), []byte("data"))
}

func TestEntriesSorted(t *testing.T) {
	m := New()

	// Scrambled insertion order, with keys that are prefixes of each other
	// and with both ends of the byte range represented.
	m.Put([]byte("b"), []byte("vb"))
	m.Put([]byte(""), []byte("vempty"))
	m.Put([]byte("ab"), []byte("vab"))
	m.Delete([]byte("\xff"))
	m.Put([]byte("a"), []byte("va"))
	m.Delete([]byte("ab"))

	want := []Entry{
		{Key: []byte(""), Value: []byte("vempty")},
		{Key: []byte("a"), Value: []byte("va")},
		{Key: []byte("ab"), Tombstone: true},
		{Key: []byte("b"), Value: []byte("vb")},
		{Key: []byte("\xff"), Tombstone: true},
	}

	got := m.Entries()
	if len(got) != len(want) {
		t.Fatalf("Entries returned %d entries, want %d: %v", len(got), len(want), got)
	}
	for i, e := range got {
		if !bytes.Equal(e.Key, want[i].Key) {
			t.Fatalf("Entries[%d].Key = %q, want %q", i, e.Key, want[i].Key)
		}
		if e.Tombstone != want[i].Tombstone {
			t.Fatalf("Entries[%d].Tombstone = %v, want %v for key %q", i, e.Tombstone, want[i].Tombstone, e.Key)
		}
		if e.Tombstone {
			if e.Value != nil {
				t.Fatalf("Entries[%d].Value = %q for tombstone key %q, want nil", i, e.Value, e.Key)
			}
		} else {
			if e.Value == nil {
				t.Fatalf("Entries[%d].Value is nil for live key %q", i, e.Key)
			}
			if !bytes.Equal(e.Value, want[i].Value) {
				t.Fatalf("Entries[%d].Value = %q, want %q", i, e.Value, want[i].Value)
			}
		}
		// Strictly ascending implies both sorted order and no duplicates.
		if i > 0 && bytes.Compare(got[i-1].Key, e.Key) >= 0 {
			t.Fatalf("Entries not strictly ascending at %d: %q then %q", i, got[i-1].Key, e.Key)
		}
	}
}

func TestLenAndApproxSize(t *testing.T) {
	m := New()
	if got := m.Len(); got != 0 {
		t.Fatalf("Len of a fresh memtable = %d, want 0", got)
	}
	if got := m.ApproxSize(); got != 0 {
		t.Fatalf("ApproxSize of a fresh memtable = %d, want 0", got)
	}

	m.Put([]byte("abc"), []byte("12345"))
	if got := m.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
	if got := m.ApproxSize(); got != 8 {
		t.Fatalf("ApproxSize after Put(abc, 12345) = %d, want 8", got)
	}

	// An overwrite replaces the old value in the accounting, never adds to it.
	m.Put([]byte("abc"), []byte("xy"))
	if got := m.Len(); got != 1 {
		t.Fatalf("Len after overwrite = %d, want 1", got)
	}
	if got := m.ApproxSize(); got != 5 {
		t.Fatalf("ApproxSize after overwrite with a 2-byte value = %d, want 5", got)
	}

	// A tombstone keeps the key but drops the value from the accounting.
	m.Delete([]byte("abc"))
	if got := m.Len(); got != 1 {
		t.Fatalf("Len after Delete = %d, want 1", got)
	}
	if got := m.ApproxSize(); got != 3 {
		t.Fatalf("ApproxSize after Delete(abc) = %d, want 3", got)
	}

	m.Put([]byte("de"), []byte("zzz"))
	if got := m.Len(); got != 2 {
		t.Fatalf("Len with two keys = %d, want 2", got)
	}
	if got := m.ApproxSize(); got != 8 {
		t.Fatalf("ApproxSize with a tombstone on abc plus de/zzz = %d, want 8", got)
	}
}

// TestModelRandomized checks the memtable against a plain map that obeys the
// same contract. The map is obviously correct but unordered, so it is a good
// oracle for everything except the ordering checked in TestEntriesSorted.
func TestModelRandomized(t *testing.T) {
	type modelEntry struct {
		value     []byte
		tombstone bool
	}

	rng := rand.New(rand.NewPCG(42, 99))
	alphabet := []byte{0x00, 0x01, 'a', 0xFF}

	randomKey := func() []byte {
		key := make([]byte, rng.IntN(5))
		for i := range key {
			key[i] = alphabet[rng.IntN(len(alphabet))]
		}
		return key
	}
	randomValue := func() []byte {
		value := make([]byte, rng.IntN(9))
		for i := range value {
			value[i] = byte(rng.IntN(256))
		}
		return value
	}

	m := New()
	model := make(map[string]modelEntry)

	const ops = 20000
	for i := 0; i < ops; i++ {
		key := randomKey()
		switch roll := rng.IntN(100); {
		case roll < 60:
			value := randomValue()
			m.Put(key, value)
			stored := make([]byte, len(value))
			copy(stored, value)
			model[string(key)] = modelEntry{value: stored}
		case roll < 85:
			m.Delete(key)
			model[string(key)] = modelEntry{tombstone: true}
		default:
			got, status := m.Get(key)
			want, ok := model[string(key)]
			switch {
			case !ok:
				if status != NotFound {
					t.Fatalf("op %d: Get(%q) status = %s, want NotFound", i, key, statusName(status))
				}
				if got != nil {
					t.Fatalf("op %d: Get(%q) value = %q, want nil", i, key, got)
				}
			case want.tombstone:
				if status != Deleted {
					t.Fatalf("op %d: Get(%q) status = %s, want Deleted", i, key, statusName(status))
				}
				if got != nil {
					t.Fatalf("op %d: Get(%q) value = %q, want nil", i, key, got)
				}
			default:
				if status != Found {
					t.Fatalf("op %d: Get(%q) status = %s, want Found", i, key, statusName(status))
				}
				if got == nil {
					t.Fatalf("op %d: Get(%q) returned a nil value with status Found", i, key)
				}
				if !bytes.Equal(got, want.value) {
					t.Fatalf("op %d: Get(%q) value = %q, want %q", i, key, got, want.value)
				}
			}
		}
	}

	keys := make([]string, 0, len(model))
	wantSize := 0
	for key, entry := range model {
		keys = append(keys, key)
		wantSize += len(key)
		if !entry.tombstone {
			wantSize += len(entry.value)
		}
	}
	// Go compares strings bytewise, so this is bytes.Compare ordering.
	sort.Strings(keys)

	if got := m.Len(); got != len(model) {
		t.Fatalf("Len = %d, want %d", got, len(model))
	}
	if got := m.ApproxSize(); got != wantSize {
		t.Fatalf("ApproxSize = %d, want %d", got, wantSize)
	}

	entries := m.Entries()
	if len(entries) != len(keys) {
		t.Fatalf("Entries returned %d entries, want %d", len(entries), len(keys))
	}
	for i, e := range entries {
		if string(e.Key) != keys[i] {
			t.Fatalf("Entries[%d].Key = %q, want %q", i, e.Key, keys[i])
		}
		want := model[keys[i]]
		if e.Tombstone != want.tombstone {
			t.Fatalf("Entries[%d].Tombstone = %v, want %v for key %q", i, e.Tombstone, want.tombstone, e.Key)
		}
		if want.tombstone {
			if e.Value != nil {
				t.Fatalf("Entries[%d].Value = %q for tombstone key %q, want nil", i, e.Value, e.Key)
			}
			continue
		}
		if e.Value == nil {
			t.Fatalf("Entries[%d].Value is nil for live key %q", i, e.Key)
		}
		if !bytes.Equal(e.Value, want.value) {
			t.Fatalf("Entries[%d].Value = %q, want %q for key %q", i, e.Value, want.value, e.Key)
		}
	}
}

func TestConcurrent(t *testing.T) {
	const (
		writers        = 8
		keysPerWriter  = 1000
		readers        = 4
		expectedLength = writers * keysPerWriter
	)

	concurrentKey := func(id, n int) string {
		return fmt.Sprintf("w%d-%04d", id, n)
	}

	m := New()

	var writeGroup, readGroup sync.WaitGroup
	done := make(chan struct{})

	for id := 0; id < writers; id++ {
		writeGroup.Add(1)
		go func(id int) {
			defer writeGroup.Done()
			for n := 0; n < keysPerWriter; n++ {
				key := concurrentKey(id, n)
				m.Put([]byte(key), []byte("v-"+key))
			}
		}(id)
	}

	// Readers run concurrently with the writers purely to give the race
	// detector something to find; whatever they read is legitimately partial,
	// so they only check self-consistency, not completeness.
	for r := 0; r < readers; r++ {
		readGroup.Add(1)
		go func(r int) {
			defer readGroup.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				for n := 0; n < keysPerWriter; n++ {
					key := concurrentKey(n%writers, n)
					if value, status := m.Get([]byte(key)); status == Found && string(value) != "v-"+key {
						t.Errorf("Get(%q) value = %q, want %q", key, value, "v-"+key)
						return
					}
				}
				for _, e := range m.Entries() {
					if e.Tombstone {
						t.Errorf("Entries contained a tombstone for %q, but nothing was deleted", e.Key)
						return
					}
				}
			}
		}(r)
	}

	writeGroup.Wait()
	close(done)
	readGroup.Wait()

	if got := m.Len(); got != expectedLength {
		t.Fatalf("Len after %d concurrent writers = %d, want %d", writers, got, expectedLength)
	}
	for id := 0; id < writers; id++ {
		for n := 0; n < keysPerWriter; n++ {
			key := concurrentKey(id, n)
			expectFound(t, m, []byte(key), []byte("v-"+key))
		}
	}
}

func BenchmarkPut(b *testing.B) {
	m := New()
	value := make([]byte, 16)
	var key [8]byte

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		m.Put(key[:], value)
	}
}

func BenchmarkGet(b *testing.B) {
	const prefill = 100000

	m := New()
	value := make([]byte, 16)
	var key [8]byte
	for i := 0; i < prefill; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i))
		m.Put(key[:], value)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(key[:], uint64(i%prefill))
		if _, status := m.Get(key[:]); status != Found {
			b.Fatalf("Get(%x) status = %s, want Found", key, statusName(status))
		}
	}
}
