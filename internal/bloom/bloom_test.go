package bloom

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"testing"
)

// randomKeys returns n distinct keys drawn from r, tagged with prefix so that
// two calls with different prefixes can never collide.
func randomKeys(r *rand.Rand, prefix string, n int) [][]byte {
	keys := make([][]byte, n)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("%s-%016x-%d", prefix, r.Uint64(), i))
	}
	return keys
}

// TestHashIsFNV1a pins the hash to the real FNV-1a, since a filter that is
// written now has to be readable by a later process: the hash is part of the
// on-disk format even though it is never stored.
func TestHashIsFNV1a(t *testing.T) {
	for _, key := range [][]byte{nil, {}, []byte("a"), []byte("hello world"), {0x00, 0xff, 0x00}} {
		h := fnv.New64a()
		h.Write(key)
		if got, want := Hash(key), h.Sum64(); got != want {
			t.Errorf("Hash(%q) = %#016x, want %#016x from hash/fnv", key, got, want)
		}
	}
}

func TestDefaultParameters(t *testing.T) {
	f := New(1000, 0)
	// 10 bits/key and k = round(10*ln2) = 7 is the combination the design
	// doc quotes a ~1% false-positive rate for.
	if f.Hashes() != 7 {
		t.Errorf("k = %d, want 7 at the default 10 bits per key", f.Hashes())
	}
	if f.Bits() != 10000 {
		t.Errorf("bits = %d, want 10000", f.Bits())
	}
	// Even a filter sized for nothing must have room to answer.
	if got := New(0, 0).Bits(); got < MinBits {
		t.Errorf("empty filter has %d bits, want at least %d", got, MinBits)
	}
	if got := New(-5, -5).Bits(); got < MinBits {
		t.Errorf("filter with negative sizing has %d bits, want at least %d", got, MinBits)
	}
	// k is clamped at both ends.
	if got := New(10, 1).Hashes(); got != 1 {
		t.Errorf("k at 1 bit per key = %d, want 1", got)
	}
	if got := New(10, 1000).Hashes(); got != MaxHashes {
		t.Errorf("k at 1000 bits per key = %d, want %d", got, MaxHashes)
	}
}

func TestNoFalseNegatives(t *testing.T) {
	const n = 50_000
	r := rand.New(rand.NewPCG(1, 2))
	keys := randomKeys(r, "present", n)

	f := New(n, 0)
	for _, k := range keys {
		f.Add(k)
	}

	for i, k := range keys {
		if !f.MayContain(k) {
			t.Fatalf("MayContain(%q) = false for added key %d; a Bloom filter must never have false negatives", k, i)
		}
	}
	// The empty key is a real key and must behave like any other.
	f2 := New(1, 0)
	f2.Add([]byte{})
	if !f2.MayContain([]byte{}) {
		t.Error("MayContain(empty key) = false after adding it")
	}
}

func TestFalsePositiveRate(t *testing.T) {
	const n = 100_000
	r := rand.New(rand.NewPCG(3, 4))
	added := randomKeys(r, "added", n)
	absent := randomKeys(r, "absent", n)

	f := New(n, DefaultBitsPerKey)
	for _, k := range added {
		f.Add(k)
	}

	falsePositives := 0
	for _, k := range absent {
		if f.MayContain(k) {
			falsePositives++
		}
	}

	rate := float64(falsePositives) / float64(n)
	t.Logf("false-positive rate at %d bits/key with k=%d: %d/%d = %.4f%%",
		DefaultBitsPerKey, f.Hashes(), falsePositives, n, rate*100)
	if rate >= 0.02 {
		t.Errorf("false-positive rate = %.4f%%, want below 2%%", rate*100)
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	const n = 5_000
	r := rand.New(rand.NewPCG(5, 6))
	added := randomKeys(r, "added", n)
	// Probe a mixture, so the round trip is checked on both answers.
	probes := append(added[:n/2:n/2], randomKeys(r, "absent", n)...)

	f := New(n, 0)
	for _, k := range added {
		f.Add(k)
	}

	raw := f.Marshal()
	if len(raw) != headerSize+f.Bits()/8 {
		t.Fatalf("Marshal produced %d bytes, want %d", len(raw), headerSize+f.Bits()/8)
	}
	if int(raw[0]) != f.Hashes() {
		t.Errorf("marshalled k = %d, want %d", raw[0], f.Hashes())
	}
	if got := binary.LittleEndian.Uint32(raw[1:5]); int(got) != f.Bits() {
		t.Errorf("marshalled bit count = %d, want %d", got, f.Bits())
	}

	// Mutating the caller's buffer afterwards must not reach the filter.
	g, err := Unmarshal(raw)
	if err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for i := range raw {
		raw[i] = 0xff
	}

	if g.Hashes() != f.Hashes() || g.Bits() != f.Bits() {
		t.Fatalf("round trip gave k=%d bits=%d, want k=%d bits=%d", g.Hashes(), g.Bits(), f.Hashes(), f.Bits())
	}
	for _, k := range probes {
		if got, want := g.MayContain(k), f.MayContain(k); got != want {
			t.Fatalf("after round trip MayContain(%q) = %v, want %v", k, got, want)
		}
	}
}

func TestUnmarshalRejectsGarbage(t *testing.T) {
	valid := New(100, 0).Marshal()

	// A filter with 800 bits, so 100 bytes of bit array after the header.
	withNbits := func(nbits uint32, payload int) []byte {
		b := make([]byte, headerSize+payload)
		b[0] = 7
		binary.LittleEndian.PutUint32(b[1:5], nbits)
		return b
	}

	cases := []struct {
		name string
		in   []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"short header", valid[:headerSize-1]},
		{"k zero", func() []byte { b := append([]byte(nil), valid...); b[0] = 0; return b }()},
		{"k above max", func() []byte { b := append([]byte(nil), valid...); b[0] = MaxHashes + 1; return b }()},
		{"k far too large", func() []byte { b := append([]byte(nil), valid...); b[0] = 0xff; return b }()},
		{"nbits zero", withNbits(0, 8)},
		{"nbits too small for payload", withNbits(8, 100)},
		{"nbits too large for payload", withNbits(8000, 100)},
		{"nbits at uint32 max", withNbits(0xFFFFFFFF, 8)},
		{"header only", valid[:headerSize]},
		{"truncated bit array", valid[:len(valid)-1]},
		{"extra trailing byte", append(append([]byte(nil), valid...), 0)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Unmarshal(tc.in)
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Unmarshal error = %v, want ErrCorrupt", err)
			}
			if f != nil {
				t.Errorf("Unmarshal returned a filter alongside an error")
			}
		})
	}

	// The unmodified filter must still be accepted, so the cases above are
	// rejected for their own reasons and not because the baseline is broken.
	if _, err := Unmarshal(valid); err != nil {
		t.Fatalf("Unmarshal(valid) = %v, want no error", err)
	}
}

func FuzzUnmarshal(f *testing.F) {
	f.Add([]byte(nil))
	f.Add(New(1, 0).Marshal())
	f.Add(New(100, 0).Marshal())
	f.Add([]byte{7, 64, 0, 0, 0})
	f.Add([]byte{0, 0, 0, 0, 0, 0})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		filter, err := Unmarshal(data)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Unmarshal returned %v, want an error wrapping ErrCorrupt", err)
			}
			return
		}
		// A filter that parsed must be internally consistent and must answer
		// without panicking or running away.
		if filter.Hashes() < 1 || filter.Hashes() > MaxHashes {
			t.Fatalf("parsed filter has k = %d", filter.Hashes())
		}
		if filter.Bits() == 0 {
			t.Fatal("parsed filter has zero bits")
		}
		for _, key := range [][]byte{nil, {}, []byte("a"), {0x00, 0xff}} {
			filter.MayContain(key)
		}
		filter.Add([]byte("after unmarshal"))
		if !filter.MayContain([]byte("after unmarshal")) {
			t.Fatal("key added to a parsed filter reads back absent")
		}
		// Marshalling again must reproduce the same bytes.
		if got := filter.Marshal(); len(got) != len(data) {
			t.Fatalf("re-marshalled to %d bytes, want %d", len(got), len(data))
		}
	})
}

func BenchmarkAdd(b *testing.B) {
	f := New(1_000_000, 0)
	key := []byte("benchmark-key-0123456789")
	b.ReportAllocs()
	for b.Loop() {
		f.Add(key)
	}
}

func BenchmarkMayContain(b *testing.B) {
	f := New(1_000_000, 0)
	key := []byte("benchmark-key-0123456789")
	f.Add(key)
	b.ReportAllocs()
	for b.Loop() {
		if !f.MayContain(key) {
			b.Fatal("false negative")
		}
	}
}
