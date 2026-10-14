package bloom

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrCorrupt reports that a marshalled filter could not be parsed: the input
// was too short, its parameters were out of range, or its declared bit count
// disagreed with how many bytes actually followed.
var ErrCorrupt = errors.New("bloom: corrupt filter")

// Tuning limits for the filter.
const (
	// DefaultBitsPerKey is the bits-per-key New uses when asked for a
	// non-positive value. Ten bits with the matching seven hash functions
	// gives roughly a one percent false-positive rate, which is the usual
	// point of diminishing returns: going to fifteen bits buys about a
	// factor of five but costs half again as much memory.
	DefaultBitsPerKey = 10
	// MinBits is the smallest bit array a filter may have. A filter sized
	// for very few keys would otherwise be so small that it saturates and
	// answers "maybe" for everything.
	MinBits = 64
	// MaxHashes caps k. Past this point the extra hashes cost more time
	// than the false positives they remove, and a corrupt k has to be
	// bounded by something before it drives a loop.
	MaxHashes = 16
)

// FNV-1a 64-bit parameters. FNV is used rather than maphash because a filter
// has to answer the same way in a later process: maphash seeds itself per
// process, which would make a persisted filter meaningless after a restart.
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// headerSize is the fixed part of the marshalled form: one byte of k and four
// bytes of bit count.
const headerSize = 5

// Filter is a Bloom filter: a bit array plus a hash count, able to say that a
// key is definitely absent or probably present.
//
// A Filter is safe for concurrent reads once it is built. Add is not safe to
// call concurrently with anything.
type Filter struct {
	// k is the number of bits each key sets, between 1 and MaxHashes.
	k int
	// nbits is the length of the bit array in bits. It is always a multiple
	// of eight for a filter New built, so bits holds exactly nbits/8 bytes.
	nbits uint32
	bits  []byte
}

// Hash returns the 64-bit FNV-1a hash of key, which is the only hash the
// filter uses. It is exported so that a caller holding a large set of keys can
// keep eight bytes per key instead of the keys themselves, and build the
// filter from the hashes later with AddHash.
//
// The result matches hash/fnv's New64a for the same input, and does not depend
// on the process, so a filter built now reads back correctly tomorrow.
func Hash(key []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, c := range key {
		h ^= uint64(c)
		h *= fnvPrime64
	}
	return h
}

// New returns an empty filter sized for expectedKeys keys at bitsPerKey bits
// each.
//
// A non-positive bitsPerKey means DefaultBitsPerKey. The bit array is at least
// MinBits long and is rounded up to a whole number of bytes. The hash count is
// round(bitsPerKey * ln2), which is the k that minimises the false-positive
// rate for a given number of bits per key, clamped to 1..MaxHashes.
func New(expectedKeys int, bitsPerKey int) *Filter {
	if bitsPerKey <= 0 {
		bitsPerKey = DefaultBitsPerKey
	}
	if expectedKeys < 0 {
		expectedKeys = 0
	}

	k := int(math.Round(float64(bitsPerKey) * math.Ln2))
	k = min(max(k, 1), MaxHashes)

	// int64 throughout, so a huge expectedKeys cannot wrap into a tiny
	// allocation on a 32-bit platform.
	nbits := int64(expectedKeys) * int64(bitsPerKey)
	if nbits < MinBits {
		nbits = MinBits
	}
	nbits = (nbits + 7) / 8 * 8
	if nbits > int64(math.MaxUint32) {
		nbits = int64(math.MaxUint32) / 8 * 8
	}

	return &Filter{k: k, nbits: uint32(nbits), bits: make([]byte, nbits/8)}
}

// Add records key as present.
func (f *Filter) Add(key []byte) {
	f.AddHash(Hash(key))
}

// AddHash records the key with hash h as present, where h came from Hash. It
// lets a caller that kept hashes instead of keys fill the filter without
// holding the keys.
func (f *Filter) AddHash(h uint64) {
	h1, h2 := split(h)
	for i := range f.k {
		bit := bitIndex(h1, h2, i, f.nbits)
		f.bits[bit/8] |= 1 << (bit % 8)
	}
}

// MayContain reports whether key might have been added.
//
// False means key was definitely never added. True means it probably was: at
// the default sizing, about one in a hundred keys that were never added still
// answer true, so a caller must confirm a true against the real data.
func (f *Filter) MayContain(key []byte) bool {
	h1, h2 := split(Hash(key))
	for i := range f.k {
		bit := bitIndex(h1, h2, i, f.nbits)
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// Bits returns the length of the bit array, and Hashes the number of bits each
// key sets. They exist so that a caller can record what a filter cost.
func (f *Filter) Bits() int { return int(f.nbits) }

// Hashes returns k, the number of bits each key sets.
func (f *Filter) Hashes() int { return f.k }

// split divides one 64-bit hash into the two independent-enough 32-bit values
// that the double-hashing scheme needs. Taking the high and low halves of a
// single hash, rather than hashing twice, is the Kirsch-Mitzenmacher result:
// the false-positive rate is asymptotically the same as with k independent
// hashes, at a fraction of the cost.
func split(h uint64) (h1, h2 uint32) {
	return uint32(h >> 32), uint32(h)
}

// bitIndex returns the bit the i'th hash function selects, as
// (h1 + i*h2) mod nbits computed in 64-bit arithmetic so the sum does not wrap
// before the modulo.
func bitIndex(h1, h2 uint32, i int, nbits uint32) uint32 {
	return uint32((uint64(h1) + uint64(i)*uint64(h2)) % uint64(nbits))
}

// Marshal returns the filter's bytes, laid out as the one-byte hash count, the
// bit count as a little-endian uint32, and then the bit array itself.
func (f *Filter) Marshal() []byte {
	out := make([]byte, 0, headerSize+len(f.bits))
	out = append(out, byte(f.k))
	out = binary.LittleEndian.AppendUint32(out, f.nbits)
	return append(out, f.bits...)
}

// Unmarshal parses the output of Marshal.
//
// Every field is checked against the others before anything is trusted: the
// input must be long enough for the header, k must be within 1..MaxHashes so
// that it cannot drive an unbounded loop, and the declared bit count must
// agree exactly with the number of bytes that follow. Anything else returns an
// error wrapping ErrCorrupt. The returned filter copies b, so the caller may
// reuse its buffer.
func Unmarshal(b []byte) (*Filter, error) {
	if len(b) < headerSize {
		return nil, fmt.Errorf("%w: %d bytes is shorter than the %d byte header", ErrCorrupt, len(b), headerSize)
	}
	k := int(b[0])
	if k < 1 || k > MaxHashes {
		return nil, fmt.Errorf("%w: hash count %d is outside 1..%d", ErrCorrupt, k, MaxHashes)
	}
	nbits := binary.LittleEndian.Uint32(b[1:headerSize])
	if nbits == 0 {
		return nil, fmt.Errorf("%w: bit count is zero", ErrCorrupt)
	}
	// Computed in 64 bits: nbits is attacker-controlled and (nbits+7) would
	// wrap at the top of the uint32 range.
	wantBytes := (uint64(nbits) + 7) / 8
	if got := uint64(len(b) - headerSize); got != wantBytes {
		return nil, fmt.Errorf("%w: bit count %d needs %d bytes, got %d", ErrCorrupt, nbits, wantBytes, got)
	}

	bits := make([]byte, len(b)-headerSize)
	copy(bits, b[headerSize:])
	return &Filter{k: k, nbits: nbits, bits: bits}, nil
}
