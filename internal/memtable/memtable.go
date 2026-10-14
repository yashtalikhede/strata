package memtable

// Status reports the outcome of a lookup. A memtable distinguishes "this key
// was deleted here" from "this key is unknown here", because a delete must be
// able to shadow an older value that still lives in an on-disk table.
type Status int

const (
	// NotFound means the memtable holds no record for the key at all. A
	// caller may still find an older value in an on-disk table.
	NotFound Status = iota
	// Found means the memtable holds a live value for the key.
	Found
	// Deleted means the memtable holds a tombstone for the key. The key is
	// absent as far as the caller is concerned, and any older on-disk value
	// must be treated as absent too.
	Deleted
)

// Entry is one record in a memtable snapshot. For a tombstone, Value is nil
// and Tombstone is true; otherwise Value is non-nil (possibly empty) and
// Tombstone is false.
type Entry struct {
	// Key is a copy of the stored key.
	Key []byte
	// Value is a copy of the stored value, or nil for a tombstone.
	Value []byte
	// Tombstone reports whether this entry records a delete.
	Tombstone bool
}

// Memtable is an in-memory, sorted, mutable write buffer holding the most
// recent write for each key.
//
// Keys compare bytewise, as by bytes.Compare, and the empty key is valid.
// Put and Delete copy their arguments, so mutating the caller's slices
// afterwards never affects stored data, and reads likewise return copies.
//
// All methods are safe for concurrent use.
type Memtable struct {
	// Fields are added when the structure is implemented.
}

// New returns an empty memtable ready for use.
func New() *Memtable {
	panic("not implemented")
}

// Put stores value under key, replacing any previous value or tombstone for
// that key, so the last write wins.
//
// Both key and value are copied, so later mutation of the caller's slices does
// not affect stored data. A nil value is stored as an empty value, which is a
// real record and not a delete: Get then reports Found with a non-nil,
// zero-length value.
func (m *Memtable) Put(key, value []byte) {
	panic("not implemented")
}

// Delete records a tombstone for key, replacing any previous value.
//
// A tombstone is stored even when the key was never present in this memtable,
// because the key may still have a value in an older on-disk table that this
// delete has to shadow. A tombstone therefore counts towards Len and appears
// in Entries. The key is copied.
func (m *Memtable) Delete(key []byte) {
	panic("not implemented")
}

// Get looks up key.
//
// It returns Found and a copy of the stored value, which is non-nil and
// possibly empty; Deleted and a nil value if the key carries a tombstone; or
// NotFound and a nil value if the memtable holds no record for the key. The
// returned slice is the caller's own, so mutating it does not affect stored
// data.
func (m *Memtable) Get(key []byte) (value []byte, status Status) {
	panic("not implemented")
}

// Len returns the number of distinct keys held, counting tombstones.
func (m *Memtable) Len() int {
	panic("not implemented")
}

// ApproxSize returns the summed size in bytes of the records held: for each
// entry len(key)+len(value), and for a tombstone len(key) alone. It
// deliberately ignores per-entry overhead, and exists so a caller can decide
// when a memtable is full enough to flush.
func (m *Memtable) ApproxSize() int {
	panic("not implemented")
}

// Entries returns a snapshot of every record held, sorted ascending by key and
// free of duplicates, with tombstones included as Value nil and Tombstone
// true.
//
// The keys and values are copies, so mutating them does not affect stored
// data. The snapshot is what a flush writes to an on-disk table, which is why
// tombstones have to be part of it.
func (m *Memtable) Entries() []Entry {
	panic("not implemented")
}
