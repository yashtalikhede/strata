# Design

## Goals

A correct, crash-safe, ordered key-value store, built from scratch on the Go
standard library alone.

## Non-goals

No network protocol, no transactions, no SQL. strata is an embedded storage
engine, not a database server.

## Data model

Keys and values are opaque byte slices. Keys are ordered bytewise, as by
`bytes.Compare`, and the empty key is valid. A delete does not erase data; it
stores a tombstone, which is a record saying the key is gone, so that it can
shadow an older value for the same key in a lower layer.

## Phase 1: memtable internals

The memtable is a skip list: a stack of linked lists over one set of nodes,
where every node appears in the full chain at level 0 and in each higher level
with probability one in four. A search starts at the sentinel head node on the
highest level in use and moves right while the next key is still smaller than
the target, dropping down a level whenever the next key would overshoot. Each
level skips over roughly three quarters of the nodes on the level below it, so
a lookup touches on the order of log n nodes instead of scanning the whole
chain. Because every key present at some level is also present at level 0, the
descent always ends one step short of the target on level 0, and a single
comparison there settles whether the key exists.

Insertion reuses that same descent, but records the last node it visited on
every level as it goes, in what the code calls the update array. Those recorded
nodes are exactly the ones whose forward pointers have to be rewritten to
splice a new node in, and they are scattered through the list, so remembering
them during the one descent avoids searching again once per level. A new node
is given a random height, level one plus a further level for as long as a
one-in-four draw keeps succeeding, capped at twelve, which keeps the index
balanced in expectation without any rebalancing work. If the key already
exists, nothing is spliced at all: the node keeps its position and its height,
and only the value and the size accounting change.

Deletes never unlink a node. They set a tombstone flag and release the value,
and if the key is not in the memtable at all they insert a node that is a
tombstone from the outset. This is necessary because the memtable is only the
newest layer of the tree: an older on-disk table may still hold a value for
that key, and the delete has to survive into the flushed table as a record in
its own right in order to shadow it. Unlinking the node instead would hide the
delete from the flush, and the old value would reappear on the next read.

## Write path

A write is logged before it is applied. Put and Delete each append one record
to the write-ahead log in `internal/wal`, and only once that append has
returned does the memtable change. The order is the whole point: if the process
dies in the gap between the two, recovery replays the record into a fresh
memtable and the write is still there. Reversed, a crash in that same gap would
leave a write the caller had been told about but that no log knew, and it would
simply vanish.

A record is laid out as follows, little endian throughout:

    [crc32 uint32][length uint32][payload ...]

    payload = [type byte][keyLen uvarint][valLen uvarint][key][value]

`length` counts payload bytes only, so a record occupies eight bytes more than
its length field reports. `type` is 1 for a put and 2 for a delete. Zero is
deliberately not a valid type, which means a run of zero bytes, from a sparse
file or a block the filesystem zero-filled, can never decode as a record. A
delete writes a `valLen` of 0 and no value bytes; a put of an empty value also
writes a `valLen` of 0 and no value bytes. The two stay distinguishable because
the type byte differs, not because their lengths do, which is what lets an
empty value survive a round trip as a real record rather than collapsing into a
tombstone. Key and value lengths are varints rather than fixed-width fields
because nearly every key is shorter than 128 bytes and would otherwise spend
three of its four length bytes on leading zeros.

The checksum is CRC-32C, the Castagnoli polynomial, and it covers the length
field as well as the payload. Covering the length is the detail that matters
most. A reader has to decide how many bytes to read for a payload before it has
anything to check that number against, so an unchecksummed length is a number
taken on faith from a file that may be damaged: a single flipped bit in it
either steps over a real record or asks for a payload that was never written,
and in both cases the reader loses its place in the stream while believing it
has not. Putting the length under the same CRC as the payload means the only
lengths the reader ever acts on are lengths it has verified. One read cannot be
protected this way, the eight header bytes themselves, so that read is bounded
instead by `MaxRecordSize`, 64 MiB: a length above the limit is treated as
corruption and rejected before any buffer is allocated for it, so a few bad
bytes can never make recovery ask the allocator for gigabytes.

Each record is built complete in one buffer and handed to a single `write`.
That does not make an append atomic, since nothing available to a user-space
program does, but it keeps the window in which a crash can tear a record as
narrow as the platform allows, and it keeps a torn record at the end of the
file rather than in the middle, which is the case recovery can actually handle.
Appends serialise on a mutex held across the write, so two writers can never
interleave halves of their records, and the encoding buffer is reused between
appends, which leaves a steady stream of writes free of allocations.

## Read path

To be written in phase 4

## File formats

To be written in phase 3

## Compaction

To be written in phase 5

## Crash-safety guarantees

What an append promises depends on the sync policy. Under `SyncAlways`, Append
fsyncs before returning, so an append that returned no error has reached stable
storage and survives the machine losing power. Under `SyncNone`, Append returns
once the bytes are in the operating system's page cache: the record survives
the process dying, being killed, or panicking, because the kernel still owns
the data and will write it out, but it does not survive a power loss or a
kernel panic until something calls Sync. `SyncNone` is therefore right for a
caller that batches durability points of its own, and wrong for one that
reports success to a user. `SyncAlways` is the zero value of the type, so a
policy nobody chose is the safe one.

Recovery is the other half of the guarantee. Recover replays the log from the
start, stops at the first record it cannot trust, truncates the file back to
the end of the last record it accepted, fsyncs that truncation, and only then
reopens the file for appending. A record is untrusted if its header is short,
its payload is short, its length exceeds `MaxRecordSize`, its CRC does not
match, its type byte is not a known operation, or its varints do not add up to
exactly the payload length.

Truncating a torn tail rather than reporting it is deliberate, and it is worth
separating from the idea of tolerating corruption. A half-written tail is the
ordinary result of a crash, not evidence that anything is wrong: the records
before it are intact, and the torn one belongs to an append that never
returned, so no caller was ever told it had succeeded. Refusing to open such a
log would turn an expected event into an outage and would lose every
acknowledged write along with the one nobody was promised. Leaving the garbage
in place would be worse still, because the next append would land behind it and
be unreachable to every later replay, which breaks the log silently and
permanently. Cutting the tail off discards only bytes that were never
acknowledged, which is exactly the set a caller has no claim on.

What the log does not guarantee is worth stating as plainly as what it does.

A corrupt record in the middle of the file ends the replay there, and every
valid record after it is lost. Replay has no way to find the start of the next
record once it has lost its place: the format carries no resynchronisation
marker, and a record boundary is only discoverable by reading forward from a
boundary already known. Recover then truncates those later records away,
because they sit beyond the last offset it can vouch for. Damage in the middle
of a log therefore costs far more than its own size, and this is the format's
main weakness. The fix is fixed-size blocks with per-block framing, of the kind
LevelDB uses, so that a reader can skip to the next block boundary and carry
on; that is a change worth making before the log is trusted with large files.

The CRC detects damage, it does not repair it. There is no redundancy in the
log, so a flipped bit costs the record it landed in.

A record whose CRC matches is trusted. CRC-32C catches every single-bit and
double-bit error and every burst up to 32 bits, which is what the bit-flip
tests exercise, but it is a checksum and not a cryptographic hash: a
deliberately crafted record will pass it. The log defends against accidental
corruption, not against someone with write access to the file.

Durability is bounded by the hardware underneath. An fsync on a device that
lies about having flushed its write cache makes nothing durable, and no code at
this layer can tell.

The log file's own creation is not fsynced. Recover fsyncs the truncation,
which flushes the inode and so makes the new size durable, but it does not
fsync the containing directory, so a log file created for a brand new database
could in principle be missing after a power loss moments later. That gap is
harmless as things stand, because a log that does not exist replays as an empty
log and an empty database is what the caller started with. It would need
closing if the log file ever came to carry information merely by existing.

## Benchmarks

To be written in phase 6
