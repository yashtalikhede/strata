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

### A write end to end

Put and Delete are the same operation with a different record type, and both
run the same three steps under the database's write lock.

First the record goes to the log. Until that append returns there is nothing to
recover, and if it fails the memtable must look exactly as it did before, so
the memtable is not touched until the log has accepted the record. A log error
is returned wrapped, with its own sentinel intact, so a caller can tell
`wal.ErrRecordTooLarge` from a full disk; in either case the key is absent
afterwards and the database is still usable.

Then the record is applied to the memtable, a Put storing the value and a
Delete storing a tombstone. Both copy their arguments, so the caller's slices
are its own afterwards. At this point the write is done as far as the caller is
concerned: it is durable to whatever degree the sync policy promises, and the
next Get will see it.

Last, the write checks whether it has made the memtable too big. If
`ApproxSize` has reached `MemtableSize`, the same call that did the write also
performs the flush, synchronously, still holding the write lock. That is what
makes a flush failure awkward to report, and why Put returns an error wrapping
`ErrFlushFailed` rather than a plain failure: the write itself succeeded and is
visible, and only the move to a table did not happen. Retrying the write would
duplicate it; the thing to look at is why the disk refused. The memtable stays
where it is and the next write will try to flush it again.

The whole sequence holds one write lock, so writes are serialised against each
other and against flushes, while reads share a read lock and run in parallel
with each other. A flush therefore stalls every writer for as long as it takes
to write a table and commit a manifest. See the limitations section.

## Read path

A Get asks each source in turn, newest first, and the first source that holds
any record for the key decides the answer. The memtable comes first, then the
tables from newest to oldest.

The word "any" is the whole of it. A source that holds a tombstone for the key
answers just as conclusively as one that holds a value: the lookup stops and
reports the key as absent. That is what makes a delete work in a structure that
never edits anything in place. The older value is still sitting in an older
table, untouched, and the only reason it does not surface is that the search
reached the newer tombstone first and stopped. The same mechanism makes an
overwrite work, since a newer value shadows an older one for exactly the same
reason.

Each table's own lookup is described under file formats: its Bloom filter turns
most absent keys away without any I/O, and otherwise its index names the single
block that could hold the key. So a Get that misses pays one Bloom check per
table and usually no disk read at all, while a Get that hits pays one block read
in the table that has it, plus a Bloom check in each newer table it looked
through first. The cost of a miss therefore grows with the number of tables,
which is the read amplification that compaction exists to bound and that this
phase does nothing about.

A table that cannot be read stops the Get and returns the error. Skipping a
corrupt table and carrying on to an older one would be worse than failing: the
older table may hold a stale value for the key, and serving it would turn
detected corruption into a silently wrong answer.

Reads hold only a read lock, so any number of them proceed together. They do
not see a consistent snapshot across sources, because nothing stops a flush
from running between one Get and the next; what they do see is one atomic
result per call, since a flush cannot run during a Get.

## File formats

The write-ahead log's record format is described under the write path above.
This section covers the sorted table, which is the engine's one durable store
of data, and the Bloom filter that fronts it.

### SSTable

A table is immutable once written. Nothing edits one in place, which is what
lets a reader hold its index in memory and trust it, and what lets compaction
treat a table as a value rather than something to lock.

The file is laid out in four parts, little endian throughout:

    [data block 0][data block 1] ... [data block N-1]
    [index block]
    [bloom block]
    [footer]

A data block is entries written back to back and then a four-byte CRC-32C of
those entry bytes. An entry is

    [flags byte][keyLen uvarint][valLen uvarint][key][value]

with bit 0 of flags marking a tombstone. A tombstone stores a value length of
zero and no value bytes, and so does a put of an empty value; the flags byte is
what tells them apart, which is why a reader rejects an entry that sets any
flag bit it does not know rather than reading it as a plain put. Entries within
a block are in ascending key order, and keys across the whole table are
strictly increasing, so the blocks partition the key space in order.

Blocks are cut around entries, never through them. The current block closes
once its entry bytes reach `TargetBlockSize`, four kilobytes, so a block
overshoots the target by at most the last entry added, and an entry that is
itself that large or larger is given a block to its own. Giving the oversized
entry its own block is the part worth stating twice: without it, a one-megabyte
value appended after a handful of small entries would pull them into its block,
and a reader wanting one of those small entries would have to fetch the whole
megabyte to reach it.

The index block holds one record per data block,

    [keyLen uvarint][firstKey][offset uint64][length uint32]

where length covers the block's entries and its CRC, followed by a four-byte
CRC of the index bytes. One key per four kilobytes of table is what makes the
index small enough to keep in memory. The bloom block is the marshalled filter
followed by its own four-byte CRC.

The footer is a fixed 56 bytes at the very end of the file:

    [ 0:8 ]  indexOffset uint64
    [ 8:16]  indexLen    uint64
    [16:24]  bloomOffset uint64
    [24:32]  bloomLen    uint64
    [32:40]  entryCount  uint64
    [40:48]  magic       uint64, 0x73747261746131, "strata1" in ASCII
    [48:52]  version     uint32, currently 1
    [52:56]  crc32c      uint32, over bytes 0 to 52

Being fixed width and last, the footer is the one thing a reader can find
without knowing anything else, which is why everything needed to navigate the
file lives in it. Because the magic is stored little endian its bytes appear
reversed in a hex dump. The four bytes that would otherwise be padding carry a
version instead, so a reader that meets a layout it does not know says so
rather than guessing.

Every block carries its own CRC rather than the file carrying one. A single
whole-file checksum would answer a question nobody asks, since verifying it
would mean reading the entire table on the way to every lookup, which is
exactly what the index and the filter exist to avoid. Per-block checksums are
checked on the one block a lookup actually reads, so the cost is proportional
to the work being done, and damage is localised: a flipped bit makes its own
block unreadable and leaves every other block, and the whole of the rest of the
table, usable.

Everything a reader takes from the file is treated as hostile until it has been
checked against something else. The footer's checksum is verified before any of
its fields are believed, then the magic and version, and only then the offsets.
The offsets have to tile the file exactly, data then index then bloom then
footer with no gap and no overlap, which is a far tighter condition than merely
pointing inside the file and leaves no length that could be used to allocate or
read somewhere it should not. The index is held to the same standard: its
blocks must start at offset zero, run consecutively, end exactly where the data
section does, and have strictly increasing first keys, which is also the
property the binary search in a lookup depends on. Nothing is allocated from a
length that has not first been bounded by the size of the file.

The index and the filter are both read into memory at open and never written
again. Both are consulted on the way to every lookup, so leaving them on disk
would add two reads per lookup to save space the table has already paid for:
together they come to a few megabytes for a gigabyte of table. Being read-only
after open is also what makes a reader safe to share, since concurrent lookups
need no lock beyond what the operating system already gives a positional read.

A point lookup therefore has three places it can stop, cheapest first. The
Bloom filter turns away a key that was never written, with no disk read at all,
and that is the common case for a table that does not hold the key. If the
filter says the key may be present, the index settles which single block could
contain it, or that none could because the key sorts below the first block, and
that decision is made in memory too. Only then is one block read, its checksum
verified, and its entries scanned, and that scan stops as soon as it reaches a
key above the one sought. So a miss usually costs no I/O, a hit costs one read,
and nothing costs two.

Writing a table is made crash-safe by never letting an incomplete one be
visible under its final name. The writer builds the file as a sibling with a
".tmp" suffix, fsyncs it, closes it, renames it into place, and then fsyncs the
containing directory. The rename is atomic, so the final name either does not
exist or refers to a table that is complete down to its footer; there is no
window in which a reader could open a half-written table, and no need for a
reader to tolerate one. The directory fsync is the step that is easy to leave
out and still appear to work: the rename is a change to the directory rather
than to the file, so until the directory itself is flushed a power loss can
leave the new name pointing at nothing, even though the file's own contents
were safely on disk. Fsyncing the file and then the directory is what makes the
table and the name it is known by durable together. A table with no entries is
a valid table, which matters because a memtable holding only writes that were
all superseded can legitimately flush to nothing.

Tombstones are written to the table like any other entry, and this is not an
optimisation that could be dropped. A table is one layer of a tree, and a
delete has to outlive the layer it was issued in: an older table lower down may
still hold a value for that key, and the only thing that can shadow it is a
record in a newer table saying the key is gone. Dropping tombstones on flush
would make a deleted key reappear as soon as the lookup reached the older
table. A tombstone can only be discarded by compaction, and only once it has
been merged past every table that could still hold an older value for its key,
which is phase 5's problem.

### Bloom filter

The filter is a bit array plus a hash count. It answers "definitely not
present" or "probably present", so it can rule a key out but never rule one in,
and a positive answer always has to be confirmed against the block.

Sizing is ten bits per key with seven hash functions, which gives a false
positive a little under one percent of the time. Seven is not a free choice: for
a given number of bits per key the optimal count is the bits per key times the
natural logarithm of two, which for ten bits rounds to seven. Ten bits is the
usual point of diminishing returns, since going to fifteen cuts the rate by
roughly a factor of five but costs half again as much memory for a filter that
has to stay resident. A one percent rate means about one lookup in a hundred for
an absent key pays for a block read it did not need, which is a good trade
against the ninety-nine that pay nothing.

Rather than compute seven independent hashes, the filter takes one 64-bit FNV-1a
hash of the key, splits it into two 32-bit halves, and derives the i'th bit as
(h1 + i*h2) modulo the bit count, in 64-bit arithmetic so the sum does not wrap
before the modulo. This is the Kirsch-Mitzenmacher construction: the false
positive rate is asymptotically the same as with seven real hashes, at the cost
of one. FNV is used rather than the standard library's maphash because a filter
is written to disk and read back by a later process, and maphash seeds itself
per process, which would make a persisted filter answer differently every run.
The one degenerate case is a key whose low half hashes to zero, which makes all
seven probes land on the same bit; at one in four billion keys this costs
nothing worth correcting for.

The marshalled form is the hash count as one byte, the bit count as a
little-endian uint32, and then the bit array. Parsing rejects anything whose
parts disagree: an input too short for the header, a hash count outside one to
sixteen, a zero bit count, or a bit count that does not match the number of
bytes that follow. Bounding the hash count matters for more than tidiness,
since it is read from disk and then used as a loop count.

### What these formats do not do

No compression. Blocks are stored as written, so a table is as large as its
keys and values. Block-level compression is the obvious next gain, and the
per-block framing is already the right shape for it.

No block cache. Every block read goes to the operating system, and a block
buffer is allocated per lookup. The page cache does the real caching, so
repeated reads of a hot block are not as expensive as they look, but a reader
holds nothing of its own between lookups, and a hit therefore allocates where
it need not.

No prefix compression in blocks or in the index. Adjacent keys in a sorted
table usually share a long prefix, and storing each key whole wastes that. The
index pays for this most, since it holds a full key per block.

No range scan. A reader can look one key up or walk the whole table from the
start, and nothing in between: there is no Seek to position an iterator at a
key. The index makes that straightforward to add, by locating the block and
then scanning forward within it, and the read path in phase 4 is where it
belongs.

No support for tables that outgrow a uint32 block length, which is a limit on
one entry rather than on the table, and no checksum over the data section as a
whole, so a block that is never read is never verified.

## Directory layout and the MANIFEST

A database is one directory:

    <dir>/LOCK          the process lock
    <dir>/MANIFEST      the authoritative list of live tables and logs
    <dir>/NNNNNN.wal    write-ahead logs
    <dir>/NNNNNN.sst    sorted tables

Every file draws its number from one counter, so a number is never reused for a
file of the other kind and the name alone identifies a file for the life of the
database. Six zero-padded digits make a directory listing sort the same way
lexically as numerically.

The manifest is what makes the directory mean something. The files on disk are
only candidates; the manifest says which of them are live, and anything it does
not account for is debris to be deleted. Its layout, little endian throughout,
is

    [magic uint64][version uint32][nextFileNum uint64][logNumber uint64]
    [count uint32][count x table number uint64, oldest to newest]
    [crc32c uint32 over everything above]

`nextFileNum` is the lowest number not yet handed out. `logNumber` is the first
log that still matters: logs numbered at or above it hold records that are not
yet in any table and must be replayed, and logs below it have been superseded
and are deleted. The table numbers are listed oldest to newest, which is the
order a read walks in reverse.

It is read as suspiciously as any other file. The length is checked, then the
checksum, because no field means anything if the bytes are damaged, then the
magic and version, then the table count, which must account for exactly the
bytes between the header and the checksum. Only after the count has been pinned
to the input's own length is memory allocated for it, so a corrupt count cannot
become a huge allocation. Then the numbers themselves: tables strictly
increasing, every number below `nextFileNum`, and a log number not in the
future. A manifest that fails any of these gives an error wrapping
`ErrCorrupt`, and since the manifest is the only thing that says what is live,
that failure stops the open rather than being worked around.

Writing it is the same temporary-file dance the tables use: write
`MANIFEST.tmp`, fsync it, rename it over `MANIFEST`, fsync the directory. The
rename is atomic, so a reader sees the old manifest or the new one and never
half of either, and the directory fsync is what makes the rename durable, since
a rename changes the directory rather than the file.

## Flush

A flush turns the memtable into a table. Its steps are ordered so that exactly
one of them commits, and everything before that point can be discarded without
consequence.

1. Snapshot the memtable with `Entries`. An empty snapshot means there is
   nothing to do, and an empty flush is a no-op rather than an empty table.
2. Write the table and open a reader on it. No manifest references the file
   yet, so as far as any future open is concerned it does not exist.
3. Create the log that writes will go to next. Also unreferenced, and still
   empty, because nothing is pointed at it until step 5.
4. Write a manifest naming the new table and the new log. **This is the commit
   point.** Before this rename the old state is whole and the two new files are
   orphans; after it the new state is whole and the old log is the orphan.
   There is no moment in between, because the rename is atomic.
5. Only now change anything in memory: close and delete the old log, install
   the new one, start a fresh memtable, and add the reader to the live list.
6. Count the flush.

A failure anywhere before step 4 leaves the in-memory state exactly as it was
and removes the files this flush created, as a courtesy rather than a
requirement: the next open would delete them anyway for not being in the
manifest. A failure to delete the old log in step 5 is ignored outright,
because the manifest has already moved past it and the next open will sweep it
up. That asymmetry is the point of having a single commit point: before it,
nothing needs to be undone; after it, nothing is allowed to fail.

## Recovery

Opening a database runs the following, in an order forced by what each step
needs from the one before it.

1. Create the directory if needed, then take the process lock. Nothing else is
   touched until the lock is held, because everything after this assumes no
   other process is doing the same thing.
2. Read the manifest. A directory with no manifest but with tables or logs in
   it is not a new database, it is one whose manifest was lost, and there is no
   way to know which tables were live or which logs still mattered; starting
   empty would silently discard all of it, so that is `ErrCorrupt` instead. A
   genuinely empty directory gets an initial manifest written before anything
   else happens.
3. Delete what the manifest does not account for: temporary files from an
   interrupted write, tables no manifest lists, and logs below the log number.
   Each is crash debris, and each is safe to delete precisely because the
   manifest does not point at it.
4. Open a reader for every table the manifest lists. A missing or unreadable
   one fails the open, since the manifest says it holds live data.
5. Replay every live log, oldest first, into a fresh memtable, so that later
   records overwrite earlier ones exactly as they did before the crash.
6. Set the next file number above both what the manifest promised and anything
   on disk, so a new file can never be given a name that already exists.
7. If the replayed memtable has anything in it, flush it. Otherwise create a
   fresh log and commit a manifest naming it.

Step 7 is the one worth dwelling on. A replayed memtable is already durable, in
the logs it was replayed from, so keeping it in memory and carrying on would be
correct. The reason to flush it instead is that it would otherwise be replayed
again on the next open, and the open after that, with the log growing each time
a process died before filling it; a database that crashes repeatedly would
spend ever longer starting up. Flushing turns those records into a table once,
which lets the logs be deleted, so every open starts from exactly one live and
empty log no matter what happened before it. That invariant is also what makes
the crash cases tractable to reason about: there is only ever one log being
appended to, and the only way to have several is a crash partway through a
flush, which the next open resolves.

Close is deliberately not a flush. The memtable's contents are in the log, and
recovery replays the log, so flushing on close would only do early the work
that the next open does anyway. Close syncs and closes the log, closes the
readers, and releases the lock.

## The directory lock

`LOCK` is held with `flock` for as long as the database is open, and a second
open of the same directory returns `ErrLocked`. The lock exists because
recovery rewrites the directory: two processes recovering at once would both
decide which files are debris, both delete them, and both write a manifest over
the other's.

`flock` is attached to the open file descriptor rather than to the path, which
gives the property that matters here: it is released by the kernel when the
process goes away, however it goes away. There is no stale lock to break after
a crash, which is exactly what a lock guarding crash recovery needs. The `LOCK`
file itself is never deleted, because removing it would let another process
create and lock a fresh one while this process still believed it held the
directory.

The lock is unix-only. On other platforms acquiring it succeeds
unconditionally and nothing prevents two processes from opening the same
database at once, which would let both recover and both write; a caller there
has to arrange exclusion itself.

## Compaction

### Why it is needed

Phase 4 left the engine correct and unbounded. Tables accumulated one per
flush, nothing ever merged them, and three costs grew without limit.

Reads got slower in proportion to the number of flushes rather than the amount
of data. A lookup that finds nothing has to rule the key out in every table,
and the phase 4 benchmark measured that directly: 44.63 Bloom checks per miss
across 45 tables. The filters kept those checks off the disk, which is why the
engine stayed usable at all, but the work still scaled with table count rather
than data size.

Space grew with every overwrite. A key written fifty times occupied fifty
entries on disk, forty-nine of them unreachable, because nothing ever removed
the older copy.

And deleting data only ever made the database bigger. A tombstone cannot be
discarded while any older table might hold the value it shadows, so under
phase 4's rules, where no table was ever rewritten, a tombstone was immortal.

Compaction is the one mechanism that addresses all three: merging a run of
tables into one keeps the newest version of each key, discards the rest, and
where the run reaches far enough back, discards tombstones too.

### The three correctness rules

Everything about the merge follows from the fact that **the manifest's order,
oldest to newest, is the only thing that says which version of a key wins**.
Not the file number, which a compaction's output takes fresh while sitting in
the middle of the list. Not the file's timestamp. Just the position.

The first rule follows immediately: **a compaction's inputs must be a
contiguous run of that list, and its output takes the run's place.** If the
inputs were scattered, the output would have to occupy several positions at
once, and there would be no position at which it held the right relationship to
the tables left between them. Contiguity is what makes one output able to stand
for all its inputs.

The second is what the merge does: **on equal keys the newest source wins and
the older versions are discarded.** That is the same rule the read path applies
when it consults tables newest first, which is why compacting cannot change any
answer: it computes in advance what a read would have concluded anyway.

The third is the one that is easy to get wrong. **A tombstone may be discarded
only if the run includes the oldest table, list index 0.** A tombstone is not
data, it is the absence of data made explicit, and its whole function is to
shadow an older value. Merge tables 1 to 4 and throw away a tombstone found
there, and table 0 is still sitting underneath with the value that tombstone
was hiding; the next lookup walks past the merged output, reaches table 0, and
returns a value the caller deleted. The deleted key comes back from the dead.
When the run starts at index 0 there is nothing older left to shadow, and only
then is the tombstone genuinely redundant. This is why `Compact`, which merges
everything, is the only operation guaranteed to reclaim deleted space, and why
the automatic policy often cannot.

### The size-tiered policy

Tables are sorted into size classes and a class holding enough of them has
them merged. The class of a table is

    bucket(size) = 0                               if size < baseSize
                   floor(log4(size / baseSize)) + 1 otherwise

computed in integer arithmetic, so the classes come out the same on every
platform and no float rounding can put one table in two classes on two runs.
The ratio is four because the trigger is four: merging four tables of one class
yields one table of roughly four times the size, which lands in the next class
up and so cannot immediately provoke another merge of the same data. A ratio
and a trigger that disagreed would either leave classes that never fill or
merge the same bytes repeatedly.

The policy scans classes from smallest to largest and takes the first that
holds a long enough run. Smallest first is deliberate: merging four small
tables costs little and removes four tables from every lookup, while merging
four large ones rewrites far more bytes to remove the same four. Cheap work
first keeps the read amplification down for the least write amplification.

Within a class it takes the oldest maximal run of tables that are adjacent *in
the manifest*, because of the contiguity rule above. A table of a different
class sitting in the middle therefore breaks a run rather than being merged
through, even though merging through it would be tempting. At most
`maxCompactionInputs`, ten, tables are taken from the run's oldest end, so that
one compaction holds the write lock for a bounded time rather than an
unpredictable one.

The policy is a pure function of the table sizes, the trigger and the base
size, which is what lets it be tested exhaustively without a database behind
it.

### The merge

`internal/compaction` does the merge and knows nothing about files. It takes a
list of sources ordered oldest to newest and a flag saying whether tombstones
may be dropped, and calls an emit function once per surviving key.

It is a k-way merge over a heap ordered by key ascending and, for equal keys,
by age with the newest first, so the top of the heap is always the entry that
wins. Each step takes that entry, hands it to emit, then advances the winner
and every other source sitting on the same key, since those are exactly the
versions it shadowed.

Two details are forced by the sources' memory contract, which is that the
slices an iterator returns expire on its next `Next`. Emit has to run before
the winning source is advanced, because the bytes it is given belong to that
source. And the key being settled has to be copied into a buffer the merge
owns before anything is advanced, because it is still needed to recognise the
shadowed versions after the advance that invalidates it. Both are the kind of
mistake that works in testing and corrupts data under a different block
boundary, so the test sources deliberately overwrite their buffers on every
call.

### The commit protocol

A compaction changes the manifest exactly once, and that write is the only
moment at which anything becomes true.

1. Merge the inputs into a new table. Nothing references it, so as far as any
   future open is concerned it does not exist. If everything was shadowed or
   dropped, there is no output table at all and the run simply disappears from
   the list.
2. Write a manifest with the inputs replaced by the output at the inputs'
   position. **This atomic rename is the commit point.**
3. Close the input readers, delete their files, and swap the output into the
   live list.

Crash analysis, one point at a time:

*Before the output is finished.* Nothing exists but a partial `.sst.tmp`, which
`sstable.Writer` never renames into place. The next open deletes temporary
files, and the manifest still names every input. No data moved.

*After the output is written, before the manifest.* The output file exists under
its final name but no manifest mentions it. The next open deletes tables the
manifest does not list, so the output is removed and the inputs are still live.
The compaction simply did not happen. The test for this crash point recovers a
six-table database, exactly as it was.

*After the manifest, before the inputs are deleted.* The manifest names the
output and not the inputs, so the output is live and the input files are
orphans. The next open deletes tables the manifest does not list, which is now
the inputs. The compaction fully happened. The test for this crash point
recovers a three-table database: six tables, less four inputs, plus one output.

The asymmetry is the point. Before the rename nothing needs to be undone,
because nothing is referenced; after it, nothing is allowed to matter, which is
why deleting an input is best effort and its failure is ignored.

An automatic compaction that fails is recorded rather than returned. The write
that triggered the flush that triggered the compaction is already durable and
visible, so failing it would invite a retry that would duplicate it. The
failure goes to `Stats().CompactionErrors` and `LastCompactionError`, the loop
stops, and the next flush tries again; the database meanwhile works correctly
with more tables than it would like. `Compact`, which a caller invoked on
purpose, reports its errors normally.

### Manifest version 2

Version 2 differs from version 1 in one rule, not in its bytes: table numbers
no longer have to increase along the list. They only have to be distinct. A
compaction's output carries a fresh, high number while taking the position of
its inputs, so a table late in the list can easily have a lower number than one
before it, and the old rule would have rejected every compacted database. Every
other validation is unchanged. A version 1 file is rejected as corrupt rather
than migrated, which costs nothing because no data was ever deployed under it.

### What it measures

Loading 200,000 entries of 16-byte keys and 100-byte values, with compaction
and without:

| | tables | write amplification | load time |
|---|---|---|---|
| compaction on | 4 | 2.74 | 1.97 s |
| compaction off | 22 | 1.04 | 1.36 s |

And the lookups that result, per operation:

| | miss | Bloom checks per miss | hit | Bloom checks per hit |
|---|---|---|---|---|
| compaction on | 240 ns | 4.97 | 2525 ns | 3.54 |
| compaction off | 1131 ns | 22.82 | 3114 ns | 11.34 |

So compaction costs about 45% more time on the way in and 2.6 times the bytes
written, and buys a miss that is 4.6 times faster. The Bloom-check column is
the read amplification itself, and it tracks the table count exactly: that is
the quantity phase 4 measured at 44.63 across 45 tables and this phase holds
near 5. Block reads per lookup stay near one either way, because the index was
already doing its job; what compaction removes is the per-table work that no
index can avoid.

A separate test writes fifty generations over the same thousand keys with
compaction off, reaching 5,070,802 bytes across 293 tables, and one `Compact`
reduces that to 100,943 bytes in one table, a factor of fifty, which is the
space amplification being reclaimed rather than the read amplification.

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

## Known limitations at the end of Phase 5

The engine works and does not lose acknowledged data, but it is not finished,
and the gaps are structural rather than cosmetic.

Flushes are synchronous and hold the write lock. A writer that trips the
memtable limit writes the whole table and commits the manifest before it
returns, and every other writer waits behind it. Write latency is therefore
smooth until it is not: most writes cost a log append, and one in however many
costs a table write and three fsyncs. Moving the flush to a background
goroutine, with a second memtable taking writes while the first is written out,
is the fix, and it is the main reason the mutex is a single one rather than
something finer.


There are no range scans, iterators or snapshots in the public API. A caller
can look one key up and nothing else. The sorted tables and the memtable both
support ordered traversal already, so the missing piece is a merging iterator
over all the sources with the same newest-wins and tombstone rules the point
lookup uses; that is also what compaction will need internally.

A log corrupted in the middle loses everything after the corruption. This is
inherited from the log format described under the write path: the format has no
resynchronisation marker, so a replay that loses its place cannot find the next
record, and recovery truncates from there. For a database it means a damaged
log costs not just the damaged record but every acknowledged write that
followed it.

The process lock is unix-only, as described above.

There is no block cache. Every block a lookup reads is read from the operating
system and allocated fresh, so a hot block is re-read and re-allocated on every
Get that touches it. The page cache absorbs the I/O, but not the allocation.

Compaction runs synchronously under the write lock, like a flush and for the
same reason, so it stalls every writer and every reader for as long as the
merge takes. A merge of ten tables is far more work than a flush, which makes
this the more painful of the two stalls. Moving compaction to a background
goroutine is the fix, and it is a larger change than moving the flush, because
a background compaction has to coordinate with the flushes and lookups running
alongside it over a table list that is no longer frozen.

The strategy is size-tiered only, with no leveled organisation and no key-range
partitioning. Every compaction therefore rewrites whole tables: there is no way
to merge the part of a table covering one range of keys and leave the rest, so
the amount of work a compaction does is set by its inputs' total size rather
than by how much of them actually overlaps. Leveled compaction, where each
level holds tables with disjoint key ranges, is what bounds that, at the cost
of more write amplification for small updates.

There is no rate limiting. A compaction proceeds as fast as the disk allows and
competes with everything else for it. A real engine budgets compaction I/O so
that a burst of merging does not starve foreground work.

The oldest table is rarely rewritten, so tombstones can live a long time. The
policy picks runs by size class, and the oldest table is usually the largest,
which puts it in a class of its own that rarely fills. Only a run reaching list
index 0 may drop tombstones, so in practice automatic compaction reclaims
overwritten values readily and deleted ones hardly at all. `Compact` is the
remedy and it rewrites everything. A policy that tracked, per table, the oldest
table whose key ranges overlap it could drop tombstones far more often.

Readers do not run during a compaction. They share a read lock that the
compaction holds exclusively, so a lookup arriving mid-merge waits. Since the
tables a compaction reads are immutable and the manifest swap is atomic, there
is no fundamental reason for that wait; it is a consequence of the single
mutex, and it goes away with background compaction.

## Benchmarks

To be written in phase 6
