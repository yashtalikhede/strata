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

To be written in phase 2

## Read path

To be written in phase 4

## File formats

To be written in phase 3

## Compaction

To be written in phase 5

## Crash-safety guarantees

To be written in phase 2

## Benchmarks

To be written in phase 6
