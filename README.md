# strata

[![CI](https://github.com/yashtalikhede/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/yashtalikhede/strata/actions/workflows/ci.yml)

An LSM-tree key-value storage engine written in Go from scratch.

**Status:** All six phases are implemented: the engine is complete, with a public Go package, a command-line tool and a benchmark harness.

## Roadmap

- [x] 1. Memtable (skip list) with Put/Get/Delete
- [x] 2. Write-ahead log with crash recovery
- [x] 3. SSTable format with index and bloom filter
- [x] 4. Flush path and merged read path
- [x] 5. Compaction with tombstone handling
- [x] 6. Benchmarks and CLI
