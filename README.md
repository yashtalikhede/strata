# strata

[![CI](https://github.com/yashtalikhede/strata/actions/workflows/ci.yml/badge.svg)](https://github.com/yashtalikhede/strata/actions/workflows/ci.yml)

An LSM-tree key-value storage engine written in Go from scratch.

**Status:** In development. Nothing in the roadmap below is implemented yet.

## Roadmap

- [ ] 1. Memtable (skip list) with Put/Get/Delete
- [ ] 2. Write-ahead log with crash recovery
- [ ] 3. SSTable format with index and bloom filter
- [ ] 4. Flush path and merged read path
- [ ] 5. Compaction with tombstone handling
- [ ] 6. Benchmarks and CLI
