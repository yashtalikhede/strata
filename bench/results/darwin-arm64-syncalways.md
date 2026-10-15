# strata benchmark

- date: 2026-10-15T05:10:30Z
- go: go1.27.2
- platform: darwin/arm64
- cpu: Apple M1 (8 cores)
- entries: 3000
- value size: 100 bytes
- threads: 1
- sync: always
- memtable: 4194304 bytes
- compaction: on
- seed: 1

| workload | ops | ops/s | MB/s | p50 | p99 | p99.9 | write-amp | tables | blockreads/op | bloomrejects/op |
|---|---|---|---|---|---|---|---|---|---|---|
| fillrandom | 3000 | 263 | 0.0 | 3.968709ms | 5.92725ms | 11.139584ms | 0.00 | 0 | 0.000 | 0.000 |
| overwrite | 3000 | 227 | 0.0 | 4.098167ms | 8.950417ms | 11.130833ms | 0.00 | 0 | 0.000 | 0.000 |
