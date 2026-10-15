# strata benchmark

- date: 2026-10-15T05:09:41Z
- go: go1.27.2
- platform: darwin/arm64
- cpu: Apple M1 (8 cores)
- entries: 500000
- value size: 100 bytes
- threads: 1
- sync: none
- memtable: 4194304 bytes
- compaction: on
- seed: 1

| workload | ops | ops/s | MB/s | p50 | p99 | p99.9 | write-amp | tables | blockreads/op | bloomrejects/op |
|---|---|---|---|---|---|---|---|---|---|---|
| fillseq | 500000 | 176620 | 19.5 | 2.709µs | 13.75µs | 24.541µs | 1.89 | 4 | n/a | n/a |
| fillrandom | 500000 | 163603 | 18.1 | 3.083µs | 15.292µs | 28.75µs | 1.89 | 4 | n/a | n/a |
| overwrite | 500000 | 109448 | 12.1 | 3.125µs | 17.917µs | 52.125µs | 2.68 | 3 | n/a | n/a |
| readhit | 500000 | 291708 | 32.3 | 3.209µs | 7.542µs | 28.833µs | 1.89 | 4 | 0.954 | 1.721 |
| readmiss | 500000 | 1115468 | 17.0 | 750ns | 3.375µs | 5.291µs | 1.89 | 4 | 0.033 | 3.967 |
| mixed | 500000 | 143275 | 15.8 | 3.291µs | 13.333µs | 42.125µs | 2.81 | 2 | n/a | n/a |
| deleterandom | 250000 | 253702 | 3.9 | 3.208µs | 9.25µs | 19.458µs | 1.84 | 5 | 0.000 | 0.000 |
