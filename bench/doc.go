// Package bench is a deterministic benchmark harness for the strata engine.
//
// It defines a handful of named workloads, runs them against a real database,
// and reports throughput, latency percentiles and the engine's own counters.
// Everything is driven from one seed, so two runs of the same configuration
// touch the same keys in the same order and their numbers can be compared.
package bench
