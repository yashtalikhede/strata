package bench

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// testN keeps the whole suite to a second or two while still producing
// several tables and at least one compaction.
const testN = 3_000

func testConfig() Config {
	return Config{
		N:            testN,
		ValueSize:    64,
		MemtableSize: 16 << 10,
		Seed:         7,
	}
}

func TestKeysAreStable(t *testing.T) {
	// Golden values: the key layout is part of what makes two runs
	// comparable, so changing it should be a deliberate act.
	cases := []struct {
		i            int
		key, missing string
	}{
		{0, "0000000000000000", "0000000000000001"},
		{1, "0000000000000002", "0000000000000003"},
		{42, "0000000000000084", "0000000000000085"},
		{999_999, "0000000001999998", "0000000001999999"},
	}
	for _, tc := range cases {
		if got := string(Key(tc.i)); got != tc.key {
			t.Errorf("Key(%d) = %q, want %q", tc.i, got, tc.key)
		}
		if got := string(MissingKey(tc.i)); got != tc.missing {
			t.Errorf("MissingKey(%d) = %q, want %q", tc.i, got, tc.missing)
		}
		if len(Key(tc.i)) != KeyLen || len(MissingKey(tc.i)) != KeyLen {
			t.Errorf("keys for %d are not %d bytes", tc.i, KeyLen)
		}
	}
}

// TestMissingKeysInterleave is the property the read-miss workload depends
// on: an absent key must fall between two present ones, not past the end of
// the key space, or the table index turns it away before the Bloom filter is
// ever asked.
func TestMissingKeysInterleave(t *testing.T) {
	present := make(map[string]bool, 1000)
	for i := range 1000 {
		present[string(Key(i))] = true
	}
	for i := range 999 {
		missing := string(MissingKey(i))
		if present[missing] {
			t.Fatalf("MissingKey(%d) = %q is also a present key", i, missing)
		}
		if missing <= string(Key(i)) {
			t.Fatalf("MissingKey(%d) = %q does not sort above Key(%d) = %q", i, missing, i, Key(i))
		}
		if missing >= string(Key(i+1)) {
			t.Fatalf("MissingKey(%d) = %q does not sort below Key(%d) = %q", i, missing, i+1, Key(i+1))
		}
	}
}

func TestPermutationIsSeeded(t *testing.T) {
	a := Permutation(1000, 5)
	b := Permutation(1000, 5)
	if !slices.Equal(a, b) {
		t.Fatal("the same seed gave two different permutations")
	}
	if c := Permutation(1000, 6); slices.Equal(a, c) {
		t.Fatal("two different seeds gave the same permutation")
	}

	// It must be a permutation, not merely a sequence of numbers in range.
	sorted := slices.Clone(a)
	slices.Sort(sorted)
	for i, v := range sorted {
		if v != i {
			t.Fatalf("permutation is missing %d", i)
		}
	}
}

func TestPercentile(t *testing.T) {
	cases := []struct {
		name    string
		samples []int64
		p       float64
		want    time.Duration
	}{
		{"empty", nil, 50, 0},
		{"one element p50", []int64{7}, 50, 7},
		{"one element p0", []int64{7}, 0, 7},
		{"one element p100", []int64{7}, 100, 7},
		{"one element p99.9", []int64{7}, 99.9, 7},
		{"two elements p50", []int64{10, 20}, 50, 10},
		{"two elements p99", []int64{10, 20}, 99, 20},
		{"two elements p100", []int64{10, 20}, 100, 20},
		{"two elements unsorted", []int64{20, 10}, 50, 10},
		{"unsorted input", []int64{50, 10, 40, 20, 30}, 50, 30},
		{"unsorted max", []int64{50, 10, 40, 20, 30}, 100, 50},
		{"unsorted min", []int64{50, 10, 40, 20, 30}, 0, 10},
		{"ten elements p90", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 90, 9},
		{"ten elements p99", []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 99, 10},
		{"out of range high", []int64{1, 2, 3}, 150, 3},
		{"out of range low", []int64{1, 2, 3}, -5, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Percentile(tc.samples, tc.p); got != tc.want {
				t.Errorf("Percentile(%v, %v) = %v, want %v", tc.samples, tc.p, got, tc.want)
			}
		})
	}

	// The caller's slice must come back untouched.
	samples := []int64{3, 1, 2}
	Percentile(samples, 50)
	if !slices.Equal(samples, []int64{3, 1, 2}) {
		t.Errorf("Percentile reordered its argument: %v", samples)
	}

	// Percentiles must not decrease as p rises.
	ladder := []int64{9, 3, 7, 1, 5}
	var previous time.Duration
	for _, p := range []float64{0, 50, 90, 99, 99.9, 100} {
		got := Percentile(ladder, p)
		if got < previous {
			t.Errorf("p%v = %v is below the previous percentile %v", p, got, previous)
		}
		previous = got
	}
}

func TestSelectWorkloads(t *testing.T) {
	all, err := selectWorkloads(nil)
	if err != nil {
		t.Fatalf("selectWorkloads(nil): %v", err)
	}
	if !slices.Equal(all, All) {
		t.Errorf("selectWorkloads(nil) = %v, want every workload", all)
	}

	// Whatever order they are asked for, they come back canonical, because
	// the read workloads have to run before Mixed writes to their database.
	got, err := selectWorkloads([]string{Mixed, ReadHit, FillSeq})
	if err != nil {
		t.Fatalf("selectWorkloads: %v", err)
	}
	if want := []string{FillSeq, ReadHit, Mixed}; !slices.Equal(got, want) {
		t.Errorf("selectWorkloads = %v, want %v", got, want)
	}

	if _, err := selectWorkloads([]string{"nonsense"}); err == nil {
		t.Error("selectWorkloads accepted an unknown workload")
	}
}

func TestConfigDefaults(t *testing.T) {
	c := Config{}.withDefaults()
	if c.N != DefaultN || c.ValueSize != DefaultValueSize || c.Threads != DefaultThreads {
		t.Errorf("defaults = %+v", c)
	}
	if c.Seed != DefaultSeed || c.MemtableSize != DefaultMemtableSize {
		t.Errorf("defaults = %+v", c)
	}
	// Benchmarks default to the fast policy, unlike the engine.
	if c.Sync != SyncNone {
		t.Errorf("default sync = %q, want %q", c.Sync, SyncNone)
	}
	if _, err := (Config{Sync: "sometimes"}).options(); err == nil {
		t.Error("an unknown sync mode was accepted")
	}
}

func TestRunEveryWorkload(t *testing.T) {
	cfg := testConfig()
	cfg.Dir = t.TempDir()

	report, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.Results) != len(All) {
		t.Fatalf("got %d results, want %d", len(report.Results), len(All))
	}

	byName := make(map[string]Result, len(report.Results))
	for i, result := range report.Results {
		if result.Name != All[i] {
			t.Errorf("result %d is %q, want %q: the canonical order was not kept", i, result.Name, All[i])
		}
		byName[result.Name] = result

		if result.Ops <= 0 {
			t.Errorf("%s: Ops = %d", result.Name, result.Ops)
		}
		if result.Duration <= 0 {
			t.Errorf("%s: Duration = %v", result.Name, result.Duration)
		}
		if result.OpsPerSec <= 0 {
			t.Errorf("%s: OpsPerSec = %v", result.Name, result.OpsPerSec)
		}
		// Percentiles must be ordered and no larger than the maximum.
		if !(result.P50 <= result.P90 && result.P90 <= result.P99 && result.P99 <= result.P999 && result.P999 <= result.Max) {
			t.Errorf("%s: percentiles out of order: p50=%v p90=%v p99=%v p99.9=%v max=%v",
				result.Name, result.P50, result.P90, result.P99, result.P999, result.Max)
		}
	}

	// Each workload's own promise.
	for _, name := range []string{FillSeq, FillRandom, Overwrite, ReadHit, ReadMiss, Mixed} {
		if got := byName[name].Ops; got != testN {
			t.Errorf("%s: Ops = %d, want %d", name, got, testN)
		}
	}
	if got := byName[ReadHit]; got.NotFound != 0 || got.Found != testN {
		t.Errorf("readhit: found %d, missed %d, want all %d found", got.Found, got.NotFound, testN)
	}
	if got := byName[ReadMiss]; got.Found != 0 || got.NotFound != testN {
		t.Errorf("readmiss: found %d, missed %d, want all %d missed", got.Found, got.NotFound, testN)
	}
	if got := byName[Mixed]; got.Found == 0 {
		t.Error("mixed: found nothing, want half its operations to be hits")
	}
	if got := byName[DeleteRandom]; got.Ops != testN/2 {
		t.Errorf("deleterandom: Ops = %d, want %d", got.Ops, testN/2)
	}
	if !byName[DeleteRandom].Verified {
		t.Error("deleterandom: Verified is false, want the harness to have checked its own effect")
	}
	for _, name := range []string{FillSeq, FillRandom, Overwrite, ReadHit} {
		if byName[name].Tables == 0 {
			t.Errorf("%s: no tables were produced; the memtable is too large for this test", name)
		}
	}
	// The Bloom filters should answer most misses, which is the whole point
	// of interleaving the absent keys with the present ones.
	if got := byName[ReadMiss].BloomRejectsPerOp; got <= 0 {
		t.Errorf("readmiss: bloomrejects/op = %v, want the filters to be exercised", got)
	}
	// A pure read workload consults a fixed set of tables, so its counters
	// are always comparable.
	for _, name := range []string{ReadHit, ReadMiss} {
		if !byName[name].CountersReliable {
			t.Errorf("%s: CountersReliable is false, but a read workload compacts nothing", name)
		}
	}
}

// TestCounterDeltasNeverUnderflow guards a real failure: the engine sums the
// reader counters over its live tables, so a compaction during a workload
// retires readers and the total falls. Subtracting two unsigned snapshots then
// wraps to something near 2^64, which once reached a published report as
// 36893488147418.242 block reads per operation.
func TestCounterDeltasNeverUnderflow(t *testing.T) {
	if got := countedSince(10, 4); got != 0 {
		t.Errorf("countedSince(10, 4) = %d, want 0", got)
	}
	if got := countedSince(4, 10); got != 6 {
		t.Errorf("countedSince(4, 10) = %d, want 6", got)
	}
	if got := countedSince(0, 0); got != 0 {
		t.Errorf("countedSince(0, 0) = %d, want 0", got)
	}

	// End to end, with a memtable small enough that the writing workloads
	// really do compact underneath themselves.
	cfg := testConfig()
	cfg.Dir = t.TempDir()
	cfg.MemtableSize = 4 << 10
	cfg.CompactionBaseSize = 16 << 10
	report, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var sawCompaction bool
	for _, result := range report.Results {
		if result.Compactions > 0 {
			sawCompaction = true
		}
		// No per-operation figure can exceed the operations themselves by a
		// wide margin; anything in the billions is an underflow.
		const absurd = 1e6
		if result.BlockReadsPerOp > absurd || result.BloomRejectsPerOp > absurd {
			t.Errorf("%s: blockreads/op = %v, bloomrejects/op = %v; one of them underflowed",
				result.Name, result.BlockReadsPerOp, result.BloomRejectsPerOp)
		}
		if result.BlockReadsPerOp < 0 || result.BloomRejectsPerOp < 0 {
			t.Errorf("%s: a negative per-operation count", result.Name)
		}
	}
	if !sawCompaction {
		t.Fatal("no workload compacted; this test is not exercising what it claims")
	}

	// And the report must say "n/a" rather than print a number it cannot
	// stand behind.
	md := report.Markdown()
	for _, result := range report.Results {
		if result.CountersReliable {
			continue
		}
		row := rowFor(md, result.Name)
		if !strings.Contains(row, "n/a") {
			t.Errorf("%s: counters are unreliable but the row shows numbers: %s", result.Name, row)
		}
	}
}

// rowFor returns the table row for a workload, or "" if there is none.
func rowFor(markdown, name string) string {
	for _, line := range strings.Split(markdown, "\n") {
		if strings.HasPrefix(line, "| "+name+" |") {
			return line
		}
	}
	return ""
}

func TestRunSelectedWorkloads(t *testing.T) {
	cfg := testConfig()
	cfg.Dir = t.TempDir()

	report, err := Run(cfg, []string{FillSeq})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.Results) != 1 || report.Results[0].Name != FillSeq {
		t.Fatalf("results = %+v, want only fillseq", report.Results)
	}

	if _, err := Run(cfg, []string{"nope"}); err == nil {
		t.Error("Run accepted an unknown workload")
	}
}

func TestRunIsThreaded(t *testing.T) {
	cfg := testConfig()
	cfg.Dir = t.TempDir()
	cfg.Threads = 4

	report, err := Run(cfg, []string{FillRandom, ReadHit, ReadMiss, Mixed})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	for _, result := range report.Results {
		if result.Ops != testN {
			t.Errorf("%s: Ops = %d, want %d; the work was not split evenly", result.Name, result.Ops, testN)
		}
	}
}

func TestShareOf(t *testing.T) {
	// Every operation must be covered exactly once, however the work
	// divides.
	for _, ops := range []int{0, 1, 7, 100, 1001} {
		for _, threads := range []int{1, 2, 3, 8} {
			covered := make([]int, ops)
			previousHi := 0
			for t2 := range threads {
				lo, hi := shareOf(ops, threads, t2)
				if lo != previousHi {
					t.Fatalf("ops=%d threads=%d: thread %d starts at %d, want %d", ops, threads, t2, lo, previousHi)
				}
				for i := lo; i < hi; i++ {
					covered[i]++
				}
				previousHi = hi
			}
			if previousHi != ops {
				t.Fatalf("ops=%d threads=%d: ranges end at %d", ops, threads, previousHi)
			}
			for i, n := range covered {
				if n != 1 {
					t.Fatalf("ops=%d threads=%d: operation %d covered %d times", ops, threads, i, n)
				}
			}
		}
	}
}

func TestReportMarkdown(t *testing.T) {
	cfg := testConfig()
	cfg.Dir = t.TempDir()
	report, err := Run(cfg, nil)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	md := report.Markdown()
	for _, name := range All {
		if !strings.Contains(md, "| "+name+" |") {
			t.Errorf("the table has no row for %q", name)
		}
	}
	for _, heading := range []string{"# strata benchmark", "- go:", "- cpu:", "- sync:"} {
		if !strings.Contains(md, heading) {
			t.Errorf("the report is missing %q", heading)
		}
	}

	// Every table line must have the same number of cells, header and
	// separator included, or it is not a table.
	var rows int
	for _, line := range strings.Split(md, "\n") {
		if !strings.HasPrefix(line, "|") {
			continue
		}
		rows++
		if got := strings.Count(line, "|"); got != len(markdownColumns)+1 {
			t.Errorf("line %q has %d separators, want %d", line, got, len(markdownColumns)+1)
		}
	}
	if want := len(All) + 2; rows != want {
		t.Errorf("the table has %d lines, want %d (a header, a separator and %d workloads)", rows, want, len(All))
	}
}

func TestReportJSON(t *testing.T) {
	cfg := testConfig()
	cfg.Dir = t.TempDir()
	report, err := Run(cfg, []string{FillSeq, ReadMiss})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	raw, err := report.JSON()
	if err != nil {
		t.Fatalf("JSON: %v", err)
	}
	var round Report
	if err := json.Unmarshal(raw, &round); err != nil {
		t.Fatalf("the report does not round trip: %v", err)
	}
	if len(round.Results) != 2 || round.Results[0].Name != FillSeq {
		t.Errorf("decoded results = %+v", round.Results)
	}
	if round.Config.N != testN {
		t.Errorf("decoded N = %d, want %d", round.Config.N, testN)
	}
	if round.Environment.GoVersion == "" || round.Environment.CPU == "" {
		t.Errorf("the environment did not survive the round trip: %+v", round.Environment)
	}
}

func TestEnvironment(t *testing.T) {
	env := environment()
	if env.GoVersion == "" || env.GOOS == "" || env.GOARCH == "" {
		t.Errorf("environment = %+v", env)
	}
	if env.NumCPU < 1 {
		t.Errorf("NumCPU = %d", env.NumCPU)
	}
	if env.CPU == "" {
		t.Error(`CPU is empty, want a model name or "unknown"`)
	}
	if _, err := time.Parse(time.RFC3339, env.Date); err != nil {
		t.Errorf("Date %q is not RFC3339: %v", env.Date, err)
	}
	t.Logf("%s/%s, %s, %d cores, %s", env.GOOS, env.GOARCH, env.CPU, env.NumCPU, env.GoVersion)
}

func TestValuePool(t *testing.T) {
	const size = 32
	p := newValuePool(size, 8, 3)
	for i := range 16 {
		if got := len(p.at(i)); got != size {
			t.Fatalf("value %d is %d bytes, want %d", i, got, size)
		}
	}
	// Reused round-robin, and seeded.
	if fmt.Sprintf("%x", p.at(0)) != fmt.Sprintf("%x", p.at(8)) {
		t.Error("the pool did not wrap round")
	}
	again := newValuePool(size, 8, 3)
	if fmt.Sprintf("%x", p.at(0)) != fmt.Sprintf("%x", again.at(0)) {
		t.Error("the same seed produced different values")
	}
	if other := newValuePool(size, 8, 4); fmt.Sprintf("%x", p.at(0)) == fmt.Sprintf("%x", other.at(0)) {
		t.Error("two seeds produced the same values")
	}
}
