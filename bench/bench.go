package bench

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/yashtalikhede/strata"
)

// Workload names. Run executes whichever are asked for in the order they are
// listed in All.
const (
	// FillSeq inserts every key in ascending key order into a fresh
	// database, which is the easiest thing an LSM tree is ever asked to do.
	FillSeq = "fillseq"
	// FillRandom inserts every key in random order into a fresh database.
	FillRandom = "fillrandom"
	// Overwrite rewrites every key of an already full database, which is
	// what produces the shadowed versions compaction exists to reclaim.
	Overwrite = "overwrite"
	// ReadHit reads keys that are present, in random order.
	ReadHit = "readhit"
	// ReadMiss reads keys that are absent, which is the case the Bloom
	// filters exist for.
	ReadMiss = "readmiss"
	// Mixed alternates reads of present keys with overwrites.
	Mixed = "mixed"
	// DeleteRandom deletes half the keys and then checks that exactly those
	// are gone.
	DeleteRandom = "deleterandom"
)

// All lists every workload, in the order Run executes them.
var All = []string{FillSeq, FillRandom, Overwrite, ReadHit, ReadMiss, Mixed, DeleteRandom}

// KeyLen is the length in bytes of every key the harness generates.
const KeyLen = 16

// valuePoolSize is how many distinct values are generated and then reused
// round-robin. Generating one per key would cost more memory than the database
// under test, and reusing a pool does not make the engine's job easier, since
// it never looks at a value's contents.
const valuePoolSize = 1024

// SyncMode selects the write-ahead log's durability policy by name, so that a
// configuration can come from a command-line flag unchanged.
type SyncMode string

// The sync modes. The zero value is the empty string, which means SyncNone:
// benchmarks default to the fast policy, unlike the engine itself, because a
// run dominated by fsync measures the disk rather than the engine.
const (
	SyncNone   SyncMode = "none"
	SyncAlways SyncMode = "always"
)

// policy converts a mode to the engine's own type.
func (m SyncMode) policy() (strata.SyncPolicy, error) {
	switch m {
	case "", SyncNone:
		return strata.SyncNone, nil
	case SyncAlways:
		return strata.SyncAlways, nil
	default:
		return 0, fmt.Errorf("bench: unknown sync mode %q, want %q or %q", m, SyncNone, SyncAlways)
	}
}

// Defaults applied to a zero-valued field of Config.
const (
	DefaultN            = 500_000
	DefaultValueSize    = 100
	DefaultThreads      = 1
	DefaultSeed         = 1
	DefaultMemtableSize = 4 << 20
)

// Config describes one benchmark run. The zero value is usable and means every
// default.
type Config struct {
	// N is the number of entries, and the number of operations each
	// workload performs.
	N int
	// ValueSize is the length in bytes of every value.
	ValueSize int
	// Threads is how many goroutines share the work. It applies to the read
	// and mixed workloads only: the fills measure single-stream throughput,
	// where concurrency would only measure the write lock.
	Threads int
	// Sync is the write-ahead log's durability policy.
	Sync SyncMode
	// Seed drives every random choice, so a run is reproducible.
	Seed uint64
	// Dir is where the databases are built. An empty Dir means a temporary
	// directory, which is removed when the run finishes.
	Dir string
	// MemtableSize, CompactionTrigger, CompactionBaseSize and
	// DisableCompaction override the engine's defaults.
	MemtableSize       int
	CompactionTrigger  int
	CompactionBaseSize int64
	DisableCompaction  bool
}

// withDefaults returns c with absent values filled in.
func (c Config) withDefaults() Config {
	if c.N <= 0 {
		c.N = DefaultN
	}
	if c.ValueSize < 0 {
		c.ValueSize = 0
	}
	if c.ValueSize == 0 {
		c.ValueSize = DefaultValueSize
	}
	if c.Threads <= 0 {
		c.Threads = DefaultThreads
	}
	if c.Seed == 0 {
		c.Seed = DefaultSeed
	}
	if c.MemtableSize <= 0 {
		c.MemtableSize = DefaultMemtableSize
	}
	if c.Sync == "" {
		c.Sync = SyncNone
	}
	return c
}

// options returns the engine options this configuration asks for.
func (c Config) options() (*strata.Options, error) {
	policy, err := c.Sync.policy()
	if err != nil {
		return nil, err
	}
	return &strata.Options{
		MemtableSize:          c.MemtableSize,
		Sync:                  policy,
		CompactionTrigger:     c.CompactionTrigger,
		CompactionBaseSize:    c.CompactionBaseSize,
		DisableAutoCompaction: c.DisableCompaction,
	}, nil
}

// Key returns the key for logical index i: a 16-digit even number.
//
// Present keys are even so that the odd number between two of them is a key
// that was never written, which is what MissingKey returns. A read miss then
// falls inside the key range rather than past the end of it, where the table
// index would turn it away without the Bloom filter ever being consulted.
func Key(i int) []byte { return []byte(fmt.Sprintf("%0*d", KeyLen, 2*i)) }

// MissingKey returns a key that no workload ever writes: the odd number that
// sorts between Key(i) and Key(i+1).
func MissingKey(i int) []byte { return []byte(fmt.Sprintf("%0*d", KeyLen, 2*i+1)) }

// Permutation returns a random permutation of the integers below n, derived
// from seed alone so that two runs agree.
func Permutation(n int, seed uint64) []int {
	return rand.New(rand.NewPCG(seed, seed)).Perm(n)
}

// valuePool holds the values a run writes.
type valuePool struct {
	values [][]byte
}

// newValuePool generates count values of size bytes each from seed.
func newValuePool(size, count int, seed uint64) *valuePool {
	rng := rand.New(rand.NewPCG(seed, ^seed))
	p := &valuePool{values: make([][]byte, count)}
	for i := range p.values {
		v := make([]byte, size)
		for j := range v {
			v[j] = byte(rng.UintN(256))
		}
		p.values[i] = v
	}
	return p
}

// at returns the value for logical index i.
func (p *valuePool) at(i int) []byte { return p.values[i%len(p.values)] }

// Result is what one workload measured.
type Result struct {
	// Name is the workload's name.
	Name string
	// Ops is how many operations were timed.
	Ops int
	// Duration is the wall time of the timed section.
	Duration time.Duration
	// OpsPerSec and MBPerSec are throughput; MBPerSec counts the key and
	// value bytes an operation moved.
	OpsPerSec float64
	MBPerSec  float64
	// Latency percentiles over the per-operation times.
	P50, P90, P99, P999, Max time.Duration
	// WriteAmplification, Tables and Compactions come from the engine's own
	// counters at the end of the timed section.
	WriteAmplification float64
	Tables             int
	Compactions        uint64
	// BlockReadsPerOp and BloomRejectsPerOp are deltas across the timed
	// section, divided by Ops.
	//
	// They are only meaningful when CountersReliable is true. The engine
	// sums these counters over the tables that are live when it is asked,
	// so a compaction retiring a table takes that table's counts with it
	// and the difference between two snapshots stops meaning anything.
	BlockReadsPerOp   float64
	BloomRejectsPerOp float64
	// CountersReliable reports whether BlockReadsPerOp and
	// BloomRejectsPerOp can be trusted: false if a compaction finished
	// during the timed section, which is detected by the compaction count
	// moving.
	CountersReliable bool
	// Found and NotFound count how the read operations answered.
	Found, NotFound int
	// Verified reports whether the workload checked its own effect
	// afterwards. Only DeleteRandom does.
	Verified bool
}

// Report is a whole run: what it ran on, what it was asked to do, and what
// each workload measured.
type Report struct {
	Environment Environment
	Config      Config
	Results     []Result
}

// Run executes the named workloads and returns a report. An empty names means
// all of them; unknown names are an error.
//
// The databases are isolated according to what each workload is measuring.
// FillSeq, FillRandom, Overwrite and DeleteRandom each build their own, so
// that a fill is always measured against an empty database and an overwrite
// always against a full one. ReadHit, ReadMiss and Mixed share one database,
// built once in FillRandom order, because building it three times would take
// far longer than the reads and would measure nothing new; they run in that
// order so that the two read workloads see it before Mixed writes to it.
func Run(cfg Config, names []string) (*Report, error) {
	cfg = cfg.withDefaults()
	if _, err := cfg.options(); err != nil {
		return nil, err
	}
	selected, err := selectWorkloads(names)
	if err != nil {
		return nil, err
	}

	root, cleanup, err := workRoot(cfg)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	r := &runner{
		cfg:       cfg,
		root:      root,
		values:    newValuePool(cfg.ValueSize, valuePoolSize, cfg.Seed),
		fillOrder: Permutation(cfg.N, cfg.Seed),
		opOrder:   Permutation(cfg.N, cfg.Seed+1),
	}
	defer r.closeShared()

	report := &Report{Environment: environment(), Config: cfg}
	for _, name := range selected {
		result, err := r.run(name)
		if err != nil {
			return nil, fmt.Errorf("bench: %s: %w", name, err)
		}
		report.Results = append(report.Results, result)
	}
	return report, nil
}

// selectWorkloads validates names and returns them in canonical order.
func selectWorkloads(names []string) ([]string, error) {
	if len(names) == 0 {
		return slices.Clone(All), nil
	}
	wanted := make(map[string]bool, len(names))
	for _, name := range names {
		if !slices.Contains(All, name) {
			return nil, fmt.Errorf("bench: unknown workload %q, want one of %v", name, All)
		}
		wanted[name] = true
	}
	var out []string
	for _, name := range All {
		if wanted[name] {
			out = append(out, name)
		}
	}
	return out, nil
}

// workRoot returns the directory the run builds its databases in, and a
// function that removes it if the harness created it.
func workRoot(cfg Config) (string, func(), error) {
	if cfg.Dir != "" {
		if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
			return "", nil, err
		}
		return cfg.Dir, func() {}, nil
	}
	dir, err := os.MkdirTemp("", "strata-bench-*")
	if err != nil {
		return "", nil, err
	}
	return dir, func() { os.RemoveAll(dir) }, nil
}

// runner carries the state shared by the workloads of one run.
type runner struct {
	cfg       Config
	root      string
	values    *valuePool
	fillOrder []int
	opOrder   []int

	shared *strata.DB
}

func (r *runner) run(name string) (Result, error) {
	switch name {
	case FillSeq:
		return r.fill(FillSeq, nil)
	case FillRandom:
		return r.fill(FillRandom, r.fillOrder)
	case Overwrite:
		return r.overwrite()
	case ReadHit:
		return r.read(ReadHit, false)
	case ReadMiss:
		return r.read(ReadMiss, true)
	case Mixed:
		return r.mixed()
	case DeleteRandom:
		return r.deleteRandom()
	default:
		return Result{}, fmt.Errorf("unknown workload %q", name)
	}
}

// open creates a fresh database in a subdirectory named for the workload.
func (r *runner) open(name string) (*strata.DB, error) {
	opts, err := r.cfg.options()
	if err != nil {
		return nil, err
	}
	return strata.Open(filepath.Join(r.root, name), opts)
}

// preload fills db with every key, in random order and untimed.
func (r *runner) preload(db *strata.DB) error {
	for _, i := range r.fillOrder {
		if err := db.Put(Key(i), r.values.at(i)); err != nil {
			return fmt.Errorf("preloading key %d: %w", i, err)
		}
	}
	return nil
}

// sharedDB returns the database the read and mixed workloads share, building
// it on first use.
func (r *runner) sharedDB() (*strata.DB, error) {
	if r.shared != nil {
		return r.shared, nil
	}
	db, err := r.open("shared")
	if err != nil {
		return nil, err
	}
	if err := r.preload(db); err != nil {
		db.Close()
		return nil, err
	}
	r.shared = db
	return db, nil
}

func (r *runner) closeShared() {
	if r.shared != nil {
		r.shared.Close()
		r.shared = nil
	}
}

// fill measures inserting every key into an empty database. A nil order means
// ascending key order.
func (r *runner) fill(name string, order []int) (Result, error) {
	db, err := r.open(name)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()

	m := newMeasurement(name, r.cfg.N, 1, db)
	m.run(func(_, op int) error {
		i := op
		if order != nil {
			i = order[op]
		}
		return db.Put(Key(i), r.values.at(i))
	})
	return m.result(db, r.cfg.N*(KeyLen+r.cfg.ValueSize))
}

// overwrite measures rewriting every key of a database that already holds
// them all. The preload is not timed.
func (r *runner) overwrite() (Result, error) {
	db, err := r.open(Overwrite)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := r.preload(db); err != nil {
		return Result{}, err
	}

	m := newMeasurement(Overwrite, r.cfg.N, 1, db)
	m.run(func(_, op int) error {
		i := r.opOrder[op]
		return db.Put(Key(i), r.values.at(i+1))
	})
	return m.result(db, r.cfg.N*(KeyLen+r.cfg.ValueSize))
}

// read measures lookups against the shared database, of keys that are all
// present or all absent.
func (r *runner) read(name string, miss bool) (Result, error) {
	db, err := r.sharedDB()
	if err != nil {
		return Result{}, err
	}

	m := newMeasurement(name, r.cfg.N, r.cfg.Threads, db)
	m.run(func(thread, op int) error {
		i := r.opOrder[op]
		key := Key(i)
		if miss {
			key = MissingKey(i)
		}
		_, err := db.Get(key)
		switch {
		case err == nil:
			m.counts[thread].found++
		case errors.Is(err, strata.ErrNotFound):
			m.counts[thread].notFound++
		default:
			return err
		}
		return nil
	})

	bytes := r.cfg.N * (KeyLen + r.cfg.ValueSize)
	if miss {
		// Nothing was found, so only the keys moved.
		bytes = r.cfg.N * KeyLen
	}
	result, err := m.result(db, bytes)
	if err != nil {
		return Result{}, err
	}
	// The workloads are only worth comparing if they did what they claim.
	if !miss && result.NotFound != 0 {
		return Result{}, fmt.Errorf("%d of %d lookups missed, but every key should be present", result.NotFound, result.Ops)
	}
	if miss && result.Found != 0 {
		return Result{}, fmt.Errorf("%d of %d lookups found a value, but no key should be present", result.Found, result.Ops)
	}
	return result, nil
}

// mixed measures an even split of reads of present keys and overwrites.
func (r *runner) mixed() (Result, error) {
	db, err := r.sharedDB()
	if err != nil {
		return Result{}, err
	}

	m := newMeasurement(Mixed, r.cfg.N, r.cfg.Threads, db)
	m.run(func(thread, op int) error {
		i := r.opOrder[op]
		if op%2 == 0 {
			_, err := db.Get(Key(i))
			switch {
			case err == nil:
				m.counts[thread].found++
			case errors.Is(err, strata.ErrNotFound):
				m.counts[thread].notFound++
			default:
				return err
			}
			return nil
		}
		return db.Put(Key(i), r.values.at(i+2))
	})
	return m.result(db, r.cfg.N*(KeyLen+r.cfg.ValueSize))
}

// deleteRandom measures deleting half the keys, then checks untimed that
// exactly those are gone and the rest survived.
func (r *runner) deleteRandom() (Result, error) {
	db, err := r.open(DeleteRandom)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	if err := r.preload(db); err != nil {
		return Result{}, err
	}

	deletes := r.cfg.N / 2
	m := newMeasurement(DeleteRandom, deletes, 1, db)
	m.run(func(_, op int) error {
		return db.Delete(Key(r.opOrder[op]))
	})
	result, err := m.result(db, deletes*KeyLen)
	if err != nil {
		return Result{}, err
	}

	deleted := make(map[int]bool, deletes)
	for _, i := range r.opOrder[:deletes] {
		deleted[i] = true
	}
	for i := range r.cfg.N {
		_, err := db.Get(Key(i))
		if deleted[i] {
			if !errors.Is(err, strata.ErrNotFound) {
				return Result{}, fmt.Errorf("key %d was deleted but reads back with %v", i, err)
			}
			continue
		}
		if err != nil {
			return Result{}, fmt.Errorf("key %d was not deleted but reads back with %v", i, err)
		}
	}
	result.Verified = true
	return result, nil
}

// opCounts are one thread's tallies, kept per thread so that counting costs
// no synchronisation.
type opCounts struct {
	found, notFound int
}

// measurement times a workload and collects its numbers.
type measurement struct {
	name    string
	ops     int
	threads int
	before  strata.Stats

	counts   []opCounts
	samples  []int64
	duration time.Duration
	err      error
}

func newMeasurement(name string, ops, threads int, db *strata.DB) *measurement {
	if threads < 1 {
		threads = 1
	}
	return &measurement{
		name:    name,
		ops:     ops,
		threads: threads,
		before:  db.Stats(),
		counts:  make([]opCounts, threads),
	}
}

// run performs ops operations across threads, timing each one.
//
// Every operation's duration is written into a slot of a preallocated
// per-thread slice, so recording a latency costs no allocation and no lock;
// the slices are concatenated once the threads are done.
func (m *measurement) run(fn func(thread, op int) error) {
	perThread := make([][]int64, m.threads)
	errs := make([]error, m.threads)

	var wg sync.WaitGroup
	start := time.Now()
	for t := range m.threads {
		lo, hi := shareOf(m.ops, m.threads, t)
		buf := make([]int64, hi-lo)
		perThread[t] = buf
		wg.Add(1)
		go func(t, lo, hi int, buf []int64) {
			defer wg.Done()
			for op := lo; op < hi; op++ {
				began := time.Now()
				err := fn(t, op)
				buf[op-lo] = int64(time.Since(began))
				if err != nil {
					errs[t] = err
					return
				}
			}
		}(t, lo, hi, buf)
	}
	wg.Wait()
	m.duration = time.Since(start)

	for _, err := range errs {
		if err != nil {
			m.err = err
			return
		}
	}
	m.samples = make([]int64, 0, m.ops)
	for _, buf := range perThread {
		m.samples = append(m.samples, buf...)
	}
}

// shareOf divides ops into threads contiguous ranges and returns the t'th.
func shareOf(ops, threads, t int) (lo, hi int) {
	base, extra := ops/threads, ops%threads
	lo = t*base + min(t, extra)
	hi = lo + base
	if t < extra {
		hi++
	}
	return lo, hi
}

// result turns the measurement into a Result, given how many bytes the
// workload moved.
func (m *measurement) result(db *strata.DB, movedBytes int) (Result, error) {
	if m.err != nil {
		return Result{}, m.err
	}
	after := db.Stats()

	r := Result{
		Name:               m.name,
		Ops:                m.ops,
		Duration:           m.duration,
		WriteAmplification: after.WriteAmplification(),
		Tables:             after.SSTables,
		Compactions:        after.Compactions,
	}
	for _, c := range m.counts {
		r.Found += c.found
		r.NotFound += c.notFound
	}
	if seconds := m.duration.Seconds(); seconds > 0 {
		r.OpsPerSec = float64(m.ops) / seconds
		r.MBPerSec = float64(movedBytes) / seconds / (1 << 20)
	}
	// A compaction closes the readers it consumed, and their counts go with
	// them, so the two snapshots are no longer measuring the same set of
	// tables. Saying so is better than reporting a difference computed from
	// a basis that moved.
	r.CountersReliable = after.Compactions == m.before.Compactions
	if m.ops > 0 {
		r.BlockReadsPerOp = float64(countedSince(m.before.BlockReads, after.BlockReads)) / float64(m.ops)
		r.BloomRejectsPerOp = float64(countedSince(m.before.BloomRejects, after.BloomRejects)) / float64(m.ops)
	}
	r.P50 = Percentile(m.samples, 50)
	r.P90 = Percentile(m.samples, 90)
	r.P99 = Percentile(m.samples, 99)
	r.P999 = Percentile(m.samples, 99.9)
	r.Max = Percentile(m.samples, 100)
	return r, nil
}

// countedSince returns after minus before, saturating at zero.
//
// These counters can fall as well as rise, because they are summed over the
// live tables and a compaction retires some. Unsigned subtraction would turn
// that into a number near 2^64, which is worse than useless in a report.
func countedSince(before, after uint64) uint64 {
	if after < before {
		return 0
	}
	return after - before
}

// Percentile returns the p'th percentile of samples, which are nanosecond
// durations in any order. It uses the nearest-rank definition, so the result
// is always one of the samples rather than an interpolation between two.
//
// It sorts a copy, leaving the caller's slice alone. An empty samples returns
// zero.
func Percentile(samples []int64, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := slices.Clone(samples)
	slices.Sort(sorted)

	switch {
	case p <= 0:
		return time.Duration(sorted[0])
	case p >= 100:
		return time.Duration(sorted[len(sorted)-1])
	}
	// Nearest rank: the smallest index covering at least p percent.
	rank := int(p/100*float64(len(sorted)) + 0.9999999)
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return time.Duration(sorted[rank-1])
}
