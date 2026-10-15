package main

import (
	"bufio"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/yashtalikhede/strata"
	"github.com/yashtalikhede/strata/bench"
)

// version is the build's version, set with -ldflags "-X main.version=...".
var version = "dev"

// Exit codes. They are the only thing a script can rely on, so they are kept
// few and meaningful.
const (
	// exitOK means the command did what was asked.
	exitOK = 0
	// exitFailure means the command ran but did not succeed, which includes
	// a key that was not found.
	exitFailure = 1
	// exitUsage means the command line itself was wrong.
	exitUsage = 2
)

const usageText = `strata is a command-line interface to the strata storage engine.

usage:
    strata [flags] <command> [arguments]

commands:
    put <key> <value>   store a value
    get <key>           print a value, or exit 1 if the key is absent
    del <key>           delete a key (alias: delete)
    flush               write the in-memory table out to disk
    compact             merge every table into one
    stats               print the engine's counters
    shell               read commands from stdin interactively
    bench [flags]       run the benchmark suite and print a report
    version             print the version
    help                print this message

flags:
    -dir DIR            the database directory (required by every command
                        that touches a database)
    -sync none|always   write-ahead log durability (default always)
    -hex                read keys and values as hex, and print them as hex

Flags may be given before or after the command.
`

// dbFlags are the options every database command shares.
type dbFlags struct {
	dir  string
	sync string
	hex  bool
}

// register adds the shared flags to fs, defaulting to whatever has been set
// already, so that they work both before and after the command name.
func (f *dbFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.dir, "dir", f.dir, "database directory")
	fs.StringVar(&f.sync, "sync", f.sync, "write-ahead log durability: none or always")
	fs.BoolVar(&f.hex, "hex", f.hex, "read and print keys and values as hex")
}

// options turns the flags into engine options, rejecting a missing directory
// and an unknown policy.
func (f *dbFlags) options() (*strata.Options, error) {
	if f.dir == "" {
		return nil, errors.New("no database directory: pass -dir DIR")
	}
	switch f.sync {
	case "", "always":
		return &strata.Options{Sync: strata.SyncAlways}, nil
	case "none":
		return &strata.Options{Sync: strata.SyncNone}, nil
	default:
		return nil, fmt.Errorf("unknown -sync %q, want none or always", f.sync)
	}
}

// decode reads a command-line argument as a key or value, honouring -hex.
func (f *dbFlags) decode(arg string) ([]byte, error) {
	if !f.hex {
		return []byte(arg), nil
	}
	raw, err := hex.DecodeString(arg)
	if err != nil {
		return nil, fmt.Errorf("invalid hex %q: %w", arg, err)
	}
	return raw, nil
}

// encode renders a value for printing, honouring -hex.
func (f *dbFlags) encode(value []byte) string {
	if f.hex {
		return hex.EncodeToString(value)
	}
	return string(value)
}

// run is the whole command line, written so that a test can drive it with
// ordinary readers and writers and inspect the exit code.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	var flags dbFlags

	// The first pass takes flags written before the command name; the
	// command's own FlagSet takes any written after it.
	global := flag.NewFlagSet("strata", flag.ContinueOnError)
	global.SetOutput(stderr)
	global.Usage = func() { fmt.Fprint(stderr, usageText) }
	flags.register(global)
	if err := global.Parse(args); err != nil {
		return exitUsage
	}

	rest := global.Args()
	if len(rest) == 0 {
		fmt.Fprint(stderr, usageText)
		return exitUsage
	}
	command, commandArgs := rest[0], rest[1:]

	switch command {
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usageText)
		return exitOK
	case "version":
		fmt.Fprintf(stdout, "strata %s\n", version)
		return exitOK
	case "bench":
		return runBench(commandArgs, flags.dir, flags.sync, stdout, stderr)
	case "put", "get", "del", "delete", "flush", "compact", "stats", "shell":
		return runDBCommand(command, commandArgs, &flags, stdin, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "strata: unknown command %q\n\n", command)
		fmt.Fprint(stderr, usageText)
		return exitUsage
	}
}

// runDBCommand parses a database command's own flags, opens the database, and
// performs the operation.
func runDBCommand(command string, args []string, flags *dbFlags, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("strata "+command, flag.ContinueOnError)
	fs.SetOutput(stderr)
	flags.register(fs)
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	operands := fs.Args()

	want := map[string]int{"put": 2, "get": 1, "del": 1, "delete": 1, "flush": 0, "compact": 0, "stats": 0, "shell": 0}
	if got := len(operands); got != want[command] {
		fmt.Fprintf(stderr, "strata %s: want %d argument(s), got %d\n", command, want[command], got)
		return exitUsage
	}

	opts, err := flags.options()
	if err != nil {
		fmt.Fprintf(stderr, "strata: %v\n", err)
		return exitUsage
	}

	// Decode the operands before opening anything, so bad hex is a usage
	// error rather than a half-done operation.
	decoded := make([][]byte, len(operands))
	for i, operand := range operands {
		if decoded[i], err = flags.decode(operand); err != nil {
			fmt.Fprintf(stderr, "strata: %v\n", err)
			return exitUsage
		}
	}

	store, err := strata.Open(flags.dir, opts)
	if err != nil {
		fmt.Fprintf(stderr, "strata: %v\n", err)
		return exitFailure
	}
	defer store.Close()

	if command == "shell" {
		return shell(store, flags, stdin, stdout, stderr)
	}
	code := perform(store, flags, command, decoded, stdout, stderr)
	// Closing can fail, and for a write that would mean losing it quietly.
	if err := store.Close(); err != nil && code == exitOK {
		fmt.Fprintf(stderr, "strata: closing: %v\n", err)
		return exitFailure
	}
	return code
}

// perform carries out one database operation, shared by the single-shot
// commands and the shell.
func perform(store *strata.DB, flags *dbFlags, command string, operands [][]byte, stdout, stderr io.Writer) int {
	switch command {
	case "put":
		if err := store.Put(operands[0], operands[1]); err != nil {
			fmt.Fprintf(stderr, "strata: %v\n", err)
			return exitFailure
		}
	case "get":
		value, err := store.Get(operands[0])
		switch {
		case errors.Is(err, strata.ErrNotFound):
			fmt.Fprintln(stderr, "not found")
			return exitFailure
		case err != nil:
			fmt.Fprintf(stderr, "strata: %v\n", err)
			return exitFailure
		}
		fmt.Fprintln(stdout, flags.encode(value))
	case "del", "delete":
		if err := store.Delete(operands[0]); err != nil {
			fmt.Fprintf(stderr, "strata: %v\n", err)
			return exitFailure
		}
	case "flush":
		if err := store.Flush(); err != nil {
			fmt.Fprintf(stderr, "strata: %v\n", err)
			return exitFailure
		}
	case "compact":
		before := store.Stats().SSTables
		if err := store.Compact(); err != nil {
			fmt.Fprintf(stderr, "strata: %v\n", err)
			return exitFailure
		}
		fmt.Fprintf(stdout, "compacted %d tables into %d\n", before, store.Stats().SSTables)
	case "stats":
		writeStats(stdout, store.Stats())
	default:
		fmt.Fprintf(stderr, "strata: unknown command %q\n", command)
		return exitUsage
	}
	return exitOK
}

// writeStats prints every counter as an aligned "name: value" line.
func writeStats(w io.Writer, s strata.Stats) {
	lines := []struct {
		name  string
		value string
	}{
		{"memtable bytes", fmt.Sprintf("%d", s.MemtableBytes)},
		{"sstables", fmt.Sprintf("%d", s.SSTables)},
		{"flushes", fmt.Sprintf("%d", s.Flushes)},
		{"compactions", fmt.Sprintf("%d", s.Compactions)},
		{"compaction errors", fmt.Sprintf("%d", s.CompactionErrors)},
		{"block reads", fmt.Sprintf("%d", s.BlockReads)},
		{"bloom rejects", fmt.Sprintf("%d", s.BloomRejects)},
		{"flush bytes written", fmt.Sprintf("%d", s.FlushBytesWritten)},
		{"compaction bytes read", fmt.Sprintf("%d", s.CompactionBytesRead)},
		{"compaction bytes written", fmt.Sprintf("%d", s.CompactionBytesWritten)},
		{"user bytes written", fmt.Sprintf("%d", s.UserBytesWritten)},
		{"tombstones dropped", fmt.Sprintf("%d", s.TombstonesDropped)},
		{"shadowed dropped", fmt.Sprintf("%d", s.ShadowedDropped)},
		{"write amplification", fmt.Sprintf("%.2f", s.WriteAmplification())},
	}
	width := 0
	for _, line := range lines {
		width = max(width, len(line.name))
	}
	for _, line := range lines {
		fmt.Fprintf(w, "%-*s  %s\n", width+1, line.name+":", line.value)
	}
}

const shellHelp = `commands:
    put <key> <value>   store a value
    get <key>           print a value
    del <key>           delete a key
    flush               write the in-memory table out to disk
    compact             merge every table into one
    stats               print the engine's counters
    help                print this message
    quit                leave the shell (alias: exit)

Arguments may be double-quoted Go strings, so a value may contain spaces.
`

// shell reads commands from stdin until EOF or quit.
//
// It keeps one database open for the session, so a shell is much faster than
// the same commands run one process at a time, and it never exits because of
// a bad line: a mistyped command prints a hint and the prompt comes back.
func shell(store *strata.DB, flags *dbFlags, stdin io.Reader, stdout, stderr io.Writer) int {
	scanner := bufio.NewScanner(stdin)
	for {
		fmt.Fprint(stdout, "strata> ")
		if !scanner.Scan() {
			break
		}
		words, err := tokenize(scanner.Text())
		if err != nil {
			fmt.Fprintf(stderr, "strata: %v\n", err)
			continue
		}
		if len(words) == 0 {
			continue
		}

		command, rawArgs := words[0], words[1:]
		switch command {
		case "quit", "exit":
			return exitOK
		case "help":
			fmt.Fprint(stdout, shellHelp)
			continue
		}

		want, known := map[string]int{"put": 2, "get": 1, "del": 1, "delete": 1, "flush": 0, "compact": 0, "stats": 0}[command]
		if !known {
			fmt.Fprintf(stderr, "strata: unknown command %q, try help\n", command)
			continue
		}
		if len(rawArgs) != want {
			fmt.Fprintf(stderr, "strata: %s wants %d argument(s), got %d\n", command, want, len(rawArgs))
			continue
		}

		operands := make([][]byte, len(rawArgs))
		bad := false
		for i, arg := range rawArgs {
			if operands[i], err = flags.decode(arg); err != nil {
				fmt.Fprintf(stderr, "strata: %v\n", err)
				bad = true
				break
			}
		}
		if bad {
			continue
		}
		// The exit code of one line does not end the session; the error has
		// already been reported.
		perform(store, flags, command, operands, stdout, stderr)
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(stderr, "strata: reading input: %v\n", err)
		return exitFailure
	}
	fmt.Fprintln(stdout)
	return exitOK
}

// tokenize splits a shell line into words, treating a double-quoted run as one
// word so that a value may contain spaces. A quoted word is a Go string
// literal, so the usual escapes work.
func tokenize(line string) ([]string, error) {
	var words []string
	for i := 0; i < len(line); {
		if isSpace(line[i]) {
			i++
			continue
		}
		if line[i] == '"' {
			end, err := endOfQuoted(line, i)
			if err != nil {
				return nil, err
			}
			word, err := strconv.Unquote(line[i:end])
			if err != nil {
				return nil, fmt.Errorf("bad quoted argument %s: %w", line[i:end], err)
			}
			words = append(words, word)
			i = end
			continue
		}
		start := i
		for i < len(line) && !isSpace(line[i]) {
			i++
		}
		words = append(words, line[start:i])
	}
	return words, nil
}

// endOfQuoted returns the index just past the closing quote of the literal
// starting at start.
func endOfQuoted(line string, start int) (int, error) {
	for i := start + 1; i < len(line); i++ {
		switch line[i] {
		case '\\':
			i++
		case '"':
			return i + 1, nil
		}
	}
	return 0, fmt.Errorf("unterminated quote in %s", line[start:])
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\r' }

// runBench parses the benchmark flags and runs the suite.
func runBench(args []string, defaultDir, defaultSync string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("strata bench", flag.ContinueOnError)
	fs.SetOutput(stderr)

	n := fs.Int("n", bench.DefaultN, "number of entries, and of operations per workload")
	valueSize := fs.Int("value-size", bench.DefaultValueSize, "value size in bytes")
	threads := fs.Int("threads", bench.DefaultThreads, "goroutines for the read and mixed workloads")
	sync := fs.String("sync", defaultSync, "write-ahead log durability: none or always")
	workloads := fs.String("workloads", "", "comma-separated workloads to run (default all)")
	seed := fs.Uint64("seed", bench.DefaultSeed, "seed for every random choice")
	dir := fs.String("dir", defaultDir, "where to build the databases (default a temporary directory)")
	jsonPath := fs.String("json", "", "also write the report as JSON to this file")
	mdPath := fs.String("md", "", "also write the report as Markdown to this file")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "strata bench: unexpected argument %q\n", fs.Arg(0))
		return exitUsage
	}

	cfg := bench.Config{
		N:         *n,
		ValueSize: *valueSize,
		Threads:   *threads,
		Sync:      bench.SyncMode(*sync),
		Seed:      *seed,
		Dir:       *dir,
	}
	var names []string
	if *workloads != "" {
		names = strings.Split(*workloads, ",")
		for i := range names {
			names[i] = strings.TrimSpace(names[i])
		}
	}

	report, err := bench.Run(cfg, names)
	if err != nil {
		fmt.Fprintf(stderr, "strata bench: %v\n", err)
		return exitFailure
	}

	markdown := report.Markdown()
	fmt.Fprint(stdout, markdown)

	if *mdPath != "" {
		if err := writeFile(*mdPath, []byte(markdown)); err != nil {
			fmt.Fprintf(stderr, "strata bench: %v\n", err)
			return exitFailure
		}
	}
	if *jsonPath != "" {
		raw, err := report.JSON()
		if err != nil {
			fmt.Fprintf(stderr, "strata bench: %v\n", err)
			return exitFailure
		}
		if err := writeFile(*jsonPath, append(raw, '\n')); err != nil {
			fmt.Fprintf(stderr, "strata bench: %v\n", err)
			return exitFailure
		}
	}
	return exitOK
}

// writeFile writes b to path, creating the parent directory if need be, so
// that -md bench/results/x.md works on a fresh checkout.
func writeFile(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
