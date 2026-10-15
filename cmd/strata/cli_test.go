package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// invocation is one run of the command line, captured as a test would see it.
type invocation struct {
	code   int
	stdout string
	stderr string
}

// exec runs the command line with no input, as a separate invocation would:
// each one opens the database and closes it again.
func exec(t *testing.T, args ...string) invocation {
	t.Helper()
	return execStdin(t, "", args...)
}

func execStdin(t *testing.T, stdin string, args ...string) invocation {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(args, strings.NewReader(stdin), &out, &errOut)
	return invocation{code: code, stdout: out.String(), stderr: errOut.String()}
}

// expectOK fails unless the invocation succeeded.
func (inv invocation) expectOK(t *testing.T, what string) invocation {
	t.Helper()
	if inv.code != exitOK {
		t.Fatalf("%s: exit %d, want 0 (stderr: %s)", what, inv.code, inv.stderr)
	}
	return inv
}

// Fast and durable in a test are not the same thing; the CLI tests are about
// argument handling, so they skip fsync.
func dbArgs(dir string, rest ...string) []string {
	return append([]string{"-dir", dir, "-sync", "none"}, rest...)
}

func TestPutGetDeleteAcrossInvocations(t *testing.T) {
	dir := t.TempDir()

	exec(t, dbArgs(dir, "put", "colour", "blue")...).expectOK(t, "put")

	got := exec(t, dbArgs(dir, "get", "colour")...).expectOK(t, "get")
	if got.stdout != "blue\n" {
		t.Errorf("get printed %q, want %q", got.stdout, "blue\n")
	}

	// Overwriting in a later invocation must stick.
	exec(t, dbArgs(dir, "put", "colour", "green")...).expectOK(t, "put")
	got = exec(t, dbArgs(dir, "get", "colour")...).expectOK(t, "get")
	if got.stdout != "green\n" {
		t.Errorf("get printed %q, want %q", got.stdout, "green\n")
	}

	exec(t, dbArgs(dir, "del", "colour")...).expectOK(t, "del")
	gone := exec(t, dbArgs(dir, "get", "colour")...)
	if gone.code != exitFailure {
		t.Errorf("get of a deleted key: exit %d, want %d", gone.code, exitFailure)
	}

	// "delete" is the same command.
	exec(t, dbArgs(dir, "put", "x", "1")...).expectOK(t, "put")
	exec(t, dbArgs(dir, "delete", "x")...).expectOK(t, "delete")
	if inv := exec(t, dbArgs(dir, "get", "x")...); inv.code != exitFailure {
		t.Errorf("get after delete: exit %d, want %d", inv.code, exitFailure)
	}

	// Flags after the command work too.
	exec(t, "put", "-dir", dir, "-sync", "none", "late", "flag").expectOK(t, "put with trailing flags")
	if inv := exec(t, "get", "-dir", dir, "-sync", "none", "late"); inv.stdout != "flag\n" {
		t.Errorf("get printed %q, want %q", inv.stdout, "flag\n")
	}
}

func TestGetMissingKey(t *testing.T) {
	dir := t.TempDir()
	inv := exec(t, dbArgs(dir, "get", "never-written")...)
	if inv.code != exitFailure {
		t.Errorf("exit %d, want %d", inv.code, exitFailure)
	}
	if !strings.Contains(inv.stderr, "not found") {
		t.Errorf("stderr = %q, want it to mention not found", inv.stderr)
	}
	if inv.stdout != "" {
		t.Errorf("stdout = %q, want nothing printed for a missing key", inv.stdout)
	}
}

func TestHexFlag(t *testing.T) {
	dir := t.TempDir()

	// 6b6579 is "key", 76616c7565 is "value".
	exec(t, dbArgs(dir, "-hex", "put", "6b6579", "76616c7565")...).expectOK(t, "hex put")

	hexGet := exec(t, dbArgs(dir, "-hex", "get", "6b6579")...).expectOK(t, "hex get")
	if hexGet.stdout != "76616c7565\n" {
		t.Errorf("hex get printed %q, want %q", hexGet.stdout, "76616c7565\n")
	}

	// The same bytes are reachable without -hex, which proves the decoding
	// happened on the way in rather than being a display trick.
	plain := exec(t, dbArgs(dir, "get", "key")...).expectOK(t, "plain get")
	if plain.stdout != "value\n" {
		t.Errorf("plain get printed %q, want %q", plain.stdout, "value\n")
	}

	// Bytes that are not valid UTF-8 survive a hex round trip.
	exec(t, dbArgs(dir, "-hex", "put", "00ff00", "fe0001")...).expectOK(t, "binary put")
	binary := exec(t, dbArgs(dir, "-hex", "get", "00ff00")...).expectOK(t, "binary get")
	if binary.stdout != "fe0001\n" {
		t.Errorf("binary get printed %q, want %q", binary.stdout, "fe0001\n")
	}

	for _, bad := range []string{"zz", "abc", "6b65791"} {
		inv := exec(t, dbArgs(dir, "-hex", "get", bad)...)
		if inv.code != exitUsage {
			t.Errorf("get -hex %q: exit %d, want %d", bad, inv.code, exitUsage)
		}
		if !strings.Contains(inv.stderr, "invalid hex") {
			t.Errorf("get -hex %q: stderr = %q, want it to mention invalid hex", bad, inv.stderr)
		}
	}
}

func TestFlushAndCompact(t *testing.T) {
	dir := t.TempDir()
	for i := range 5 {
		exec(t, dbArgs(dir, "put", string(rune('a'+i)), "v")...).expectOK(t, "put")
		exec(t, dbArgs(dir, "flush")...).expectOK(t, "flush")
	}

	inv := exec(t, dbArgs(dir, "compact")...).expectOK(t, "compact")
	if !strings.Contains(inv.stdout, "compacted") || !strings.Contains(inv.stdout, " into 1\n") {
		t.Errorf("compact printed %q, want a before and after table count", inv.stdout)
	}

	// Everything still reads back.
	for i := range 5 {
		got := exec(t, dbArgs(dir, "get", string(rune('a'+i)))...).expectOK(t, "get")
		if got.stdout != "v\n" {
			t.Errorf("get printed %q, want %q", got.stdout, "v\n")
		}
	}
}

func TestStats(t *testing.T) {
	dir := t.TempDir()
	exec(t, dbArgs(dir, "put", "k", "v")...).expectOK(t, "put")

	inv := exec(t, dbArgs(dir, "stats")...).expectOK(t, "stats")
	for _, want := range []string{"write amplification", "sstables", "flushes", "compactions", "user bytes written"} {
		if !strings.Contains(inv.stdout, want) {
			t.Errorf("stats output has no %q:\n%s", want, inv.stdout)
		}
	}
	// Aligned: every line's value starts in the same column.
	column := -1
	for _, line := range strings.Split(strings.TrimSpace(inv.stdout), "\n") {
		at := strings.Index(line, ": ")
		if at < 0 {
			t.Fatalf("line %q is not a name: value pair", line)
		}
		valueAt := len(line) - len(strings.TrimLeft(line[at+1:], " ")) + 1
		if column == -1 {
			column = valueAt
			continue
		}
		if valueAt != column {
			t.Errorf("line %q starts its value at %d, want %d", line, valueAt, column)
		}
	}
}

func TestShellSession(t *testing.T) {
	dir := t.TempDir()
	session := strings.Join([]string{
		`put greeting "hello there"`,
		`get greeting`,
		`put "quoted key" "with \"escapes\" inside"`,
		`get "quoted key"`,
		``,
		`nonsense`,
		`put only-one-argument`,
		`flush`,
		`compact`,
		`stats`,
		`help`,
		`del greeting`,
		`get greeting`,
		`quit`,
		`put after-quit never`,
	}, "\n")

	inv := execStdin(t, session, dbArgs(dir, "shell")...).expectOK(t, "shell")

	if n := strings.Count(inv.stdout, "strata> "); n < 10 {
		t.Errorf("saw %d prompts, want one per line", n)
	}
	if !strings.Contains(inv.stdout, "hello there\n") {
		t.Errorf("a quoted value with spaces did not survive:\n%s", inv.stdout)
	}
	if !strings.Contains(inv.stdout, `with "escapes" inside`) {
		t.Errorf("escapes inside a quoted value did not survive:\n%s", inv.stdout)
	}
	if !strings.Contains(inv.stderr, `unknown command "nonsense"`) {
		t.Errorf("stderr = %q, want a hint about the unknown command", inv.stderr)
	}
	if !strings.Contains(inv.stderr, "put wants 2 argument(s)") {
		t.Errorf("stderr = %q, want a complaint about the argument count", inv.stderr)
	}
	if !strings.Contains(inv.stdout, "write amplification") {
		t.Error("stats did not run inside the shell")
	}
	if !strings.Contains(inv.stdout, "quit") {
		t.Error("help did not run inside the shell")
	}
	if !strings.Contains(inv.stderr, "not found") {
		t.Error("the deleted key still reads back")
	}
	// quit ends the session, so what follows it is never executed.
	if inv := exec(t, dbArgs(dir, "get", "after-quit")...); inv.code != exitFailure {
		t.Error("a line after quit was executed")
	}
}

func TestShellEOFAndBadQuote(t *testing.T) {
	dir := t.TempDir()

	// No quit: EOF must end the session cleanly.
	execStdin(t, "put a 1\nget a\n", dbArgs(dir, "shell")...).expectOK(t, "shell ending at EOF")

	inv := execStdin(t, `put a "unterminated`+"\n", dbArgs(dir, "shell")...).expectOK(t, "shell with a bad quote")
	if !strings.Contains(inv.stderr, "unterminated quote") {
		t.Errorf("stderr = %q, want a complaint about the quote", inv.stderr)
	}

	// An empty session is fine.
	execStdin(t, "", dbArgs(dir, "shell")...).expectOK(t, "empty shell")
}

func TestTokenize(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{"put a b", []string{"put", "a", "b"}},
		{"  put   a    b  ", []string{"put", "a", "b"}},
		{`put a "b c"`, []string{"put", "a", "b c"}},
		{`put "a b" "c d"`, []string{"put", "a b", "c d"}},
		{`put a "with \"quotes\""`, []string{"put", "a", `with "quotes"`}},
		{`put a "tab\there"`, []string{"put", "a", "tab\there"}},
		{`get ""`, []string{"get", ""}},
		{"put\ta\tb", []string{"put", "a", "b"}},
	}
	for _, tc := range cases {
		got, err := tokenize(tc.in)
		if err != nil {
			t.Errorf("tokenize(%q) = %v", tc.in, err)
			continue
		}
		if len(got) != len(tc.want) {
			t.Errorf("tokenize(%q) = %q, want %q", tc.in, got, tc.want)
			continue
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Errorf("tokenize(%q) = %q, want %q", tc.in, got, tc.want)
				break
			}
		}
	}

	for _, bad := range []string{`"unterminated`, `put "a`} {
		if _, err := tokenize(bad); err == nil {
			t.Errorf("tokenize(%q) = nil, want an error", bad)
		}
	}
}

func TestUsageErrors(t *testing.T) {
	dir := t.TempDir()
	cases := []struct {
		name string
		args []string
	}{
		{"no arguments at all", nil},
		{"missing -dir", []string{"put", "a", "b"}},
		{"missing -dir on get", []string{"get", "a"}},
		{"missing -dir on shell", []string{"shell"}},
		{"unknown command", []string{"-dir", dir, "frobnicate"}},
		{"too few arguments", []string{"-dir", dir, "put", "only-key"}},
		{"too many arguments", []string{"-dir", dir, "get", "a", "b"}},
		{"unknown sync policy", []string{"-dir", dir, "-sync", "sometimes", "stats"}},
		{"unknown flag", []string{"-dir", dir, "-nonsense", "stats"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inv := exec(t, tc.args...)
			if inv.code != exitUsage {
				t.Fatalf("exit %d, want %d (stdout %q, stderr %q)", inv.code, exitUsage, inv.stdout, inv.stderr)
			}
			if inv.stderr == "" {
				t.Error("nothing was written to stderr")
			}
		})
	}
}

func TestHelpAndVersion(t *testing.T) {
	inv := exec(t, "version").expectOK(t, "version")
	if inv.stdout != "strata dev\n" {
		t.Errorf("version printed %q, want %q", inv.stdout, "strata dev\n")
	}

	inv = exec(t, "help").expectOK(t, "help")
	for _, want := range []string{"usage:", "put <key> <value>", "shell", "bench", "-dir"} {
		if !strings.Contains(inv.stdout, want) {
			t.Errorf("help does not mention %q", want)
		}
	}
}

func TestBenchSubcommand(t *testing.T) {
	dir := t.TempDir()
	mdPath := filepath.Join(dir, "report.md")
	jsonPath := filepath.Join(dir, "nested", "report.json")

	inv := exec(t,
		"bench",
		"-n", "500",
		"-value-size", "32",
		"-workloads", "fillseq,readhit",
		"-dir", filepath.Join(dir, "data"),
		"-md", mdPath,
		"-json", jsonPath,
	).expectOK(t, "bench")

	for _, want := range []string{"fillseq", "readhit", "# strata benchmark", "ops/s"} {
		if !strings.Contains(inv.stdout, want) {
			t.Errorf("the report does not mention %q:\n%s", want, inv.stdout)
		}
	}
	// Only what was asked for.
	if strings.Contains(inv.stdout, "| deleterandom |") {
		t.Error("the report includes a workload that was not selected")
	}

	md, err := os.ReadFile(mdPath)
	if err != nil {
		t.Fatalf("reading the markdown file: %v", err)
	}
	if string(md) != inv.stdout {
		t.Error("the markdown file and the printed report differ")
	}

	raw, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatalf("reading the json file: %v", err)
	}
	var decoded struct {
		Results []struct{ Name string }
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("the json report does not parse: %v", err)
	}
	if len(decoded.Results) != 2 {
		t.Errorf("the json report has %d results, want 2", len(decoded.Results))
	}
}

func TestBenchUsageErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		code int
	}{
		{"unknown workload", []string{"bench", "-n", "100", "-workloads", "nonsense"}, exitFailure},
		{"unknown sync", []string{"bench", "-n", "100", "-sync", "sometimes"}, exitFailure},
		{"stray argument", []string{"bench", "-n", "100", "extra"}, exitUsage},
		{"bad flag value", []string{"bench", "-n", "not-a-number"}, exitUsage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if inv := exec(t, tc.args...); inv.code != tc.code {
				t.Errorf("exit %d, want %d (stderr %q)", inv.code, tc.code, inv.stderr)
			}
		})
	}
}

func TestOpenFailureIsReported(t *testing.T) {
	// A directory holding a table but no manifest is corrupt, not empty.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000001.sst"), []byte("rubbish"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	inv := exec(t, dbArgs(dir, "stats")...)
	if inv.code != exitFailure {
		t.Errorf("exit %d, want %d", inv.code, exitFailure)
	}
	if !strings.Contains(inv.stderr, "strata:") {
		t.Errorf("stderr = %q, want the error reported", inv.stderr)
	}
}
