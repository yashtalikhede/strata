package bench

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

// Environment records what the run was measured on, so that two reports can
// be compared with some idea of whether the comparison means anything.
type Environment struct {
	// Date is when the run finished, in UTC.
	Date string
	// GoVersion, GOOS, GOARCH and NumCPU come from the runtime.
	GoVersion string
	GOOS      string
	GOARCH    string
	NumCPU    int
	// CPU is the processor model, best effort: it is "unknown" on any
	// platform this does not know how to ask.
	CPU string
}

// environment gathers what can be learned about the machine.
func environment() Environment {
	return Environment{
		Date:      time.Now().UTC().Format(time.RFC3339),
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		CPU:       cpuModel(),
	}
}

// cpuModel returns the processor's model name, or "unknown".
//
// There is no portable way to ask, so this knows two platforms and gives up
// gracefully everywhere else: an unknown CPU makes a report less useful to
// compare, never wrong.
func cpuModel() string {
	switch runtime.GOOS {
	case "darwin":
		out, err := exec.Command("sysctl", "-n", "machdep.cpu.brand_string").Output()
		if err != nil {
			return "unknown"
		}
		if model := strings.TrimSpace(string(out)); model != "" {
			return model
		}
	case "linux":
		f, err := os.Open("/proc/cpuinfo")
		if err != nil {
			return "unknown"
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			name, value, found := strings.Cut(scanner.Text(), ":")
			if !found || strings.TrimSpace(name) != "model name" {
				continue
			}
			if model := strings.TrimSpace(value); model != "" {
				return model
			}
		}
	}
	return "unknown"
}

// JSON returns the report as indented JSON. Durations are nanosecond counts,
// which is how time.Duration marshals.
func (r *Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// markdownColumns are the table's headings, in order.
var markdownColumns = []string{
	"workload", "ops", "ops/s", "MB/s", "p50", "p99", "p99.9",
	"write-amp", "tables", "blockreads/op", "bloomrejects/op",
}

// Markdown returns the report as a Markdown document: a block describing the
// run, then one table with a row per workload.
func (r *Report) Markdown() string {
	var b strings.Builder

	fmt.Fprintf(&b, "# strata benchmark\n\n")
	fmt.Fprintf(&b, "- date: %s\n", r.Environment.Date)
	fmt.Fprintf(&b, "- go: %s\n", r.Environment.GoVersion)
	fmt.Fprintf(&b, "- platform: %s/%s\n", r.Environment.GOOS, r.Environment.GOARCH)
	fmt.Fprintf(&b, "- cpu: %s (%d cores)\n", r.Environment.CPU, r.Environment.NumCPU)
	fmt.Fprintf(&b, "- entries: %d\n", r.Config.N)
	fmt.Fprintf(&b, "- value size: %d bytes\n", r.Config.ValueSize)
	fmt.Fprintf(&b, "- threads: %d\n", r.Config.Threads)
	fmt.Fprintf(&b, "- sync: %s\n", r.Config.Sync)
	fmt.Fprintf(&b, "- memtable: %d bytes\n", r.Config.MemtableSize)
	fmt.Fprintf(&b, "- compaction: %s\n", enabledOrNot(!r.Config.DisableCompaction))
	fmt.Fprintf(&b, "- seed: %d\n", r.Config.Seed)
	b.WriteString("\n")

	b.WriteString("| " + strings.Join(markdownColumns, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat("---|", len(markdownColumns)) + "\n")
	for _, result := range r.Results {
		cells := []string{
			result.Name,
			fmt.Sprintf("%d", result.Ops),
			fmt.Sprintf("%.0f", result.OpsPerSec),
			fmt.Sprintf("%.1f", result.MBPerSec),
			result.P50.String(),
			result.P99.String(),
			result.P999.String(),
			fmt.Sprintf("%.2f", result.WriteAmplification),
			fmt.Sprintf("%d", result.Tables),
			perOp(result.BlockReadsPerOp, result.CountersReliable),
			perOp(result.BloomRejectsPerOp, result.CountersReliable),
		}
		b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
	}
	return b.String()
}

// perOp renders a per-operation counter, or "n/a" when a compaction during
// the workload made the two snapshots incomparable.
func perOp(value float64, reliable bool) string {
	if !reliable {
		return "n/a"
	}
	return fmt.Sprintf("%.3f", value)
}

func enabledOrNot(on bool) string {
	if on {
		return "on"
	}
	return "off"
}
