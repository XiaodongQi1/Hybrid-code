package runner

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
)

var interrupted = errors.New("simulated interruption")

func resumeConfig() Config {
	return Config{Schema: "versioned-evm-run/v1", Codec: codec.Ethereum,
		Synthetic:       &workload.Options{BackgroundAccounts: 8, Blocks: 3, TransactionsPerBlock: 8, Contracts: 2, Seed: 73},
		BlockCacheBytes: 1 << 20, WriteBufferBytes: 1 << 20, OpenFiles: 32, MaxBatchBytes: 2 << 20}
}

func contents(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func stopAt(stage string, index int) func(string, int) error {
	return func(s string, i int) error {
		if s == stage && i == index {
			return interrupted
		}
		return nil
	}
}

func checkRecovered(t *testing.T, cfgPath, out string, want Report, log []byte, reused int) Report {
	t.Helper()
	r, err := RunWithOptions(cfgPath, out, Options{Resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if r.FinalRoot != want.FinalRoot || r.FinalHeight != want.FinalHeight || r.WorkloadID != want.WorkloadID || r.Transactions != want.Transactions || r.FailedTransactions != want.FailedTransactions || r.AccountProofs != want.AccountProofs || r.StorageProofs != want.StorageProofs || !r.ReadOnlyReopen || !r.Resumed || r.ReusedBlocks != reused || r.CacheSessionOnly != (reused > 0) {
		t.Fatalf("recovery differs: got %+v, want %+v; reused=%d", r, want, reused)
	}
	wantNew := want.ExecutedBlocks - reused + 1
	if reused == 0 {
		wantNew = want.ExecutedBlocks
	}
	if r.ExecutedBlocksThisRun != wantNew {
		t.Fatalf("reran committed blocks: got %d, want %d", r.ExecutedBlocksThisRun, wantNew)
	}
	if got := contents(t, filepath.Join(out, "blocks.jsonl")); !bytes.Equal(got, log) {
		t.Fatal("recovered roots/receipts/results differ from uninterrupted execution")
	}
	if _, err = os.Stat(filepath.Join(out, "pending-block.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pending result was not cleared", err)
	}
	sealed := contents(t, filepath.Join(out, "report.json"))
	again, err := RunWithOptions(cfgPath, out, Options{Resume: true})
	if err != nil || again != r || !bytes.Equal(sealed, contents(t, filepath.Join(out, "report.json"))) || !bytes.Equal(log, contents(t, filepath.Join(out, "blocks.jsonl"))) {
		t.Fatal("completed resume was not idempotent", err)
	}
	return r
}

func exerciseResume(t *testing.T, cfg Config) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := jsonFile(path, cfg); err != nil {
		t.Fatal(err)
	}
	want, err := Run(path, filepath.Join(dir, "reference"))
	if err != nil {
		t.Fatal(err)
	}
	log := contents(t, filepath.Join(dir, "reference/blocks.jsonl"))
	for _, tc := range []struct {
		stage         string
		index, reused int
	}{
		{"before-block", 0, 0}, {"prepared", 0, 0}, {"committed", 0, 1},
		{"before-block", 2, 2}, {"prepared", 2, 2}, {"committed", 2, 3},
		{"logged", 2, 3}, {"before-report", 4, 4},
	} {
		t.Run(tc.stage+string(rune('0'+tc.index)), func(t *testing.T) {
			out := filepath.Join(t.TempDir(), "run")
			if _, e := run(path, out, Options{}, stopAt(tc.stage, tc.index)); !errors.Is(e, interrupted) {
				t.Fatal("did not stop at the requested boundary", e)
			}
			r := checkRecovered(t, path, out, want, log, tc.reused)
			if r.InitializationStatsMissing != (tc.stage == "committed" && tc.index == 0) {
				t.Fatal("initialization metrics were fabricated or lost", r.InitializationStatsMissing)
			}
		})
	}
}

func TestResumeAtCommitBoundaries(t *testing.T) { exerciseResume(t, resumeConfig()) }

func TestResumeRepairsOnlyTornTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := jsonFile(path, resumeConfig()); err != nil {
		t.Fatal(err)
	}
	want, err := Run(path, filepath.Join(dir, "reference"))
	if err != nil {
		t.Fatal(err)
	}
	log := contents(t, filepath.Join(dir, "reference/blocks.jsonl"))
	for _, tail := range []string{`{"Height":`, `{"Height":2}`} {
		out := filepath.Join(t.TempDir(), "run")
		if _, err = run(path, out, Options{}, stopAt("committed", 2)); !errors.Is(err, interrupted) {
			t.Fatal(err)
		}
		f, err := os.OpenFile(filepath.Join(out, "blocks.jsonl"), os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.WriteString(tail)
		if err != nil {
			t.Fatal(err)
		}
		f.Close()
		checkRecovered(t, path, out, want, log, 3)
	}
}

func artifactHashes(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	h := map[string][32]byte{}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			h[entry.Name()] = sha256.Sum256(contents(t, filepath.Join(dir, entry.Name())))
		}
	}
	return h
}

func TestResumeRejectsMismatchesWithoutChangingArtifacts(t *testing.T) {
	for _, kind := range []string{"config", "workload", "metadata", "missing-pending", "wrong-pending-root", "invalid-line", "head-ID", "missing-manifest"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			path, out := filepath.Join(dir, "config.json"), filepath.Join(dir, "run")
			cfg := resumeConfig()
			if err := jsonFile(path, cfg); err != nil {
				t.Fatal(err)
			}
			if _, err := run(path, out, Options{}, stopAt("committed", 2)); !errors.Is(err, interrupted) {
				t.Fatal(err)
			}
			switch kind {
			case "config":
				cfg.MaxBatchBytes *= 2
				jsonFile(path, cfg)
			case "workload":
				cfg.Synthetic.Seed++
				jsonFile(path, cfg)
			case "metadata":
				os.WriteFile(filepath.Join(out, "chain-config.json"), []byte("{}\n"), 0600)
			case "missing-pending":
				os.Remove(filepath.Join(out, "pending-block.json"))
			case "missing-manifest":
				os.Remove(filepath.Join(out, "run-manifest.json"))
			case "wrong-pending-root", "head-ID":
				var p pendingBlock
				if err := readJSON(filepath.Join(out, "pending-block.json"), &p); err != nil {
					t.Fatal(err)
				}
				if kind == "head-ID" {
					p.Result.ID[0] ^= 1
				} else {
					p.Result.Root[0] ^= 1
				}
				jsonFile(filepath.Join(out, "pending-block.json"), p)
			case "invalid-line":
				os.WriteFile(filepath.Join(out, "blocks.jsonl"), []byte("invalid complete line\n"), 0600)
			}
			before := artifactHashes(t, out)
			if _, err := RunWithOptions(path, out, Options{Resume: true}); err == nil {
				t.Fatal("accepted incompatible or damaged artifacts")
			}
			if !reflect.DeepEqual(before, artifactHashes(t, out)) {
				t.Fatal("rejected resume changed result artifacts")
			}
		})
	}
}

func TestResumeRunLock(t *testing.T) {
	dir := t.TempDir()
	path, out := filepath.Join(dir, "config.json"), filepath.Join(dir, "run")
	if err := jsonFile(path, resumeConfig()); err != nil {
		t.Fatal(err)
	}
	_, err := run(path, out, Options{}, func(stage string, i int) error {
		if stage == "before-block" && i == 1 {
			_, e := RunWithOptions(path, out, Options{Resume: true})
			if e == nil || !strings.Contains(e.Error(), "already active") {
				t.Fatal("concurrent runner acquired the run", e)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// os.Exit skips all Go defers: this checks actual OS-lock and database recovery,
// including the commit-before-result-log window, not just a returned error.
func TestResumeProcessCrash(t *testing.T) {
	if stage := os.Getenv("STATERA_RESUME_CRASH_STAGE"); stage != "" {
		_, err := run(os.Getenv("STATERA_RESUME_CONFIG"), os.Getenv("STATERA_RESUME_OUTPUT"), Options{}, func(s string, i int) error {
			if s == stage && i == 2 {
				os.Exit(73)
			}
			return nil
		})
		t.Fatal("crash boundary not reached", err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := jsonFile(path, resumeConfig()); err != nil {
		t.Fatal(err)
	}
	want, err := Run(path, filepath.Join(dir, "reference"))
	if err != nil {
		t.Fatal(err)
	}
	log := contents(t, filepath.Join(dir, "reference/blocks.jsonl"))
	exerciseCrashResume(t, path, want, log)
}

func exerciseCrashResume(t *testing.T, path string, want Report, log []byte) {
	t.Helper()
	for _, stage := range []string{"prepared", "committed"} {
		out := filepath.Join(t.TempDir(), "run")
		cmd := exec.Command(os.Args[0], "-test.run=^TestResumeProcessCrash$")
		cmd.Env = append(os.Environ(), "STATERA_RESUME_CRASH_STAGE="+stage, "STATERA_RESUME_CONFIG="+path, "STATERA_RESUME_OUTPUT="+out)
		result, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 73 {
			t.Fatalf("unexpected child exit: %v %s", err, result)
		}
		reused := 2
		if stage == "committed" {
			reused++
		}
		checkRecovered(t, path, out, want, log, reused)
	}
}

func TestResumeMissingDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := jsonFile(path, resumeConfig()); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "absent")
	if _, err := RunWithOptions(path, out, Options{Resume: true}); err == nil {
		t.Fatal("resume created a new run")
	}
	if _, err := os.Stat(out); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("resume created output files", err)
	}
}

func TestResumeFrozenInputAndFailedOutcomes(t *testing.T) {
	dir := t.TempDir()
	path, input := filepath.Join(dir, "config.json"), filepath.Join(dir, "input.json")
	cfg := resumeConfig()
	w, err := workload.Synthetic(*cfg.Synthetic)
	if err != nil {
		t.Fatal(err)
	}
	for i := range w.Blocks[0].Allocations {
		if len(w.Blocks[0].Allocations[i].Code) > 0 {
			w.Blocks[0].Allocations[i].Code = []byte{0x60, 0, 0x60, 0, 0xfd} // REVERT
		}
	}
	if err = jsonFile(input, w); err != nil {
		t.Fatal(err)
	}
	cfg.Synthetic, cfg.WorkloadFile = nil, "input.json"
	if err = jsonFile(path, cfg); err != nil {
		t.Fatal(err)
	}
	want, err := Run(path, filepath.Join(dir, "reference"))
	if err != nil || want.FailedTransactions == 0 {
		t.Fatal("fixture did not include VM failures", err)
	}
	log := contents(t, filepath.Join(dir, "reference/blocks.jsonl"))
	out := filepath.Join(dir, "interrupted")
	if _, err = run(path, out, Options{}, stopAt("committed", 2)); !errors.Is(err, interrupted) {
		t.Fatal(err)
	}
	// Same file name and configuration, changed bytes in the external input.
	var changed block.Workload = w
	changed.Blocks = append([]block.InputBlock(nil), w.Blocks...)
	changed.Blocks[1].Context.Time++
	if err = jsonFile(input, changed); err != nil {
		t.Fatal(err)
	}
	before := artifactHashes(t, out)
	if _, err = RunWithOptions(path, out, Options{Resume: true}); err == nil || !reflect.DeepEqual(before, artifactHashes(t, out)) {
		t.Fatal("changed frozen input was accepted or modified artifacts", err)
	}
	if err = jsonFile(input, w); err != nil {
		t.Fatal(err)
	}
	checkRecovered(t, path, out, want, log, 3)
}
