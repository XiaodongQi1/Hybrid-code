package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/comparison"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

// Run the actual controller in a separate process. os.Exit bypasses all Go
// defers, including DB.Close and the directory unlock.
func TestCrashWorker(t *testing.T) {
	dir := os.Getenv("HYBRID_COMPARE_CRASH_FIXTURE")
	if dir == "" {
		t.Skip("subprocess fixture")
	}
	target := os.Getenv("HYBRID_COMPARE_CRASH_POINT")
	ctx := context.WithValue(context.Background(), boundaryKey{}, func(stage, method string, i int) error {
		if fmt.Sprintf("%s/%s/%d", stage, method, i) == target {
			os.Exit(73)
		}
		return nil
	})
	if e := runMode(ctx, filepath.Join(dir, "config.json"), filepath.Join(dir, "resumed"), runOptions{resume: os.Getenv("HYBRID_COMPARE_RESUME") == "1"}); e != nil {
		t.Fatal(e)
	}
	t.Fatal("missed crash point", target)
}
func TestCrashRecoveryLevelDB(t *testing.T) { testRecovery(t, kvstore.LevelDB) }
func testRecovery(t *testing.T, engine kvstore.Engine) {
	t.Helper()
	dir := t.TempDir()
	cp := filepath.Join(dir, "config.json")
	c := config{Engine: engine, Resources: comparison.DefaultResourceBudget(), Methods: allMethods, Suites: allSuites, Synthetic: &workload.Options{BackgroundAccounts: 24, Blocks: 5, Contracts: 3, TransactionsPerBlock: 6, Seed: 7}, Trials: 2, Queries: 8, WarmupBlocks: 1}
	c.Resources.ApplicationCacheBytes = 8 << 20
	c.Resources.BlockCacheBytes = 1 << 20
	c.Resources.GlobalWriteBufferBytes = 1 << 20
	if e := save(cp, c); e != nil {
		t.Fatal(e)
	}
	ctx := context.Background()
	if e := run(ctx, cp, filepath.Join(dir, "continuous")); e != nil {
		t.Fatal(e)
	}
	var preserved []byte
	resumed := filepath.Join(dir, "resumed")
	group := filepath.Join(resumed, "trial-01-archive.json")
	for i, point := range []string{"reference/archive/2", "prepared/archive/2", "committed/archive/3", "committed/path/2", "committed/hybrid/3", "execution/path/2", "query/path/3"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrashWorker$")
		cmd.Env = append(os.Environ(), "HYBRID_COMPARE_CRASH_FIXTURE="+dir, "HYBRID_COMPARE_CRASH_POINT="+point)
		if i > 0 {
			cmd.Env = append(cmd.Env, "HYBRID_COMPARE_RESUME=1")
		}
		output, e := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(e, &exit) || exit.ExitCode() != 73 {
			t.Fatalf("%s: %v %s", point, e, output)
		}
	}
	preserved, e := os.ReadFile(group)
	if e != nil {
		t.Fatal(e)
	}
	if e = runMode(ctx, cp, resumed, runOptions{resume: true}); e != nil {
		t.Fatal(e)
	}
	now, e := os.ReadFile(group)
	if e != nil || string(now) != string(preserved) {
		t.Fatal("sealed trial repeated")
	}
	var control, r reportData
	if e = strictFile(filepath.Join(dir, "continuous", "results.json"), &control); e != nil {
		t.Fatal(e)
	}
	if e = strictFile(filepath.Join(resumed, "results.json"), &r); e != nil {
		t.Fatal(e)
	}
	if r.WorkloadID != control.WorkloadID || r.CorpusID != control.CorpusID || len(r.Trials) != 6 || len(r.Storage) != 3 {
		t.Fatal("incomplete report")
	}
	for i, s := range r.Storage {
		want := control.Storage[i]
		if s.EngineCountersSingleScope || s.Final.Inventory.SHA256 != want.Final.Inventory.SHA256 || !reflect.DeepEqual(s.Increment, want.Increment) || !reflect.DeepEqual(s.Writes, want.Writes) {
			t.Fatalf("%s resumed storage differs\n writes %+v want %+v", s.Method, s.Writes, want.Writes)
		}
	}
	for i, g := range r.Trials {
		if !g.Verified || !g.SamplesComplete || !g.ReadOnlyUnchanged || g.CaseID != control.Trials[i].CaseID || len(g.Blocks) != 4 || len(g.Queries) != 8 {
			t.Fatal("invalid samples", g.Method)
		}
	}
	if paths, _ := filepath.Glob(filepath.Join(resumed, "trial-01-path-interrupted-*")); len(paths) != 2 {
		t.Fatal("expected two interrupted attempts")
	}
	// Changing a bound setting must fail without overwriting finished groups.
	c.QuerySeed++
	if e = save(cp, c); e != nil {
		t.Fatal(e)
	}
	if runMode(ctx, cp, resumed, runOptions{resume: true}) == nil {
		t.Fatal("changed config accepted")
	}
	c.QuerySeed--
	if e = save(cp, c); e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile(group, []byte("{}"), 0600); e != nil {
		t.Fatal(e)
	}
	if runMode(ctx, cp, resumed, runOptions{resume: true}) == nil {
		t.Fatal("damaged trial accepted")
	}
}
