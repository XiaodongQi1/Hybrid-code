package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"

	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/comparison"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

func TestComparisonSuites(t *testing.T) {
	dir := t.TempDir()
	c := config{
		Engine: kvstore.LevelDB, Resources: comparison.DefaultResourceBudget(),
		Methods: allMethods, Suites: allSuites,
		Synthetic: &workload.Options{BackgroundAccounts: 12, Blocks: 3, Contracts: 2, TransactionsPerBlock: 4, Seed: 17},
		Trials:    2, WarmupBlocks: 1, Queries: 8,
	}
	c.Resources.ApplicationCacheBytes = 8 << 20
	c.Resources.BlockCacheBytes = 1 << 20
	c.Resources.GlobalWriteBufferBytes = 1 << 20
	cp := filepath.Join(dir, "config.json")
	execute := func(name string) reportData {
		t.Helper()
		if e := save(cp, c); e != nil {
			t.Fatal(e)
		}
		path := filepath.Join(dir, name)
		opts := runOptions{}
		if name == "all" {
			if e := runMode(context.Background(), cp, path, runOptions{snapshotOnly: true}); e != nil {
				t.Fatal(e)
			}
			if _, e := os.Stat(filepath.Join(path, "snapshot.json")); e != nil {
				t.Fatal(e)
			}
			for _, absent := range []string{"artifacts", "reference", "results.json", "state-archive", "state-path", "state-hybrid"} {
				if _, e := os.Stat(filepath.Join(path, absent)); !os.IsNotExist(e) {
					t.Fatalf("snapshot preparation unexpectedly created %s: %v", absent, e)
				}
			}
			if e := runMode(context.Background(), cp, path, runOptions{resume: true, throughStorage: true}); e != nil {
				t.Fatal(e)
			}
			for _, method := range c.Methods {
				if _, e := os.Stat(filepath.Join(path, "storage-"+method+".json.seal")); e != nil {
					t.Fatal(e)
				}
			}
			if files, e := filepath.Glob(filepath.Join(path, "trial-*")); e != nil || len(files) != 0 {
				t.Fatalf("storage stage created trials: %v, %v", files, e)
			}
			if _, e := os.Stat(filepath.Join(path, "results.json")); !os.IsNotExist(e) {
				t.Fatalf("storage stage finalized timing report: %v", e)
			}
			opts.resume = true
		}
		if e := runMode(context.Background(), cp, path, opts); e != nil {
			t.Fatal(e)
		}
		var r reportData
		if e := strictFile(filepath.Join(path, "results.json"), &r); e != nil {
			t.Fatal(e)
		}
		return r
	}
	r := execute("all")
	if len(r.Storage) != 3 || len(r.Trials) != 6 || r.Trials[3].Method != "path" || r.CorpusID == "" {
		t.Fatal("incomplete or unrotated comparison")
	}
	for _, s := range r.Storage {
		if !s.Initial.Complete || !s.Final.Complete || s.Writes.Commits != 3 || s.StateBytes != int64(s.Final.Inventory.State.Bytes())-int64(s.Initial.Inventory.State.Bytes()) {
			t.Fatal("storage did not exclude snapshot", s.Method)
		}
	}
	for _, g := range r.Trials {
		if !g.Verified || !g.ReadOnlyUnchanged || !g.SamplesComplete || len(g.Blocks) != 2 || len(g.Queries) != 8 || g.Blocks[0].Height != 2 || g.QueryEndToEnd.Count != 8 {
			t.Fatal("incomplete trial", g.Method)
		}
		if g.CacheAfterWarmup == nil || g.CacheAfterExecution == nil || g.CacheAfterWarmup.AllocatedBytes == 0 ||
			g.CacheAfterExecution.AllocatedBytes != g.CacheAfterWarmup.AllocatedBytes ||
			g.CacheAfterWarmup.AllocatedBytes != r.Trials[0].CacheAfterWarmup.AllocatedBytes ||
			g.CacheAfterExecution.Hits <= g.CacheAfterWarmup.Hits {
			t.Fatal("missing or inactive matched execution cache", g.Method, g.CacheAfterWarmup, g.CacheAfterExecution)
		}
		for _, b := range g.Blocks {
			if b.PrepareNS+b.EVMNS+b.MaterializeNS+b.PublishNS != b.TotalNS {
				t.Fatal("block timing did not close")
			}
		}
		for _, q := range g.Queries {
			if q.ServiceNS+q.VerifyNS != q.TotalNS {
				t.Fatal("query timing did not close")
			}
		}
	}
	if e := run(context.Background(), cp, filepath.Join(dir, "all")); e == nil {
		t.Fatal("existing output accepted")
	}
	c.Methods = []string{"hybrid"}
	c.Suites = []string{"historical-query"}
	c.Synthetic = nil
	c.WorkloadFile = "all/workload.json"
	c.QueryFile = "all/queries.json"
	c.Trials = 1
	c.PrefixSnapshot = true
	second := execute("explicit")
	g := second.Trials[0]
	if r.WorkloadID != second.WorkloadID || len(second.Storage) != 0 || len(g.Blocks) != 0 || len(g.Queries) != 8 || g.Queries[0].HeightDistribution != "explicit" {
		t.Fatal("frozen-file selection changed")
	}
	var cases []q.Request
	if e := strictFile(filepath.Join(dir, "all", "queries.json"), &cases); e != nil {
		t.Fatal(e)
	}
	for _, v := range g.Queries {
		if v.Height != cases[v.Index].Height || v.Kind != cases[v.Index].Kind {
			t.Fatal("request order changed")
		}
	}
	c.Methods = []string{"archive", "path"}
	c.Suites = []string{"execution"}
	r = execute("execution")
	for _, g := range r.Trials {
		if len(g.Queries) != 0 || !g.Verified || len(g.Blocks) != 2 {
			t.Fatal("execution-only selection")
		}
	}
	c.Methods = []string{"archive"}
	c.Suites = []string{"storage"}
	r = execute("storage")
	if len(r.Storage) != 1 || len(r.Trials) != 0 {
		t.Fatal("storage-only selection")
	}
}

func TestRejectAmbiguousConfig(t *testing.T) {
	for _, selection := range [][]string{{}, {"path", "path"}, {"legacy"}} {
		if validateSelection(selection, allMethods) == nil {
			t.Fatal(selection)
		}
	}
	d := t.TempDir()
	p := filepath.Join(d, "bad.json")
	for _, raw := range []string{`{"Engien":"rocksdb"}`, `{} {}`} {
		if e := os.WriteFile(p, []byte(raw), 0600); e != nil {
			t.Fatal(e)
		}
		var c config
		if strictFile(p, &c) == nil {
			t.Fatal(raw)
		}
	}
	// Summarize uses nearest-rank quantiles and preserves input order.
	v := []int64{100, 2, 3, 1}
	dist := summarize(v)
	if dist.P50NS != 2 || dist.P95NS != 100 || !slices.Equal(v, []int64{100, 2, 3, 1}) {
		b, _ := json.Marshal(dist)
		t.Fatal(string(b))
	}
}
