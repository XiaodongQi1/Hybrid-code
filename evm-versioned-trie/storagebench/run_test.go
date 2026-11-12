package storagebench

import (
	"encoding/json"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
	"github.com/XiaodongQi1/HybridTrie/internal/kvcache"
	"testing"
)

func TestEVMStorageComparisonIdenticalInputsAndSealing(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "config.json")
	out := filepath.Join(dir, "run")
	c := Config{Schema: "versioned-evm-storage/v1", Workload: workload.Options{BackgroundAccounts: 32, Blocks: 4, TransactionsPerBlock: 12, Contracts: 2, Seed: 11}, Storage: kvcache.Options{BlockCacheBytes: 2 << 20, WriteBufferBytes: 1 << 20, OpenFiles: 64}, MaxBatchBytes: 8 << 20}
	if e := writeJSON(config, c); e != nil {
		t.Fatal(e)
	}
	s, e := Run(config, out)
	if e != nil {
		t.Fatal(e)
	}
	if len(s.Methods) != 3 || s.Transactions != 48 || s.TotalInitialAccounts != 35 {
		t.Fatalf("unexpected summary %+v", s)
	}
	for i, r := range s.Methods {
		if !r.Verified || !r.ReadOnlyUnchanged || r.VerifiedRoots != 5 || r.VerifiedQueries != 40 {
			t.Fatalf("unverified method %+v", r)
		}
		wantExecuted := 0
		if i == 0 {
			wantExecuted = 48
		}
		if r.ExecutedEVMTransactions != wantExecuted {
			t.Fatal("EVM execution not reused")
		}
		for _, m := range []Measurement{r.Initial, r.Final} {
			var sum int64
			for _, v := range m.Components {
				sum += v.Bytes()
			}
			if sum != m.StateBytes || m.Projected.Total.Bytes() != m.StateBytes+m.CanonicalRootBytes || m.Projected.OnDiskSSTBytes != m.Projected.SSTBytes {
				t.Fatal("measurement does not reconcile")
			}
		}
		if r.Initial.CanonicalRootBytes != 73 || r.Final.CanonicalRootBytes != 365 {
			t.Fatal("canonical roots not charged identically")
		}
	}
	raw, e := os.ReadFile(filepath.Join(out, "results.json"))
	if e != nil {
		t.Fatal(e)
	}
	var saved Summary
	if e = json.Unmarshal(raw, &saved); e != nil || saved.WorkloadID != s.WorkloadID {
		t.Fatal("saved report mismatch", e)
	}
	before, e := os.ReadFile(filepath.Join(out, "execution.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = Run(config, out); e == nil {
		t.Fatal("existing experiment overwritten")
	}
	after, e := os.ReadFile(filepath.Join(out, "execution.jsonl"))
	if e != nil || string(before) != string(after) {
		t.Fatal("previous execution changed", e)
	}
}
