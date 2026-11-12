package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
	"testing"
)

func TestConfiguredRunAndRefuseExistingOutput(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	out := filepath.Join(dir, "run")
	cfg := Config{Schema: "versioned-evm-run/v1", Codec: codec.Ethereum, Synthetic: &workload.Options{BackgroundAccounts: 32, Blocks: 3, TransactionsPerBlock: 12, Contracts: 2, Seed: 17}, BlockCacheBytes: 2 << 20, WriteBufferBytes: 1 << 20, OpenFiles: 64, MaxBatchBytes: 1 << 20}
	if e := jsonFile(path, cfg); e != nil {
		t.Fatal(e)
	}
	r, e := Run(path, out)
	if e != nil {
		t.Fatal(e)
	}
	if r.ExecutedBlocks != 3 || r.Transactions != 36 || r.FailedTransactions != 0 || !r.ReadOnlyReopen || r.AccountProofs != 128 || r.StorageProofs != 512 {
		t.Fatalf("unexpected report %+v", r)
	}
	log, e := os.ReadFile(filepath.Join(out, "blocks.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = Run(path, out); e == nil {
		t.Fatal("existing results overwritten")
	}
	got, e := os.ReadFile(filepath.Join(out, "blocks.jsonl"))
	if e != nil || string(got) != string(log) {
		t.Fatal("previous results changed", e)
	}
	// Use the same frozen input through a relative WorkloadFile path, then
	// compare the roots across independently created databases.
	cfg.Synthetic = nil
	cfg.WorkloadFile = "run/workload.json"
	if e = jsonFile(path, cfg); e != nil {
		t.Fatal(e)
	}
	r2, e := Run(path, filepath.Join(dir, "frozen"))
	if e != nil || r2.FinalRoot != r.FinalRoot || r2.WorkloadID != r.WorkloadID {
		t.Fatal("frozen workload differs", e)
	}
	var saved Report
	raw, e := os.ReadFile(filepath.Join(out, "report.json"))
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &saved); e != nil || saved != r {
		t.Fatal("report mismatch", e)
	}
}
