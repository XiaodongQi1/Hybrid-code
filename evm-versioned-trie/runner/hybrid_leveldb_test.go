package runner

import (
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

func hybridLevelConfig() Config {
	cache := ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	cache.Boundary.PolicyIntervalBlocks = 2
	options := kvstore.DefaultOptions(kvstore.LevelDB)
	options.BlockCacheBytes = 1 << 20
	options.WriteBufferBytes = 1 << 20
	return Config{Schema: "versioned-evm-run/v1", Backend: "hybrid", Codec: codec.Ethereum, HybridStorage: &options, HybridCache: &cache, HybridInitialization: "hash-cold", MaxBatchBytes: 2 << 20, Synthetic: &workload.Options{BackgroundAccounts: 16, Blocks: 3, TransactionsPerBlock: 8, Contracts: 2, Seed: 73}}
}
func TestHybridLevelDBConfiguredRunAndBackendIdentity(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	out := filepath.Join(dir, "run")
	cfg := hybridLevelConfig()
	if e := jsonFile(path, cfg); e != nil {
		t.Fatal(e)
	}
	r, e := Run(path, out)
	if e != nil {
		t.Fatal(e)
	}
	if r.KVEngine != kvstore.LevelDB || r.Transactions != 24 || !r.ReadOnlyReopen || r.Cache.Boundary.HeatUpdates == 0 {
		t.Fatal(r)
	}
	log := contents(t, filepath.Join(out, "blocks.jsonl"))
	again, e := RunWithOptions(path, out, Options{Resume: true})
	if e != nil || again != r || string(log) != string(contents(t, filepath.Join(out, "blocks.jsonl"))) {
		t.Fatal("completed run changed on resume", e)
	}
	// Engine and resource changes are rejected by the immutable resume identity,
	// before opening a different database engine on existing bytes.
	cfg.HybridStorage.Engine = kvstore.RocksDB
	if e = jsonFile(path, cfg); e != nil {
		t.Fatal(e)
	}
	if _, e = RunWithOptions(path, out, Options{Resume: true}); e == nil {
		t.Fatal("changed engine resumed")
	}
	if string(log) != string(contents(t, filepath.Join(out, "blocks.jsonl"))) {
		t.Fatal("failed resume changed results")
	}
}
func TestHybridLevelDBResumeCommitBoundaries(t *testing.T) { exerciseResume(t, hybridLevelConfig()) }
