//go:build rocksdb && cgo

package runner

import (
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"testing"
)

func TestHybridConfiguredEVMRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	cfg := Config{Schema: "versioned-evm-run/v1", Backend: "hybrid-rocksdb", Codec: codec.Ethereum, Synthetic: &workload.Options{BackgroundAccounts: 32, Blocks: 3, TransactionsPerBlock: 12, Contracts: 3, Seed: 37}, MaxBatchBytes: 8 << 20}
	cache := ht.DefaultForestCacheConfig()
	cache.Slots = 2
	cfg.HybridCache = &cache
	if err := jsonFile(path, cfg); err != nil {
		t.Fatal(err)
	}
	r, err := Run(path, filepath.Join(dir, "hybrid"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Backend != cfg.Backend || r.ExecutedBlocks != 3 || r.Transactions != 36 || r.FailedTransactions != 0 || !r.ReadOnlyReopen || r.Cache.Partitions > 2 || r.Cache.NodeBodyHits == 0 || r.AccountProofs != 128 || r.StorageProofs != 512 {
		t.Fatal(r)
	}
	if _, err = Run(path, filepath.Join(dir, "hybrid")); err == nil {
		t.Fatal("existing output overwritten")
	}
	// Freeze once, then execute the identical transactions through the old
	// independent physical backend and compare final roots and workload IDs.
	cfg.Backend = "versioned"
	cfg.HybridCache = nil
	cfg.Synthetic = nil
	cfg.WorkloadFile = "hybrid/workload.json"
	cfg.BlockCacheBytes = 2 << 20
	cfg.WriteBufferBytes = 1 << 20
	cfg.OpenFiles = 64
	if err = jsonFile(path, cfg); err != nil {
		t.Fatal(err)
	}
	old, err := Run(path, filepath.Join(dir, "versioned"))
	if err != nil {
		t.Fatal(err)
	}
	if old.FinalRoot != r.FinalRoot || old.WorkloadID != r.WorkloadID || old.Transactions != r.Transactions {
		t.Fatal("backends differ", old, r)
	}
}

func TestHybridSharedConfiguredEVMRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shared.json")
	cache := ht.DefaultReservedSharedForestCacheConfig()
	cfg := Config{Schema: "versioned-evm-run/v1", Backend: "hybrid-rocksdb", Codec: codec.Ethereum, HybridCache: &cache, Synthetic: &workload.Options{BackgroundAccounts: 32, Blocks: 3, TransactionsPerBlock: 16, Contracts: 5, Pattern: "rotate", Seed: 71}, MaxBatchBytes: 8 << 20}
	if err := jsonFile(path, cfg); err != nil {
		t.Fatal(err)
	}
	r, err := Run(path, filepath.Join(dir, "run"))
	if err != nil {
		t.Fatal(err)
	}
	if r.Cache.Mode != ht.ReservedSharedCache || !r.ReadOnlyReopen || r.Transactions != 48 || r.Cache.NodeBodyHits == 0 {
		t.Fatal(r)
	}
	if _, err = Run(path, filepath.Join(dir, "run")); err == nil {
		t.Fatal("overwrote output")
	}
}

func TestHybridBoundaryConfiguredEVMRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "boundary.json")
	cache := ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	cache.Boundary.PolicyIntervalBlocks = 4
	cfg := Config{Schema: "versioned-evm-run/v1", Backend: "hybrid-rocksdb", Codec: codec.Ethereum, HybridCache: &cache, Synthetic: &workload.Options{BackgroundAccounts: 32, Blocks: 6, TransactionsPerBlock: 16, Contracts: 5, Pattern: "migrate", Seed: 71}, MaxBatchBytes: 8 << 20}
	if e := jsonFile(path, cfg); e != nil {
		t.Fatal(e)
	}
	r, e := Run(path, filepath.Join(dir, "run"))
	if e != nil {
		t.Fatal(e)
	}
	if r.Cache.Mode != ht.PrefixBoundaryCache || r.Cache.HashAllocatedBytes != 0 || r.Cache.Boundary.PolicyRuns != 1 || !r.ReadOnlyReopen || r.Transactions != 96 || r.Cache.NodeBodyHits == 0 || r.Cache.Boundary.AccessEvents == 0 {
		t.Fatal(r)
	}
	if _, e = Run(path, filepath.Join(dir, "run")); e == nil {
		t.Fatal("overwrote sealed output")
	}
}

func TestHybridHashInitialConfiguredEVMRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hash-initial.json")
	cache := ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	cfg := Config{Schema: "versioned-evm-run/v1", Backend: "hybrid-rocksdb", Codec: codec.Ethereum, HybridInitialization: "hash-cold", HybridCache: &cache, Synthetic: &workload.Options{BackgroundAccounts: 64, Blocks: 5, TransactionsPerBlock: 16, Contracts: 5, Pattern: "migrate", Seed: 71}, MaxBatchBytes: 8 << 20}
	if e := jsonFile(path, cfg); e != nil {
		t.Fatal(e)
	}
	r, e := Run(path, filepath.Join(dir, "hash"))
	if e != nil {
		t.Fatal(e)
	}
	if r.Snapshot.Mode != "hash-cold" || !r.Snapshot.ColdCache || !r.Snapshot.AtomicCommit.InitialHashCommit || r.Snapshot.AtomicCommit.PrefixRecords != 0 || r.Snapshot.AtomicCommit.HashRecords == 0 || !r.ReadOnlyReopen || r.Transactions != 80 || r.Cache.Boundary.HeatUpdates == 0 {
		t.Fatal(r)
	}
	cfg.HybridInitialization = "prefix"
	if e = jsonFile(path, cfg); e != nil {
		t.Fatal(e)
	}
	prefix, e := Run(path, filepath.Join(dir, "prefix"))
	if e != nil {
		t.Fatal(e)
	}
	if prefix.WorkloadID != r.WorkloadID || prefix.FinalRoot != r.FinalRoot || prefix.AccountProofs != r.AccountProofs || prefix.StorageProofs != r.StorageProofs {
		t.Fatal("initialization changed workload/state", prefix, r)
	}
}
