//go:build rocksdb && cgo

package backend

import (
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
	"testing"
)

func hashInitialOpener(t *testing.T) testOpener {
	return func(t *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
		if c.CodecID != string(codec.Ethereum) {
			t.Skip("native Ethereum only")
		}
		cfg := DefaultHybridConfig(c.WorkloadID)
		cfg.Initialization = InitializationHashCold
		cfg.Store.InitialHeight = uint64(c.InitialHeight)
		cfg.Store.MaxBatchBytes = c.MaxBatchBytes
		cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
		cfg.Cache.Boundary.PolicyIntervalBlocks = 4
		cfg.Cache.Boundary.PolicyOriginHeight = uint64(c.InitialHeight)
		return OpenHybrid(path, cfg, create, ro)
	}
}
func TestHybridHashInitialEVMRootsReceiptsHistoryAndReopen(t *testing.T) {
	t.Setenv("VT_EVM_PROOF_REPORT", os.Getenv("HYBRID_HASH_INITIAL_PROOF_REPORT"))
	t.Setenv("VT_EVM_STATE_REPORT", os.Getenv("HYBRID_HASH_INITIAL_STATE_REPORT"))
	runTestLiveEVMRootsReceiptsHistoryAndReopen(t, hashInitialOpener(t))
}
func TestHybridHashInitialEVMConformance(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, testOpener)
	}{
		{"rejected", runTestRejectedBlockLeavesDurableHeadUnchanged}, {"lifecycle", runTestLifecycleResetSurvivesFinaliseAndRollback},
		{"create2", runTestEVMSelfDestructAndSameAddressCREATE2Redeployment}, {"limit", runTestAtomicLimitFailureDoesNotPublishInitialization},
		{"cancun", runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, hashInitialOpener(t)) })
	}
}
func TestHybridHashInitialColdAndCacheModes(t *testing.T) {
	w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 64, Blocks: 6, TransactionsPerBlock: 16, Contracts: 5, Seed: 71})
	if e != nil {
		t.Fatal(e)
	}
	for _, cache := range []ht.ForestCacheConfig{ht.DefaultForestCacheConfig(), ht.DefaultReservedSharedForestCacheConfig(), ht.DefaultEthereumPrefixBoundaryForestCacheConfig()} {
		t.Run(cache.Mode, func(t *testing.T) {
			cfg := DefaultHybridConfig("hash-cold-modes")
			cfg.Initialization = InitializationHashCold
			cfg.Cache = cache
			b, e := OpenHybrid(filepath.Join(t.TempDir(), "db"), cfg, true, false)
			if e != nil {
				t.Fatal(e)
			}
			defer b.Close()
			for i, input := range w.Blocks {
				if _, e = b.Apply(input, nil); e != nil {
					t.Fatal(i, e)
				}
				if i == 0 {
					s, e := b.CacheStats()
					if e != nil {
						t.Fatal(e)
					}
					snap := b.SnapshotStats()
					if snap.Mode != InitializationHashCold || !snap.ColdCache || !snap.AtomicCommit.InitialHashCommit || snap.AtomicCommit.PrefixRecords != 0 || snap.AtomicCommit.HashRecords == 0 {
						t.Fatal("initial layout", snap)
					}
					if s.Admissions != 0 || s.NodeBodyHits != 0 || s.Boundary.NodeEntries != 0 || s.Boundary.HeatUpdates != 0 || s.Shared.NodeEntries != 0 {
						t.Fatal("cache prewarmed", s)
					}
				}
			}
		})
	}
	cfg := DefaultHybridConfig("hash-config")
	cfg.Initialization = InitializationHashCold
	v := DefaultSnapshotConfig()
	cfg.Snapshot = &v
	if _, e = OpenHybrid(filepath.Join(t.TempDir(), "conflict"), cfg, true, false); e == nil {
		t.Fatal("conflicting initialization accepted")
	}
	cfg.Snapshot = nil
	cfg.Initialization = "typo"
	if _, e = OpenHybrid(filepath.Join(t.TempDir(), "unknown"), cfg, true, false); e == nil {
		t.Fatal("unknown mode accepted")
	}
}
