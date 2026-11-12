package backend

import (
	"fmt"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
)

func levelHybridOpener(stream bool) testOpener {
	return func(t *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
		if c.CodecID != string(codec.Ethereum) {
			t.Skip("HybridTrie uses native Ethereum leaves")
		}
		cfg := DefaultHybridConfig(c.WorkloadID)
		options := kvstore.DefaultOptions(kvstore.LevelDB)
		options.BlockCacheBytes = 1 << 20
		options.WriteBufferBytes = 1 << 20
		cfg.Store.Storage = &options
		cfg.Store.InitialHeight = uint64(c.InitialHeight)
		cfg.Store.MaxBatchBytes = c.MaxBatchBytes
		cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
		cfg.Cache.Boundary.PolicyIntervalBlocks = 4
		cfg.Cache.Boundary.PolicyOriginHeight = uint64(c.InitialHeight)
		if stream {
			snap := DefaultSnapshotConfig()
			snap.BatchBytes = 8192
			snap.ChunkEntries = 8
			snap.CheckpointAccounts = 8
			cfg.Snapshot = &snap
		} else {
			cfg.Initialization = InitializationHashCold
		}
		return OpenHybrid(path, cfg, create, ro)
	}
}

func TestHybridLevelDBLiveEVMRootsReceiptsHistoryAndReopen(t *testing.T) {
	runTestLiveEVMRootsReceiptsHistoryAndReopen(t, levelHybridOpener(false))
}
func TestHybridLevelDBLogicalTraceImport(t *testing.T) {
	runLogicalTraceImport(t, levelHybridOpener(false))
}
func TestHybridLevelDBStreamingInitialization(t *testing.T) {
	runTestLiveEVMRootsReceiptsHistoryAndReopen(t, levelHybridOpener(true))
}
func TestHybridLevelDBExecutionConformance(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, testOpener)
	}{
		{"reject", runTestRejectedBlockLeavesDurableHeadUnchanged},
		{"lifecycle", runTestLifecycleResetSurvivesFinaliseAndRollback},
		{"create2", runTestEVMSelfDestructAndSameAddressCREATE2Redeployment},
		{"cancun", runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID},
		{"limit", runTestAtomicLimitFailureDoesNotPublishInitialization},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, levelHybridOpener(false)) })
	}
}

func executionReadsOpener(engine kvstore.Engine, workers uint32, prefix bool, adaptive ...bool) testOpener {
	return func(t *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
		if c.CodecID != string(codec.Ethereum) {
			t.Skip("HybridTrie uses native Ethereum leaves")
		}
		cfg := DefaultHybridConfig(c.WorkloadID)
		options := kvstore.DefaultOptions(engine)
		options.BlockCacheBytes = 1 << 20
		options.WriteBufferBytes = 1 << 20
		cfg.Store.Storage = &options
		cfg.Store.InitialHeight = uint64(c.InitialHeight)
		cfg.Store.MaxBatchBytes = c.MaxBatchBytes
		cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
		if len(adaptive) > 0 && adaptive[0] {
			cfg.Cache = ht.DefaultAdaptiveEthereumForestCacheConfig()
		}
		cfg.Cache.Boundary.PolicyIntervalBlocks = 1
		cfg.Cache.Boundary.ExecutionReads = ht.DefaultExecutionReadConfig()
		cfg.Cache.Boundary.ExecutionReads.Workers = workers
		allocation, e := ht.BoundaryCacheAllocation(cfg.Cache.Boundary)
		if e != nil {
			return nil, e
		}
		cfg.Cache.Boundary.NodeBudget = ht.ClassBudget{TotalBytes: 8 * allocation.Boundary.SlotBytes}
		if len(adaptive) > 0 && adaptive[0] {
			q := allocation.Boundary.AllocationQuantumBytes
			cfg.Cache.Boundary.NodeBudget = ht.ClassBudget{TotalBytes: 4 * q, AccountReserveBytes: q, StorageReserveBytes: q}
		}
		if prefix {
			cfg.Initialization = InitializationPrefix
		} else {
			cfg.Initialization = InitializationHashCold
		}
		return OpenHybrid(path, cfg, create, ro)
	}
}
func testHybridExecutionReads(t *testing.T, engine kvstore.Engine) {
	for _, workers := range []uint32{0, 1, 2} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			t.Run("adaptive", func(t *testing.T) {
				runTestLiveEVMRootsReceiptsHistoryAndReopen(t, executionReadsOpener(engine, workers, true, true))
			})
			for _, prefix := range []bool{false, true} {
				t.Run(fmt.Sprint("prefix-", prefix), func(t *testing.T) {
					open := executionReadsOpener(engine, workers, prefix)
					runTestLiveEVMRootsReceiptsHistoryAndReopen(t, open)
					for _, tc := range []struct {
						name string
						run  func(*testing.T, testOpener)
					}{
						{"rejected", runTestRejectedBlockLeavesDurableHeadUnchanged},
						{"lifecycle", runTestLifecycleResetSurvivesFinaliseAndRollback},
						{"create2", runTestEVMSelfDestructAndSameAddressCREATE2Redeployment},
						{"cancun", runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID},
					} {
						t.Run(tc.name, func(t *testing.T) { tc.run(t, open) })
					}
				})
			}
		})
	}
}
func TestHybridExecutionReadsLevelDB(t *testing.T) { testHybridExecutionReads(t, kvstore.LevelDB) }
