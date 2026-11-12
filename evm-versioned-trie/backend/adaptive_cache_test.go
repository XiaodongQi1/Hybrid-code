package backend

import (
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
	"testing"
)

func adaptiveCacheOpener(engine kvstore.Engine) testOpener {
	return func(t *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
		if c.CodecID != string(codec.Ethereum) {
			t.Skip("native Ethereum only")
		}
		cfg := DefaultHybridConfig(c.WorkloadID)
		o := kvstore.DefaultOptions(engine)
		o.BlockCacheBytes = 1 << 20
		o.WriteBufferBytes = 1 << 20
		cfg.Store.Storage = &o
		cfg.Store.InitialHeight = uint64(c.InitialHeight)
		cfg.Store.MaxBatchBytes = c.MaxBatchBytes
		cfg.Initialization = InitializationHashCold
		cfg.Cache = ht.DefaultAdaptiveEthereumForestCacheConfig()
		cfg.Cache.Boundary.PolicyIntervalBlocks = 2
		cfg.Cache.Boundary.PolicyOriginHeight = uint64(c.InitialHeight)
		cfg.Cache.Boundary.NodeBudget = ht.ClassBudget{TotalBytes: 16 << 10, AccountReserveBytes: 4 << 10, StorageReserveBytes: 4 << 10}
		return OpenHybrid(path, cfg, create, ro)
	}
}
func runAdaptiveConformance(t *testing.T, engine kvstore.Engine) {
	open := adaptiveCacheOpener(engine)
	for _, tc := range []struct {
		name string
		run  func(*testing.T, testOpener)
	}{
		{"roots-receipts-proofs-reopen", runTestLiveEVMRootsReceiptsHistoryAndReopen},
		{"rejected", runTestRejectedBlockLeavesDurableHeadUnchanged},
		{"lifecycle", runTestLifecycleResetSurvivesFinaliseAndRollback},
		{"create2", runTestEVMSelfDestructAndSameAddressCREATE2Redeployment},
		{"cancun", runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID},
		{"limit", runTestAtomicLimitFailureDoesNotPublishInitialization},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, open) })
	}
}
func TestHybridAdaptiveLevelDBConformance(t *testing.T) { runAdaptiveConformance(t, kvstore.LevelDB) }
