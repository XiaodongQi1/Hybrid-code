//go:build rocksdb && cgo

package backend

import (
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
	"testing"
)

func hybridTestOpener(t *testing.T) testOpener {
	return func(t *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
		if c.CodecID != string(codec.Ethereum) {
			t.Skip("live HybridTrie uses native Ethereum codec")
		}
		cfg := DefaultHybridConfig(c.WorkloadID)
		cfg.Store.InitialHeight = uint64(c.InitialHeight)
		cfg.Store.MaxBatchBytes = c.MaxBatchBytes
		cfg.Cache.Slots = 2 // Exercise overflow storage through disk readers.
		return OpenHybrid(path, cfg, create, ro)
	}
}
func TestHybridEVMRootsReceiptsHistoryAndReopen(t *testing.T) {
	t.Setenv("VT_EVM_PROOF_REPORT", os.Getenv("HYBRID_EVM_PROOF_REPORT"))
	t.Setenv("VT_EVM_STATE_REPORT", os.Getenv("HYBRID_EVM_STATE_REPORT"))
	runTestLiveEVMRootsReceiptsHistoryAndReopen(t, hybridTestOpener(t))
}
func TestHybridEVMConformance(t *testing.T) {
	for _, test := range []struct {
		name string
		run  func(*testing.T, testOpener)
	}{
		{"rejected-block", runTestRejectedBlockLeavesDurableHeadUnchanged},
		{"journal-lifecycle", runTestLifecycleResetSurvivesFinaliseAndRollback},
		{"create2-redeploy", runTestEVMSelfDestructAndSameAddressCREATE2Redeployment},
		{"atomic-limit", runTestAtomicLimitFailureDoesNotPublishInitialization},
		{"cancun-empty-deployment", runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID},
	} {
		t.Run(test.name, func(t *testing.T) { test.run(t, hybridTestOpener(t)) })
	}
}

func sharedTestOpener(t *testing.T) testOpener {
	return func(t *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
		if c.CodecID != string(codec.Ethereum) {
			t.Skip("Hybrid uses native Ethereum codec")
		}
		cfg := DefaultHybridConfig(c.WorkloadID)
		cfg.Store.InitialHeight = uint64(c.InitialHeight)
		cfg.Store.MaxBatchBytes = c.MaxBatchBytes
		cfg.Cache = ht.DefaultReservedSharedForestCacheConfig()
		return OpenHybrid(path, cfg, create, ro)
	}
}
func TestHybridSharedEVMRootsReceiptsHistoryAndReopen(t *testing.T) {
	t.Setenv("VT_EVM_PROOF_REPORT", os.Getenv("HYBRID_SHARED_PROOF_REPORT"))
	t.Setenv("VT_EVM_STATE_REPORT", os.Getenv("HYBRID_SHARED_STATE_REPORT"))
	runTestLiveEVMRootsReceiptsHistoryAndReopen(t, sharedTestOpener(t))
}
func TestHybridSharedEVMConformance(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, testOpener)
	}{
		{"rejected", runTestRejectedBlockLeavesDurableHeadUnchanged},
		{"lifecycle", runTestLifecycleResetSurvivesFinaliseAndRollback},
		{"create2", runTestEVMSelfDestructAndSameAddressCREATE2Redeployment},
		{"limit", runTestAtomicLimitFailureDoesNotPublishInitialization},
		{"cancun", runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, sharedTestOpener(t)) })
	}
}

func TestHybridEVMCachePartitionsAndDisabledCache(t *testing.T) {
	w, err := workload.Synthetic(workload.Options{BackgroundAccounts: 32, Blocks: 6, TransactionsPerBlock: 24, Contracts: 6, Seed: 29})
	if err != nil {
		t.Fatal(err)
	}
	var expected []mpt.Hash
	for _, slots := range []uint32{0, 1, 2, 4} {
		t.Run(big.NewInt(int64(slots)).String(), func(t *testing.T) {
			cfg := DefaultHybridConfig("hybrid-cache-evm")
			cfg.Cache.Slots = slots
			b, err := OpenHybrid(filepath.Join(t.TempDir(), "db"), cfg, true, false)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			for i, input := range w.Blocks {
				r, err := b.Apply(input, nil)
				if err != nil {
					t.Fatal(i, err)
				}
				if slots == 0 {
					expected = append(expected, r.Root)
				} else if r.Root != expected[i] {
					t.Fatal("cache altered EVM root", i)
				}
			}
			s, err := b.CacheStats()
			if err != nil {
				t.Fatal(err)
			}
			if s.Partitions > uint64(slots) || slots > 0 && s.NodeBodyHits == 0 || slots == 0 && s.Partitions != 0 || s.DiskReaders == 0 {
				t.Fatal("cache accounting", s)
			}
			if s.NodeAllocatedBytes > uint64(slots)*cfg.Cache.PerTrie.NodeBytes || s.PlanAllocatedBytes > uint64(slots)*cfg.Cache.PerTrie.PlanBytes || s.PolicyAllocatedBytes > uint64(slots)*cfg.Cache.PerTrie.Policy.StatsBytes {
				t.Fatal("forest cache exceeded configured bound", s)
			}
			t.Logf("slots=%d partitions=%d N/H=%d/%d bytes execution hits/misses=%d/%d disk readers=%d", slots, s.Partitions, s.NodeAllocatedBytes, s.HashAllocatedBytes, s.NodeBodyHits, s.NodeBodyMisses, s.DiskReaders)
		})
	}
}

func TestHybridEVMFull48BitHeightAndBlockID(t *testing.T) {
	cfg := DefaultHybridConfig("hybrid-wide-height")
	cfg.Store.InitialHeight = 1<<40 + 17
	b, err := OpenHybrid(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	h := cfg.Store.InitialHeight
	genesis := block.InputBlock{Height: h, ID: common.HexToHash("0x1234"), Context: contextAt(h), Allocations: []block.Allocation{{Address: common.HexToAddress("0x1000"), Balance: big.NewInt(10)}}}
	r, err := b.Apply(genesis, nil)
	if err != nil {
		t.Fatal(err)
	}
	head, err := b.Head()
	if err != nil || head.Height != h || head.CommitID != [32]byte(genesis.ID) || head.Root != r.Root {
		t.Fatal(head, err)
	}
	local, err := b.store.(*hybridStore).store.Head()
	if err != nil || local.CommitID == head.CommitID {
		t.Fatal("local CAS substituted for EVM block ID", local, err)
	}
	input := block.InputBlock{Height: h + 1, ID: common.HexToHash("0x5678"), ParentID: genesis.ID, Context: contextAt(h + 1)}
	if _, err = b.Apply(input, nil); err != nil {
		t.Fatal(err)
	}
	v, err := b.ViewAt(h)
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if v.snapshot.CommitID() != [32]byte(genesis.ID) {
		t.Fatal("historical EVM block ID")
	}
	input.Height = ht.MaxHeight + 1
	if _, err = b.Apply(input, nil); !errors.Is(err, vt.ErrLimit) {
		t.Fatal("height truncation", err)
	}
}

func TestHybridEVMBusyCloseStaleViewAndMissingAncestor(t *testing.T) {
	cfg := DefaultHybridConfig("hybrid-view-errors")
	b, err := OpenHybrid(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	genesis := inputAt(0)
	genesis.Allocations = []block.Allocation{{Address: common.HexToAddress("0x1000"), Balance: big.NewInt(10)}}
	apply(t, b, genesis, nil)
	old, err := b.ViewAt(0)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Close(); !errors.Is(err, vt.ErrBusy) {
		t.Fatal("busy close", err)
	}
	apply(t, b, inputAt(1), nil)
	if _, err = b.commit(old, 2, [32]byte(inputAt(2).ID), map[common.Address]state.Change{}); !errors.Is(err, vt.ErrConflict) {
		t.Fatal("stale base accepted", err)
	}
	old.Close()
	apply(t, b, inputAt(2), nil)
	if err = b.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.Store.InitialHeight = 100
	b, err = OpenHybrid(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	if err != nil {
		t.Fatal(err)
	}
	from, target := common.HexToAddress("0x1000"), common.HexToAddress("0x2000")
	genesis = block.InputBlock{Height: 100, ID: common.HexToHash("0x100"), Context: contextAt(100), Allocations: []block.Allocation{{Address: from, Balance: big.NewInt(10)}, {Address: target, Balance: new(big.Int), Code: common.FromHex("0x60004060005260206000f3")}}}
	apply(t, b, genesis, nil)
	head, _ := b.Head()
	input := block.InputBlock{Height: 101, ID: common.HexToHash("0x101"), ParentID: genesis.ID, Context: contextAt(101), Transactions: [][]byte{encoded(t, from, target, 0, 100_000)}}
	if _, err = b.Apply(input, nil); !errors.Is(err, vt.ErrNotFound) {
		t.Fatal("missing pre-snapshot BLOCKHASH history silently invented", err)
	}
	if got, _ := b.Head(); got != head {
		t.Fatal("missing ancestor published block")
	}
}

func boundaryTestOpener(t *testing.T) testOpener {
	return func(t *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
		if c.CodecID != string(codec.Ethereum) {
			t.Skip("native Ethereum only")
		}
		cfg := DefaultHybridConfig(c.WorkloadID)
		cfg.Store.InitialHeight = uint64(c.InitialHeight)
		cfg.Store.MaxBatchBytes = c.MaxBatchBytes
		cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
		cfg.Cache.Boundary.PolicyOriginHeight = uint64(c.InitialHeight)
		cfg.Cache.Boundary.PolicyIntervalBlocks = 4
		return OpenHybrid(path, cfg, create, ro)
	}
}
func TestHybridBoundaryEVMRootsReceiptsHistoryAndReopen(t *testing.T) {
	t.Setenv("VT_EVM_PROOF_REPORT", os.Getenv("HYBRID_BOUNDARY_PROOF_REPORT"))
	t.Setenv("VT_EVM_STATE_REPORT", os.Getenv("HYBRID_BOUNDARY_STATE_REPORT"))
	runTestLiveEVMRootsReceiptsHistoryAndReopen(t, boundaryTestOpener(t))
}
func TestHybridBoundaryEVMConformance(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, testOpener)
	}{
		{"rejected", runTestRejectedBlockLeavesDurableHeadUnchanged}, {"lifecycle", runTestLifecycleResetSurvivesFinaliseAndRollback}, {"create2", runTestEVMSelfDestructAndSameAddressCREATE2Redeployment}, {"limit", runTestAtomicLimitFailureDoesNotPublishInitialization}, {"cancun", runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, boundaryTestOpener(t)) })
	}
}
