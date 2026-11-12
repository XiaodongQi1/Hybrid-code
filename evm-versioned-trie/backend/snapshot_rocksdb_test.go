//go:build rocksdb && cgo

package backend

import (
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
	"testing"
)

func snapshotOpener(t *testing.T) testOpener {
	return func(t *testing.T, path string, c vt.Config, create, ro bool) (*Backend, error) {
		if c.CodecID != string(codec.Ethereum) {
			t.Skip("native Ethereum only")
		}
		cfg := DefaultHybridConfig(c.WorkloadID)
		cfg.Store.InitialHeight = uint64(c.InitialHeight)
		cfg.Store.MaxBatchBytes = c.MaxBatchBytes
		cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
		snap := DefaultSnapshotConfig()
		snap.BatchBytes = 8192
		snap.ChunkEntries = 8
		snap.CheckpointAccounts = 8
		cfg.Snapshot = &snap
		return OpenHybrid(path, cfg, create, ro)
	}
}
func TestHybridSnapshotEVMRootsReceiptsHistoryAndReopen(t *testing.T) {
	t.Setenv("VT_EVM_PROOF_REPORT", os.Getenv("HYBRID_SNAPSHOT_PROOF_REPORT"))
	t.Setenv("VT_EVM_STATE_REPORT", os.Getenv("HYBRID_SNAPSHOT_STATE_REPORT"))
	runTestLiveEVMRootsReceiptsHistoryAndReopen(t, snapshotOpener(t))
}
func TestHybridSnapshotEVMConformance(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, testOpener)
	}{
		{"rejected", runTestRejectedBlockLeavesDurableHeadUnchanged}, {"lifecycle", runTestLifecycleResetSurvivesFinaliseAndRollback}, {"create2", runTestEVMSelfDestructAndSameAddressCREATE2Redeployment}, {"cancun", runTestCancunRetainsExistingContractAndEmptyDeploymentGetsID},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.run(t, snapshotOpener(t)) })
	}
}
func TestHybridSnapshotMatchesAtomicInitializationAndContinues(t *testing.T) {
	w, e := workload.Synthetic(workload.Options{BackgroundAccounts: 300, Blocks: 4, TransactionsPerBlock: 16, Contracts: 5, Seed: 23})
	if e != nil {
		t.Fatal(e)
	}
	var expected []*Result
	for _, stream := range []bool{false, true} {
		cfg := DefaultHybridConfig("snapshot-compare")
		cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
		if stream {
			v := DefaultSnapshotConfig()
			v.BatchBytes = 4096
			v.ChunkEntries = 16
			cfg.Snapshot = &v
		}
		path := filepath.Join(t.TempDir(), "db")
		b, e := OpenHybrid(path, cfg, true, false)
		if e != nil {
			t.Fatal(e)
		}
		for i, in := range w.Blocks {
			got, e := b.Apply(in, nil)
			if e != nil {
				t.Fatal(stream, i, e)
			}
			if !stream {
				expected = append(expected, got)
			} else if !reflect.DeepEqual(got, expected[i]) {
				t.Fatal("stream changed EVM result", i)
			}
		}
		if stream {
			s := b.SnapshotStats()
			if s.Import.Batches < 2 || s.Import.PeakBatchBytes > 4096 || s.Build.PeakBatchBytes > 4096 || s.Accounts != uint64(len(w.Blocks[0].Allocations)) {
				t.Fatal(s)
			}
			if _, e = os.Stat(path + ".snapshot-build"); !os.IsNotExist(e) {
				t.Fatal("scratch not removed", e)
			}
		}
		if e = b.Close(); e != nil {
			t.Fatal(e)
		}
	}
}
func snapshotRows(n int) []block.Allocation {
	a := make([]block.Allocation, n)
	for i := range a {
		a[i] = block.Allocation{Address: common.BigToAddress(big.NewInt(int64(i + 1))), Balance: big.NewInt(int64(i + 1))}
		if i%17 == 0 {
			a[i].Code = []byte{0}
			a[i].Storage = map[common.Hash]common.Hash{{}: common.HexToHash("0x12")}
		}
	}
	return a
}
func rowIterator(rows []block.Allocation, fail int) func() (block.Allocation, error) {
	i := 0
	return func() (block.Allocation, error) {
		if fail >= 0 && i == fail {
			return block.Allocation{}, errors.New("source interrupted")
		}
		if i == len(rows) {
			return block.Allocation{}, io.EOF
		}
		a := rows[i]
		i++
		return a, nil
	}
}
func TestHybridSnapshotSourceResumeAndPrefixBinding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	cfg := DefaultHybridConfig("source-resume")
	snap := DefaultSnapshotConfig()
	snap.BatchBytes = 4096
	snap.ChunkEntries = 8
	snap.CheckpointAccounts = 8
	cfg.Snapshot = &snap
	cfg.Cache = ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	rows := snapshotRows(80)
	header := block.InputBlock{Height: 0, ID: common.HexToHash("0x1234"), Context: contextAt(0)}
	b, e := OpenHybrid(path, cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = b.ImportSnapshot(header, rowIterator(rows, 37), nil); e == nil {
		t.Fatal("expected source interruption")
	}
	head, e := b.Head()
	if e != nil || head.Initialized {
		t.Fatal("partial construction published", head, e)
	}
	b.Close()
	b, e = OpenHybrid(path, cfg, false, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	bad := append([]block.Allocation(nil), rows...)
	bad[0].Balance = big.NewInt(999)
	if _, _, e = b.ImportSnapshot(header, rowIterator(bad, -1), nil); e == nil {
		t.Fatal("changed source prefix accepted")
	}
	_, report, e := b.ImportSnapshot(header, rowIterator(rows, -1), nil)
	if e != nil || !report.ResumedBuild || report.Accounts != 80 {
		t.Fatal(report, e)
	}
	v, e := b.ViewAt(0)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	for _, a := range rows {
		got, ok, e := v.Account(a.Address)
		if e != nil || !ok || got.Balance.ToBig().Cmp(a.Balance) != 0 {
			t.Fatal(a.Address, e)
		}
	}
}
func TestHybridSnapshotRejectsExpectedRootBeforePublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	cfg := DefaultHybridConfig("snapshot-root")
	opts := DefaultSnapshotConfig()
	cfg.Snapshot = &opts
	b, e := OpenHybrid(path, cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	head := block.InputBlock{ID: common.HexToHash("0x55"), Context: contextAt(0)}
	wrong := mpt.Hash{1}
	if _, _, e = b.ImportSnapshot(head, rowIterator(snapshotRows(20), -1), &wrong); e == nil {
		t.Fatal("wrong expected root accepted")
	}
	h, _ := b.Head()
	if h.Initialized {
		t.Fatal("published rejected root")
	}
	r, rep, e := b.ImportSnapshot(head, nil, nil)
	if e != nil || r.Root != rep.Root || rep.Accounts != 20 {
		t.Fatal(rep, e)
	}
}
func TestHybridSnapshotDuplicateAndOrderValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	cfg := DefaultHybridConfig("snapshot-order")
	opts := DefaultSnapshotConfig()
	cfg.Snapshot = &opts
	b, e := OpenHybrid(path, cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	rows := snapshotRows(2)
	rows[1].Address = rows[0].Address
	header := block.InputBlock{ID: common.HexToHash("0x56"), Context: contextAt(0)}
	if _, _, e = b.ImportSnapshot(header, rowIterator(rows, -1), nil); e == nil {
		t.Fatal("duplicate accepted")
	}
	h, _ := b.Head()
	if h.Initialized {
		t.Fatal("invalid source published")
	}
}

func TestHybridSnapshotPublishedCleanupOnRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	cfg := DefaultHybridConfig("snapshot-cleanup")
	opts := DefaultSnapshotConfig()
	opts.KeepScratch = true
	cfg.Snapshot = &opts
	b, e := OpenHybrid(path, cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	header := block.InputBlock{ID: common.HexToHash("0x7788"), Context: contextAt(0)}
	_, first, e := b.ImportSnapshot(header, rowIterator(snapshotRows(24), -1), nil)
	if e != nil {
		t.Fatal(e)
	}
	b.Close()
	opts.KeepScratch = false
	b, e = OpenHybrid(path, cfg, false, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	_, again, e := b.ImportSnapshot(header, nil, nil)
	if e != nil || !again.AlreadyComplete || again.Accounts != 24 || again.InputDigest != first.InputDigest {
		t.Fatal(again, e)
	}
	if _, e = os.Stat(path + ".snapshot-build"); !os.IsNotExist(e) {
		t.Fatal("publication retry did not clean owned scratch", e)
	}
}
