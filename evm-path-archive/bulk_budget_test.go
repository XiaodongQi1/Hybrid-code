package patharchive

import (
	"context"
	"path/filepath"
	"testing"
)

func TestBulkBudgetPreservesStateAndRejectsUnboundedConfig(t *testing.T) {
	w, _, _ := dataset(t)
	var expectedState uint64
	var previous []Record
	for i, limit := range []uint64{128 << 20, 3 << 30} {
		cfg := DefaultConfig("bulk-budget-equivalence")
		cfg.Format = PackedFormat
		cfg.MaxBatchBytes = limit
		cfg.MaxStagingBytes = limit
		a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
		must(t, e)
		for j, b := range w.Blocks {
			out, e := a.ExecuteBlock(context.Background(), b)
			must(t, e)
			if i == 0 {
				previous = append(previous, out)
			} else {
				equal(t, out, previous[j])
			}
		}
		inv, e := a.StorageInventory()
		must(t, e)
		if i == 0 {
			expectedState = uint64(inv.StateBytes)
		} else if uint64(inv.StateBytes) != expectedState {
			t.Fatal("resource ceiling changed retained state bytes")
		}
		must(t, a.Close())
	}
	for _, limit := range []uint64{1023, MaxWriteBudgetBytes + 1} {
		cfg := DefaultConfig("invalid-bulk-budget")
		cfg.MaxBatchBytes = limit
		if _, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false); e == nil {
			t.Fatal("accepted invalid batch limit")
		}
		cfg = DefaultConfig("invalid-bulk-budget")
		cfg.MaxStagingBytes = limit
		if _, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false); e == nil {
			t.Fatal("accepted invalid staging limit")
		}
	}
}
