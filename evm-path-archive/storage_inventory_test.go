package patharchive

import (
	"context"
	"path/filepath"
	"testing"
)

func TestG4StorageInventoryCloses(t *testing.T) {
	w, _, _ := dataset(t)
	for _, format := range []string{LegacyFormat, CompactFormat, FullHistoryFormat, DifferentialFormat, Format, IndexedFormat, PackedFormat} {
		t.Run(format, func(t *testing.T) {
			c := DefaultConfig("g4-inventory")
			c.Format = format
			a, e := Open(filepath.Join(t.TempDir(), "db"), c, true, false)
			must(t, e)
			defer a.Close()
			for _, b := range w.Blocks {
				_, e = a.ExecuteBlock(context.Background(), b)
				must(t, e)
			}
			inventory, e := a.StorageInventory()
			must(t, e)
			kv, e := a.Breakdown()
			must(t, e)
			var all, state uint64
			for _, n := range kv {
				all += uint64(n)
			}
			for _, k := range []string{"current-trie", "current-flat", "reverse-history", "history-directory", "history-index", "history-checkpoints", "hash-location-index", "code"} {
				state += uint64(kv[k])
			}
			if inventory.StateBytes != state || inventory.AllKVBytes != all || inventory.OtherBytes+state != all {
				t.Fatalf("inventory totals: %+v vs %v", inventory, kv)
			}
			if isDifferential(format) {
				encoding, e := a.EncodingMetrics()
				must(t, e)
				if encoding.CheckpointValueBytes+encoding.StructureValueBytes+encoding.NoGainValueBytes != inventory.Components["node-full"] {
					t.Fatal("FULL reason bytes do not close")
				}
			}
		})
	}
}
