package patharchive

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	dq "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type queryCountingEngine struct {
	diskEngine
	gets map[string]int
}

func (e queryCountingEngine) GetSnapshot() (diskSnapshot, error) {
	s, err := e.diskEngine.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return queryCountingSnapshot{s, e.gets}, nil
}

type queryCountingSnapshot struct {
	diskSnapshot
	gets map[string]int
}

func (s queryCountingSnapshot) Get(key []byte) ([]byte, error) {
	s.gets[string(key)]++
	return s.diskSnapshot.Get(key)
}

func TestHistoricalSlotQueryResolvesEachTriePathOnce(t *testing.T) {
	ctx := context.Background()
	w, addresses, slots := dataset(t)
	cfg := DefaultConfig("historical-slot-reads")
	cfg.Format = PackedFormat
	a, err := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
	must(t, err)
	defer a.Close()
	var root mpt.Hash
	for _, block := range w.Blocks {
		rec, err := a.ExecuteBlock(ctx, block)
		must(t, err)
		if block.Height == 3 {
			root = rec.Root
		}
	}
	gets := make(map[string]int)
	a.s.db = queryCountingEngine{a.s.db, gets}
	req := dq.Request{Height: 3, Kind: dq.Storage, Address: addresses[1], Slot: slots[0]}
	resp, err := a.Query(ctx, req)
	must(t, err)
	ans, err := dq.VerifyWithCodec(ctx, root, req, resp, dq.DefaultLimits(), cfg.codec())
	must(t, err)
	if !ans.SlotExists || ans.Value != word(77) {
		t.Fatal("incorrect historical slot")
	}
	r := reader{format: cfg.Format}
	for _, owner := range [][]byte{{0}, append([]byte{1}, state.AccountKey(req.Address)...)} {
		key := indexKey(r.key('P', owner, nil))
		if n := gets[string(key)]; n != 1 {
			t.Fatalf("root index %x read %d times", key, n)
		}
	}
	for key, n := range gets {
		if strings.HasPrefix(key, "XF") || strings.HasPrefix(key, "F") {
			t.Fatalf("unexpected flat-value lookup %x", key)
		}
		if strings.HasPrefix(key, "XP") && n > 1 {
			t.Fatalf("trie path index %x read %d times", key, n)
		}
	}
}
