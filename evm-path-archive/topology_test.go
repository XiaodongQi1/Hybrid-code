package patharchive

import (
	"context"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/forest"
	ds "github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// Exercise path splits/merges, removal of entire storage trees, identical
// roots in distinct contracts, and resurrection of an earlier node hash.
func TestRandomizedPathHistoryAgainstRebuiltTries(t *testing.T) {
	for _, format := range []string{DifferentialFormat, Format, IndexedFormat, PackedFormat} {
		ctx := context.Background()
		cfg := DefaultConfig("topology")
		cfg.Format = format
		a, e := Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
		must(t, e)
		defer a.Close()
		rng := rand.New(rand.NewSource(20261016))
		values := map[common.Address]map[common.Hash]common.Hash{}
		var addresses []common.Address
		var slots []common.Hash
		for i := 0; i < 24; i++ {
			addresses = append(addresses, common.BytesToAddress(word(uint64(100+i)).Bytes()))
		}
		for i := 0; i < 8; i++ {
			slots = append(slots, word(uint64(i)))
		}
		roots := []mpt.Hash{mpt.EmptyRootHash}
		for h := uint64(1); h <= 24; h++ {
			v, e := forest.Open(a.s.head.Root, a.s.live(ctx))
			must(t, e)
			changes := map[common.Address]ds.Change{}
			for _, addr := range addresses {
				action := rng.Intn(5)
				if action == 0 {
					delete(values, addr)
					changes[addr] = ds.Change{}
					continue
				}
				if action == 1 {
					continue
				}
				account, ok, e := v.Account(addr)
				must(t, e)
				if !ok {
					account = ds.EmptyAccount()
					account.Nonce = 1
					values[addr] = map[common.Hash]common.Hash{}
				}
				updates := map[common.Hash]common.Hash{}
				for _, slot := range slots {
					if rng.Intn(3) == 0 {
						val := word(uint64(rng.Intn(4)))
						updates[slot] = val
						if val == (common.Hash{}) {
							delete(values[addr], slot)
						} else {
							values[addr][slot] = val
						}
					}
				}
				changes[addr] = ds.Change{Account: &account, Slots: updates}
			}
			plan, e := v.Materialize(changes, a.s.cfg.MaxStagingBytes)
			must(t, e)
			// Fresh trees from the independent logical map; no archive node reads.
			mem := mpt.NewMemoryStore()
			at, e := mpt.NewEmpty(mem)
			must(t, e)
			for addr, words := range values {
				st, e := mpt.NewEmpty(mem)
				must(t, e)
				for slot, val := range words {
					must(t, st.Update(ds.SlotKey(slot), val.Bytes()))
				}
				sr, e := st.Commit()
				must(t, e)
				account := ds.EmptyAccount()
				account.Nonce = 1
				account.StorageRoot = sr
				must(t, at.Update(ds.AccountKey(addr), ds.Encode(account)))
			}
			want, e := at.Commit()
			must(t, e)
			equal(t, plan.Root, want)
			_, e = a.s.commit(ctx, plan, Record{Height: h, ID: word(h), Root: plan.Root, Input: []byte("{}")})
			must(t, e)
			roots = append(roots, want)
		}
		verifyHistory(t, a, roots, addresses, slots)
	}
}
