package workload

import (
	"bytes"
	"encoding/json"
	"math/big"
	"testing"

	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
)

func TestSyntheticPreservesExistingFrozenWorkload(t *testing.T) {
	w, err := Synthetic(Options{BackgroundAccounts: 1000, Blocks: 20, TransactionsPerBlock: 20, Contracts: 4, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := w.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if id != "3f4c7f12fb4ff0c9ecc66f14256c85285a8ed2c92df6950584bbed0b829a6b38" {
		t.Fatal("default workload changed", id)
	}
}
func TestSyntheticCachePatternsAndLimits(t *testing.T) {
	for _, pattern := range []string{"account-hot", "account-sweep", "storage-hot", "rotate", "migrate", "revisit"} {
		for _, contracts := range []int{1, 5} {
			o := Options{BackgroundAccounts: 8, Blocks: 6, TransactionsPerBlock: 12, Contracts: contracts, Pattern: pattern, StorageSlots: 64, Seed: 1}
			a, err := Synthetic(o)
			if err != nil {
				t.Fatal(err)
			}
			b, err := Synthetic(o)
			if err != nil {
				t.Fatal(err)
			}
			ah, _, _ := a.Freeze()
			bh, _, _ := b.Freeze()
			if ah != bh {
				t.Fatal("nondeterministic", pattern)
			}
		}
	}
	for _, o := range []Options{{BackgroundAccounts: 8, Blocks: 3, TransactionsPerBlock: 3, Contracts: 1, Pattern: "unknown"}, {BackgroundAccounts: 8, Blocks: 3, TransactionsPerBlock: 3, Contracts: 1, StorageSlots: 4097}} {
		if _, err := Synthetic(o); err == nil {
			t.Fatal("invalid profile accepted")
		}
	}
}

func TestAccountSweepCoversBackgroundWithoutReplayingWarmup(t *testing.T) {
	o := Options{BackgroundAccounts: 64, Blocks: 15, TransactionsPerBlock: 16, Contracts: 2, Pattern: "account-sweep", Seed: 71}
	w, err := Synthetic(o)
	if err != nil {
		t.Fatal(err)
	}
	seenWires := map[string]bool{}
	seenAccounts := map[common.Address]bool{}
	spent := new(big.Int)
	index := 0
	for _, b := range w.Blocks[1:] {
		for _, wire := range b.Transactions {
			if index%o.BackgroundAccounts == 0 {
				if index > 0 && len(seenAccounts) != o.BackgroundAccounts {
					t.Fatal("incomplete background coverage", len(seenAccounts))
				}
				seenAccounts = map[common.Address]bool{}
			}
			var tx rt.ExperimentTransaction
			parts := bytes.SplitN(wire, []byte{'\n'}, 2)
			if len(parts) != 2 || json.Unmarshal(parts[1], &tx) != nil || tx.To == nil {
				t.Fatal("invalid experiment transaction")
			}
			a := new(big.Int).SetBytes(tx.To[:]).Int64() - 0x100000
			if a < 0 || a >= int64(o.BackgroundAccounts) || len(tx.Data) != 0 || seenAccounts[*tx.To] || seenWires[string(wire)] {
				t.Fatal("invalid recipient or repeated transaction", index)
			}
			if tx.Value.Int64() != int64(index/o.BackgroundAccounts)+1 {
				t.Fatal("pass value did not advance")
			}
			seenAccounts[*tx.To], seenWires[string(wire)] = true, true
			spent.Add(spent, tx.Value)
			index++
		}
	}
	if spent.Cmp(w.Blocks[0].Allocations[0].Balance) >= 0 {
		t.Fatal("sender cannot fund all transfers")
	}
}
