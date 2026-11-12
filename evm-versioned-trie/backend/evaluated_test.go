package backend

import (
	"math/big"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

func TestEvaluatedRootMismatchCannotPublish(t *testing.T) {
	b, e := Open(t.TempDir()+"/db", smallConfig(codec.Legacy), true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	a := common.HexToAddress("0x1000")
	genesis := inputAt(0)
	genesis.Allocations = []block.Allocation{{Address: a, Balance: big.NewInt(1)}}
	apply(t, b, genesis, nil)
	head, _ := b.Head()
	value := state.EmptyAccount()
	value.Balance.SetUint64(1)
	value.Nonce = 1
	eval := block.EvaluatedBlock{Accounts: []block.EvaluatedAccount{{Address: a, Value: state.Encode(value)}}}
	if _, e = b.ApplyEvaluated(inputAt(1), mpt.EmptyRootHash, eval); e == nil {
		t.Fatal("wrong expected root accepted")
	}
	got, e := b.Head()
	if e != nil || got != head {
		t.Fatal("mismatch published a head", e)
	}
	trie, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	if e = trie.Update(state.AccountKey(a), state.Encode(value)); e != nil {
		t.Fatal(e)
	}
	want, e := trie.Hash()
	if e != nil {
		t.Fatal(e)
	}
	r, e := b.ApplyEvaluated(inputAt(1), want, eval)
	if e != nil || r.Root != want {
		t.Fatal("valid evaluated block rejected", e)
	}
}
