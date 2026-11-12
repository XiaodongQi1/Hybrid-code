package backend

import (
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

func TestTransactionTrieWritesVisibleBeforeBlockFlush(t *testing.T) {
	base := emptyView(codec.Ethereum)
	defer base.Close()
	live := newTransactionTrie(base, true)
	s := state.New(live)
	addr := common.HexToAddress("0x1234")
	key := common.HexToHash("0x01")
	one := common.HexToHash("0x11")
	two := common.HexToHash("0x22")
	flush := func() {
		t.Helper()
		s.Finalise(true)
		writes, e := s.Changes()
		if e != nil {
			t.Fatal(e)
		}
		if e = live.apply(writes); e != nil {
			t.Fatal(e)
		}
		s.ResetTransaction(live)
		empty, e := s.Changes()
		if e != nil || len(empty) != 0 {
			t.Fatal("previous transaction write set retained", e)
		}
	}
	s.CreateAccount(addr)
	s.SetNonce(addr, 1)
	s.SetState(addr, key, one)
	flush()
	// No database or block commit has run. The next transaction reads the trie.
	if s.GetNonce(addr) != 1 || s.GetState(addr, key) != one {
		t.Fatal("previous transaction not visible")
	}
	a, ok, e := live.Account(addr)
	if e != nil || !ok || a.StorageRoot == mpt.EmptyRootHash {
		t.Fatal("not written to account trie", e)
	}
	snap := s.Snapshot()
	s.SetState(addr, key, two)
	s.RevertToSnapshot(snap)
	s.SetNonce(addr, 2)
	flush()
	if s.GetState(addr, key) != one || s.GetNonce(addr) != 2 {
		t.Fatal("reverted VM write leaked")
	}
	s.SetState(addr, key, two)
	flush()
	expectedStorage, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	raw, e := codec.Ethereum.EncodeStorage(two)
	if e != nil {
		t.Fatal(e)
	}
	if e = expectedStorage.Update(state.SlotKey(key), raw); e != nil {
		t.Fatal(e)
	}
	expected := state.EmptyAccount()
	expected.Nonce = 2
	expected.StorageRoot, e = expectedStorage.Hash()
	if e != nil {
		t.Fatal(e)
	}
	expectedAccounts, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	raw, e = codec.Ethereum.EncodeAccount(expected)
	if e != nil {
		t.Fatal(e)
	}
	if e = expectedAccounts.Update(state.AccountKey(addr), raw); e != nil {
		t.Fatal(e)
	}
	want, _ := expectedAccounts.Hash()
	got, e := live.accounts.Hash()
	if e != nil || want != got {
		t.Fatal("transaction roots", got, want, e)
	}
	if live.trace[addr].Slots[key] != two {
		t.Fatal("trace did not retain final value")
	}
	checkpoint := s.Snapshot()
	s.CreateAccount(addr)
	if s.GetState(addr, key) != (common.Hash{}) {
		t.Fatal("new VM incarnation read old live storage")
	}
	s.RevertToSnapshot(checkpoint)
	// Deletion followed by recreation must not retain an earlier slot incarnation.
	s.SelfDestruct(addr)
	flush()
	s.CreateAccount(addr)
	s.SetNonce(addr, 3)
	flush()
	if s.GetState(addr, key) != (common.Hash{}) || len(live.trace[addr].Slots) != 0 || !live.trace[addr].ResetStorage {
		t.Fatal("recreated account retained old slots")
	}
	if live.storageTrees[addr].tree == nil || !live.storageTrees[addr].reset {
		t.Fatal("missing reset storage trie")
	}
}
