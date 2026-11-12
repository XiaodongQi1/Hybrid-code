package backend

import (
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"testing"
)

type plainLifeSnapshot struct {
	stateSnapshot
	calls  int
	active bool
}

func (s *plainLifeSnapshot) StorageID(common.Address) (uint64, bool, error) {
	s.calls++
	if s.active {
		return 7, true, nil
	}
	return 0, false, nil
}
func (*plainLifeSnapshot) Close() error { return nil }

type plainVisits struct{ roots int }

func (v *plainVisits) ObserveTrieAccess(p []byte) {
	if len(p) == 0 {
		v.roots++
	}
}

func TestPlainTransactionReusesBaseReadAndSkipsEmptyStorage(t *testing.T) {
	base := emptyView(codec.Ethereum)
	defer base.Close()
	addr := common.Address{19: 1}
	account := state.EmptyAccount()
	raw, e := codec.Ethereum.EncodeAccount(account)
	if e != nil {
		t.Fatal(e)
	}
	if e = base.accounts.Update(state.AccountKey(addr), raw); e != nil {
		t.Fatal(e)
	}
	life := &plainLifeSnapshot{}
	base.snapshot = life
	visits := &plainVisits{}
	base.accounts.SetAccessObserver(visits)
	live := newTransactionTrie(base, false)
	for nonce := uint64(1); nonce <= 3; nonce++ {
		account.Nonce = nonce
		visits.roots = 0
		if e = live.apply(map[common.Address]state.Change{addr: {Account: &account}}); e != nil {
			t.Fatal(e)
		}
		// One current account read plus one update; no second base-account Get.
		if visits.roots != 2 {
			t.Fatal("duplicate account traversal", visits.roots)
		}
		if p := live.storageTrees[addr]; p == nil || p.tree != nil || p.needed {
			t.Fatal("allocated empty storage trie")
		}
		if len(live.plans()) != 0 {
			t.Fatal("published inert storage plan")
		}
	}
	if life.calls != 1 {
		t.Fatal("repeated lifecycle read", life.calls)
	}
	// Empty storage is not proof of an absent lifecycle. An empty-code contract
	// remains a real lifetime and must still be prepared for the backend.
	live = newTransactionTrie(base, false)
	life.active = true
	if e = live.apply(map[common.Address]state.Change{addr: {Account: &account}}); e != nil {
		t.Fatal(e)
	}
	if p := live.storageTrees[addr]; p.tree == nil || !p.active || len(live.plans()) != 1 {
		t.Fatal("dropped empty active lifetime")
	}
	if e = live.apply(map[common.Address]state.Change{addr: {}}); e != nil {
		t.Fatal(e)
	}
	if p := live.storageTrees[addr]; !p.reset || p.tree != nil {
		t.Fatal("lost deletion")
	}
}
