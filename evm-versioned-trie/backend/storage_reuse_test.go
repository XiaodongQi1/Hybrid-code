package backend

import (
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

type reuseSnapshot struct {
	stateSnapshot
	db           mpt.Store
	root         mpt.Hash
	readers, ids int
}

func (s *reuseSnapshot) Root() mpt.Hash { return s.root }
func (s *reuseSnapshot) Reader(id uint64, root mpt.Hash) (mpt.Store, error) {
	if id != 0 {
		s.readers++
	}
	return s.db, nil
}
func (s *reuseSnapshot) StorageID(common.Address) (uint64, bool, error) { s.ids++; return 1, true, nil }
func (s *reuseSnapshot) Close() error                                   { return nil }

func TestStorageViewReuseIsolatedFromMutations(t *testing.T) {
	db := mpt.NewMemoryStore()
	mode := codec.Ethereum
	slots, _ := mpt.NewEmpty(db)
	key := common.Hash{31: 1}
	value := common.Hash{31: 7}
	raw, e := mode.EncodeStorage(value)
	if e != nil {
		t.Fatal(e)
	}
	if e = slots.Update(state.SlotKey(key), raw); e != nil {
		t.Fatal(e)
	}
	sr, e := slots.Commit()
	if e != nil {
		t.Fatal(e)
	}
	accounts, _ := mpt.NewEmpty(db)
	addr := common.Address{19: 1}
	a := state.Account{StorageRoot: sr, CodeHash: state.EmptyCode}
	ar, e := mode.EncodeAccount(a)
	if e != nil {
		t.Fatal(e)
	}
	if e = accounts.Update(state.AccountKey(addr), ar); e != nil {
		t.Fatal(e)
	}
	root, e := accounts.Commit()
	if e != nil {
		t.Fatal(e)
	}
	snapshot := &reuseSnapshot{db: db, root: root}
	v, e := openView(snapshot, mode)
	if e != nil {
		t.Fatal(e)
	}
	defer v.Close()
	for i := 0; i < 4; i++ {
		got, e := v.SlotAt(addr, sr, key)
		if e != nil || got != value {
			t.Fatal(got, e)
		}
	}
	if snapshot.readers != 1 || snapshot.ids != 1 {
		t.Fatal("reopened storage", snapshot.readers, snapshot.ids)
	}
	// The returned tree is a private COW handle, including speculative probes.
	private, e := v.storage(addr, sr)
	if e != nil {
		t.Fatal(e)
	}
	if e = private.Delete(state.SlotKey(key)); e != nil {
		t.Fatal(e)
	}
	nonempty, e := v.HasStorageAt(addr, sr, map[common.Hash]common.Hash{key: {}})
	if e != nil || nonempty {
		t.Fatal("probe", e)
	}
	got, e := v.SlotAt(addr, sr, key)
	if e != nil || got != value {
		t.Fatal("polluted base", e)
	}
	if _, e = v.storage(addr, mpt.Hash{1}); e == nil {
		t.Fatal("accepted foreign root")
	}
	v.Close()
	if _, e = v.storage(addr, sr); e == nil {
		t.Fatal("closed view reused")
	}
}
