package backend

import (
	"bytes"
	"fmt"
	"sort"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// transactionTrie applies each completed transaction's key writes to one live
// account/storage forest. It contains no node cache copy or parent-link journal.
// The initial root Copy is constant-size; MPT updates only changed paths.
type transactionTrie struct {
	base         *View
	accounts     *mpt.Trie
	storageTrees map[common.Address]*storagePlan
	codes        map[common.Hash][]byte
	created      map[common.Address]bool
	trace        map[common.Address]state.Change // optional output; never applied again
}

func newTransactionTrie(base *View, trace bool) *transactionTrie {
	t := &transactionTrie{base: base, accounts: base.accounts.Copy(), storageTrees: map[common.Address]*storagePlan{}, codes: map[common.Hash][]byte{}, created: map[common.Address]bool{}}
	if trace {
		t.trace = map[common.Address]state.Change{}
	}
	return t
}
func (t *transactionTrie) Account(a common.Address) (state.Account, bool, error) {
	raw, e := t.accounts.Get(state.AccountKey(a))
	if e != nil || raw == nil {
		return state.Account{}, false, e
	}
	v, e := t.base.mode.DecodeAccount(raw)
	return v, e == nil, e
}
func (t *transactionTrie) Code(h common.Hash) ([]byte, error) {
	if code, ok := t.codes[h]; ok {
		return bytes.Clone(code), nil
	}
	return t.base.Code(h)
}
func (t *transactionTrie) storage(a common.Address, root mpt.Hash) (*mpt.Trie, error) {
	// A newly created/reset VM object has an empty storage base even before
	// its transaction writes have been applied to this live forest.
	if root == mpt.EmptyRootHash {
		return mpt.NewEmpty(mpt.NewMemoryStore())
	}
	if p := t.storageTrees[a]; p != nil && p.tree != nil {
		actual, e := p.tree.Hash()
		if e != nil {
			return nil, e
		}
		if actual != root {
			return nil, state.ErrCorrupt
		}
		return p.tree, nil
	}
	return t.base.storage(a, root)
}
func (t *transactionTrie) SlotAt(a common.Address, root mpt.Hash, key common.Hash) (common.Hash, error) {
	tr, e := t.storage(a, root)
	if e != nil {
		return common.Hash{}, e
	}
	raw, e := tr.Get(state.SlotKey(key))
	if e != nil {
		return common.Hash{}, e
	}
	return t.base.mode.DecodeStorage(raw)
}
func (t *transactionTrie) HasStorageAt(a common.Address, root mpt.Hash, overlay map[common.Hash]common.Hash) (bool, error) {
	tr, e := t.storage(a, root)
	if e != nil {
		return false, e
	}
	// A collision query must not apply speculative VM writes to the live trie.
	tr = tr.Copy()
	if e = applySlots(tr, t.base.mode, overlay); e != nil {
		return false, e
	}
	h, e := tr.Hash()
	return h != mpt.EmptyRootHash, e
}
func (*transactionTrie) Slot(mpt.Hash, common.Hash) (common.Hash, error) {
	return common.Hash{}, ErrOwnerRequired
}
func (*transactionTrie) HasStorage(mpt.Hash, map[common.Hash]common.Hash) (bool, error) {
	return false, ErrOwnerRequired
}
func (t *transactionTrie) apply(changes map[common.Address]state.Change) error {
	for _, addr := range orderedAddresses(changes) {
		c := changes[addr]
		old, exists, e := t.Account(addr)
		if e != nil {
			return e
		}
		p := t.storageTrees[addr]
		if p == nil {
			// Every previously applied address has a plan (including plain
			// accounts). Before its first write the live value is the base value.
			original, existed := old, exists
			id, active, e := t.base.StorageID(addr)
			if e != nil {
				return e
			}
			if active && !existed || existed && original.StorageRoot != mpt.EmptyRootHash && !active {
				return state.ErrCorrupt
			}
			p = &storagePlan{address: addr, oldID: id, active: active, before: mpt.EmptyRootHash}
			if existed {
				p.before = original.StorageRoot
			}
			t.storageTrees[addr] = p
		}
		if c.Account == nil {
			if e = t.accounts.Delete(state.AccountKey(addr)); e != nil {
				return e
			}
			p.tree = nil
			p.reset = true
			p.needed = false
			t.created[addr] = false
			t.record(addr, c)
			continue
		}
		a := *c.Account
		before := mpt.EmptyRootHash
		if exists && !c.ResetStorage {
			before = old.StorageRoot
		}
		if a.StorageRoot != before {
			return fmt.Errorf("%w: unbound transaction storage base", state.ErrCorrupt)
		}
		if c.ResetStorage {
			p.tree = nil
			p.reset = true
			t.created[addr] = false
		}
		plain := before == mpt.EmptyRootHash && len(c.Slots) == 0 && !p.active && !p.reset && !c.ContractCreated && a.CodeHash == state.EmptyCode
		if !plain {
			if p.tree == nil {
				if before == mpt.EmptyRootHash {
					p.tree, e = mpt.NewEmpty(mpt.NewMemoryStore())
				} else {
					p.tree, e = t.base.storage(addr, before)
				}
				if e != nil {
					return e
				}
			}
			if e = applySlots(p.tree, t.base.mode, c.Slots); e != nil {
				return e
			}
			a.StorageRoot, e = p.tree.Hash()
			if e != nil {
				return e
			}
		}

		if a.CodeHash != state.EmptyCode {
			if c.CodeLoaded {
				if crypto.Keccak256Hash(c.Code) != a.CodeHash {
					return state.ErrCorrupt
				}
				if !exists || old.CodeHash != a.CodeHash {
					t.codes[a.CodeHash] = bytes.Clone(c.Code)
				}
			} else if _, e = t.Code(a.CodeHash); e != nil {
				return e
			}
		}
		t.created[addr] = t.created[addr] || c.ContractCreated
		p.needed = t.created[addr] || a.CodeHash != state.EmptyCode || a.StorageRoot != mpt.EmptyRootHash
		raw, e := t.base.mode.EncodeAccount(a)
		if e != nil {
			return e
		}
		if e = t.accounts.Update(state.AccountKey(addr), raw); e != nil {
			return e
		}
		t.record(addr, c)
	}
	return nil
}
func (t *transactionTrie) plans() []storagePlan {
	addresses := make([]common.Address, 0, len(t.storageTrees))
	for a := range t.storageTrees {
		addresses = append(addresses, a)
	}
	sort.Slice(addresses, func(i, j int) bool { return bytes.Compare(addresses[i][:], addresses[j][:]) < 0 })
	out := make([]storagePlan, 0, len(addresses))
	for _, a := range addresses {
		p := *t.storageTrees[a]
		if !p.active && !p.reset && !p.needed {
			continue
		}
		if p.reset {
			p.before = mpt.EmptyRootHash
		}
		out = append(out, p)
	}
	return out
}
func (t *transactionTrie) record(a common.Address, c state.Change) {
	if t.trace == nil {
		return
	}
	if c.Account == nil {
		t.trace[a] = state.Change{}
		return
	}
	old, seen := t.trace[a]
	if c.ResetStorage || seen && old.Account == nil {
		old = state.Change{ResetStorage: true}
	}
	if old.Slots == nil {
		old.Slots = map[common.Hash]common.Hash{}
	}
	for k, v := range c.Slots {
		old.Slots[k] = v
	}
	value := *c.Account
	old.Account = &value
	old.ResetStorage = old.ResetStorage || c.ResetStorage
	old.ContractCreated = old.ContractCreated || c.ContractCreated
	if c.CodeLoaded {
		old.Code = bytes.Clone(c.Code)
		old.CodeLoaded = true
	}
	t.trace[a] = old
}
