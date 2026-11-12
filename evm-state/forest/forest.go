// Package forest owns raw account/storage tries; it has no durable coordinator.
package forest

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	leafcodec "github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	codec "github.com/XiaodongQi1/HybridTrie/evm-state/internal/triecodec"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type Store interface {
	mpt.Store
	Code(common.Hash) ([]byte, error)
}
type View struct {
	root     mpt.Hash
	store    Store
	accounts *mpt.Trie
	mode     leafcodec.Mode
}

// Open retains the legacy raw105 account/raw32 slot format.
func Open(root mpt.Hash, s Store) (*View, error) {
	return OpenWithCodec(root, s, leafcodec.Legacy)
}

// OpenWithCodec changes leaf serialization only. MPT paths, RLP, inline-child
// rules and hashing remain shared by every physical storage adapter.
func OpenWithCodec(root mpt.Hash, s Store, mode leafcodec.Mode) (*View, error) {
	if e := mode.Validate(); e != nil {
		return nil, e
	}
	t, e := mpt.Open(root, s)
	if e != nil {
		return nil, e
	}
	return &View{root: root, store: s, accounts: t, mode: mode}, nil
}
func (v *View) Root() mpt.Hash { return v.root }
func (v *View) Account(a common.Address) (state.Account, bool, error) {
	b, e := v.accounts.Get(state.AccountKey(a))
	if e != nil || b == nil {
		return state.Account{}, false, e
	}
	x, e := v.mode.DecodeAccount(b)
	return x, e == nil, e
}
func (v *View) Code(h common.Hash) ([]byte, error) {
	if h == state.EmptyCode {
		return nil, nil
	}
	b, e := v.store.Code(h)
	if e == nil && crypto.Keccak256Hash(b) != h {
		e = state.ErrCorrupt
	}
	return b, e
}
func (v *View) Slot(root mpt.Hash, k common.Hash) (common.Hash, error) {
	t, e := mpt.Open(root, v.store)
	if e != nil {
		return common.Hash{}, e
	}
	b, e := t.Get(state.SlotKey(k))
	if e != nil || b == nil {
		return common.Hash{}, e
	}
	return v.mode.DecodeStorage(b)
}
func (v *View) HasStorage(root mpt.Hash, overlay map[common.Hash]common.Hash) (bool, error) {
	t, e := mpt.Open(root, v.store)
	if e != nil {
		return false, e
	}
	for k, w := range overlay {
		var raw []byte
		raw, e = v.mode.EncodeStorage(w)
		if e == nil {
			e = t.Update(state.SlotKey(k), raw)
		}
		if e != nil {
			return false, e
		}
	}
	h, e := t.Hash()
	return h != mpt.EmptyRootHash, e
}

// OwnerStore is optional; hashed stores need no owner context.
type OwnerStore interface{ WithOwner(accountKey []byte) Store }

func ownedStore(s Store, key []byte) Store {
	if o, ok := s.(OwnerStore); ok {
		return o.WithOwner(key)
	}
	return s
}
func (v *View) SlotAt(a common.Address, root mpt.Hash, k common.Hash) (common.Hash, error) {
	scoped := *v
	scoped.store = ownedStore(v.store, state.AccountKey(a))
	return scoped.Slot(root, k)
}
func (v *View) HasStorageAt(a common.Address, root mpt.Hash, overlay map[common.Hash]common.Hash) (bool, error) {
	scoped := *v
	scoped.store = ownedStore(v.store, state.AccountKey(a))
	return scoped.HasStorage(root, overlay)
}

type Pair struct {
	Address       common.Address
	Before, After mpt.Hash
}

// Plan contains owned canonical nodes; Publish belongs exclusively to storage.
type Plan struct {
	Root        mpt.Hash
	Nodes       map[mpt.Hash][]byte
	Codes       map[common.Hash][]byte
	Pairs       []Pair
	Accounts    []state.Account
	Limit, used uint64
}

func (p *Plan) Put(key, value []byte) error {
	if len(key) != 32 {
		return state.ErrCorrupt
	}
	var h mpt.Hash
	copy(h[:], key)
	if codec.NodeHash(value) != h {
		return state.ErrCorrupt
	}
	if b, ok := p.Nodes[h]; ok {
		if !bytes.Equal(b, value) {
			return state.ErrCorrupt
		}
		return nil
	}
	n := uint64(len(value)) + 64
	if n > p.Limit-p.used {
		return codec.ErrLimit
	}
	p.used += n
	p.Nodes[h] = bytes.Clone(value)
	return nil
}
func (p *Plan) Write() error { return errors.New("forest plans cannot publish") }
func (v *View) Materialize(changes map[common.Address]state.Change, limit uint64) (*Plan, error) {
	p := &Plan{Nodes: map[mpt.Hash][]byte{}, Codes: map[common.Hash][]byte{}, Limit: limit}
	at := v.accounts.Copy()
	addresses := make([]common.Address, 0, len(changes))
	for a := range changes {
		addresses = append(addresses, a)
	}
	sort.Slice(addresses, func(i, j int) bool { return bytes.Compare(addresses[i][:], addresses[j][:]) < 0 })
	for _, addr := range addresses {
		c := changes[addr]
		if c.Account == nil {
			if e := at.Delete(state.AccountKey(addr)); e != nil {
				return nil, e
			}
			continue
		}
		a := *c.Account
		before := a.StorageRoot
		t, e := mpt.Open(before, ownedStore(v.store, state.AccountKey(addr)))
		if e != nil {
			return nil, e
		}
		slots := make([]common.Hash, 0, len(c.Slots))
		for k := range c.Slots {
			slots = append(slots, k)
		}
		sort.Slice(slots, func(i, j int) bool { return bytes.Compare(slots[i][:], slots[j][:]) < 0 })
		for _, k := range slots {
			w := c.Slots[k]
			raw, err := v.mode.EncodeStorage(w)
			if err != nil {
				return nil, err
			}
			e = t.Update(state.SlotKey(k), raw)
			if e != nil {
				return nil, e
			}
		}
		a.StorageRoot, e = t.Stage(p)
		if e != nil {
			return nil, e
		}
		p.Pairs = append(p.Pairs, Pair{addr, before, a.StorageRoot})
		if c.CodeLoaded && a.CodeHash != state.EmptyCode {
			if crypto.Keccak256Hash(c.Code) != a.CodeHash {
				return nil, state.ErrCorrupt
			}
			if _, ok := p.Codes[a.CodeHash]; !ok {
				n := uint64(len(c.Code)) + 64
				if n > p.Limit-p.used {
					return nil, codec.ErrLimit
				}
				p.used += n
				p.Codes[a.CodeHash] = bytes.Clone(c.Code)
			}
		}
		p.Accounts = append(p.Accounts, a)
		raw, e := v.mode.EncodeAccount(a)
		if e != nil {
			return nil, e
		}
		if e = at.Update(state.AccountKey(addr), raw); e != nil {
			return nil, e
		}
	}
	h, e := at.Stage(p)
	p.Root = h
	return p, e
}

// Audit includes references embedded in account payloads, which generic MPT
// traversal cannot discover. No cache/visited shortcut hides a missing subtree.
func (v *View) Audit(ctx context.Context, visits *uint64, limit uint64) error {
	if _, ok := v.store.(OwnerStore); ok {
		check := func(_ []byte, _ mpt.Hash, _ []byte) (bool, error) {
			if e := ctx.Err(); e != nil {
				return false, e
			}
			if *visits >= limit {
				return false, codec.ErrLimit
			}
			*visits++
			return true, nil
		}
		return mpt.WalkPaths(v.root, v.store, check, func(key, b []byte) error {
			if len(key) != 32 {
				return state.ErrCorrupt
			}
			a, e := v.mode.DecodeAccount(b)
			if e != nil {
				return e
			}
			if _, e = v.Code(a.CodeHash); e != nil {
				return e
			}
			return mpt.WalkPaths(a.StorageRoot, ownedStore(v.store, key), check, func(key, word []byte) error {
				if len(key) != 32 || len(word) == 0 {
					return state.ErrCorrupt
				}
				_, e := v.mode.DecodeStorage(word)
				return e
			})
		})
	}
	return codec.Walk(ctx, v.root, v.store, visits, limit, func(b []byte) error {
		a, e := v.mode.DecodeAccount(b)
		if e != nil {
			return e
		}
		if _, e = v.Code(a.CodeHash); e != nil {
			return e
		}
		return codec.Walk(ctx, a.StorageRoot, v.store, visits, limit, func(word []byte) error {
			if len(word) == 0 {
				return state.ErrCorrupt
			}
			_, e := v.mode.DecodeStorage(word)
			return e
		})
	})
}

// Proof is a low-level D1/D2 correctness seam; the public query service is D4.
func (v *View) Prove(a common.Address, k common.Hash) (mpt.Proof, mpt.Proof, error) {
	p, e := v.accounts.Prove(state.AccountKey(a))
	if e != nil {
		return nil, nil, e
	}
	x, ok, e := v.Account(a)
	if e != nil || !ok {
		return p, nil, e
	}
	t, e := mpt.Open(x.StorageRoot, ownedStore(v.store, state.AccountKey(a)))
	if e != nil {
		return nil, nil, e
	}
	sp, e := t.Prove(state.SlotKey(k))
	return p, sp, e
}
