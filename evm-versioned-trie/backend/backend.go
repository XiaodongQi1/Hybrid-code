package backend

import (
	"bytes"
	"fmt"
	"sort"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
	"sync"
)

// Backend serializes writers and shares one EVM materialization path across
// versioned and HybridTrie adapters. HybridTrie retains its closed Hash Store.
type Backend struct {
	mu                       sync.Mutex
	store                    stateStore
	initialHeight, maxHeight uint64
	lastSnapshot             SnapshotReport
	mode                     codec.Mode
	readOnly                 bool
}

func Open(path string, cfg vt.Config, create, readOnly bool) (*Backend, error) {
	mode := codec.Mode(cfg.CodecID)
	if e := mode.Validate(); e != nil {
		return nil, e
	}
	s, e := vt.Open(path, cfg, create, readOnly)
	if e != nil {
		return nil, e
	}
	return &Backend{store: versionStore{s}, initialHeight: uint64(cfg.InitialHeight), maxHeight: maxHeightForLegacy(), mode: mode, readOnly: readOnly}, nil
}
func (b *Backend) Close() error        { b.mu.Lock(); defer b.mu.Unlock(); return b.store.Close() }
func (b *Backend) Head() (Head, error) { return b.store.Head() }
func (b *Backend) ViewAt(height uint64) (*View, error) {
	s, e := b.store.ViewAt(height)
	if e != nil {
		return nil, e
	}
	v, e := openView(s, b.mode)
	if e != nil {
		s.Close()
	}
	return v, e
}

func orderedAddresses(changes map[common.Address]state.Change) []common.Address {
	out := make([]common.Address, 0, len(changes))
	for a := range changes {
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}
func applySlots(t *mpt.Trie, mode codec.Mode, slots map[common.Hash]common.Hash) error {
	keys := make([]common.Hash, 0, len(slots))
	for k := range slots {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	for _, k := range keys {
		value, e := mode.EncodeStorage(slots[k])
		if e != nil {
			return e
		}
		if e = t.Update(state.SlotKey(k), value); e != nil {
			return e
		}
	}
	return nil
}

type storagePlan struct {
	address               common.Address
	tree                  *mpt.Trie
	before                mpt.Hash
	oldID                 uint64
	active, reset, needed bool
}

// commit applies a supplied final key set (snapshot or trace import), then
// projects each changed trie once. Live execution uses transactionTrie instead.
// Caller holds b.mu and owns the immutable base View for the entire operation.
func (b *Backend) commit(base *View, height uint64, id [32]byte, changes map[common.Address]state.Change) (mpt.Hash, error) {
	return b.commitExpected(base, height, id, changes, nil)
}

func (b *Backend) commitExpected(base *View, height uint64, id [32]byte, changes map[common.Address]state.Change, expected *mpt.Hash) (mpt.Hash, error) {
	return b.commitRecorded(base, height, id, changes, expected, nil, nil)
}

func (b *Backend) commitRecorded(base *View, height uint64, id [32]byte, changes map[common.Address]state.Change, expected *mpt.Hash, beforeCommit func(mpt.Hash) error, timing *block.ExecutionTiming) (mpt.Hash, error) {
	head, err := b.store.Head()
	if err != nil {
		return mpt.Hash{}, err
	}
	if head.Initialized && (base.snapshot == nil || base.Root() != head.Root || base.snapshot.CommitID() != head.CommitID) || !head.Initialized && base.snapshot != nil {
		return mpt.Hash{}, vt.ErrConflict
	}
	at := base.accounts.Copy()
	var plans []storagePlan
	codes := map[common.Hash][]byte{}
	for _, addr := range orderedAddresses(changes) {
		c := changes[addr]
		old, exists, e := base.Account(addr)
		if e != nil {
			return mpt.Hash{}, e
		}
		oldID, active, e := base.StorageID(addr)
		if e != nil {
			return mpt.Hash{}, e
		}
		if active && !exists || exists && old.StorageRoot != mpt.EmptyRootHash && !active {
			return mpt.Hash{}, state.ErrCorrupt
		}
		if c.Account == nil {
			if e = at.Delete(state.AccountKey(addr)); e != nil {
				return mpt.Hash{}, e
			}
			if active {
				plans = append(plans, storagePlan{address: addr, oldID: oldID, active: true, reset: true})
			}
			continue
		}
		a := *c.Account
		before := mpt.EmptyRootHash
		if exists && !c.ResetStorage {
			before = old.StorageRoot
		}
		if a.StorageRoot != before {
			return mpt.Hash{}, fmt.Errorf("%w: unbound storage base", state.ErrCorrupt)
		}
		t, e := base.storage(addr, before)
		if e != nil {
			return mpt.Hash{}, e
		}
		if e = applySlots(t, b.mode, c.Slots); e != nil {
			return mpt.Hash{}, e
		}
		a.StorageRoot, e = t.Hash()
		if e != nil {
			return mpt.Hash{}, e
		}
		if a.CodeHash != state.EmptyCode {
			if c.CodeLoaded {
				if crypto.Keccak256Hash(c.Code) != a.CodeHash {
					return mpt.Hash{}, state.ErrCorrupt
				}
				// Only newly introduced bytecode needs a Put; ordinary calls
				// may have loaded the existing immutable code without changing it.
				if !exists || old.CodeHash != a.CodeHash {
					codes[a.CodeHash] = bytes.Clone(c.Code)
				}
			} else if _, e = base.Code(a.CodeHash); e != nil {
				return mpt.Hash{}, e
			}
		}
		needed := c.ContractCreated || a.CodeHash != state.EmptyCode || a.StorageRoot != mpt.EmptyRootHash
		plans = append(plans, storagePlan{addr, t, before, oldID, active, c.ResetStorage, needed})
		raw, e := b.mode.EncodeAccount(a)
		if e != nil {
			return mpt.Hash{}, e
		}
		if e = at.Update(state.AccountKey(addr), raw); e != nil {
			return mpt.Hash{}, e
		}
	}
	return b.publishTries(base, height, id, at, plans, codes, expected, beforeCommit, timing)
}

// publishTries persists the final live tries once per block. Already applied
// transaction key writes are not replayed here.
func (b *Backend) publishTries(base *View, height uint64, id [32]byte, at *mpt.Trie, plans []storagePlan, codes map[common.Hash][]byte, expected *mpt.Hash, beforeCommit func(mpt.Hash) error, timing *block.ExecutionTiming) (mpt.Hash, error) {
	if finisher, ok := b.store.(interface{ FinishExecutionReads() }); ok {
		finisher.FinishExecutionReads()
	}
	root, e := at.Hash()
	if e != nil {
		return mpt.Hash{}, e
	}
	if expected != nil && root != *expected {
		return mpt.Hash{}, fmt.Errorf("%w: evaluated block root mismatch", state.ErrCorrupt)
	}
	timing.StartPublish()
	u, e := b.store.Begin(height, root, id)
	if e != nil {
		return mpt.Hash{}, e
	}
	defer u.Abort()
	for hash, code := range codes {
		if e = u.PutCode(mpt.Hash(hash), code); e != nil {
			return mpt.Hash{}, e
		}
	}
	for _, p := range plans {
		if e = u.Storage(p); e != nil {
			return mpt.Hash{}, e
		}
	}
	if e = u.Accounts(base.Root(), at); e != nil {
		return mpt.Hash{}, e
	}
	if beforeCommit != nil {
		if e = beforeCommit(root); e != nil {
			return mpt.Hash{}, e
		}
	}
	if e = u.Commit(); e != nil {
		return mpt.Hash{}, e
	}
	return root, nil
}

func (b *Backend) CommittedHead() (uint64, mpt.Hash, [32]byte, error) {
	h, e := b.Head()
	return h.Height, h.Root, h.CommitID, e
}
