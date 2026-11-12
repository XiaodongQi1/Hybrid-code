// Package backend connects the existing journaled EVM StateDB to compact,
// path-versioned account and storage tries. It does not change EVM execution.
package backend

import (
	"context"
	"errors"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
)

var ErrOwnerRequired = errors.New("versioned EVM: storage owner required")

// View pins one committed height. A State and its View are single-owner;
// release the State before closing its View, and all Views before the Backend.
type View struct {
	snapshot     stateSnapshot
	accounts     *mpt.Trie
	mode         codec.Mode
	serving      bool // Query assembles a local response for independent client verification.
	storageViews map[common.Address]openedStorage
	closed       bool
}

type openedStorage struct {
	root mpt.Hash
	tree *mpt.Trie
}

var _ state.Reader = (*View)(nil)
var _ state.OwnedReader = (*View)(nil)

func openView(snapshot stateSnapshot, mode codec.Mode) (*View, error) {
	r, e := snapshot.Reader(0, snapshot.Root())
	if e != nil {
		return nil, e
	}
	t, e := mpt.Open(snapshot.Root(), r)
	if e != nil {
		return nil, e
	}
	return &View{snapshot: snapshot, accounts: t, mode: mode}, nil
}
func emptyView(mode codec.Mode) *View {
	t, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	return &View{accounts: t, mode: mode}
}
func (v *View) check() error {
	if v.closed {
		return vt.ErrClosed
	}
	return nil
}
func (v *View) Close() error {
	if v.closed {
		return nil
	}
	v.closed = true
	v.storageViews = nil
	if v.snapshot != nil {
		return v.snapshot.Close()
	}
	return nil
}
func (v *View) Root() mpt.Hash {
	if v.snapshot == nil {
		return mpt.EmptyRootHash
	}
	return v.snapshot.Root()
}

// BlockID identifies the canonical input block bound to this historical view.
func (v *View) BlockID() common.Hash {
	if v.snapshot == nil {
		return common.Hash{}
	}
	return common.Hash(v.snapshot.CommitID())
}
func owner(a common.Address) (out [32]byte) { copy(out[:], state.AccountKey(a)); return }

func (v *View) Account(a common.Address) (state.Account, bool, error) {
	if e := v.check(); e != nil {
		return state.Account{}, false, e
	}
	raw, e := v.accounts.Get(state.AccountKey(a))
	if e != nil || raw == nil {
		return state.Account{}, false, e
	}
	value, e := v.mode.DecodeAccount(raw)
	return value, e == nil, e
}
func (v *View) Code(h common.Hash) ([]byte, error) {
	if e := v.check(); e != nil {
		return nil, e
	}
	if h == state.EmptyCode {
		return nil, nil
	}
	if v.snapshot == nil {
		return nil, vt.ErrNotFound
	}
	return v.snapshot.Code(mpt.Hash(h))
}

// StorageID is local physical metadata, never part of an account payload/hash.
func (v *View) StorageID(a common.Address) (uint64, bool, error) {
	if e := v.check(); e != nil {
		return 0, false, e
	}
	if v.snapshot == nil {
		return 0, false, nil
	}
	return v.snapshot.StorageID(a)
}
func (v *View) storage(a common.Address, root mpt.Hash) (*mpt.Trie, error) {
	if e := v.check(); e != nil {
		return nil, e
	}
	// New/recreated objects use an empty private base, even if a previous
	// incarnation exists at this address in the immutable block-start view.
	if root == mpt.EmptyRootHash {
		return mpt.NewEmpty(mpt.NewMemoryStore())
	}
	if cached, ok := v.storageViews[a]; ok && cached.root == root {
		return cached.tree.Copy(), nil
	}
	account, exists, e := v.Account(a)
	if e != nil {
		return nil, e
	}
	if !exists || account.StorageRoot != root {
		return nil, state.ErrCorrupt
	}
	id, active, e := v.StorageID(a)
	if e != nil {
		return nil, e
	}
	if !active {
		return nil, state.ErrCorrupt
	}
	r, e := v.snapshot.Reader(id, root)
	if e != nil {
		return nil, e
	}
	tree, e := mpt.Open(root, r)
	if e != nil {
		return nil, e
	}
	// This View pins the account lifecycle. Bound retained root handles;
	// callers receive COW copies so VM probes/updates cannot mutate the base.
	if v.storageViews == nil {
		v.storageViews = make(map[common.Address]openedStorage)
	}
	if len(v.storageViews) < 256 {
		v.storageViews[a] = openedStorage{root, tree}
	}
	return tree.Copy(), nil
}
func (v *View) SlotAt(a common.Address, root mpt.Hash, k common.Hash) (common.Hash, error) {
	t, e := v.storage(a, root)
	if e != nil {
		return common.Hash{}, e
	}
	raw, e := t.Get(state.SlotKey(k))
	if e != nil {
		return common.Hash{}, e
	}
	return v.mode.DecodeStorage(raw)
}
func (v *View) HasStorageAt(a common.Address, root mpt.Hash, overlay map[common.Hash]common.Hash) (bool, error) {
	t, e := v.storage(a, root)
	if e != nil {
		return false, e
	}
	if e = applySlots(t, v.mode, overlay); e != nil {
		return false, e
	}
	h, e := t.Hash()
	return h != mpt.EmptyRootHash, e
}
func (v *View) Slot(mpt.Hash, common.Hash) (common.Hash, error) {
	return common.Hash{}, ErrOwnerRequired
}
func (v *View) HasStorage(mpt.Hash, map[common.Hash]common.Hash) (bool, error) {
	return false, ErrOwnerRequired
}
func (v *View) AccountProof(a common.Address) (mpt.Proof, error) {
	if e := v.check(); e != nil {
		return nil, e
	}
	if prover, ok := v.snapshot.(historicalProver); ok {
		p, e := prover.HistoricalValueProof(0, v.Root(), state.AccountKey(a), false)
		return p.Nodes, e
	}
	return v.accounts.Prove(state.AccountKey(a))
}
func (v *View) StorageProof(a common.Address, k common.Hash) (mpt.Hash, mpt.Proof, error) {
	account, exists, e := v.Account(a)
	if e != nil {
		return mpt.Hash{}, nil, e
	}
	root := mpt.EmptyRootHash
	if exists {
		root = account.StorageRoot
	}
	if root == mpt.EmptyRootHash {
		return root, mpt.Proof{}, nil
	}
	p, e := v.proveValue(&a, root, state.SlotKey(k))
	return root, p.Nodes, e
}

// StorageProofAt implements the common query.Reader seam. The supplied root is
// checked against this view's authenticated account, never a root-to-owner map.
func (v *View) StorageProofAt(a common.Address, root mpt.Hash, k common.Hash) (mpt.Proof, error) {
	actual, p, e := v.StorageProof(a, k)
	if e != nil {
		return nil, e
	}
	if actual != root {
		return nil, state.ErrCorrupt
	}
	return p, nil
}

// ReadWithProof obtains storage identity only after decoding the owner from
// this view's account proof. Standalone SlotAt keeps its external root check.
func (v *View) ReadWithProof(ctx context.Context, req query.Request) (query.Response, error) {
	if e := v.check(); e != nil {
		return query.Response{}, e
	}
	return query.ReadTrie(ctx, req, v.Root(), v.mode, v.proveValue, v.Code)
}
func (v *View) proveValue(owner *common.Address, root mpt.Hash, key []byte) (mpt.ProofResult, error) {
	var id uint64
	if owner != nil {
		var active bool
		var err error
		id, active, err = v.StorageID(*owner)
		if err != nil {
			return mpt.ProofResult{}, err
		}
		if !active {
			return mpt.ProofResult{}, state.ErrCorrupt
		}
	}
	if p, ok := v.snapshot.(historicalProver); ok {
		return p.HistoricalValueProof(id, root, key, v.serving)
	}
	if owner == nil && v.accounts != nil {
		return v.accounts.ProveValue(key)
	}
	r, e := v.snapshot.Reader(id, root)
	if e != nil {
		return mpt.ProofResult{}, e
	}
	if v.serving {
		return mpt.AssembleValueProofAt(root, r, key)
	}
	return mpt.ProveValueAt(root, r, key)
}

// Query pins the requested height for one compact response. Opening an
// execution trie here would resolve the account root before the proof walk.
func (b *Backend) Query(ctx context.Context, req query.Request) (query.Response, error) {
	if e := ctx.Err(); e != nil {
		return query.Response{}, e
	}
	open := b.store.ViewAt
	if hs, ok := b.store.(*hybridStore); ok {
		if hs.parallelProof.Enabled {
			out, _, e := hs.queryPlanned(ctx, req, b.mode)
			return out, e
		}
		open = hs.proofViewAt
	}
	s, e := open(req.Height)
	if e != nil {
		return query.Response{}, e
	}
	defer s.Close()
	v := View{snapshot: s, mode: b.mode, serving: true}
	out, e := v.ReadWithProof(ctx, req)
	if e != nil {
		return out, e
	}
	return out.Compact(), nil
}
