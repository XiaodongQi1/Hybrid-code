package forest

import (
	"context"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

func (v *View) AccountProof(a common.Address) (mpt.Proof, error) {
	return v.accounts.Prove(state.AccountKey(a))
}
func (v *View) StorageProof(root mpt.Hash, k common.Hash) (mpt.Proof, error) {
	t, e := mpt.Open(root, v.store)
	if e != nil {
		return nil, e
	}
	return t.Prove(state.SlotKey(k))
}

func (v *View) StorageProofAt(a common.Address, root mpt.Hash, k common.Hash) (mpt.Proof, error) {
	scoped := *v
	scoped.store = ownedStore(v.store, state.AccountKey(a))
	return scoped.StorageProof(root, k)
}

func (v *View) ReadWithProof(ctx context.Context, req query.Request) (query.Response, error) {
	return query.ReadTrie(ctx, req, v.root, v.mode, v.proveValue, v.Code)
}

func (v *View) proveValue(owner *common.Address, root mpt.Hash, key []byte) (mpt.ProofResult, error) {
	return mpt.ProveValueAt(root, v.proofStore(owner), key)
}

func (v *View) proofStore(owner *common.Address) Store {
	store := v.store
	if owner != nil {
		store = ownedStore(store, state.AccountKey(*owner))
	}
	return store
}

// ReadAtWithProof serves a single query from an already pinned store, without
// opening an execution trie before reading the authenticated account root.
func ReadAtWithProof(ctx context.Context, req query.Request, root mpt.Hash, s Store, mode codec.Mode) (query.Response, error) {
	v := View{root: root, store: s, mode: mode}
	return v.ReadWithProof(ctx, req)
}

// AssembleAtWithProof reads local trie nodes for a server response. The caller
// supplies a pinned local store and the recipient independently verifies it.
func AssembleAtWithProof(ctx context.Context, req query.Request, root mpt.Hash, s Store, mode codec.Mode) (query.Response, error) {
	v := View{root: root, store: s, mode: mode}
	lookup := func(owner *common.Address, root mpt.Hash, key []byte) (mpt.ProofResult, error) {
		return mpt.AssembleValueProofAt(root, v.proofStore(owner), key)
	}
	return query.ReadTrie(ctx, req, root, mode, lookup, v.Code)
}
