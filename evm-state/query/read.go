package query

import (
	"context"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// Reader is an already pinned, decoded state view. The caller owns its height,
// lifetime and storage read budgets; Read neither opens nor retains a view.
type Reader interface {
	Root() mpt.Hash
	Account(common.Address) (state.Account, bool, error)
	Code(common.Hash) ([]byte, error)
	SlotAt(common.Address, mpt.Hash, common.Hash) (common.Hash, error)
	AccountProof(common.Address) (mpt.Proof, error)
	StorageProofAt(common.Address, mpt.Hash, common.Hash) (mpt.Proof, error)
}

// Read assembles the same account/account-plus-slot/code response for every
// adapter, using compact proofs. It does not verify its own response: the client
// uses VerifyWithCodec and an independently trusted historical root.
func Read(ctx context.Context, req Request, view Reader) (Response, error) {
	if r, ok := view.(interface {
		ReadWithProof(context.Context, Request) (Response, error)
	}); ok {
		out, e := r.ReadWithProof(ctx, req)
		if e != nil {
			return out, e
		}
		if out.ProofFormat == CompactProof {
			return out, nil
		}
		return out.Compact(), nil
	}
	out := Response{Request: req, Root: view.Root()}
	if e := req.validate(); e != nil {
		return out, e
	}
	if e := ctx.Err(); e != nil {
		return out, e
	}
	var e error
	out.AccountProof, e = view.AccountProof(req.Address)
	if e != nil {
		return out, e
	}
	a, exists, e := view.Account(req.Address)
	if e != nil {
		return out, e
	}
	if exists {
		out.Account = &a
		switch req.Kind {
		case Storage:
			out.Value, e = view.SlotAt(req.Address, a.StorageRoot, req.Slot)
			if e == nil {
				out.StorageProof, e = view.StorageProofAt(req.Address, a.StorageRoot, req.Slot)
			}
			out.SlotExists = out.Value != (common.Hash{})
		case Code:
			out.Code, e = view.Code(a.CodeHash)
		}
	}
	if e != nil {
		return out, e
	}
	if e = ctx.Err(); e != nil {
		return out, e
	}
	return out.Compact(), nil
}

// TrieLookup proves one key on a pinned view. A nil owner selects the account
// trie. Storage lookups receive only the root decoded from that account leaf.
type TrieLookup func(owner *common.Address, root mpt.Hash, key []byte) (mpt.ProofResult, error)

// ReadTrie assembles a response while preserving the adapter's proof format.
// Read converts the result to compact format for the common wire protocol.
func ReadTrie(ctx context.Context, req Request, root mpt.Hash, mode codec.Mode, lookup TrieLookup, code func(common.Hash) ([]byte, error)) (Response, error) {
	return readTrie(ctx, req, root, mode, lookup, code, nil)
}

// ReadTrieDeferred uses the same request/value decoding as ReadTrie, but lookup
// may return terminal fields before its proof is complete. complete joins once
// after both lookups and transfers already compact, caller-owned proof bytes.
func ReadTrieDeferred(ctx context.Context, req Request, root mpt.Hash, mode codec.Mode, lookup TrieLookup, code func(common.Hash) ([]byte, error), complete func() (mpt.Proof, mpt.Proof, error)) (Response, error) {
	if complete == nil {
		return Response{}, ErrProof
	}
	return readTrie(ctx, req, root, mode, lookup, code, complete)
}

func readTrie(ctx context.Context, req Request, root mpt.Hash, mode codec.Mode, lookup TrieLookup, code func(common.Hash) ([]byte, error), complete func() (mpt.Proof, mpt.Proof, error)) (Response, error) {
	out := Response{Request: req, Root: root}
	if e := req.validate(); e != nil {
		return out, e
	}
	if e := ctx.Err(); e != nil {
		return out, e
	}
	account, e := lookup(nil, root, state.AccountKey(req.Address))
	if e != nil {
		return out, e
	}
	out.AccountProof = account.Nodes
	if account.Exists {
		a, e := mode.DecodeAccount(account.Value)
		if e != nil {
			return out, e
		}
		out.Account = &a
		if e = ctx.Err(); e != nil {
			return out, e
		}
		switch req.Kind {
		case Storage:
			if a.StorageRoot == mpt.EmptyRootHash {
				out.StorageProof = mpt.Proof{}
				break
			}
			slot, e := lookup(&req.Address, a.StorageRoot, state.SlotKey(req.Slot))
			if e != nil {
				return out, e
			}
			out.StorageProof = slot.Nodes
			if slot.Exists {
				out.Value, e = mode.DecodeStorage(slot.Value)
				if e != nil {
					return out, e
				}
				if out.Value == (common.Hash{}) {
					return out, state.ErrCorrupt
				}
				out.SlotExists = true
			}
		case Code:
			out.Code, e = code(a.CodeHash)
			if e != nil {
				return out, e
			}
		}
	}
	if e = ctx.Err(); e != nil {
		return out, e
	}
	if complete != nil {
		out.AccountProof, out.StorageProof, e = complete()
		if e != nil {
			return Response{}, e
		}
		out.ProofFormat = CompactProof
	}
	return out, ctx.Err()
}
