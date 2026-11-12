package backend

import (
	"context"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// QueryWithProofStats exposes planned-proof diagnostics without mutable global
// last-query counters. Legacy queries return an empty planned-proof statistic.
func (b *Backend) QueryWithProofStats(ctx context.Context, req query.Request) (query.Response, ht.ParallelProofStats, error) {
	if s, ok := b.store.(*hybridStore); ok && s.parallelProof.Enabled {
		return s.queryPlanned(ctx, req, b.mode)
	}
	r, e := b.Query(ctx, req)
	return r, ht.ParallelProofStats{}, e
}

func (s *hybridStore) queryPlanned(ctx context.Context, req query.Request, mode codec.Mode) (out query.Response, stats ht.ParallelProofStats, err error) {
	if e := ctx.Err(); e != nil {
		return out, stats, e
	}
	// Do not wrap with newHybridSnapshot: it would independently load Root and
	// BlockID before this operation loads its directory entry again.
	v, e := s.store.ProofViewAt(req.Height)
	if e != nil {
		return out, stats, hybridError(e)
	}
	defer v.Close()
	ps, e := v.BeginProofSession(ctx, s.parallelProof)
	if e != nil {
		return out, stats, hybridError(e)
	}
	defer func() {
		ps.Close()
		stats = ps.Stats()
		if err != nil {
			out = query.Response{}
			err = hybridError(err)
		}
	}()
	root, e := ps.Root(ht.AccountTrieID)
	if e != nil {
		return out, stats, e
	}
	lookup := func(owner *common.Address, root mpt.Hash, key []byte) (mpt.ProofResult, error) {
		id := uint64(0)
		if owner != nil {
			var active bool
			var e error
			id, active, e = ps.StorageTrieID(*owner)
			if e != nil {
				return mpt.ProofResult{}, e
			}
			if !active {
				return mpt.ProofResult{}, state.ErrCorrupt
			}
		}
		return ps.Walk(id, root, key)
	}
	code := func(h common.Hash) ([]byte, error) { return ps.Code(mpt.Hash(h)) }
	complete := func() (mpt.Proof, mpt.Proof, error) {
		plans, _, e := ps.Finish()
		if e != nil {
			return nil, nil, e
		}
		if len(plans) < 1 || len(plans) > 2 {
			return nil, nil, state.ErrCorrupt
		}
		storage := mpt.Proof{}
		if len(plans) == 2 {
			storage = plans[1].Nodes
		}
		return plans[0].Nodes, storage, nil
	}
	out, err = query.ReadTrieDeferred(ctx, req, root, mode, lookup, code, complete)
	return
}
