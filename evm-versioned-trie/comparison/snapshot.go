package comparison

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	snap "github.com/XiaodongQi1/HybridTrie/evm-state/snapshot"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// ImportSnapshot projects one immutable prepared source, using bounded writes.
// Initial roots and Head become visible only after complete byte verification.
// The source is caller-owned and can be reused by Path and HybridTrie.
func (a *Archive) ImportSnapshot(ctx context.Context, header block.InputBlock, source *ht.SnapshotSource, expected *mpt.Hash) (ArchiveResult, kvstore.BulkStats, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out ArchiveResult
	var stats kvstore.BulkStats
	if a.closed {
		return out, stats, kvstore.ErrClosed
	}
	if a.uncertain {
		return out, stats, kvstore.ErrUncertain
	}
	if a.readOnly {
		return out, stats, kvstore.ErrReadOnly
	}
	if source == nil || header.Height != a.cfg.InitialHeight || !source.IndependentOf(a.db.Path()) {
		return out, stats, kvstore.ErrInvalid
	}
	m, e := snap.Verify(ctx, source, header)
	if e != nil {
		return out, stats, e
	}
	if m.WorkloadID != a.cfg.WorkloadID || expected != nil && m.Root != *expected {
		return out, stats, state.ErrCorrupt
	}
	raw, e := json.Marshal(m)
	if e != nil {
		return out, stats, e
	}
	id, e := a.artifacts.Ensure("snapshot-manifest", raw)
	if e != nil {
		return out, stats, e
	}
	input, e := json.Marshal(header)
	if e != nil {
		return out, stats, e
	}
	inputID, e := a.artifacts.Ensure("block-input", input)
	if e != nil {
		return out, stats, e
	}
	outcomesID, e := a.artifacts.Ensure("block-outcomes", []byte("null"))
	if e != nil {
		return out, stats, e
	}
	r := ArchiveHeader{Height: header.Height, ID: header.ID, ParentID: header.ParentID, Root: m.Root, Input: inputID, Outcomes: outcomesID, SnapshotManifest: id.Hex()}
	if a.head != nil {
		previous, e := a.header(header.Height)
		if e != nil || previous != r {
			return out, stats, state.ErrCorrupt
		}
		marker, e := a.db.BulkStatus([]byte("M/initial-import"))
		if e != nil || !marker.Complete {
			return out, stats, state.ErrCorrupt
		}
		return ArchiveResult{ArchiveHeader: previous}, marker.Stats, nil
	}
	encoded, e := json.Marshal(r)
	if e != nil {
		return out, stats, e
	}
	final := kvstore.NewBatch()
	final.Put("default", archiveRootKey(header.Height), encoded)
	final.Put("default", []byte("M/head"), encoded)
	identity := id.Hex() + ":" + inputID.Hex()
	limit := a.cfg.MaxBatchBytes
	if source.Info().BatchBytes < limit {
		limit = source.Info().BatchBytes
	}
	a.importPending = true
	stats, e = a.db.ImportInitial(ctx, kvstore.BulkPlan{Identity: identity, MarkerKey: []byte("M/initial-import"), BatchBytes: limit, Final: final, Committed: func(b *kvstore.Batch) { a.logical.AddKVBatch(b) }}, func(emit kvstore.BulkEmit) error {
		e := snap.WalkTries(ctx, source, func(_ []byte, root mpt.Hash) error {
			return mpt.WalkPaths(root, source, func(_ []byte, h mpt.Hash, raw []byte) (bool, error) {
				if e := emit("hash", h[:], raw); e != nil {
					return false, e
				}
				return true, nil
			}, nil)
		})
		if e != nil {
			return e
		}
		return source.WalkCode(ctx, func(hash mpt.Hash, raw []byte) error { return emit("default", archiveCodeKey(common.Hash(hash)), raw) })
	})
	if e != nil {
		if errors.Is(e, kvstore.ErrUncertain) {
			a.uncertain = true
		}
		return out, stats, e
	}
	a.head = &r
	a.importPending = false
	a.hashes.Remember(header.Height, header.ID)
	return ArchiveResult{ArchiveHeader: r}, stats, nil
}
