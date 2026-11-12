package patharchive

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	snap "github.com/XiaodongQi1/HybridTrie/evm-state/snapshot"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// ImportSnapshot creates the indexed/packed native Path initial state directly
// from the common canonical source. It writes no reverse history before the
// supplied initial height and never materializes the whole forest in RAM.
func (a *Archive) ImportSnapshot(ctx context.Context, header block.InputBlock, source *ht.SnapshotSource, expected *mpt.Hash) (Record, kvstore.BulkStats, error) {
	s := a.s
	s.mu.Lock()
	defer s.mu.Unlock()
	var record Record
	var stats kvstore.BulkStats
	if s.closed {
		return record, stats, kvstore.ErrClosed
	}
	if s.uncertain {
		return record, stats, ErrUnknown
	}
	if s.readOnly {
		return record, stats, ErrReadOnly
	}
	d, ok := s.db.(*sharedEngine)
	if !ok || !s.cfg.StateOnly || !IsIndexedFormat(s.cfg.Format) {
		return record, stats, kvstore.ErrUnsupported
	}
	if source == nil || header.Height != s.cfg.initial() || !source.IndependentOf(d.db.Path()) {
		return record, stats, ErrCorrupt
	}
	m, e := snap.Verify(ctx, source, header)
	if e != nil {
		return record, stats, e
	}
	if m.WorkloadID != s.cfg.WorkloadID || expected != nil && m.Root != *expected {
		return record, stats, ErrCorrupt
	}
	raw, e := json.Marshal(m)
	if e != nil {
		return record, stats, e
	}
	id, e := s.artifacts.Ensure("snapshot-manifest", raw)
	if e != nil {
		return record, stats, e
	}
	input, e := json.Marshal(header)
	if e != nil {
		return record, stats, e
	}
	if s.head.ID != (common.Hash{}) {
		prior, e := s.record(header.Height)
		if e != nil || prior.Root != m.Root || prior.ID != header.ID || prior.Artifacts.SnapshotManifest != id.Hex() {
			return record, stats, ErrCorrupt
		}
		marker, e := d.db.BulkStatus([]byte("M/initial-import"))
		if e != nil || !marker.Complete {
			return record, stats, ErrCorrupt
		}
		return prior, marker.Stats, nil
	}
	digester := sha256.New()
	digester.Write([]byte(s.cfg.Format + "\x00" + s.cfg.WorkloadID))
	digester.Write(make([]byte, 32))
	digester.Write(input)
	record = Record{Height: header.Height, ID: header.ID, Root: m.Root, Digest: common.BytesToHash(digester.Sum(nil)), Input: input, Artifacts: &ArtifactRefs{SnapshotManifest: id.Hex()}}
	// Count only bounded output records; the construction source retains inputs.
	walk := func(emit kvstore.BulkEmit) error {
		var nodes, values uint64
		r := s.live(ctx)
		e := snap.WalkTries(ctx, source, func(owner []byte, root mpt.Hash) error {
			return mpt.WalkPaths(root, source, func(path []byte, _ mpt.Hash, raw []byte) (bool, error) {
				key := r.key('P', owner, path)
				value, e := encodeValueWithCodec(key, raw, s.cfg.codec())
				if e != nil {
					return false, e
				}
				if e = emit("default", key, value); e != nil {
					return false, e
				}
				nodes++
				return true, nil
			}, func(key, value []byte) error {
				key = latestKey('F', owner, key)
				encoded, e := encodeValueWithCodec(key, value, s.cfg.codec())
				if e != nil {
					return e
				}
				if e = emit("default", key, encoded); e != nil {
					return e
				}
				values++
				return nil
			})
		})
		if e != nil {
			return e
		}
		record.Changes.Nodes, record.Changes.Values = nodes, values
		return source.WalkCode(ctx, func(hash mpt.Hash, raw []byte) error { return emit("default", append([]byte{'C'}, hash[:]...), raw) })
	}
	// Header metrics are an external artifact. A bounded counting pass supplies
	// them before any target state is published; it retains no rows.
	if e = walk(func(string, []byte, []byte) error { return ctx.Err() }); e != nil {
		return record, stats, e
	}
	record, raw, e = s.externalRecord(record)
	if e != nil {
		return record, stats, e
	}
	final := kvstore.NewBatch()
	final.Put("default", heightKey('B', header.Height), raw)
	final.Put("default", []byte("M/head"), raw)
	s.importPending = true
	limit := s.cfg.MaxBatchBytes
	if source.Info().BatchBytes < limit {
		limit = source.Info().BatchBytes
	}
	stats, e = d.db.ImportInitial(ctx, kvstore.BulkPlan{Identity: id.Hex() + ":" + s.cfg.Format, MarkerKey: []byte("M/initial-import"), BatchBytes: limit, Final: final, Committed: func(b *kvstore.Batch) { s.logical.AddKVBatch(b) }}, walk)
	if e != nil {
		if errors.Is(e, kvstore.ErrUncertain) {
			s.uncertain = true
		}
		return Record{}, stats, e
	}
	s.head = record
	s.importPending = false
	a.hashes.Remember(header.Height, header.ID)
	return record, stats, nil
}
