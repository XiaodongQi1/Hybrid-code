package hybridtrie

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

var ErrSnapshotPending = errors.New("hybridtrie: snapshot import incomplete; resume the same source")
var snapshotMarkerKey = []byte("M/snapshot-import")

type snapshotSpec struct {
	Representation     string `json:",omitempty"`
	ProvenanceSHA256   string `json:",omitempty"`
	Schema, SourceID   string
	Height, NextTrieID uint64
	SourceRecords      uint64
	SourceDigest       [32]byte
	Root               mpt.Hash
	BlockID            [32]byte
}
type SnapshotImportStats struct {
	Batches, Records, Bytes, PeakBatchBytes, VerifiedNodes, SkippedRecords uint64
	SourceRecords                                                          uint64
	SourceDigest                                                           [32]byte
	Resumed                                                                bool
}
type snapshotMarker struct {
	Spec     snapshotSpec
	Complete bool
	Stats    SnapshotImportStats
}

// Include the marker itself in the logical Key+Value write budget. Decimal
// counters can change its own length; resolve that small fixed point first.
// Native WAL/SST overhead is not included.
func snapshotBatch(m snapshotMarker, es []entry, limit uint64) (snapshotMarker, []entry, error) {
	base := m.Stats.Bytes
	m.Stats.Batches++
	var payload uint64
	for _, e := range es {
		payload += uint64(len(e.key) + len(e.value))
	}
	for i := 0; i < 16; i++ {
		raw, _ := json.Marshal(m)
		n := payload + uint64(len(snapshotMarkerKey)+len(raw))
		if n > limit {
			return m, nil, ErrLimit
		}
		peak := m.Stats.PeakBatchBytes
		if n > peak {
			peak = n
		}
		if m.Stats.Bytes == base+n && m.Stats.PeakBatchBytes == peak {
			return m, append(es, entry{metaColumn, snapshotMarkerKey, raw}), nil
		}
		m.Stats.Bytes, m.Stats.PeakBatchBytes = base+n, peak
	}
	return m, nil, ErrCorrupt
}

func decodeSnapshotMarker(raw []byte) (snapshotMarker, error) {
	var m snapshotMarker
	if e := json.Unmarshal(raw, &m); e != nil {
		return m, ErrCorrupt
	}
	canonical, _ := json.Marshal(m)
	if m.Spec.Representation != "" && m.Spec.Representation != "hash-cold-bulk" {
		return m, ErrCorrupt
	}
	if !bytes.Equal(raw, canonical) || m.Spec.Schema != "hybrid-snapshot-import/v1" || m.Spec.SourceID == "" || m.Spec.Height > MaxHeight || m.Spec.NextTrieID < 1 || m.Spec.NextTrieID > MaxTrieID+1 || m.Spec.BlockID == ([32]byte{}) || m.Stats.SourceRecords != m.Spec.SourceRecords || m.Stats.SourceDigest != m.Spec.SourceDigest {
		return m, ErrCorrupt
	}
	return m, nil
}
func (s *Store) loadSnapshotMarker(snap diskSnapshot) error {
	raw, e := snap.get(metaColumn, snapshotMarkerKey)
	if errors.Is(e, ErrNotFound) {
		return nil
	}
	if e != nil {
		return e
	}
	m, e := decodeSnapshotMarker(raw)
	if e != nil {
		return e
	}
	if m.Spec.Height != s.cfg.InitialHeight || m.Spec.SourceID != s.cfg.WorkloadID || m.Complete != s.head.Initialized {
		return ErrCorrupt
	}
	if m.Complete {
		root, e := readRoot(snap, 0, m.Spec.Height)
		if e != nil {
			return e
		}
		if root != m.Spec.Root {
			return ErrHashMismatch
		}
	} else {
		s.importPending = true
	}
	return nil
}
func (s *Store) SnapshotImportStatus() (SnapshotImportStats, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return SnapshotImportStats{}, false, e
	}
	snap := s.db.snapshot()
	defer snap.close()
	raw, e := snap.get(metaColumn, snapshotMarkerKey)
	if errors.Is(e, ErrNotFound) {
		return SnapshotImportStats{}, false, nil
	}
	if e != nil {
		return SnapshotImportStats{}, false, e
	}
	m, e := decodeSnapshotMarker(raw)
	return m.Stats, m.Complete, e
}

type snapshotReadStore struct{ snap diskSnapshot }

func (r snapshotReadStore) Get(k []byte) ([]byte, error) { return r.snap.get(hashColumn, k) }
func (r snapshotReadStore) NewBatch() mpt.Batch          { return deniedBatch{} }

// ImportSnapshot writes an owned ready construction database in bounded batches.
// Until the final WAL+Sync, Head remains uninitialized and normal writers are
// blocked by a durable marker. Reopen and pass the same builder to resume.
// The builder is single-owner and must not be modified during this operation.
func (s *Store) ImportSnapshot(b *SnapshotBuilder) (head Head, stats SnapshotImportStats, err error) {
	return s.ImportSnapshotContext(context.Background(), b)
}

// ImportSnapshotContext stops between records without exposing a partial
// snapshot. Cancelled batches are discarded; already synced batches can resume.
func (s *Store) ImportSnapshotContext(ctx context.Context, b *SnapshotBuilder) (head Head, stats SnapshotImportStats, err error) {
	return s.importSnapshotContext(ctx, b, false)
}
func (s *Store) ImportHashSnapshotContext(ctx context.Context, b *SnapshotBuilder) (Head, SnapshotImportStats, error) {
	return s.importSnapshotContext(ctx, b, true)
}
func (s *Store) importSnapshotContext(ctx context.Context, b *SnapshotBuilder, hashCold bool) (head Head, stats SnapshotImportStats, err error) {
	if e := ctx.Err(); e != nil {
		return head, stats, e
	}

	if b == nil {
		return Head{}, stats, ErrConflict
	}
	if e := b.check(); e != nil {
		return Head{}, stats, e
	}
	if !b.progress.Ready || !b.ownedTarget(s.path) {
		return Head{}, stats, ErrConflict
	}
	src := b.db.snapshot()
	defer src.close()
	root, e := readRoot(src, 0, b.cfg.Height)
	if e != nil {
		return Head{}, stats, e
	}
	blockID, e := src.get(metaColumn, evmBlockKey(b.cfg.Height))
	if e != nil {
		return head, stats, e
	}
	if !bytes.Equal(blockID, b.cfg.BlockID[:]) {
		return head, stats, ErrCorrupt
	}
	spec := snapshotSpec{Schema: "hybrid-snapshot-import/v1", SourceID: b.cfg.SourceID, Height: b.cfg.Height, NextTrieID: b.progress.NextTrieID, SourceRecords: b.progress.Cursor, SourceDigest: b.progress.Digest, Root: root, BlockID: b.cfg.BlockID}
	if hashCold {
		spec.Representation = "hash-cold-bulk"
	}
	if hashCold && len(b.cfg.Provenance) > 0 {
		sum := sha256.Sum256(b.cfg.Provenance)
		spec.ProvenanceSHA256 = hex.EncodeToString(sum[:])
	}
	limit := b.cfg.BatchBytes
	if limit > s.cfg.MaxBatchBytes {
		limit = s.cfg.MaxBatchBytes
	}
	if limit < 4096 {
		return head, stats, ErrLimit
	}
	s.mu.Lock()
	if e = s.check(); e == nil && s.readOnly {
		e = ErrReadOnly
	}
	if e == nil && (s.importActive || s.views != 0) {
		e = ErrBusy
	}
	if e == nil && (spec.Height != s.cfg.InitialHeight || spec.SourceID != s.cfg.WorkloadID) {
		e = ErrConflict
	}
	var marker snapshotMarker
	if e == nil {
		snap := s.db.snapshot()
		raw, re := snap.get(metaColumn, snapshotMarkerKey)
		snap.close()
		if re == nil {
			marker, e = decodeSnapshotMarker(raw)
			if e == nil && marker.Spec != spec {
				e = ErrConflict
			}
			if e == nil && marker.Complete {
				head = s.head
				stats = marker.Stats
				s.mu.Unlock()
				return head, stats, nil
			}
			marker.Stats.Resumed = true
		} else if errors.Is(re, ErrNotFound) {
			if s.head.Initialized {
				e = ErrConflict
			} else {
				marker.Spec = spec
				marker.Stats.SourceRecords, marker.Stats.SourceDigest = spec.SourceRecords, spec.SourceDigest
				var es []entry
				marker, es, e = snapshotBatch(marker, nil, limit)
				if e != nil {
					s.mu.Unlock()
					return head, stats, e
				}
				e = s.write(es)
				if e != nil {
					s.uncertain = true
					e = fmt.Errorf("%w: %v", ErrUncertain, e)
				}
			}
		} else {
			e = re
		}
	}
	if e != nil {
		head = s.head
		s.mu.Unlock()
		return head, stats, e
	}
	s.importPending, s.importActive = true, true
	head = s.head
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.importActive = false; s.mu.Unlock() }()
	// Marker size is checked again before each write; reserve room before copying.
	const reserve = 2048
	var entries []entry
	var batchBytes, seq uint64
	stats = marker.Stats
	flush := func() error {
		if len(entries) == 0 {
			return nil
		}
		pending := marker
		pending.Stats = stats
		pending.Stats.Records = seq
		pending, es, e := snapshotBatch(pending, entries, limit)
		if e != nil {
			return e
		}
		s.mu.Lock()
		e = s.write(es)
		if e != nil {
			s.uncertain = true
		}
		s.mu.Unlock()
		if e != nil {
			return fmt.Errorf("%w: %v", ErrUncertain, e)
		}
		stats = pending.Stats
		marker = pending
		entries = nil
		batchBytes = 0
		return nil
	}
	emit := func(cf column, k, v []byte) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		ordinal := seq + 1
		if ordinal <= marker.Stats.Records {
			seq = ordinal
			stats.SkippedRecords++
			return nil
		}
		n := uint64(len(k) + len(v))
		if n+reserve > limit {
			return ErrLimit
		}
		if batchBytes+n+reserve > limit {
			if e := flush(); e != nil {
				return e
			}
		}
		seq = ordinal
		entries = append(entries, entry{cf, bytes.Clone(k), bytes.Clone(v)})
		batchBytes += n
		return nil
	}
	// Metadata and roots are visited in deterministic order both on export and
	// verification. Hash construction records themselves are never copied.
	walk := func(emit func(column, []byte, []byte) error) ([32]byte, error) {
		digest := sha256.New()
		digest.Write([]byte(StoreFormat))
		digest.Write(make([]byte, 32))
		hs, _ := heightSuffix(spec.Height)
		digest.Write(hs)
		reader := snapshotReadStore{src}
		for id := uint64(0); id < spec.NextTrieID; id++ {
			if e := ctx.Err(); e != nil {
				return [32]byte{}, e
			}
			r, e := readRoot(src, id, spec.Height)
			if errors.Is(e, ErrNotFound) && id != 0 {
				r = mpt.EmptyRootHash
			} else if e != nil {
				return [32]byte{}, e
			}
			if id > 0 {
				if _, ok, e := readStorageOwner(src, id); e != nil {
					return [32]byte{}, e
				} else if !ok {
					return [32]byte{}, ErrCorrupt
				}
			}
			if e = mpt.WalkPaths(r, reader, func(path []byte, h mpt.Hash, raw []byte) (bool, error) {
				if hashCold {
					if e := emit(hashColumn, h[:], raw); e != nil {
						return false, e
					}
					return true, nil
				}
				ctx := NodeKey{id, path, spec.Height}
				rec, e := FromCanonical(ctx, raw, independentMask(raw))
				if e != nil {
					return false, e
				}
				if rec.Hash != h {
					return false, ErrHashMismatch
				}
				value, e := EncodeRecord(rec)
				if e != nil {
					return false, e
				}
				key, e := EncodeNodeKey(ctx)
				if e != nil {
					return false, e
				}
				if e = emit(prefixColumn, key, value); e != nil {
					return false, e
				}
				return true, nil
			}, nil); e != nil {
				return [32]byte{}, e
			}
			key, _ := rootKey(id, spec.Height)
			rootValue := r[:]
			if hashCold {
				rootValue, _ = encodeRootLocation(r, Hash)
			}
			if e = emit(metaColumn, key, rootValue); e != nil {
				return [32]byte{}, e
			}
			digest.Write(key)
			digest.Write(rootValue)
		}
		scanner, ok := src.(rangeSource)
		if !ok {
			return [32]byte{}, ErrEngineUnavailable
		}
		e := scanner.scan(metaColumn, []byte{'C'}, []byte{'P'}, func(k, v []byte) error {
			if len(k) == 0 {
				return ErrCorrupt
			}
			switch k[0] {
			case 'C':
				if len(k) != 33 || codeDigest(v) != mpt.Hash(*(*[32]byte)(k[1:])) {
					return ErrCorrupt
				}
			case 'E':
				if !bytes.Equal(k, evmBlockKey(spec.Height)) || !bytes.Equal(v, spec.BlockID[:]) {
					return ErrCorrupt
				}
			case 'L':
				if len(k) != 1+20+HeightBytes || read48(k[21:]) != spec.Height {
					return ErrCorrupt
				}
				life, e := decodeStorageLifecycle(v)
				if e != nil || !life.Active || life.TrieID >= spec.NextTrieID {
					return ErrCorrupt
				}
			case 'O':
				if len(k) != 1+TrieIDBytes || len(v) != 20 || read48(k[1:]) >= spec.NextTrieID {
					return ErrCorrupt
				}
			default:
				return ErrCorrupt
			}
			if e := emit(metaColumn, k, v); e != nil {
				return e
			}
			digest.Write(k)
			digest.Write(v)
			return nil
		})
		if e != nil {
			return [32]byte{}, e
		}
		var high [TrieIDBytes]byte
		put48(high[:], spec.NextTrieID-1)
		digest.Write(high[:])
		var out [32]byte
		copy(out[:], digest.Sum(nil))
		return out, nil
	}
	commitID, e := walk(emit)
	if e != nil {
		return head, stats, e
	}
	if e = flush(); e != nil {
		return head, stats, e
	}
	if seq < marker.Stats.Records {
		return head, stats, ErrCorrupt
	}
	s.mu.Lock()
	target := s.db.snapshot()
	s.mu.Unlock()
	_, e = walk(func(cf column, k, v []byte) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		actual, e := target.get(cf, k)
		if e != nil {
			return e
		}
		if !bytes.Equal(actual, v) {
			return ErrHashMismatch
		}
		if cf == prefixColumn || cf == hashColumn {
			stats.VerifiedNodes++
		}
		return nil
	})
	target.close()
	if e != nil {
		return head, stats, e
	}
	next := Head{Initialized: true, Height: spec.Height, CommitID: commitID, NextTrieID: spec.NextTrieID}
	rawHead, _ := json.Marshal(next)
	marker.Complete = true
	marker.Stats = stats
	bk, _ := blockKey(spec.Height)
	marker, final, e := snapshotBatch(marker, []entry{{metaColumn, headKey, rawHead}, {metaColumn, bk, commitID[:]}}, limit)
	if e != nil {
		return head, stats, e
	}
	if e = ctx.Err(); e != nil {
		return head, stats, e
	}

	s.mu.Lock()
	e = s.write(final)
	if e != nil {
		s.uncertain = true
	} else {
		s.head = next
		s.importPending = false
	}
	s.mu.Unlock()
	if e != nil {
		return head, stats, fmt.Errorf("%w: %v", ErrUncertain, e)
	}
	return next, marker.Stats, nil
}
func independentMask(raw []byte) uint16 {
	d, e := mpt.InspectCanonicalNode(raw)
	if e != nil {
		return 0
	}
	var mask uint16
	for i, r := range d.Children {
		if r.Present && len(r.Inline) == 0 {
			mask |= 1 << i
		}
	}
	return mask
}

func (s *Store) SnapshotImportBinding() (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return "", "", e
	}
	snap := s.db.snapshot()
	defer snap.close()
	raw, e := snap.get(metaColumn, snapshotMarkerKey)
	if e != nil {
		return "", "", e
	}
	m, e := decodeSnapshotMarker(raw)
	return m.Spec.Representation, m.Spec.ProvenanceSHA256, e
}
