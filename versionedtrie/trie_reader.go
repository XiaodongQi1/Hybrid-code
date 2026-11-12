package versionedtrie

import (
	"errors"
	"fmt"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

var ErrPathRequired = errors.New("versionedtrie: trieID and nibble path required")

type ReadLimits struct{ MaxRecords, MaxRecordBytes uint64 }

func DefaultReadLimits() ReadLimits { return ReadLimits{4096, 16 << 20} }
func (l ReadLimits) validate() error {
	if l.MaxRecords == 0 || l.MaxRecords > 1<<20 || l.MaxRecordBytes == 0 || l.MaxRecordBytes > 1<<30 {
		return ErrLimit
	}
	return nil
}

type ReadStats struct{ Records, RecordBytes, RebuiltNodes, HashReferences, InlineReferences uint64 }

// TrieReader binds one immutable trie instance to one pinned historical view.
// All counters and reconstruction buffers are per call, not shared caches.
type TrieReader struct {
	view   *View
	id     uint32
	limits ReadLimits
}

var _ mpt.Store = (*TrieReader)(nil)
var _ mpt.PathStore = (*TrieReader)(nil)

func (v *View) TrieReader(id uint64) (*TrieReader, error) {
	q, e := u32(id)
	if e != nil {
		return nil, e
	}
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil, ErrClosed
	}
	return &TrieReader{v, q, DefaultReadLimits()}, nil
}
func (r *TrieReader) WithLimits(limits ReadLimits) (*TrieReader, error) {
	if e := limits.validate(); e != nil {
		return nil, e
	}
	return &TrieReader{r.view, r.id, limits}, nil
}
func (r *TrieReader) Get([]byte) ([]byte, error) { return nil, ErrPathRequired }

type deniedBatch struct{}

func (deniedBatch) Put([]byte, []byte) error { return ErrReadOnly }
func (deniedBatch) Write() error             { return ErrReadOnly }
func (r *TrieReader) NewBatch() mpt.Batch    { return deniedBatch{} }
func (r *TrieReader) GetNode(path []byte, expected mpt.Hash) ([]byte, error) {
	raw, _, e := r.ReadNodeWithStats(path, expected)
	return raw, e
}
func (r *TrieReader) ReadNodeWithStats(path []byte, expected mpt.Hash) ([]byte, ReadStats, error) {
	var stats ReadStats
	if _, e := nodeHeader(uint64(r.id), path); e != nil {
		return nil, stats, e
	}
	r.view.mu.RLock()
	defer r.view.mu.RUnlock()
	if r.view.closed {
		return nil, stats, ErrClosed
	}
	load := func(path []byte) (Record, error) {
		if stats.Records >= r.limits.MaxRecords {
			return Record{}, ErrLimit
		}
		header, e := nodeHeader(uint64(r.id), path)
		if e != nil {
			return Record{}, e
		}
		_, raw, e := predecessor(r.view.snap, header, r.view.height)
		if e != nil {
			if errors.Is(e, ErrNotFound) {
				return Record{}, fmt.Errorf("%w: missing required node", ErrCorrupt)
			}
			return Record{}, e
		}
		if uint64(len(raw)) > r.limits.MaxRecordBytes-stats.RecordBytes {
			return Record{}, ErrLimit
		}
		stats.Records++
		stats.RecordBytes += uint64(len(raw))
		rec, e := DecodeRecord(raw)
		if e != nil {
			return Record{}, e
		}
		if e = rec.validate(len(path)); e != nil {
			return Record{}, e
		}
		return rec, nil
	}
	var build func([]byte, Record) ([]byte, error)
	var child func([]byte) (mpt.CanonicalReference, error)
	child = func(path []byte) (mpt.CanonicalReference, error) {
		rec, e := load(path)
		if e != nil {
			return mpt.CanonicalReference{}, e
		}
		if !rec.Inline {
			stats.HashReferences++
			return mpt.CanonicalReference{Present: true, Hash: rec.Hash}, nil
		}
		stats.InlineReferences++
		raw, e := build(path, rec)
		if e != nil {
			return mpt.CanonicalReference{}, e
		}
		return mpt.CanonicalReference{Present: true, Inline: raw}, nil
	}
	build = func(path []byte, rec Record) ([]byte, error) {
		desc := mpt.CanonicalNode{Kind: mpt.CanonicalKind(rec.Kind), Path: rec.Path, Value: rec.Value}
		switch rec.Kind {
		case Branch:
			for i := 0; i < 16; i++ {
				if rec.Bitmap&(1<<uint(i)) == 0 {
					continue
				}
				ref, e := child(append(append([]byte(nil), path...), byte(i)))
				if e != nil {
					return nil, e
				}
				desc.Children[i] = ref
			}
		case Extension:
			fragment, leaf, e := mpt.DecodeCompactPath(rec.Path)
			if e != nil || leaf {
				return nil, ErrCorrupt
			}
			ref, e := child(append(append([]byte(nil), path...), fragment...))
			if e != nil {
				return nil, e
			}
			desc.Children[0] = ref
		}
		raw, hash, e := mpt.RebuildCanonicalNode(desc)
		if e != nil {
			return nil, fmt.Errorf("%w: %v", ErrCorrupt, e)
		}
		if (len(raw) < 32) != rec.Inline || hash != rec.Hash {
			return nil, fmt.Errorf("%w: reconstructed node mismatch", ErrCorrupt)
		}
		stats.RebuiltNodes++
		return raw, nil
	}
	rec, e := load(path)
	if e != nil {
		return nil, stats, e
	}
	if rec.Hash != expected {
		return nil, stats, fmt.Errorf("%w: unexpected node hash", ErrCorrupt)
	}
	raw, e := build(path, rec)
	return raw, stats, e
}
