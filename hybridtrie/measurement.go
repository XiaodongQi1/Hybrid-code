package hybridtrie

import (
	"context"
	"fmt"

	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

func (s *Store) EngineStats() (kvstore.Statistics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return kvstore.Statistics{}, e
	}
	if d, ok := s.db.(*kvEngine); ok {
		return d.store.Statistics()
	}
	return kvstore.Statistics{}, kvstore.ErrUnsupported
}
func (s *Store) MeasureStorage(ctx context.Context, stabilize bool) (kvstore.Measurement, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return kvstore.Measurement{}, e
	}
	if s.importActive || s.importPending {
		return kvstore.Measurement{}, ErrSnapshotPending
	}
	d, ok := s.db.(*kvEngine)
	if !ok {
		return kvstore.Measurement{}, kvstore.ErrUnsupported
	}
	return d.store.Measure(ctx, func(column string, k, v []byte) (kvstore.Classification, error) {
		state := func(name string) (kvstore.Classification, error) {
			return kvstore.Classification{Name: name, State: true}, nil
		}
		if column == "prefix" {
			key, e := DecodeNodeKey(k)
			if e != nil {
				return kvstore.Classification{}, e
			}
			record, e := DecodeRecord(v)
			if e != nil {
				return kvstore.Classification{}, e
			}
			if e = record.ValidateAt(key); e != nil {
				return kvstore.Classification{}, e
			}
			if key.TrieID == 0 {
				return state("account-prefix")
			}
			return state("storage-prefix")
		}
		if column == "hash" {
			if len(k) != 32 {
				return kvstore.Classification{}, ErrCorrupt
			}
			var hash mpt.Hash
			copy(hash[:], k)
			n, e := canonicalHash(v, hash)
			if e != nil {
				return kvstore.Classification{}, e
			}
			switch n.Kind {
			case mpt.CanonicalBranch:
				return state("hash-branch")
			case mpt.CanonicalExtension:
				return state("hash-extension")
			case mpt.CanonicalLeaf:
				return state("hash-leaf")
			}
		}
		if column == "default" && len(k) > 0 {
			switch {
			case string(k) == string(manifestKey):
				return kvstore.Classification{Name: "configuration"}, nil
			case string(k) == string(headKey):
				return state("coordination")
			case string(k) == string(snapshotMarkerKey):
				return state("initialization-marker")
			case k[0] == 'R':
				return state("root-headers")
			case k[0] == 'B' || k[0] == 'E':
				return state("block-headers")
			case k[0] == 'C':
				return state("code")
			case k[0] == 'L' || k[0] == 'O':
				return state("lifecycle")
			}
		}
		return kvstore.Classification{}, fmt.Errorf("%w: unclassified Hybrid state record", kvstore.ErrCorrupt)
	}, stabilize)
}
