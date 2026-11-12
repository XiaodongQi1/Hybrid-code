package patharchive

import (
	"encoding/json"
	"errors"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

var ErrArtifactsUnavailable = errors.New("path archive: external artifact source required")

type stateHeader struct {
	StateID    uint64 `json:",omitempty"`
	Height     uint64
	ID, Digest common.Hash
	Root       mpt.Hash
	Artifacts  *ArtifactRefs
}

func (s *Store) externalRecord(r Record) (Record, []byte, error) {
	if s.artifacts == nil {
		return r, nil, ErrArtifactsUnavailable
	}
	refs := ArtifactRefs{}
	if r.Artifacts != nil {
		refs = *r.Artifacts
	}
	var e error
	if refs.Input, e = s.artifacts.Ensure("block-input", r.Input); e != nil {
		return r, nil, e
	}
	raw, e := json.Marshal(r.Outcomes)
	if e != nil {
		return r, nil, e
	}
	if refs.Outcomes, e = s.artifacts.Ensure("block-outcomes", raw); e != nil {
		return r, nil, e
	}
	raw, e = json.Marshal(r.Changes)
	if e != nil {
		return r, nil, e
	}
	if refs.Changes, e = s.artifacts.Ensure("path-changes", raw); e != nil {
		return r, nil, e
	}
	r.Artifacts = &refs
	wire, e := json.Marshal(stateHeader{r.StateID, r.Height, r.ID, r.Digest, r.Root, r.Artifacts})
	return r, wire, e
}
func (s *Store) input(h uint64) ([]byte, error) {
	if !s.cfg.StateOnly {
		return s.db.Get(heightKey('L', h))
	}
	r, e := s.stateRecord(h)
	if e != nil {
		return nil, e
	}
	if s.artifacts == nil {
		return nil, ErrArtifactsUnavailable
	}
	return s.artifacts.Get("block-input", r.Artifacts.Input)
}
func (s *Store) record(h uint64) (Record, error) {
	r, e := s.stateRecord(h)
	if e != nil || !s.cfg.StateOnly {
		return r, e
	}
	if s.artifacts == nil {
		return r, ErrArtifactsUnavailable
	}
	raw, e := s.artifacts.Get("block-outcomes", r.Artifacts.Outcomes)
	if e != nil {
		return r, e
	}
	if e = json.Unmarshal(raw, &r.Outcomes); e != nil {
		return r, ErrCorrupt
	}
	raw, e = s.artifacts.Get("path-changes", r.Artifacts.Changes)
	if e != nil {
		return r, e
	}
	if e = json.Unmarshal(raw, &r.Changes); e != nil {
		return r, ErrCorrupt
	}
	return r, nil
}

// KVInfo identifies the actual shared driver. Legacy cache tuning remains a
// separate compatibility path and does not pretend to be a kvstore database.
func (a *Archive) KVInfo() (kvstore.Info, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	if e := a.s.check(); e != nil {
		return kvstore.Info{}, e
	}
	if d, ok := a.s.db.(*sharedEngine); ok {
		return d.db.Info(), nil
	}
	return kvstore.Info{}, kvstore.ErrUnsupported
}
