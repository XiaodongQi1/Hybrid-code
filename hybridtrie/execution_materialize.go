package hybridtrie

import (
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"time"
	"unsafe"
)

// A reference follows the immutable source position even if deletion merges or
// insertion splits its logical path. Only execution calls Materialize.
type executionReference struct {
	reader   *boundaryReader
	key      executionReadKey
	location mpt.NodeLocation
	hash     mpt.Hash
	known    bool
}

func (r *executionReference) Location() mpt.NodeLocation    { return r.location }
func (r *executionReference) KnownDigest() (mpt.Hash, bool) { return r.hash, r.known }
func (r *executionReference) Prefetch() {
	if !r.known && ReferenceKind(r.location.Kind) == Prefix {
		r.reader.block.reads.prefetch(r.key, r.location)
	}
}
func (r *executionReference) Digest() (mpt.Hash, error) {
	if r.known {
		return r.hash, nil
	}
	s := r.reader.block.reads
	e, err := s.need(r.key, r.location, r.hash, false, false, r.reader.admission)
	if err != nil {
		return mpt.Hash{}, err
	}
	s.mu.Lock()
	s.stats.HashHeaderHits++
	h := e.hash
	r.location = e.loc
	r.hash = h
	r.known = true
	if !e.tracked {
		e.raw = nil
	}
	s.mu.Unlock()
	return h, nil
}
func (r *executionReference) Materialize() (mpt.MaterializedNode, error) {
	return r.reader.executionNode(r.key, r.hash, r.known, r.location, &r.location)
}
func (r *boundaryReader) DeferredReads() bool { return r.block.reads != nil }

func (r *boundaryReader) executionMaterialized(path []byte, expected mpt.Hash, location *mpt.NodeLocation) (mpt.MaterializedNode, error) {
	b := r.block
	f := b.owner.boundary
	if !b.sharedPreparing {
		b.owner.mu.Lock()
	}
	if !r.scope() {
		if !b.sharedPreparing {
			b.owner.mu.Unlock()
		}
		return mpt.MaterializedNode{}, ErrClosed
	}
	if _, err := pack(path); err != nil {
		if !b.sharedPreparing {
			b.owner.mu.Unlock()
		}
		return mpt.MaterializedNode{}, err
	}
	if i := f.current.find(position(r.base.id, path)); i >= 0 && f.current.hashAt(i) == expected && f.current.versionAt(i).Height <= r.head.Height {
		n, err := r.residentMaterialized(i)
		if !b.sharedPreparing {
			b.owner.mu.Unlock()
		}
		return n, err
	}
	loc, err := r.materializedLocation(path, expected, location)
	if !b.sharedPreparing {
		f.stats.NodeMisses++
		b.owner.mu.Unlock()
	}
	if err != nil {
		return mpt.MaterializedNode{}, err
	}
	return r.executionNode(executionKey(r.base.id, path), expected, true, loc)
}
func (r *boundaryReader) executionNode(key executionReadKey, expected mpt.Hash, known bool, loc mpt.NodeLocation, resolved ...*mpt.NodeLocation) (mpt.MaterializedNode, error) {
	s := r.block.reads
	e, err := s.need(key, loc, expected, known, true, r.admission)
	if err != nil {
		return mpt.MaterializedNode{}, err
	}
	if len(resolved) > 0 {
		*resolved[0] = e.loc
	}
	// Foreground alone publishes parsed fields; workers publish bytes only.
	s.mu.Lock()
	if e.parsed {
		s.stats.ParsedReuseHits++
		n := e.node
		s.mu.Unlock()
		return n, nil
	}
	raw := e.raw
	s.mu.Unlock()
	started := time.Now()
	n, refs, err := r.parseExecution(e, raw)
	elapsed := uint64(time.Since(started))
	s.mu.Lock()
	s.stats.NodeDecodeCount++
	s.stats.NodeDecodeBytes += uint64(len(raw))
	s.stats.NodeDecodeNanos += elapsed
	s.rawBytes -= e.rawCharge
	e.rawCharge = 0
	e.raw = nil
	if err == nil {
		charge := n.RetainedBytes() + uint64(refs)*uint64(unsafe.Sizeof(executionReference{}))
		scratch := uint64(cap(raw)) + charge + 128 // detached fragment and compact-path scratch
		if scratch > s.stats.PeakDecodeScratchBytes {
			s.stats.PeakDecodeScratchBytes = scratch
		}
		if e.tracked && s.rawBytes+s.nodeBytes+charge <= s.cfg.BodyBytes {
			e.node = n
			e.parsed = true
			e.nodeBytes = charge
			s.nodeBytes += charge
			s.peaks()
		} else {
			s.stats.BodyDrops++
		}
	} else {
		e.bodyErr = err
	}
	s.mu.Unlock()
	if err != nil {
		return mpt.MaterializedNode{}, err
	}
	b := r.block
	if !b.sharedPreparing {
		b.owner.mu.Lock()
	}
	// Do not manufacture canonical bytes just for a cache observation. Admission
	// can obtain the same parsed node from the table when it actually needs it.
	if r.scope() {
		obs := boundaryObservation{Pos: position(key.ID, key.path()), PreparationOnly: b.sharedPreparing, boundaryNode: boundaryNode{Hash: e.hash, Version: forestVersion{Height: e.loc.Height, Source: ReferenceKind(e.loc.Kind), Known: e.loc.Exact}}}
		b.owner.boundary.observe(obs, nil)
	}
	if !b.sharedPreparing {
		b.owner.mu.Unlock()
	}
	return n, nil
}
func (r *boundaryReader) parseExecution(e *executionReadEntry, raw []byte) (mpt.MaterializedNode, int, error) {
	var d mpt.CanonicalNode
	var rec Record
	var err error
	path := e.key.path()
	ctx := NodeKey{e.key.ID, path, r.head.Height}
	if ReferenceKind(e.loc.Kind) == Prefix {
		rec, err = decodeRecord(raw, true)
		if err == nil {
			err = rec.validatePlacement(ctx)
		}
		if err == nil {
			d.Kind, d.Value = rec.Kind, rec.Value
			if rec.Kind != mpt.CanonicalBranch {
				d.Path, err = mpt.EncodeCompactPath(rec.Fragment, rec.Kind == mpt.CanonicalLeaf)
			}
			for i, ref := range rec.Children {
				d.Children[i] = mpt.CanonicalReference{Present: ref.Kind != Empty, Hash: ref.Hash, Inline: ref.Inline}
			}
		}
	} else {
		d, err = mpt.DecodeLocalNodeFields(raw)
		if err == nil {
			rec, err = recordFromFields(ctx, d, e.hash, 0)
		}
	}
	if err != nil {
		return mpt.MaterializedNode{}, 0, err
	}
	var refs [16]mpt.LocalNodeReference
	count := 0
	for i, ref := range rec.Children {
		if ref.Kind != Prefix && ref.Kind != Hash {
			continue
		}
		child, err := locateChild(ctx, rec, i)
		if err != nil {
			return mpt.MaterializedNode{}, 0, err
		}
		refs[i] = &executionReference{reader: r, key: executionKey(ctx.TrieID, child.Target.Path), location: mpt.NodeLocation{Kind: uint8(ref.Kind)}, hash: ref.Hash, known: ref.Kind == Hash}
		count++
	}
	n, err := mpt.MaterializeDeferredLocal(d, e.hash, refs)
	return n, count, err
}

func (r *boundaryReader) executionCanonical(pos forestPosition, want mpt.Hash, loc mpt.NodeLocation) ([]byte, boundaryObservation, error) {
	session := r.block.reads
	previous := session.foregroundAdmission
	session.foregroundAdmission = r.admission
	defer func() { session.foregroundAdmission = previous }()
	n, err := r.executionNode(executionKey(pos.ID, pos.Path[:pos.Length]), want, true, loc)
	if err != nil {
		return nil, boundaryObservation{}, err
	}
	raw, err := n.LocalRLP()
	if err != nil {
		return nil, boundaryObservation{}, err
	}
	s := r.block.reads
	e, err := s.need(executionKey(pos.ID, pos.Path[:pos.Length]), loc, want, true, false)
	if err != nil {
		return nil, boundaryObservation{}, err
	}
	obs := boundaryObservation{Pos: pos, PreparationOnly: r.block.sharedPreparing, boundaryNode: boundaryNode{Hash: want, Version: forestVersion{Height: e.loc.Height, Source: ReferenceKind(e.loc.Kind), Known: e.loc.Exact}}}
	// LocalRLP has resolved every non-inline child digest. Preserve the exact
	// physical hints as well, so read-only admissions do not force a later root
	// retrace. This inspects local references only; it performs no further I/O.
	for i, child := range n.LocalChildLocations() {
		if ReferenceKind(child.Kind) == Hash {
			obs.Edges[i] = edgeAt(0, boundaryHash)
		} else if ReferenceKind(child.Kind) == Prefix && child.Exact {
			obs.Edges[i] = edgeAt(child.Height, boundaryPrefix)
		}
	}
	return raw, obs, nil
}
