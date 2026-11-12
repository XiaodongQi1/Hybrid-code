package hybridtrie

import (
	"bytes"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"time"
)

type boundaryReader struct {
	admission  *admissionReadBudget
	block      *ForestBlock
	base       *trieReader
	head       Head
	generation uint64
}

func (r *boundaryReader) Get([]byte) ([]byte, error) { return nil, ErrReadOnly }
func (r *boundaryReader) NewBatch() mpt.Batch        { return deniedBatch{} }
func (b *ForestBlock) boundaryReader(id uint64, root mpt.Hash) (mpt.Store, error) {
	if r := b.boundaryReaders[id]; r != nil {
		if r.base.root != root {
			return nil, ErrHashMismatch
		}
		return r, nil
	}
	s, e := b.base.TrieReader(id, root)
	if e != nil {
		return nil, e
	}
	r := &boundaryReader{block: b, base: s.(*trieReader), head: b.update.base, generation: b.owner.boundary.generation}
	b.boundaryReaders[id] = r
	return r, nil
}
func (r *boundaryReader) scope() bool {
	b := r.block
	f := b.owner.boundary
	return f != nil && !b.done && !b.owner.closed && b.owner.active == b && !f.uncertain && f.head == r.head && f.generation == r.generation && b.base == r.base.view
}
func (r *boundaryReader) ObserveTrieAccess(path []byte) {
	b := r.block
	if b.sharedPreparing {
		return
	}
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()
	if !r.scope() || len(path) > 64 {
		return
	}
	f := b.owner.boundary
	t := time.Now()
	if len(path) == 0 {
		f.stats.AccessEvents++
	}
	f.event(position(r.base.id, path), b.update.height)
	f.stats.HeatNanos += uint64(time.Since(t))
}
func (r *boundaryReader) GetNode(path []byte, h mpt.Hash) ([]byte, error) {
	if r.DeferredReads() {
		n, err := r.executionMaterialized(path, h, nil)
		if err != nil {
			return nil, err
		}
		raw, err := n.LocalRLP()
		if err != nil {
			return nil, err
		}
		if _, err = canonicalHash(raw, h); err != nil {
			return nil, err
		}
		return raw, nil
	}
	if !r.block.sharedPreparing {
		r.block.owner.mu.Lock()
		defer r.block.owner.mu.Unlock()
	}
	raw, err := r.getNode(path, h)
	if err != nil {
		return nil, err
	}
	// A resident or candidate may have been populated by a trusted local read.
	// Authenticate bytes returned through the public canonical node interface.
	if _, err = canonicalHash(raw, h); err != nil {
		return nil, err
	}
	return raw, nil
}

// GetMaterializedNode bypasses the RLP byte interface on a retained N hit.
// Returned COW objects own their fields; bank reuse cannot mutate older tries.
func (r *boundaryReader) GetMaterializedNode(path []byte, expected mpt.Hash) (mpt.MaterializedNode, error) {
	if r.DeferredReads() {
		return r.executionMaterialized(path, expected, nil)
	}
	if r.block.sharedPreparing {
		return r.getMaterializedNode(path, expected)
	}
	r.block.owner.mu.Lock()
	defer r.block.owner.mu.Unlock()
	return r.getMaterializedNode(path, expected)
}
func (r *boundaryReader) getMaterializedNode(path []byte, expected mpt.Hash) (mpt.MaterializedNode, error) {
	return r.getLocatedMaterializedNode(path, expected, nil)
}

func (r *boundaryReader) getLocatedMaterializedNode(path []byte, expected mpt.Hash, location *mpt.NodeLocation) (mpt.MaterializedNode, error) {
	if !r.scope() {
		return mpt.MaterializedNode{}, ErrClosed
	}
	if _, e := pack(path); e != nil {
		return mpt.MaterializedNode{}, e
	}
	f := r.block.owner.boundary
	if i := f.current.find(position(r.base.id, path)); i >= 0 && f.current.hashAt(i) == expected && f.current.versionAt(i).Height <= r.head.Height {
		return r.residentMaterialized(i)
	}
	if !r.block.sharedPreparing {
		f.stats.NodeMisses++
	}
	loc, e := r.materializedLocation(path, expected, location)
	if e != nil {
		return mpt.MaterializedNode{}, e
	}
	var n mpt.MaterializedNode
	raw, observed, e := r.localReader().loadLocatedInto(position(r.base.id, path), expected, loc, &n)
	if e != nil {
		return mpt.MaterializedNode{}, e
	}
	observed.PreparationOnly = r.block.sharedPreparing
	f.observe(observed, raw)
	return withBoundaryLocations(n, observed.Edges), nil
}

// Keep the canonical byte/proof interface strict, even when the same pinned
// block reader also serves ordinary local execution.
func (r *boundaryReader) localReader() *boundaryReader {
	base := *r.base
	base.local = true
	if !r.block.sharedPreparing {
		f := r.block.owner.boundary
		base.sibling = func(at NodeKey, rec Record, raw []byte) {
			pos := position(at.TrieID, at.Path)
			if i := f.current.find(pos); i >= 0 && f.current.hashAt(i) == rec.Hash {
				return
			}
			n := boundaryObservation{Pos: pos, boundaryNode: boundaryNode{Hash: rec.Hash, Version: forestVersion{at.Height, Prefix, true}}}
			f.observePayload(n, raw, true)
		}
	}
	local := *r
	local.base = &base
	return &local
}

// A current N parent authenticates both the digest and position of this edge.
// Version information never substitutes for that digest check.
func boundaryImageChild(n boundaryObservation, image mpt.NodeImage, slot int) (forestPosition, mpt.Hash, bool) {
	identity, e := image.Identity()
	if e != nil {
		return forestPosition{}, mpt.Hash{}, false
	}
	hash, ok := image.ChildHash(slot)
	if !ok {
		return forestPosition{}, mpt.Hash{}, false
	}
	p := append([]byte(nil), n.Pos.Path[:n.Pos.Length]...)
	if identity.Kind == mpt.CanonicalExtension {
		if slot != 0 {
			return forestPosition{}, mpt.Hash{}, false
		}
		p = append(p, identity.Fragment[:identity.Length]...)
	} else if identity.Kind == mpt.CanonicalBranch {
		p = append(p, byte(slot))
	} else {
		return forestPosition{}, mpt.Hash{}, false
	}
	return position(n.Pos.ID, p), hash, true
}
func (f *boundaryForest) parent(b *boundaryBank, pos forestPosition, hash mpt.Hash) (int, int) {
	best := -1
	for depth := int(pos.Length) - 1; depth >= 0; depth-- {
		if i := b.find(position(pos.ID, pos.Path[:depth])); i >= 0 {
			best = i
			break
		}
	}

	if best < 0 {
		return -1, -1
	}
	n := b.observation(best)
	d, e := n.Image.Identity()
	if e != nil {
		return -1, -1
	}
	slot := 0
	if d.Kind == mpt.CanonicalBranch {
		slot = int(pos.Path[n.Pos.Length])
	}
	p, h, ok := boundaryImageChild(n, n.Image, slot)
	if !ok || p != pos || h != hash {
		return -1, -1
	}
	return best, slot
}

func (r *boundaryReader) counters(s ResolutionStats, exact uint64) {
	f := r.block.owner.boundary
	if r.block.sharedPreparing {
		f.stats.PreparationExactGets += exact
		f.stats.PreparationSeeks += s.PrefixReads
		f.stats.PreparationHashGets += s.HashReads
	} else {
		f.stats.PrefixExactGets += exact
		f.stats.PrefixSeeks += s.PrefixReads
		f.stats.HashGets += s.HashReads
	}
}
func (r *boundaryReader) getNode(path []byte, expected mpt.Hash) ([]byte, error) {
	if !r.scope() {
		return nil, ErrClosed
	}
	if _, e := pack(path); e != nil {
		return nil, e
	}
	f := r.block.owner.boundary
	pos := position(r.base.id, path)
	if i := f.current.find(pos); i >= 0 && f.current.hashAt(i) == expected && f.current.versionAt(i).Height <= r.head.Height {
		if !r.block.sharedPreparing {
			f.stats.NodeHits++
		}
		f.stats.TemporaryRLPEncodes++
		return f.current.imageAt(i).RLP()
	}
	if !r.block.sharedPreparing {
		f.stats.NodeMisses++
	}
	// Reuse authenticated bodies already resolved during this block. This is the
	// existing bounded candidate/commit buffer, not an additional retained cache.
	if i := f.findCandidate(pos, expected); i >= 0 {
		c := f.candidates[i]
		v := c.Node.Version
		if c.Node.Size != 0 && (v.Source == Hash || v.Source == Prefix && v.Known && v.Height <= r.head.Height) {
			if r.block.sharedPreparing {
				f.stats.PreparationBodyReuses++
			} else {
				f.stats.TransientBodyHits++
				f.candidates[i].Node.PreparationOnly = false
			}
			return append([]byte(nil), f.candidateBody[c.Offset:c.Offset+c.Node.Size]...), nil
		}
	}

	var route boundaryEdge
	if len(path) == 0 {
		if i := f.current.root(pos.ID); i >= 0 && f.current.Roots[i].Hash == expected {
			v := f.current.Roots[i].Version
			if v.Source == Prefix && v.Known {
				route = edgeAt(v.Height, boundaryPrefix)
			} else if v.Source == Hash {
				route.Kind = boundaryHash
			}
		}
	} else if i, slot := f.parent(&f.current, pos, expected); i >= 0 {
		route = f.current.edgeAtSlot(i, slot)
		if !r.block.sharedPreparing {
			f.stats.ParentHashReuses++
		}
	}
	if route.Kind == boundaryCold {
		if i := f.findCandidate(pos, expected); i >= 0 {
			c := f.candidates[i]
			if c.CompactSize != 0 && c.Node.Version.Height <= r.head.Height {
				route = edgeAt(c.Node.Version.Height, boundaryPrefix)
			}
		}
	}
	if route.Kind == boundaryPrefix && route.height() > r.head.Height {
		route = boundaryEdge{}
	}
	if route.Kind != boundaryCold {
		raw, n, e := r.exact(pos, expected, route)
		if e != nil {
			return nil, e
		}
		n.PreparationOnly = r.block.sharedPreparing
		f.observe(n, raw)
		return raw, nil
	}
	// A cold path walks from the trusted root. Capture actual predecessor keys,
	// but publish observations only after each node's parent hash is checked.
	var stats ResolutionStats
	versions := make(map[forestPosition]uint64)
	matched := func(q, a NodeKey) { versions[position(q.TrieID, q.Path)] = a.Height }
	opened := func(ctx NodeKey, source ReferenceKind, record Record, raw []byte) {
		n := boundaryObservation{Pos: position(ctx.TrieID, ctx.Path), boundaryNode: boundaryNode{Hash: record.Hash, Version: forestVersion{Source: source}}}
		if source == Prefix {
			n.Version.Height, n.Version.Known = versions[n.Pos]
		}
		for slot, ref := range record.Children {
			if ref.Kind == Hash {
				n.Edges[slot].Kind = boundaryHash
			}
			if ref.Kind == Prefix {
				loc, _ := locateChild(ctx, record, slot)
				if h, ok := versions[position(ctx.TrieID, loc.Target.Path)]; ok {
					n.Edges[slot] = edgeAt(h, boundaryPrefix)
				}
			}
		}
		n.PreparationOnly = r.block.sharedPreparing
		f.observe(n, raw)
	}
	raw, e := r.base.getNodeObserved(path, expected, nil, &stats, matched, opened)
	r.counters(stats, 0)
	if e != nil { // Discard observations from a failed traversal; they are optional.
		f.candidateN, f.candidateUsed = 0, 0
		clearBoundary(f.candidateIndex)
		return nil, e
	}
	if !r.block.sharedPreparing {
		f.stats.ColdResolutions++
	}
	return raw, nil
}

// exact reconstructs only the requested node. BP grandchildren may still need
// predecessor reads for their digests; those are counted separately.
func (r *boundaryReader) exact(pos forestPosition, want mpt.Hash, route boundaryEdge) ([]byte, boundaryObservation, error) {
	if r.DeferredReads() {
		return r.executionCanonical(pos, want, locationForEdge(route))
	}
	loc := locationForEdge(route)
	if loc.Kind == 0 {
		return nil, boundaryObservation{}, ErrCorrupt
	}
	return r.loadLocated(pos, want, loc)
}

func (r *boundaryReader) loadLocated(pos forestPosition, want mpt.Hash, location mpt.NodeLocation) ([]byte, boundaryObservation, error) {
	return r.loadLocatedInto(pos, want, location, nil)
}
func (r *boundaryReader) loadLocatedInto(pos forestPosition, want mpt.Hash, location mpt.NodeLocation, out *mpt.MaterializedNode) ([]byte, boundaryObservation, error) {
	var n boundaryObservation
	r.base.view.mu.RLock()
	defer r.base.view.mu.RUnlock()
	if r.base.view.closed {
		return nil, n, ErrClosed
	}
	n.Pos, n.Hash = pos, want
	if i := r.stagedCandidate(pos, want, &location); i >= 0 {
		f := r.block.owner.boundary
		c := f.candidates[i]
		if c.Node.Size != 0 {
			if r.block.sharedPreparing {
				f.stats.PreparationBodyReuses++
			} else {
				f.stats.TransientBodyHits++
				f.candidates[i].Node.PreparationOnly = false
			}
			raw := f.candidateBody[c.Offset : c.Offset+c.Node.Size]
			if out != nil {
				decoded, e := mpt.DecodeTrustedLocalNode(raw, want)
				if e != nil {
					return nil, n, e
				}
				*out = decoded
			}
			return raw, c.Node, nil
		}
	}
	ctx := NodeKey{pos.ID, pos.Path[:pos.Length], r.base.height}
	stats := ResolutionStats{}
	exact := uint64(0)
	defer func() { r.counters(stats, exact) }()
	if ReferenceKind(location.Kind) == Hash {
		budget := &readBudget{cfg: r.base.cfg, admission: r.admission}
		if e := budget.beforeRead(); e != nil {
			return nil, n, e
		}
		stats.HashReads++
		raw, e := r.base.snapshot.get(hashColumn, want[:])
		if e != nil {
			return nil, n, e
		}
		if e = budget.charge(len(raw) + 32); e != nil {
			return nil, n, e
		}
		if len(raw) > mpt.MaxCanonicalNodeBytes || len(pos.Path[:pos.Length]) > 0 && len(raw) < 32 {
			return nil, n, ErrCorrupt
		}
		var d mpt.CanonicalNode
		if r.base.local {
			d, e = mpt.DecodeLocalNodeFields(raw)
		} else {
			d, e = canonicalHash(raw, want)
		}
		if e != nil {
			return nil, n, e
		}
		for i, ref := range d.Children {
			if ref.Present && len(ref.Inline) == 0 {
				n.Edges[i].Kind = boundaryHash
			}
		}
		n.Version.Source = Hash
		if out != nil {
			decoded, e := mpt.MaterializeLocalFields(d, want, uint32(len(raw)))
			if e != nil {
				return nil, n, e
			}
			*out = decoded
		}
		return raw, n, nil
	}
	if ReferenceKind(location.Kind) != Prefix || location.Exact && location.Height > r.base.height {
		return nil, n, ErrCorrupt
	}
	b := &readBudget{cfg: r.base.cfg, stats: &stats, admission: r.admission}
	var cursor diskPredecessor
	defer func() {
		if cursor != nil {
			cursor.close()
		}
	}()
	ensureCursor := func() error {
		if cursor == nil {
			var e error
			cursor, _, e = openPredecessor(r.base.snapshot, prefixColumn)
			if e != nil {
				return e
			}
			b.prefix = cursor
		}
		return nil
	}
	var rec Record
	var e error
	if location.Exact {
		actual := ctx
		actual.Height = location.Height
		key, err := EncodeNodeKey(actual)
		if err != nil {
			return nil, n, err
		}
		var raw []byte
		f := r.block.owner.boundary
		if i := f.findCandidate(pos, want); i >= 0 {
			c := f.candidates[i]
			if c.CompactSize != 0 && c.Node.Version.Height == actual.Height {
				raw = f.candidateBody[c.Offset : c.Offset+c.CompactSize]
			}
		}
		if raw == nil {
			if e = b.beforeRead(); e != nil {
				return nil, n, e
			}
			exact++
			raw, e = r.base.snapshot.get(prefixColumn, key)
		}
		if e != nil {
			return nil, n, e
		}
		if e = b.charge(len(raw) + len(key)); e != nil {
			return nil, n, e
		}
		rec, e = DecodeRecord(raw)
	} else {
		if e = ensureCursor(); e != nil {
			return nil, n, e
		}
		b.resolved = func(_, actual NodeKey) { location.Height = actual.Height }
		rec, e = r.base.prefix(ctx, b)
	}
	if e != nil {
		return nil, n, e
	}
	if e = rec.ValidateAt(ctx); e != nil {
		return nil, n, e
	}
	if rec.Hash != want {
		return nil, n, ErrHashMismatch
	}
	n.Version = forestVersion{location.Height, Prefix, true}
	b.resolved = func(q, a NodeKey) {
		for i, ref := range rec.Children {
			if ref.Kind == Prefix {
				loc, _ := locateChild(ctx, rec, i)
				if bytes.Equal(loc.Target.Path, q.Path) {
					n.Edges[i] = edgeAt(a.Height, boundaryPrefix)
				}
			}
		}
	}
	for i, ref := range rec.Children {
		if ref.Kind == Hash {
			n.Edges[i].Kind = boundaryHash
		}
	}
	fields, e := canonicalFields(ctx, rec, func(loc ChildLocator) (mpt.Hash, error) {
		if e := ensureCursor(); e != nil {
			return mpt.Hash{}, e
		}
		child, e := r.base.prefixObserved(loc.Target, b, r.base.sibling)
		return child.Hash, e
	})
	if e != nil {
		return nil, n, e
	}
	var canonical []byte
	if r.base.local {
		canonical, e = fields.LocalRLP()
	} else {
		var h mpt.Hash
		canonical, h, e = mpt.RebuildCanonicalNode(fields)
		if e == nil && h != want {
			e = ErrHashMismatch
		}
	}
	if e != nil {
		return nil, n, e
	}
	if out != nil {
		decoded, err := mpt.MaterializeLocalFields(fields, want, uint32(len(canonical)))
		if err != nil {
			return nil, n, err
		}
		*out = decoded
	}
	if len(pos.Path[:pos.Length]) > 0 && len(canonical) < 32 {
		return nil, n, ErrCorrupt
	}
	return canonical, n, nil
}
