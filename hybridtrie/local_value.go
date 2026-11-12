package hybridtrie

import (
	"bytes"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"time"
)

// Local point reads walk just the requested branch. Digests of Prefix
// siblings are needed by canonical writers and provers, not by this walk.
type localVisit func(NodeKey, ReferenceKind, uint64, Record)

// A COW node can reference an unchanged subtree outside the resident frontier.
// Follow the base tree to that exact position before emitting logical visits.
type localAnchor struct {
	path []byte
	hash mpt.Hash
}

func (r *trieReader) localValue(ctx NodeKey, want mpt.Hash, source ReferenceKind, exact boundaryEdge, firstBody, rest []byte, b *readBudget, anchor *localAnchor, visit localVisit) ([]byte, error) {
	if len(ctx.Path)+len(rest) > MaxDepth {
		return nil, ErrLimit
	}
	var inline []byte
	var version uint64
	b.resolved = func(_, found NodeKey) { version = found.Height }
	var cursor diskPredecessor
	defer func() {
		if cursor != nil {
			cursor.close()
		}
	}()
	known := true
	for {
		var rec Record
		var err error
		switch source {
		case Prefix:
			if exact.Kind == boundaryPrefix {
				version = exact.height()
				if version > ctx.Height {
					return nil, ErrCorrupt
				}
				at := ctx
				at.Height = version
				key, e := EncodeNodeKey(at)
				if e != nil {
					return nil, e
				}
				raw := firstBody
				firstBody = nil
				if raw == nil {
					raw, e = r.snapshot.get(prefixColumn, key)
				}
				if e != nil {
					return nil, e
				}
				if e = b.charge(len(key) + len(raw)); e != nil {
					return nil, e
				}
				rec, err = DecodeRecord(raw)
				if err == nil {
					err = rec.ValidateAt(ctx)
				}
			} else {
				if cursor == nil {
					cursor, _, err = openPredecessor(r.snapshot, prefixColumn)
					if err != nil {
						return nil, err
					}
					b.prefix = cursor
				}
				rec, err = r.prefix(ctx, b)
			}
			if err == nil && known && rec.Hash != want {
				return nil, ErrHashMismatch
			}
		case Hash:
			rec, _, err = r.hash(ctx, want, b)
		case Embed:
			var d mpt.CanonicalNode
			d, err = mpt.DecodeLocalNodeFields(inline)
			if err == nil {
				rec, err = recordFromFields(ctx, d, mpt.Hash{}, 0)
			}
		default:
			return nil, ErrCorrupt
		}
		if err != nil {
			return nil, err
		}
		if anchor != nil && len(ctx.Path) >= len(anchor.path) {
			if !bytes.Equal(ctx.Path, anchor.path) || source == Embed || rec.Hash != anchor.hash {
				return nil, ErrHashMismatch
			}
			anchor = nil
		}
		if anchor == nil && visit != nil {
			visit(ctx, source, version, rec)
		}
		slot := 0
		switch rec.Kind {
		case mpt.CanonicalLeaf:
			if anchor != nil {
				return nil, ErrNotFound
			}
			if bytes.Equal(rest, rec.Fragment) {
				return rec.Value, nil
			}
			return nil, nil
		case mpt.CanonicalExtension:
			if !bytes.HasPrefix(rest, rec.Fragment) {
				if anchor != nil {
					return nil, ErrNotFound
				}
				return nil, nil
			}
			rest = rest[len(rec.Fragment):]
		case mpt.CanonicalBranch:
			if len(rest) == 0 {
				return rec.Value, nil
			}
			slot = int(rest[0])
			if slot >= 16 {
				return nil, ErrCorrupt
			}
			rest = rest[1:]
		}
		loc, e := locateChild(ctx, rec, slot)
		if e != nil {
			return nil, e
		}
		if loc.Kind == Empty {
			if anchor != nil {
				return nil, ErrNotFound
			}
			return nil, nil
		}
		source, ctx, want, inline = loc.Kind, loc.Target, loc.Hash, loc.Inline
		exact, known = boundaryEdge{}, source == Hash
	}
}

func (r *trieReader) DeferLocalRoot(root mpt.Hash) bool {
	if r.view == nil || root != r.root {
		return false
	}
	r.view.mu.RLock()
	defer r.view.mu.RUnlock()
	return !r.view.closed
}

func (r *trieReader) LocalValue(path []byte, h mpt.Hash, rest []byte, observer mpt.TrieAccessObserver) ([]byte, bool, error) {
	if r.view == nil {
		return nil, false, nil
	}
	var visits [][]byte
	defer func() {
		for _, p := range visits {
			observer.ObserveTrieAccess(p)
		}
	}()
	r.view.mu.RLock()
	defer r.view.mu.RUnlock()
	if r.view.closed {
		return nil, true, ErrClosed
	}
	base := *r
	base.local = true
	var visit localVisit
	if observer != nil {
		visit = func(ctx NodeKey, _ ReferenceKind, _ uint64, _ Record) { visits = append(visits, ctx.Path) }
	}
	rest = append(bytes.Clone(path), rest...)
	v, e := base.localValue(NodeKey{r.id, nil, r.height}, r.root, r.rootSource, boundaryEdge{}, nil, rest, &readBudget{cfg: r.cfg}, &localAnchor{path, h}, visit)
	return v, true, e
}

func (r *boundaryReader) DeferLocalRoot(root mpt.Hash) bool {
	b := r.block
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()
	return r.scope() && root == r.base.root && b.owner.boundary.current.root(r.base.id) < 0
}

func (r *boundaryReader) LocalValue(path []byte, h mpt.Hash, rest []byte, observer mpt.TrieAccessObserver) ([]byte, bool, error) {
	return r.locatedValue(path, h, rest, nil, observer, nil)
}
func (r *boundaryReader) LocatedLocalValue(path []byte, h mpt.Hash, rest []byte, location mpt.NodeLocation, observer mpt.TrieAccessObserver) ([]byte, bool, error) {
	return r.locatedValue(path, h, rest, &location, observer, nil)
}
func (r *boundaryReader) locatedValue(path []byte, h mpt.Hash, rest []byte, location *mpt.NodeLocation, observer mpt.TrieAccessObserver, materialized *mpt.MaterializedNode) ([]byte, bool, error) {
	b := r.block
	// Custom observers may access another reader on this same forest. Deliver
	// their events after the snapshot and forest locks have been released.
	var externalVisits [][]byte
	defer func() {
		for _, p := range externalVisits {
			observer.ObserveTrieAccess(p)
		}
	}()
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()
	if !r.scope() {
		return nil, true, ErrClosed
	}
	r.base.view.mu.RLock()
	defer r.base.view.mu.RUnlock()
	if r.base.view.closed {
		return nil, true, ErrClosed
	}
	if len(path)+len(rest) > MaxDepth {
		return nil, true, ErrLimit
	}
	f := b.owner.boundary
	pos := position(r.base.id, path)
	if i := f.current.find(pos); i >= 0 && f.current.hashAt(i) == h && f.current.versionAt(i).Height <= r.head.Height {
		if materialized != nil {
			node, err := r.residentMaterialized(i)
			*materialized = node
			return nil, false, err
		}
		return nil, false, nil
	}
	candidate := r.stagedCandidate(pos, h, location)
	if candidate >= 0 && f.candidates[candidate].Node.Size != 0 {
		// Let materialization consume the staged body, even when the parent
		// supplies a locator. A locator is not a reason to bypass read reuse.
		return nil, false, nil
	}
	var exact boundaryEdge
	var firstBody []byte
	var anchor *localAnchor
	startPath, startHash := path, h
	source := r.base.rootSource
	if location != nil {
		source = ReferenceKind(location.Kind)
		if source != Prefix && source != Hash || location.Exact && location.Height > r.head.Height {
			return nil, true, ErrCorrupt
		}
		if source == Hash {
			exact.Kind = boundaryHash
		} else if location.Exact {
			exact = edgeAt(location.Height, boundaryPrefix)
		}
	} else if i := f.findCandidate(pos, h); i >= 0 {
		c := f.candidates[i]
		n := c.Node
		if n.Size > 0 {
			return nil, false, nil
		}
		source = n.Version.Source
		if source == Prefix {
			exact = edgeAt(n.Version.Height, boundaryPrefix)
			if c.CompactSize != 0 {
				firstBody = f.candidateBody[c.Offset : c.Offset+c.CompactSize]
				f.stats.TransientBodyHits++
			}
		}
	} else if len(path) > 0 {
		i, slot := f.parent(&f.current, pos, h)
		if i >= 0 {
			exact = f.current.edgeAtSlot(i, slot)
		}
		switch exact.Kind {
		case boundaryHash:
			source = Hash
		case boundaryPrefix:
			source = Prefix
		default:
			anchor = &localAnchor{path, h}
			startPath, startHash = nil, r.base.root
			rest = append(bytes.Clone(path), rest...)
		}
		if anchor == nil {
			f.stats.ParentHashReuses++
		}
	} else if h != r.base.root {
		return nil, false, nil
	}
	if candidate >= 0 && source == Prefix {
		c := f.candidates[candidate]
		if c.CompactSize != 0 && firstBody == nil {
			exact = edgeAt(c.Node.Version.Height, boundaryPrefix)
			firstBody = f.candidateBody[c.Offset : c.Offset+c.CompactSize]
			f.stats.TransientBodyHits++
		}
	}
	visit := func(ctx NodeKey, source ReferenceKind, version uint64, rec Record) {
		p := position(ctx.TrieID, ctx.Path)
		if own, ok := observer.(*boundaryReader); ok && own == r {
			start := time.Now()
			if len(ctx.Path) == 0 {
				f.stats.AccessEvents++
			}
			f.event(p, b.update.height)
			f.stats.HeatNanos += uint64(time.Since(start))
		} else if observer != nil {
			externalVisits = append(externalVisits, ctx.Path)
		}
		if source == Embed {
			return
		}
		f.stats.NodeMisses++
		f.observe(boundaryObservation{Pos: p, boundaryNode: boundaryNode{Hash: rec.Hash, Version: forestVersion{Height: version, Source: source, Known: source == Prefix}}}, nil)
	}
	base := *r.base
	base.local = true
	var stats ResolutionStats
	nExact := uint64(0)
	if exact.Kind == boundaryPrefix && firstBody == nil {
		nExact = 1
	}
	v, e := base.localValue(NodeKey{pos.ID, startPath, base.height}, startHash, source, exact, firstBody, rest, &readBudget{cfg: base.cfg, stats: &stats}, anchor, visit)
	r.counters(stats, nExact)
	return v, true, e
}

func (r *boundaryReader) LookupLocalValue(path []byte, h mpt.Hash, rest []byte, location *mpt.NodeLocation, observer mpt.TrieAccessObserver) (value []byte, n mpt.MaterializedNode, handled bool, err error) {
	if r.DeferredReads() {
		n, err = r.executionMaterialized(path, h, location)
		return
	}
	value, handled, err = r.locatedValue(path, h, rest, location, observer, &n)
	return
}
