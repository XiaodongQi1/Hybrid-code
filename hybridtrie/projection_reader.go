package hybridtrie

import (
	"bytes"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// These hints belong to one trie at one pinned pre-commit root. A parent can
// authenticate a child's location and digest without authenticating its body.
type projectionRoute struct {
	hash   mpt.Hash
	height uint64
	source ReferenceKind
	opened bool
}

type projectionReader struct {
	*trieReader
	routes    map[string]projectionRoute
	cursor    *projectionCursor
	baseNodes *boundaryBank
	physical  map[string]physicalProjectionNode
}

type physicalProjectionNode struct {
	record Record
	route  projectionRoute
	depth  uint8 // stored delta depth of a Prefix node, zero otherwise
}

// retainedPhysicalRoute follows only physical locators. It never reconstructs
// an old canonical parent or fetches siblings just to recover their digests.
// Cached ancestors let subsequent requests continue down already opened paths.
// This fallback is needed after cold reads or when bounded resident hints fill.
//
// When needHeight is false the caller only needs the child's physical kind and
// digest, both already recorded by its authenticated parent, so the child's own
// record is not opened (route.height is then unknown). Callers that publish the
// physical height of a resident edge must pass true.
func (p *projectionReader) retainedPhysicalRoute(path []byte, want mpt.Hash, needHeight bool) (projectionRoute, error) {
	// Public imports retain strict body authentication. Only a pinned local
	// execution forest may rely on physical routing without rebuilding hashes.
	if !p.local {
		return p.retainedRoute(path, want)
	}
	_, route, e := p.physicalWalk(path, want, true, needHeight)
	return route, e
}

// oldVersion opens the previous version of the node at path by physical
// routing alone, for delta encoding in a commit. ErrNotFound means the old tree
// has no node at exactly this path. The node's own digest is not checked here:
// the commit never trusts it, it only chooses an encoding from it.
func (p *projectionReader) oldVersion(path []byte) (physicalProjectionNode, error) {
	n, _, e := p.physicalWalk(path, mpt.Hash{}, false, true)
	return n, e
}

func (p *projectionReader) physicalWalk(path []byte, want mpt.Hash, checkWant, needHeight bool) (physicalProjectionNode, projectionRoute, error) {
	var zeroNode physicalProjectionNode
	if p.physical == nil {
		p.physical = make(map[string]physicalProjectionNode)
	}
	if p.cursor.diskPredecessor == nil {
		var err error
		p.cursor.diskPredecessor, _, err = openPredecessor(p.snapshot, prefixColumn)
		if err != nil {
			return zeroNode, projectionRoute{}, err
		}
	}
	var version uint64
	b := readBudget{cfg: p.cfg, prefix: p.cursor.diskPredecessor, resolved: func(_, a NodeKey) { version = a.Height }}
	var at []byte
	route := projectionRoute{hash: p.root, source: p.rootSource}
	// Select the deepest already opened ancestor, entirely in memory.
	for depth := len(path); depth >= 0; depth-- {
		if n, ok := p.physical[string(path[:depth])]; ok {
			at = path[:depth]
			route = n.route
			break
		}
	}
	for {
		n, ok := p.physical[string(at)]
		if !ok && !needHeight && len(at) > 0 && bytes.Equal(at, path) {
			// Prefix references omit the child digest; only a Hash reference
			// carries one to compare.
			if route.hash != (mpt.Hash{}) && route.hash != want {
				return zeroNode, projectionRoute{}, ErrHashMismatch
			}
			route.hash = want
			return zeroNode, route, nil
		}
		if !ok {
			ctx := NodeKey{p.id, at, p.height}
			var rec Record
			var err error
			b.depth = 0
			if route.source == Hash {
				rec, _, err = p.hash(ctx, route.hash, &b)
			} else if route.source == Prefix {
				rec, err = p.prefix(ctx, &b)
				route.height = version
			} else {
				return zeroNode, projectionRoute{}, ErrCorrupt
			}
			if err != nil {
				return zeroNode, projectionRoute{}, err
			}
			if route.hash != (mpt.Hash{}) && rec.Hash != route.hash {
				return zeroNode, projectionRoute{}, ErrHashMismatch
			}
			route.hash = rec.Hash
			route.opened = true
			n = physicalProjectionNode{rec, route, b.depth}
			if len(p.physical) < 4096 {
				p.physical[string(at)] = n
			}
		}
		if bytes.Equal(at, path) {
			if checkWant && n.route.hash != want {
				return zeroNode, projectionRoute{}, ErrHashMismatch
			}
			return n, n.route, nil
		}
		if len(at) >= len(path) {
			return zeroNode, projectionRoute{}, ErrNotFound
		}
		slot := 0
		switch n.record.Kind {
		case mpt.CanonicalBranch:
			slot = int(path[len(at)])
			if n.record.Children[slot].Kind == Empty {
				// The old tree has no child here; a commit may be creating it.
				return zeroNode, projectionRoute{}, ErrNotFound
			}
		case mpt.CanonicalLeaf:
			return zeroNode, projectionRoute{}, ErrNotFound
		}
		loc, err := locateChild(NodeKey{p.id, at, p.height}, n.record, slot)
		if err != nil {
			return zeroNode, projectionRoute{}, err
		}
		if len(loc.Target.Path) <= len(at) || !bytes.HasPrefix(path, loc.Target.Path) {
			return zeroNode, projectionRoute{}, ErrNotFound
		}
		ref := n.record.Children[slot]
		if ref.Kind != Prefix && ref.Kind != Hash {
			return zeroNode, projectionRoute{}, ErrNotFound
		}
		at = loc.Target.Path
		route = projectionRoute{source: ref.Kind, hash: ref.Hash}
	}
}

type projectionCursor struct{ diskPredecessor }

func (c *projectionCursor) close() {
	if c.diskPredecessor != nil {
		c.diskPredecessor.close()
	}
}

func (p *projectionReader) remember(path []byte, route projectionRoute) {
	// Keep temporary routing memory independent of the size of the forest.
	// On overflow the original bounded root walk remains available.
	const maxRoutes = 4096
	old, exists := p.routes[string(path)]
	if old.opened || !exists && len(p.routes) >= maxRoutes {
		return
	}
	p.routes[string(path)] = route
}

// A route from the pinned local parent is enough to retain an unchanged edge.
// Strict callers also require the child's body to have been opened and checked.
func (p *projectionReader) retainedRoute(path []byte, want mpt.Hash) (projectionRoute, error) {
	if r, ok := p.routes[string(path)]; ok && (p.local || r.opened) {
		if r.hash != want {
			return projectionRoute{}, ErrHashMismatch
		}
		return r, nil
	}
	_, route, e := p.read(path, want)
	return route, e
}

func (p *projectionReader) GetNode(path []byte, want mpt.Hash) ([]byte, error) {
	raw, _, e := p.read(path, want)
	return raw, e
}

// The old tree belongs to our own pinned pre-commit snapshot.
func (p *projectionReader) GetMaterializedNode(path []byte, want mpt.Hash) (mpt.MaterializedNode, error) {
	raw, err := p.GetNode(path, want)
	if err != nil {
		return mpt.MaterializedNode{}, err
	}
	return mpt.DecodeTrustedLocalNode(raw, want)
}

// Use only the published version of this exact parent. A canonical digest
// can recur at another height with different physical child locators.
func (p *projectionReader) cachedParent(path []byte, route projectionRoute) int {
	if p.baseNodes == nil || !p.local {
		return -1
	}
	i := p.baseNodes.find(position(p.id, path))
	if i < 0 {
		return -1
	}
	v := p.baseNodes.versionAt(i)
	if p.baseNodes.hashAt(i) != route.hash || !v.Known || v.Source != Prefix || v.Height != route.height {
		return -1
	}
	return i
}

func (p *projectionReader) read(path []byte, want mpt.Hash) ([]byte, projectionRoute, error) {
	ctx := NodeKey{p.id, path, p.height}
	route, found := p.routes[string(path)]
	if !found && len(path) != 0 {
		// A compressed-path change or a full hints table can miss a route.
		var actual projectionRoute
		raw, e := p.getNodeObserved(path, want, nil, nil, func(q, a NodeKey) {
			if bytes.Equal(q.Path, path) {
				actual.height = a.Height
			}
		}, func(_ NodeKey, s ReferenceKind, _ Record, _ []byte) { actual.source = s })
		actual.hash, actual.opened = want, e == nil
		return raw, actual, e
	}
	if !found {
		route = projectionRoute{hash: p.root, source: p.rootSource}
	}
	if route.hash != want {
		return nil, projectionRoute{}, ErrHashMismatch
	}

	var version uint64
	b := readBudget{cfg: p.cfg, resolved: func(_, a NodeKey) { version = a.Height }}
	var rec Record
	var raw []byte
	var e error
	if route.source == Hash {
		rec, raw, e = p.hash(ctx, want, &b)
	} else if route.source == Prefix {
		if p.cursor.diskPredecessor == nil {
			p.cursor.diskPredecessor, _, e = openPredecessor(p.snapshot, prefixColumn)
			if e != nil {
				return nil, projectionRoute{}, e
			}
		}
		b.prefix = p.cursor.diskPredecessor
		if found {
			key, err := EncodeNodeKey(NodeKey{p.id, path, route.height})
			if err != nil {
				return nil, projectionRoute{}, err
			}
			var data []byte
			data, e = p.snapshot.get(prefixColumn, key)
			if e == nil {
				e = b.charge(len(key) + len(data))
			}
			if e == nil {
				data, e = normalizePrefix(p.snapshot, NodeKey{p.id, path, route.height}, data, b.charge)
			}
			if e == nil {
				rec, e = DecodeRecord(data)
			}
		} else {
			rec, e = p.prefix(ctx, &b)
			route.height = version
		}
	} else {
		return nil, projectionRoute{}, ErrCorrupt
	}
	if e != nil {
		return nil, projectionRoute{}, e
	}

	var children [16]projectionRoute
	if route.source == Prefix {
		parent := p.cachedParent(path, route)
		raw, _, e = p.canonical(ctx, rec, func(loc ChildLocator) (mpt.Hash, error) {
			slot := 0
			if rec.Kind == mpt.CanonicalBranch {
				slot = int(loc.Target.Path[len(path)])
			}
			if parent >= 0 {
				edge := p.baseNodes.edgeAtSlot(parent, slot)
				if edge.Kind == boundaryPrefix && edge.height() <= p.height {
					if h, ok := p.baseNodes.imageAt(parent).ChildHash(slot); ok {
						children[slot] = projectionRoute{source: Prefix, hash: h, height: edge.height()}
						return h, nil
					}
				}
			}
			child, e := p.prefix(loc.Target, &b)
			if e != nil {
				return mpt.Hash{}, e
			}
			children[slot] = projectionRoute{source: Prefix, hash: child.Hash, height: version}
			return child.Hash, nil
		})
		if e != nil {
			return nil, projectionRoute{}, e
		}
	}
	if rec.Hash != want {
		return nil, projectionRoute{}, ErrHashMismatch
	}
	if len(path) > 0 && len(raw) < 32 {
		return nil, projectionRoute{}, ErrCorrupt
	}
	route.opened = true
	p.remember(path, route)
	for slot, ref := range rec.Children {
		if ref.Kind != Prefix && ref.Kind != Hash {
			continue
		}
		loc, e := locateChild(ctx, rec, slot)
		if e != nil {
			return nil, projectionRoute{}, e
		}
		child := children[slot]
		if ref.Kind == Hash {
			child = projectionRoute{source: Hash, hash: ref.Hash}
		}
		p.remember(loc.Target.Path, child)
	}
	return raw, route, nil
}
