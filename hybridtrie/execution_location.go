package hybridtrie

import "github.com/XiaodongQi1/Hybrid-code/mpt"

// A staged body belongs to this block's pinned base view. The same logical
// digest may occur in different physical versions, so an explicit locator
// must agree before its child routing hints can be reused.
func (r *boundaryReader) stagedCandidate(pos forestPosition, h mpt.Hash, loc *mpt.NodeLocation) int {
	f := r.block.owner.boundary
	i := f.findCandidate(pos, h)
	if i < 0 {
		return -1
	}
	v := f.candidates[i].Node.Version
	if v.Source != Hash && (v.Source != Prefix || !v.Known || v.Height > r.head.Height) {
		return -1
	}
	if loc != nil && (ReferenceKind(loc.Kind) != v.Source || loc.Exact && v.Source == Prefix && loc.Height != v.Height) {
		return -1
	}
	return i
}

func locationForEdge(edge boundaryEdge) mpt.NodeLocation {
	switch edge.Kind {
	case boundaryPrefix:
		return mpt.NodeLocation{Kind: uint8(Prefix), Height: edge.height(), Exact: true}
	case boundaryHash:
		return mpt.NodeLocation{Kind: uint8(Hash)}
	default:
		return mpt.NodeLocation{}
	}
}

func withBoundaryLocations(n mpt.MaterializedNode, edges [16]boundaryEdge) mpt.MaterializedNode {
	return n.WithChildLocations(boundaryLocations(edges))
}

func (r *boundaryReader) GetLocatedMaterializedNode(path []byte, h mpt.Hash, loc mpt.NodeLocation) (mpt.MaterializedNode, error) {
	if r.DeferredReads() {
		return r.executionMaterialized(path, h, &loc)
	}
	if !r.block.sharedPreparing {
		r.block.owner.mu.Lock()
		defer r.block.owner.mu.Unlock()
	}
	return r.getLocatedMaterializedNode(path, h, &loc)
}

// Normal MPT descent brings the locator from its materialized parent. The
// fallback serves legacy/direct calls without one and follows physical links
// only; it never reconstructs every ancestor or reads all their siblings.
func (r *boundaryReader) materializedLocation(path []byte, h mpt.Hash, supplied *mpt.NodeLocation) (mpt.NodeLocation, error) {
	if supplied != nil {
		if ReferenceKind(supplied.Kind) != Prefix && ReferenceKind(supplied.Kind) != Hash || supplied.Exact && supplied.Height > r.head.Height {
			return mpt.NodeLocation{}, ErrCorrupt
		}
		return *supplied, nil
	}
	f := r.block.owner.boundary
	pos := position(r.base.id, path)
	if i := f.findCandidate(pos, h); i >= 0 {
		v := f.candidates[i].Node.Version
		if v.Source == Hash {
			return mpt.NodeLocation{Kind: uint8(Hash)}, nil
		}
		if v.Source == Prefix && v.Known && v.Height <= r.head.Height {
			return mpt.NodeLocation{Kind: uint8(Prefix), Height: v.Height, Exact: true}, nil
		}
	}
	if len(path) == 0 {
		if h != r.base.root {
			return mpt.NodeLocation{}, ErrHashMismatch
		}
		return mpt.NodeLocation{Kind: uint8(r.base.rootSource)}, nil
	}
	if i, slot := f.parent(&f.current, pos, h); i >= 0 {
		loc := locationForEdge(f.current.edgeAtSlot(i, slot))
		if loc.Kind != 0 && (!loc.Exact || loc.Height <= r.head.Height) {
			return loc, nil
		}
	}
	r.base.view.mu.RLock()
	defer r.base.view.mu.RUnlock()
	if r.base.view.closed {
		return mpt.NodeLocation{}, ErrClosed
	}
	base := *r.base
	base.local = true
	var cursor projectionCursor
	defer cursor.close()
	reader := projectionReader{trieReader: &base, cursor: &cursor}
	route, e := reader.retainedPhysicalRoute(path, h)
	if e != nil {
		return mpt.NodeLocation{}, e
	}
	if !r.block.sharedPreparing {
		f.stats.ColdResolutions++
	}
	return mpt.NodeLocation{Kind: uint8(route.source), Height: route.height, Exact: route.source == Prefix}, nil
}

func boundaryLocations(edges [16]boundaryEdge) (locations [16]mpt.NodeLocation) {
	for i, edge := range edges {
		locations[i] = locationForEdge(edge)
	}
	return
}
func (r *boundaryReader) residentMaterialized(i int) (mpt.MaterializedNode, error) {
	f := r.block.owner.boundary
	if !r.block.sharedPreparing {
		f.stats.NodeHits++
		f.stats.MaterializedHits++
	}
	return f.current.imageAt(i).MaterializeWithChildLocations(boundaryLocations(f.current.edgesAt(i)))
}
