package hybridtrie

import (
	"bytes"
	"errors"
	"math/rand"
	"sort"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"time"
	"unsafe"
)

func (f *boundaryForest) rootFor(b *boundaryBank, id uint64, hash mpt.Hash, v forestVersion) bool {
	i := b.root(id)
	if i < 0 {
		for k := range b.Roots {
			if !b.Roots[k].Used {
				i = k
				break
			}
		}
	}
	if i < 0 {
		return false
	}
	b.Roots[i] = boundaryRoot{ID: id, Hash: hash, Version: v, Used: true}
	return true
}
func boundaryLess(a, b boundaryObservation, height, halfLife uint64) bool {
	x, y := a.Heat.score(height, halfLife), b.Heat.score(height, halfLife)
	if x != y {
		return x < y
	}
	if a.Pos.Length != b.Pos.Length {
		return a.Pos.Length > b.Pos.Length
	}
	if a.Pos.ID != b.Pos.ID {
		return a.Pos.ID < b.Pos.ID
	}
	return bytes.Compare(a.Pos.Path[:a.Pos.Length], b.Pos.Path[:b.Pos.Length]) < 0
}
func (f *boundaryForest) cleanRoots(b *boundaryBank) {
	for i, r := range b.Roots {
		if !r.Used {
			continue
		}
		if b.find(position(r.ID, nil)) < 0 {
			b.Roots[i] = boundaryRoot{}
		}
	}
}
func (f *boundaryForest) repairBoundary() {
	// Only changed parents can invalidate an old child. Each has at most 16
	// resident children; removals walk only the affected subtree.
	for at := 0; at < len(f.current.changed); at++ {
		i := int(f.current.changed[at])
		n := f.current.snapshot(i)
		if !n.Used {
			continue
		}
		for child := n.Link.FirstChild; child != 0; {
			ci := int(child - 1)
			c := f.current.snapshot(ci)
			child = c.Link.NextSibling
			hash, ok := n.Image.ChildHash(int(c.Link.Edge))
			if !ok || hash != c.Hash {
				f.stats.StructuralRemovals += uint64(f.current.remove(ci))
			}
		}
	}
}
func (f *boundaryForest) runBoundary(h uint64, load func(boundaryObservation) ([]byte, boundaryObservation, error)) error {
	due := f.policyDue(h)
	if due {
		f.stats.PolicyRuns++
		f.stats.LastPolicyHeight = h
	} else {
		f.stats.PolicySkips++
	}
	// Candidate state is only from this block. No previous dirty queue exists.
	work := uint64(f.cfg.WorkUnits)
	sortCost := uint64(f.candidateN)
	for k := f.candidateN; k > 1; k = (k + 1) / 2 {
		sortCost += uint64(f.candidateN)
	}
	if sortCost > work {
		f.stats.WorkExhaustions++
		return nil
	}
	if f.candidateN == 0 {
		return nil
	}
	indexCost := uint64(len(f.current.changed))
	if indexCost > work-sortCost {
		f.stats.WorkExhaustions++
		return nil
	}
	work -= sortCost + indexCost
	start := time.Now()
	if err := f.indexSelection(due); err != nil {
		return err
	}
	f.stats.SelectionIndexNanos += uint64(time.Since(start))
	seed := boundarySeed(f.cfg.SampleSeed ^ h)
	rng := rand.New(&seed)
	s := &f.selection
	sort.Slice(f.candidates[:f.candidateN], func(i, j int) bool {
		a, b := f.candidates[i].Node, f.candidates[j].Node
		if a.Pos.Length != b.Pos.Length {
			return a.Pos.Length < b.Pos.Length
		}
		if a.Pos.ID != b.Pos.ID {
			return a.Pos.ID < b.Pos.ID
		}
		return bytes.Compare(a.Pos.Path[:a.Pos.Length], b.Pos.Path[:b.Pos.Length]) < 0
	})
	f.reindexCandidates() // exact admission reads may reuse a staged physical body
	for ci := 0; ci < f.candidateN; ci++ {
		c := f.candidates[ci]
		n := c.Node
		if n.PreparationOnly {
			continue
		}
		if work == 0 {
			f.stats.WorkExhaustions++
			break
		}
		work--
		if f.current.find(n.Pos) >= 0 {
			continue
		}
		raw := f.candidateBody[c.Offset : c.Offset+n.Size]
		parent, parentEdge := -1, -1
		if n.Pos.Length != 0 {
			parent, parentEdge = f.parent(&f.current, n.Pos, n.Hash)
			if parent < 0 {
				f.stats.AdmissionRejects++
				continue
			}
		}
		if n.Version.Source == Prefix && !n.Version.Known {
			f.stats.AdmissionRejects++
			continue
		}
		if f.current.root(n.Pos.ID) < 0 {
			room := false
			for _, r := range f.current.Roots {
				if !r.Used {
					room = true
					break
				}
			}
			if !room {
				f.stats.AdmissionRejects++
				continue
			}
		}
		lookupCost := uint64(boundaryHeatBucketCount) * uint64(f.cfg.HeatSketchRows)
		if lookupCost > work {
			f.stats.WorkExhaustions++
			break
		}
		work -= lookupCost
		n.Heat = f.initialHeat(n.Pos)
		if parent >= 0 && n.Heat.clip(f.current.heatAt(parent), h, f.cfg.HalfLife) {
			f.stats.HeatParentClamps++
		}
		if f.adaptive() {
			if e := f.admitAdaptive(n, raw, parent, parentEdge, h, due, rng, &work, load); e != nil {
				return e
			}
			continue
		}
		use := s.use
		request := cacheClass(n.Pos.ID)
		after := use
		after[request] += f.charge()
		slot := f.current.freeSlot()
		if n.Size == 0 && slot >= 0 && f.cfg.NodeBudget.fits(after) {
			// Only materialize a read-only Prefix observation once chosen for N.
			if load == nil {
				return ErrCorrupt
			}
			body, loaded, err := load(n)
			if err != nil {
				return err
			}
			if len(body) > int(f.cfg.MaxNodeBytes) {
				f.stats.AdmissionRejects++
				continue
			}
			raw, n.Edges = body, loaded.Edges
		}
		if slot < 0 || !f.cfg.NodeBudget.fits(after) {
			if !due || !n.Updated {
				f.stats.AdmissionRejects++
				continue
			}
			sampleStart := time.Now()
			victim, exhausted := f.sampledVictim(request, parent, after, rng, &work)
			f.stats.SamplingNanos += uint64(time.Since(sampleStart))
			if exhausted {
				f.stats.WorkExhaustions++
				break
			}
			if victim < 0 {
				f.stats.ProtectedRejects++
				continue
			}
			if n.Heat.score(h, f.cfg.HalfLife) <= f.current.heatAt(victim).score(h, f.cfg.HalfLife) {
				f.stats.ProtectedRejects++
				f.stats.HeatRejects++
				continue
			}
			slot = victim
			old := f.current.observation(slot)
			s.use[cacheClass(old.Pos.ID)] -= f.charge()
			f.withdrawBoundary(slot)
			oldParent := int(f.current.linkAt(slot).Parent) - 1
			f.current.remove(slot)
			if oldParent >= 0 {
				f.refreshSelection(oldParent)
				f.offerBoundary(oldParent)
			}
			f.stats.PolicyEvictions++
		}
		if n.Pos.Length == 0 && !f.rootFor(&f.current, n.Pos.ID, n.Hash, n.Version) {
			return ErrCorrupt // capacity was checked before selecting a slot
		}
		if e := f.current.put(slot, n, raw); e != nil {
			return e
		}
		s.use[request] += f.charge()
		if due {
			s.slots[slot] = selectionSlot{parent: int32(parent), offset: -1}
			if parent >= 0 {
				f.withdrawBoundary(parent)
				f.refreshSelection(parent)
			}
			f.offerBoundary(slot)
		}
		f.stats.Admissions++
	}
	return nil
}
func minBoundary(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func (b *ForestBlock) prepareBoundary() error {
	f := b.owner.boundary
	t := time.Now()
	defer func() { f.stats.PreparationNanos += uint64(time.Since(t)) }()
	f.current.beginChanges()
	b.update.pendingChanges = make(map[uint64][]preparedChange)
	b.update.retainedRoutes = make(map[forestPosition]projectionRoute)
	var pendingBytes uint64
	// Refresh only nodes marked persistDirty during logical updates. Keep the
	// same emissions for storage projection, rather than walking an old tree.
	for id := range b.retired {
		if i := f.current.find(position(id, nil)); i >= 0 {
			f.stats.StructuralRemovals += uint64(f.current.remove(i))
		}
	}
	ids := make([]uint64, 0, len(b.update.updates))
	for id := range b.update.updates {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if b.retired[id] {
			continue
		}
		u := b.update.updates[id]
		baseRoot := mpt.EmptyRootHash
		if b.base != nil {
			var e error
			baseRoot, e = b.base.Root(id)
			if e != nil && !errors.Is(e, ErrNotFound) {
				return e
			}
			if errors.Is(e, ErrNotFound) {
				baseRoot = mpt.EmptyRootHash
			}
		}
		root, e := u.Tree.Hash()
		if e != nil {
			return e
		}
		if b.update.pendingChanges != nil {
			b.update.pendingChanges[id] = nil
		}
		emit := func(c mpt.LogicalChange) error {
			fields, err := mpt.DecodeLocalNodeFields(c.RLP)
			if err != nil {
				return err
			}
			if b.update.pendingChanges != nil {
				// Charge backing capacity and all owned field slices as well as RLP.
				cost := uint64(len(c.Path)+len(c.RLP)+len(fields.Path)+len(fields.Value)) + 2*uint64(unsafe.Sizeof(preparedChange{}))
				for _, ref := range fields.Children {
					cost += uint64(len(ref.Inline))
				}
				pendingBytes += cost
				if pendingBytes > b.owner.store.cfg.MaxBatchBytes {
					b.update.pendingChanges = nil
				} else {
					b.update.pendingChanges[id] = append(b.update.pendingChanges[id], preparedChange{c, fields})
				}
			}
			pos := position(id, c.Path)
			if e := f.validateFields(pos, len(c.RLP), fields); e != nil {
				return e
			}
			i := f.current.find(pos)
			if len(c.Path) > 0 && len(c.RLP) < 32 {
				if i >= 0 {
					f.stats.StructuralRemovals += uint64(f.current.remove(i))
				}
				return nil
			}
			n := boundaryObservation{Pos: pos, boundaryNode: boundaryNode{Hash: c.Hash, Version: forestVersion{b.update.height, Prefix, true}, Updated: true}}
			if i >= 0 {
				resident := f.current.observation(i)
				// Retain only bounded routing metadata before overwriting the old
				// resident. Unchanged children can reuse it at publication.
				if resident.Version.Known && resident.Version.Height <= b.update.base.Height {
					for slot, edge := range resident.Edges {
						if len(b.update.retainedRoutes) >= 4096 {
							break
						}
						p, h, ok := boundaryImageChild(resident, resident.Image, slot)
						if !ok {
							continue
						}
						switch edge.Kind {
						case boundaryPrefix:
							b.update.retainedRoutes[p] = projectionRoute{hash: h, source: Prefix, height: edge.height()}
						case boundaryHash:
							b.update.retainedRoutes[p] = projectionRoute{hash: h, source: Hash}
						}
					}
				}
				resident.Hash, resident.Version, resident.Updated = n.Hash, n.Version, true
				resident.Edges = [16]boundaryEdge{}
				if e := f.current.putFields(i, resident, fields, uint32(len(c.RLP))); e != nil {
					return e
				}
			}
			f.observe(n, c.RLP)
			return nil
		}
		if root != baseRoot {
			_, e = u.Tree.PendingLogicalNodes(baseRoot, b.owner.store.cfg.MaxProjectionNodes, emit)
		}
		if e != nil {
			return e
		}
		// An empty trie emits no new root in PendingLogicalNodes.
		if i := f.current.find(position(id, nil)); i >= 0 {
			h, e := u.Tree.Hash()
			if e != nil {
				return e
			}
			if h == mpt.EmptyRootHash {
				f.stats.StructuralRemovals += uint64(f.current.remove(i))
			}
		}
	}
	// Deleted or compressed paths have no new body. Drop stale branches from
	// the root down by checking each parent's final authenticated child link.
	f.stats.StructuralRemovals += f.current.Pruned
	f.repairBoundary()
	f.cleanRoots(&f.current)
	// Every non-root admission below is authenticated by its final N parent's
	// canonical child reference. Retained N chains were checked above. Only root
	// candidates need a separate final-root check; repeating a COW root walk for
	// every already authenticated candidate is unnecessary. Preparation-only base
	// bodies stay available for bounded read reuse but never become admissions.
	dst := 0
	for i := 0; i < f.candidateN; i++ {
		c := f.candidates[i]
		if b.retired[c.Node.Pos.ID] {
			continue
		}
		if c.Node.Pos.Length == 0 && !c.Node.PreparationOnly {
			if u, ok := b.update.updates[c.Node.Pos.ID]; ok {
				h, e := u.Tree.Hash()
				if e != nil {
					return e
				}
				if h != c.Node.Hash {
					continue
				}
			}
		}
		f.candidates[dst] = c
		dst++
	}
	f.candidateN = dst
	f.reindexCandidates()

	// Bind resident root descriptors to the final root and its real Prefix version.
	for i, r := range f.current.Roots {
		if !r.Used {
			continue
		}
		if n := f.current.find(position(r.ID, nil)); n >= 0 {
			e := f.current.snapshot(n)
			f.current.Roots[i] = boundaryRoot{r.ID, e.Hash, e.Version, true}
		}
	}
	p := time.Now()
	var optional *admissionReadBudget
	if f.adaptive() {
		optional = newAdmissionReadBudget(f.cfg)
		defer func() { f.stats.AdmissionReads += optional.nodes; f.stats.AdmissionBytes += optional.bytes }()
	}
	load := func(n boundaryObservation) ([]byte, boundaryObservation, error) {
		r := b.boundaryReaders[n.Pos.ID]
		if r == nil {
			return nil, boundaryObservation{}, ErrCorrupt
		}
		edge := boundaryEdge{Kind: boundaryHash}
		if n.Version.Source == Prefix {
			edge = edgeAt(n.Version.Height, boundaryPrefix)
		}
		reader := r.localReader()
		reader.admission = optional
		return reader.exact(n.Pos, n.Hash, edge)
	}
	if e := f.runBoundary(b.update.height, load); e != nil {
		return e
	}
	f.stats.PolicyNanos += uint64(time.Since(p))
	// Admission caps the candidate at its parent at the same height. Subsequent
	// path visits and common exponential decay preserve the ordering; sketch
	// bucket expiration never modifies resident scores.
	for _, slot := range f.current.changed {
		n := f.current.snapshot(int(slot))
		if !n.Used {
			continue
		}
		f.stats.IncrementalChecks++
		if n.Link.Parent == 0 {
			continue
		}
		parent := f.current.snapshot(int(n.Link.Parent) - 1)
		hash, ok := parent.Image.ChildHash(int(n.Link.Edge))
		if !parent.Used || !ok || hash != n.Hash || !n.Heat.boundedBy(parent.Heat, b.update.height, f.cfg.HalfLife) {
			return ErrCorrupt
		}
	}
	f.stats.ChangedNodes += uint64(len(f.current.changed))

	return nil
}
func (b *ForestBlock) commitBoundary() (Head, CommitStats, error) {
	f := b.owner.boundary
	previous := f.stats
	published := false
	// Cache contents are disposable. On exceptional preparation/storage failure
	// invalidate them; no node copy or rollback journal is maintained.
	b.update.retainedPrefix = make(map[forestPosition]bool)
	f.current.removed = func(pos forestPosition) { b.update.retainedPrefix[pos] = true }
	defer func() {
		f.current.removed = nil
		b.update.retainedPrefix = nil
		if !published {
			f.stats.Admissions = previous.Admissions
			f.stats.PolicyEvictions = previous.PolicyEvictions
			f.stats.StructuralRemovals = previous.StructuralRemovals
			if !f.uncertain {
				f.invalidateNodes()
			}
		}
		f.current.finishChanges()
	}()
	b.sharedPreparing = true
	defer func() { b.sharedPreparing = false }()
	if e := b.prepareBoundary(); e != nil {
		return b.update.base, CommitStats{}, e
	}
	// A resident parent keeps Prefix children. Removed cache positions are
	// remembered as keys only so eviction itself cannot trigger Hash conversion.
	// Refresh routing hints directly on the resident nodes. Historical reads
	// remain bound to their disk view and expected version/hash.
	b.update.projectedEdges = func(key NodeKey, edges [16]boundaryEdge) {
		if i := f.current.find(position(key.TrieID, key.Path)); i >= 0 {
			f.current.setEdges(i, edges)
		}
	}
	b.update.cacheBoundary = &f.current
	if f.head == b.update.base {
		b.update.baseNodes = &f.current
	}
	defer func() {
		b.update.projectedEdges = nil
		b.update.baseNodes = nil
		b.update.cacheBoundary = nil
	}()
	head, stats, e := b.update.CommitWithLayout(func(_ NodeKey, eligible uint16) (uint16, error) { return eligible, nil })
	f.stats.ProjectionNanos += stats.ProjectionNanos
	f.stats.WriteNanos += stats.WriteNanos
	if e != nil {
		if errors.Is(e, ErrUncertain) {
			f.uncertain = true
			b.release()
		}
		return head, stats, e
	}
	t := time.Now()
	published = true // disk state is now durable; cache publication has no merge
	if f.beforePublish != nil {
		if e = f.beforePublish(); e != nil {
			f.stats.PublicationFailures++
			f.invalidateNodes()
			f.head = head
			b.release()
			return head, stats, nil
		}
	}
	f.head = head
	f.generation++
	f.stats.PublicationNanos += uint64(time.Since(t))
	b.release()
	return head, stats, nil
}
