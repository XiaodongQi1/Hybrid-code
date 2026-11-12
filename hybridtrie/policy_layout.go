package hybridtrie

import "github.com/XiaodongQi1/Hybrid-code/mpt"

func (b *cacheBuffer) nodePosition(path []byte) (int, bool) {
	for i := 0; i < b.n; i++ {
		if pathEqual(b.nodes[i].Path, b.nodes[i].Length, path) {
			return i, true
		}
	}
	return 0, false
}
func (b *cacheBuffer) hashPosition(path []byte) (int, bool) {
	for i := 0; i < b.h; i++ {
		if pathEqual(b.hashes[i].Path, b.hashes[i].Length, path) {
			return i, true
		}
	}
	return 0, false
}
func (c *residentCache) observeBody(path []byte, h uint64) error {
	p := c.policy
	p.heat.begin(h)
	e, err := p.heat.event(path)
	if err != nil {
		return err
	}
	if e.Access {
		return nil
	}
	e.Access = true
	p.heat.accessEvents++
	if i, ok := c.current.nodePosition(path); ok {
		c.current.nodes[i].A.event(h, p.cfg.HalfLife)
	} else {
		p.heat.increment(c.trieID, path)
	}
	return nil
}
func (c *residentCache) observeUpdate(path []byte) error {
	e, err := c.policy.heat.event(path)
	if err != nil {
		return err
	}
	e.Flags |= eventUpdated
	return nil
}

// A replacement at an old N position is a nonresident candidate. Its current
// access must reach CMS even if reading the old host already set Access. H/D
// positions already use CMS, so their ordinary per-position event is reused.
func (c *residentCache) observeNewHost(path []byte, h uint64) error {
	p := c.policy
	p.heat.begin(h)
	e, err := p.heat.event(path)
	if err != nil {
		return err
	}
	e.Flags |= eventIdentityReset | eventIdentityChecked
	if e.Flags&eventReplacementAccess != 0 {
		return nil
	}
	if _, oldN := c.current.nodePosition(path); oldN {
		p.heat.increment(c.trieID, path)
		p.heat.accessEvents++
	} else if err := c.observeBody(path, h); err != nil {
		return err
	}
	e.Flags |= eventReplacementAccess
	return nil
}

func (c *residentCache) configureNode(i int32) error {
	p := c.policy
	e := &p.graph[i]
	path := e.Path[:e.Length]
	e.A = initialFrequency(p.heat.estimate(c.trieID, path), p.height)
	if p.heat.updated(path) {
		e.U = initialFrequency(1, p.height)
	}
	oldN, hadN := c.current.nodePosition(path)
	oldH, hadH := c.current.hashPosition(path)
	reset := p.heat.identityReset(path)
	// No additional reads are needed: resident and observed bodies are already
	// authenticated. The fallback also protects direct prepare callers.
	if !reset && hadN && !p.heat.identityChecked(path) && len(e.Body) != 0 && c.current.nodes[oldN].Hash != e.Hash {
		oldIdentity, err := mpt.InspectNodeIdentity(c.current.raw(oldN))
		if err != nil {
			return err
		}
		newIdentity, err := mpt.InspectNodeIdentity(e.Body)
		if err != nil {
			return err
		}
		reset = oldIdentity != newIdentity
	}
	if reset {
		if hadN || hadH {
			p.metrics.IdentityResets++
			p.metrics.ForcedRepairs++
		}
		// Replaced hosts have no inherited residency or allocation token. The
		// root remains pinned; other new hosts use ordinary optional admission.
		hadN, hadH = false, false
	}
	if hadN {
		old := c.current.nodes[oldN]
		e.A, e.U = old.A, old.U
		if p.heat.updated(path) {
			e.U.event(p.height, p.cfg.HalfLife)
		}
		if old.Hash == e.Hash {
			e.Allocation = old.Allocation
		}
	}
	if hadH && c.current.hashes[oldH].Hash == e.Hash {
		e.Allocation = c.current.hashes[oldH].Allocation
	}
	parentN := e.Parent < 0 || p.graph[e.Parent].State == nodeState
	if i == 0 || (hadN && parentN && len(e.Body) > 0 && p.nodeRoom(i)) {
		p.apply(i, nodeState)
		return nil
	}
	if (hadN || hadH) && parentN && p.hashRoom(i) {
		p.apply(i, hashState)
		if hadH {
			return nil
		}
	}
	if hadN || hadH {
		p.metrics.ForcedRepairs++
	}
	return nil
}

// Pools carry allocation-checked handles across blocks. Repair maps the previous
// compact resident indices into this block's graph; dead handles stay invalid.
func (c *residentCache) importPools() {
	p := c.policy
	for _, pool := range []*samplePool{&p.nodes, &p.hashes} {
		for k, h := range pool.slots {
			if h.Allocation == 0 {
				continue
			}
			allocation := uint64(0)
			if h.Index >= 0 && int(h.Index) < c.current.n {
				allocation = c.current.nodes[h.Index].Allocation
			}
			if h.Index < 0 {
				j := int(-h.Index - 1)
				if j < c.current.h {
					allocation = c.current.hashes[j].Allocation
				}
			}
			pool.slots[k] = sampleHandle{}
			if allocation != h.Allocation {
				continue
			}
			for i := 0; i < p.count; i++ {
				if p.graph[i].Allocation == allocation {
					pool.slots[k] = p.handle(int32(i))
					break
				}
			}
		}
	}
}
func (c *residentCache) exportPools() {
	p := c.policy
	for _, pool := range []*samplePool{&p.nodes, &p.hashes} {
		for k, h := range pool.slots {
			pool.slots[k] = sampleHandle{}
			if h.Allocation == 0 || h.Index < 0 || int(h.Index) >= p.count {
				continue
			}
			e := p.graph[h.Index]
			if e.Allocation != h.Allocation {
				continue
			}
			if e.State == nodeState {
				if i, ok := c.scratch.findNode(e.Path[:e.Length], e.Hash); ok {
					pool.slots[k] = sampleHandle{int32(i), h.Allocation}
				}
			}
			if e.State == hashState {
				if i, ok := c.scratch.findHash(e.Path[:e.Length], e.Hash); ok {
					pool.slots[k] = sampleHandle{-int32(i) - 1, h.Allocation}
				}
			}
		}
	}
}
func (c *residentCache) prepare(root mpt.Hash, raw []byte, h uint64) (CacheStats, error) {
	p, b := c.policy, &c.scratch
	p.begin(h)
	p.count = 0
	p.nUsed = 0
	p.hUsed = 0
	p.bodyUsed = 0
	p.nCount = 0
	p.hCount = 0
	b.n, b.h, b.used = 0, 0, 0
	defer func() {
		for i := 0; i < p.count; i++ {
			p.graph[i].Body = nil
		}
	}()
	if root == mpt.EmptyRootHash {
		c.importPools()
		c.exportPools()
		return c.layoutDelta(), nil
	}
	if len(raw) > len(b.body) {
		return CacheStats{}, ErrLimit
	}
	if _, err := p.newNode(nil, root, -1, raw); err != nil {
		return CacheStats{}, err
	}
	if err := c.configureNode(0); err != nil {
		return CacheStats{}, err
	}
	// Only bodies already obtained from execution or N are inspected. Logical
	// child links also describe D candidates, but no such body is fetched here.
	oldRemaining := c.current.n
	for i := 0; i < p.count; i++ {
		e := &p.graph[i]
		_, oldNode := c.current.nodePosition(e.Path[:e.Length])
		if oldNode {
			oldRemaining--
		}
		if !oldNode && i != 0 && len(p.graph)-p.count < 16*(oldRemaining+1) {
			e.Body = nil
		}
		if len(e.Body) == 0 {
			continue
		}
		desc, err := mpt.InspectCanonicalNode(e.Body)
		if err != nil {
			return CacheStats{}, err
		}
		var fragment []byte
		if desc.Kind == mpt.CanonicalExtension {
			fragment, _, err = mpt.DecodeCompactPath(desc.Path)
			if err != nil {
				return CacheStats{}, err
			}
		}
		for slot, ref := range desc.Children {
			if !ref.Present || len(ref.Inline) != 0 {
				continue
			}
			path := append(append([]byte(nil), e.Path[:e.Length]...), byte(slot))
			if desc.Kind == mpt.CanonicalExtension {
				path = append(append([]byte(nil), e.Path[:e.Length]...), fragment...)
			}
			if len(path) > MaxDepth {
				return CacheStats{}, ErrCorrupt
			}
			body, _, _ := c.bodyFor(path, ref.Hash)
			child, err := p.newNode(path, ref.Hash, int32(i), body)
			if err != nil {
				return CacheStats{}, err
			}
			e.Children[slot] = child
			if err := c.configureNode(child); err != nil {
				return CacheStats{}, err
			}
		}
	}
	// Account for old positions removed entirely by MPT split/merge, deletion
	// or becoming inline. This repair is mandatory and outside optional work.
	for i := 0; i < c.current.n+c.current.h; i++ {
		var path []byte
		if i < c.current.n {
			e := c.current.nodes[i]
			path = e.Path[:e.Length]
		} else {
			e := c.current.hashes[i-c.current.n]
			path = e.Path[:e.Length]
		}
		found := false
		for j := 0; j < p.count; j++ {
			if pathEqual(p.graph[j].Path, p.graph[j].Length, path) {
				found = true
				break
			}
		}
		if !found {
			p.metrics.ForcedRepairs++
		}
	}
	c.importPools()
	p.run()
	var export func(int32, uint32) (uint32, error)
	export = func(i int32, parent uint32) (uint32, error) {
		e := &p.graph[i]
		path := e.Path[:e.Length]
		if e.State == hashState {
			v, ok := b.addHash(pendingEntry{Path: e.Path, Length: e.Length, Hash: e.Hash, Parent: parent}, c.generation+1)
			if !ok {
				return 0, ErrLimit
			}
			b.hashes[b.h-1].Allocation = e.Allocation
			return v, nil
		}
		if e.State != nodeState {
			return 0, nil
		}
		v, ok := b.addNode(path, e.Hash, e.Body, h, c.generation+1, parent)
		if !ok {
			return 0, ErrLimit
		}
		b.nodes[v-1].A, b.nodes[v-1].U, b.nodes[v-1].Allocation = e.A, e.U, e.Allocation
		for slot, ch := range e.Children {
			if ch < 0 {
				continue
			}
			handle, err := export(ch, v)
			if err != nil {
				return 0, err
			}
			b.nodes[v-1].Children[slot] = handle
		}
		return v, nil
	}
	if _, err := export(0, 0); err != nil {
		return CacheStats{}, err
	}
	c.exportPools()
	return c.layoutDelta(), nil
}
