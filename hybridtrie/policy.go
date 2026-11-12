package hybridtrie

import (
	"bytes"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"unsafe"
)

const (
	diskState uint8 = iota
	nodeState
	hashState
)

type sampleHandle struct {
	Index      int32
	Allocation uint64
}
type samplePool struct {
	slots []sampleHandle
	next  int
}

func (p *samplePool) offer(h sampleHandle) {
	for _, v := range p.slots {
		if v == h {
			return
		}
	}
	p.slots[p.next%len(p.slots)] = h
	p.next++
}

type policyNode struct {
	Path         [64]byte
	Hash         mpt.Hash
	Children     [16]int32
	Body         []byte // borrowed only during prepare, cleared before returning
	A, U         frequency
	Allocation   uint64
	Parent       int32
	NSize, HSize uint64
	Length       uint8
	State        uint8
}
type stateChange struct {
	Index  int32
	Before uint8
}
type PolicyMetrics struct {
	IdentityResets                                                                         uint64
	NodeAttempts, NodeRejects, NodeRollbacks, HashAttempts, HashRejects, HashRollbacks     uint64
	CascadeNodes, MaxCascade, ForcedHashDrops, WorkUnits, BudgetExhaustions, ForcedRepairs uint64
}
type cachePolicy struct {
	cfg                       PolicyConfig
	heat                      heatStats
	graph                     []policyNode
	count                     int
	undo                      []stateChange
	undoN                     int
	order, temp               []int32
	nodes, hashes             samplePool
	random, allocation        uint64
	remaining                 uint64
	metrics                   PolicyMetrics
	nUsed, hUsed, bodyUsed    uint64
	nCount, hCount            int
	nLimit, hLimit, bodyLimit uint64
	nSlots, hSlots            int
	height                    uint64
	budgetHeight              uint64
	budgetSet                 bool
}

func newCachePolicy(cfg PolicyConfig, ns, hs, cs int, nCharge, hCharge, bodyLimit, planLimit uint64) (*cachePolicy, uint64, uint64, error) {
	if !cfg.valid() {
		return nil, 0, 0, ErrLimit
	}
	count := ns + hs + cs + ns*16 + 1
	plan := page(uint64(count)*uint64(unsafe.Sizeof(policyNode{}))) + page(uint64(cfg.WorkUnits)*uint64(unsafe.Sizeof(stateChange{}))) + 2*page(uint64(count)*4)
	stats := page(uint64(8*cfg.SketchWidth)*2) + page(uint64(cfg.EventCapacity)*2*uint64(unsafe.Sizeof(heatEvent{}))) + page(uint64(cfg.NodePool+cfg.HashPool)*uint64(unsafe.Sizeof(sampleHandle{})))
	if stats > cfg.StatsBytes || plan+nCharge+hCharge > planLimit {
		return nil, 0, 0, ErrLimit
	}
	p := &cachePolicy{cfg: cfg, graph: make([]policyNode, count), undo: make([]stateChange, cfg.WorkUnits), order: make([]int32, count), temp: make([]int32, count), nodes: samplePool{slots: make([]sampleHandle, cfg.NodePool)}, hashes: samplePool{slots: make([]sampleHandle, cfg.HashPool)}, random: cfg.Seed, nLimit: nCharge, hLimit: hCharge, bodyLimit: bodyLimit, nSlots: ns, hSlots: hs}
	if p.random == 0 {
		p.random = 0x9e3779b97f4a7c15
	}
	p.heat = heatStats{cfg: cfg, counters: make([]uint16, 8*int(cfg.SketchWidth)), events: make([]heatEvent, 2*int(cfg.EventCapacity))}
	return p, plan, stats, nil
}
func (p *cachePolicy) nextAllocation() uint64 { p.allocation++; return p.allocation }
func (p *cachePolicy) rng(n int) int {
	x := p.random
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	p.random = x
	return int(x % uint64(n))
}
func (p *cachePolicy) spend(n uint64) bool {
	if n > p.remaining {
		p.remaining = 0
		return false
	}
	p.remaining -= n
	p.metrics.WorkUnits += n
	return true
}
func (p *cachePolicy) begin(h uint64) {
	p.heat.begin(h)
	p.height = h
	if !p.budgetSet || p.budgetHeight != h {
		p.remaining = uint64(p.cfg.WorkUnits)
		p.budgetHeight = h
		p.budgetSet = true
	}
	p.metrics = PolicyMetrics{}
	p.undoN = 0
}
func (p *cachePolicy) newNode(path []byte, hash mpt.Hash, parent int32, body []byte) (int32, error) {
	if p.count == len(p.graph) {
		return -1, ErrLimit
	}
	i := p.count
	p.count++
	e := policyNode{Hash: hash, Parent: parent, Length: uint8(len(path)), Body: body, HSize: uint64(unsafe.Sizeof(hashEntry{})), Allocation: p.nextAllocation()}
	copy(e.Path[:], path)
	for j := range e.Children {
		e.Children[j] = -1
	}
	e.NSize = uint64(unsafe.Sizeof(nodeEntry{})) + uint64(len(body))
	p.graph[i] = e
	return int32(i), nil
}
func (p *cachePolicy) scoreN(i int32) float64 { return p.graph[i].A.at(p.height, p.cfg.HalfLife) }
func (p *cachePolicy) scoreH(i int32) float64 {
	parent := p.graph[i].Parent
	if parent < 0 {
		return 0
	}
	return p.graph[parent].U.at(p.height, p.cfg.HalfLife)
}
func (p *cachePolicy) apply(i int32, state uint8) {
	e := &p.graph[i]
	switch e.State {
	case nodeState:
		p.nUsed -= e.NSize
		p.bodyUsed -= uint64(len(e.Body))
		p.nCount--
	case hashState:
		p.hUsed -= e.HSize
		p.hCount--
	}
	e.State = state
	switch state {
	case nodeState:
		p.nUsed += e.NSize
		p.bodyUsed += uint64(len(e.Body))
		p.nCount++
	case hashState:
		p.hUsed += e.HSize
		p.hCount++
	}
}
func (p *cachePolicy) change(i int32, state uint8) bool {
	if p.undoN == len(p.undo) || !p.spend(3) {
		return false
	}
	p.undo[p.undoN] = stateChange{i, p.graph[i].State}
	p.undoN++
	p.apply(i, state)
	return true
}
func (p *cachePolicy) rollback(start int) {
	for p.undoN > start {
		p.undoN--
		v := p.undo[p.undoN]
		p.apply(v.Index, v.Before)
	}
}
func (p *cachePolicy) nodeRoom(i int32) bool {
	e := &p.graph[i]
	return p.nUsed+e.NSize <= p.nLimit && p.bodyUsed+uint64(len(e.Body)) <= p.bodyLimit && p.nCount < p.nSlots
}
func (p *cachePolicy) hashRoom(i int32) bool {
	return p.hUsed+p.graph[i].HSize <= p.hLimit && p.hCount < p.hSlots
}
func (p *cachePolicy) frontier(i int32) bool {
	if i <= 0 || p.graph[i].State != nodeState {
		return false
	}
	for _, ch := range p.graph[i].Children {
		if ch >= 0 && p.graph[ch].State == nodeState {
			return false
		}
	}
	return true
}
func (p *cachePolicy) protected(v, x int32) bool {
	return v == 0 || bytes.HasPrefix(p.graph[x].Path[:p.graph[x].Length], p.graph[v].Path[:p.graph[v].Length])
}
func (p *cachePolicy) handle(i int32) sampleHandle { return sampleHandle{i, p.graph[i].Allocation} }

// offer charges duplicate/stale comparisons and the ring write. Pool discovery
// is advisory, so exhaustion may leave a discovered handle unrecorded.
func (p *cachePolicy) offer(pool *samplePool, h sampleHandle) bool {
	for _, v := range pool.slots {
		if !p.spend(1) {
			return false
		}
		if v == h {
			return true
		}
	}
	if !p.spend(1) {
		return false
	}
	pool.slots[pool.next%len(pool.slots)] = h
	pool.next++
	return true
}
func (p *cachePolicy) sample() {
	for k := uint32(0); k < p.cfg.PathsPerAttempt; k++ {
		i := int32(0)
		for p.count > 0 && p.graph[i].State == nodeState {
			if !p.spend(1) {
				return
			}
			var ns, hs [16]int32
			nn, hn := 0, 0
			for _, ch := range p.graph[i].Children {
				if ch < 0 {
					continue
				}
				switch p.graph[ch].State {
				case nodeState:
					ns[nn] = ch
					nn++
				case hashState:
					hs[hn] = ch
					hn++
				}
			}
			if hn > 0 {
				p.offer(&p.hashes, p.handle(hs[p.rng(hn)]))
			}
			if nn == 0 {
				if i != 0 {
					p.offer(&p.nodes, p.handle(i))
				}
				break
			}
			i = ns[p.rng(nn)]
		}
	}
}
func (p *cachePolicy) victim(node bool, x int32) int32 {
	p.sample()
	pool := &p.hashes
	if node {
		pool = &p.nodes
	}
	best := int32(-1)
	bestScore := float64(0)
	for k, h := range pool.slots {
		if !p.spend(1) {
			return -1
		}
		if h.Allocation == 0 || h.Index < 0 || int(h.Index) >= p.count || p.graph[h.Index].Allocation != h.Allocation {
			pool.slots[k] = sampleHandle{}
			continue
		}
		i := h.Index
		valid := p.graph[i].State == hashState
		if node {
			valid = p.frontier(i) && !p.protected(i, x)
		}
		if !valid {
			continue
		}
		if !p.spend(1) {
			return -1
		}
		score := p.scoreH(i)
		if node {
			score = p.scoreN(i)
		}
		if best < 0 || score < bestScore || (score == bestScore && lessPath(p.graph[i].Path, p.graph[i].Length, p.graph[best].Path, p.graph[best].Length)) {
			best = i
			bestScore = score
		}
	}
	return best
}
func (p *cachePolicy) removeNode(i int32) bool {
	h := 0
	for _, ch := range p.graph[i].Children {
		if ch >= 0 && p.graph[ch].State == hashState {
			h++
		}
	}
	// Reserve forward work and undo capacity before touching any entry. The extra
	// units cover discovering/offering the newly exposed parent. Three units
	// per state change cover the state write, undo append and reserved rollback.
	if p.remaining < uint64(3*(h+1)+2+len(p.nodes.slots)) || p.undoN+h+1 > len(p.undo) {
		return false
	}
	for _, ch := range p.graph[i].Children {
		if ch >= 0 && p.graph[ch].State == hashState {
			p.change(ch, diskState)
		}
	}
	p.change(i, diskState)
	p.spend(1)
	if parent := p.graph[i].Parent; p.frontier(parent) {
		p.offer(&p.nodes, p.handle(parent))
	}
	return true
}
func (p *cachePolicy) tryNode(x int32) bool {
	e := &p.graph[x]
	if e.State == nodeState {
		return true
	}
	p.metrics.NodeAttempts++
	if len(e.Body) == 0 || e.Parent < 0 || p.graph[e.Parent].State != nodeState || e.NSize > p.nLimit || uint64(len(e.Body)) > p.bodyLimit {
		p.metrics.NodeRejects++
		return false
	}
	start := p.undoN
	fail := func() bool {
		if p.undoN > start {
			p.metrics.NodeRollbacks++
		}
		p.rollback(start)
		p.metrics.NodeRejects++
		return false
	}
	for !p.nodeRoom(x) {
		v := p.victim(true, x)
		if v < 0 || !p.spend(1) || p.scoreN(x) <= (1+p.cfg.Epsilon)*p.scoreN(v) {
			return fail()
		}
		if !p.removeNode(v) {
			return fail()
		}
	}
	if !p.change(x, nodeState) {
		return fail()
	}
	end := p.undoN
	removed := uint64(0)
	for i := end - 1; i >= start; i-- {
		v := p.undo[i]
		if v.Before == nodeState {
			removed++
			if parent := p.graph[v.Index].Parent; parent >= 0 && p.graph[parent].State == nodeState && p.spend(1) {
				p.tryHash(v.Index, true)
			}
		}
	}
	p.metrics.CascadeNodes += removed
	if removed > p.metrics.MaxCascade {
		p.metrics.MaxCascade = removed
	}
	for i := start; i < end; i++ {
		if p.undo[i].Before == hashState && p.graph[p.undo[i].Index].State == diskState {
			p.metrics.ForcedHashDrops++
		}
	}
	p.undoN = start
	return true
}
func (p *cachePolicy) tryHash(x int32, nested bool) bool {
	e := &p.graph[x]
	if e.State == hashState {
		return true
	}
	if e.State == nodeState || e.Parent < 0 || p.graph[e.Parent].State != nodeState {
		return false
	}
	p.metrics.HashAttempts++
	start := p.undoN
	fail := func() bool {
		if p.undoN > start {
			p.metrics.HashRollbacks++
		}
		p.rollback(start)
		p.metrics.HashRejects++
		return false
	}
	for !p.hashRoom(x) {
		v := p.victim(false, x)
		if v < 0 || !p.spend(1) || p.scoreH(x) <= (1+p.cfg.Epsilon)*p.scoreH(v) {
			return fail()
		}
		if !p.change(v, diskState) {
			return fail()
		}
	}
	if !p.change(x, hashState) {
		return fail()
	}
	if !nested {
		p.undoN = start
	}
	return true
}

// Merge-sort candidate handles in a charged bounded workspace. Exhaustion stops
// optional admission; it never prevents topology repair or persistence.
func (p *cachePolicy) ordered(node bool) (int, bool) {
	n := 0
	for i := 0; i < p.count; i++ {
		e := &p.graph[i]
		if e.State == nodeState || e.Parent < 0 {
			continue
		}
		if node && len(e.Body) == 0 {
			continue
		}
		if !node && (e.State != diskState || p.graph[e.Parent].State != nodeState) {
			continue
		}
		if !p.spend(1) {
			return 0, false
		}
		p.order[n] = int32(i)
		n++
	}
	less := func(a, b int32) bool {
		if node && p.graph[a].Length != p.graph[b].Length {
			return p.graph[a].Length < p.graph[b].Length
		}
		sa, sb := p.scoreH(a), p.scoreH(b)
		if node {
			sa, sb = p.scoreN(a), p.scoreN(b)
		}
		if sa != sb {
			return sa > sb
		}
		return lessPath(p.graph[a].Path, p.graph[a].Length, p.graph[b].Path, p.graph[b].Length)
	}
	for width := 1; width < n; width *= 2 {
		for lo := 0; lo < n; lo += 2 * width {
			mid, hi := lo+width, lo+2*width
			if mid > n {
				mid = n
			}
			if hi > n {
				hi = n
			}
			a, b := lo, mid
			for k := lo; k < hi; k++ {
				if !p.spend(1) {
					return 0, false
				}
				if a < mid && (b >= hi || less(p.order[a], p.order[b])) {
					p.temp[k] = p.order[a]
					a++
				} else {
					p.temp[k] = p.order[b]
					b++
				}
			}
		}
		copy(p.order[:n], p.temp[:n])
	}
	return n, true
}
func (p *cachePolicy) run() {
	if p.remaining == 0 {
		p.metrics.BudgetExhaustions = 1
		return
	}
	if n, ok := p.ordered(true); ok {
		for k := 0; k < n; k++ {
			if p.remaining == 0 {
				break
			}
			if !p.tryNode(p.order[k]) {
				p.tryHash(p.order[k], false)
			}
		}
	}
	if p.remaining == 0 {
		p.metrics.BudgetExhaustions = 1
		return
	}
	if n, ok := p.ordered(false); ok {
		for k := 0; k < n; k++ {
			if p.remaining == 0 {
				break
			}
			p.tryHash(p.order[k], false)
		}
	}
	if p.remaining == 0 {
		p.metrics.BudgetExhaustions = 1
	}
}

func (m *PolicyMetrics) add(v PolicyMetrics) {
	m.IdentityResets += v.IdentityResets
	m.NodeAttempts += v.NodeAttempts
	m.NodeRejects += v.NodeRejects
	m.NodeRollbacks += v.NodeRollbacks
	m.HashAttempts += v.HashAttempts
	m.HashRejects += v.HashRejects
	m.HashRollbacks += v.HashRollbacks
	m.CascadeNodes += v.CascadeNodes
	if v.MaxCascade > m.MaxCascade {
		m.MaxCascade = v.MaxCascade
	}
	m.ForcedHashDrops += v.ForcedHashDrops
	m.WorkUnits += v.WorkUnits
	m.BudgetExhaustions += v.BudgetExhaustions
	m.ForcedRepairs += v.ForcedRepairs
}
