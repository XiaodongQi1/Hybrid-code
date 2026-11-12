package hybridtrie

import (
	"bytes"
	"fmt"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"unsafe"
)

const cachePage = uint64(8192)
const hashHandle = uint32(1 << 31)

// CacheConfig bounds retained backing storage, separately from transient work.
// Root is pinned and charged to NodeBytes. PlanBytes includes the second cache
// buffer, bounded graph, candidate order and local undo log.
// MaxBlockCharge is a conservative cumulative work-allocation guard, not an RSS cap.
type CacheConfig struct {
	NodeBytes, HashBytes, CandidateBytes, PlanBytes, MaxBlockCharge uint64
	MaxOperations                                                   uint32
	Policy                                                          PolicyConfig
}

func DefaultCacheConfig() CacheConfig {
	return CacheConfig{NodeBytes: 64 << 10, HashBytes: 16 << 10, CandidateBytes: 256 << 10, PlanBytes: 2 << 20, MaxBlockCharge: 32 << 20, MaxOperations: 1024, Policy: DefaultPolicyConfig()}
}

type nodeEntry struct {
	Path             [64]byte
	Hash             mpt.Hash
	Tick, Generation uint64
	Allocation       uint64
	A, U             frequency
	Offset, Size     uint32
	Parent           uint32
	Children         [16]uint32
	Length           uint8
}
type hashEntry struct {
	Path       [64]byte
	Hash       mpt.Hash
	Generation uint64
	Allocation uint64
	Parent     uint32
	Length     uint8
}
type observedEntry struct {
	Path         [64]byte
	Hash         mpt.Hash
	Tick         uint64
	Offset, Size uint32
	Length       uint8
}
type pendingEntry struct {
	Path         [64]byte
	Hash         mpt.Hash
	Parent       uint32
	Slot, Length uint8
	SkipNode     bool
}
type cacheBuffer struct {
	nodes      []nodeEntry
	hashes     []hashEntry
	body       []byte
	n, h, used int
}
type observations struct {
	entries []observedEntry
	body    []byte
	n, used int
	dropped uint64
}
type residentCache struct {
	cfg                                                                 CacheConfig
	current, scratch                                                    cacheBuffer
	observed                                                            observations
	pending                                                             []pendingEntry
	nodeCharge, hashCharge, planCharge, candidateCharge, instanceCharge uint64
	generation                                                          uint64
	policy                                                              *cachePolicy
	policyCharge                                                        uint64
	trieID                                                              uint64
}

type CacheStats struct {
	PolicyMetrics
	ExecutionResolution, PreparationResolution                                                             ResolutionStats
	PolicyStatsBudgetBytes, PolicyAllocatedBytes, BodyAccessEvents, SketchResets                           uint64
	NodeBudgetBytes, HashBudgetBytes, PlanBudgetBytes, CandidateBudgetBytes                                uint64
	NodeAllocatedBytes, HashAllocatedBytes, PlanAllocatedBytes, CandidateAllocatedBytes, InstanceBytes     uint64
	NodeLiveBytes, HashLiveBytes                                                                           uint64
	NodeEntries, HashEntries, NodeCapacity, HashCapacity                                                   uint64
	Generation                                                                                             uint64
	NodeBodyHits, NodeBodyMisses, PreparationHits, PreparationMisses, HashReferenceSelections              uint64
	NodeAdmissions, NodeEvictions, HashAdmissions, HashEvictions, Promotions, Demotions, DroppedCandidates uint64
	MaxBlockChargedBytes                                                                                   uint64
}

func page(n uint64) uint64 {
	if n == 0 {
		return 0
	}
	return (n + cachePage - 1) / cachePage * cachePage
}
func splitArena(budget, entrySize uint64) (slots int, payload, charge uint64) {
	if budget < 2*cachePage {
		return 0, 0, 0
	}
	count := budget / 2 / entrySize
	if count > 4096 {
		count = 4096
	}
	header := page(count * entrySize)
	remaining := (budget - header) / cachePage * cachePage
	return int(count), remaining, header + remaining
}
func newResidentCache(cfg CacheConfig) (*residentCache, error) {
	if cfg.Policy == (PolicyConfig{}) {
		cfg.Policy = DefaultPolicyConfig()
	}
	if cfg.NodeBytes > 64<<20 || cfg.HashBytes > 64<<20 || cfg.CandidateBytes > 64<<20 || cfg.PlanBytes > 512<<20 || cfg.MaxBlockCharge < 1<<20 || cfg.MaxBlockCharge > 1<<30 || cfg.MaxOperations == 0 || cfg.MaxOperations > 65536 {
		return nil, ErrLimit
	}
	ns, body, ncharge := splitArena(cfg.NodeBytes, uint64(unsafe.Sizeof(nodeEntry{})))
	if ns == 0 || body == 0 {
		return nil, fmt.Errorf("%w: NodeBytes must provide at least 16 KiB", ErrLimit)
	}
	hs := int(cfg.HashBytes / cachePage * cachePage / uint64(unsafe.Sizeof(hashEntry{})))
	if hs > 4096 {
		hs = 4096
	}
	hcharge := page(uint64(hs) * uint64(unsafe.Sizeof(hashEntry{})))
	cs, cb, ccharge := splitArena(cfg.CandidateBytes, uint64(unsafe.Sizeof(observedEntry{})))
	policy, extra, policyCharge, err := newCachePolicy(cfg.Policy, ns, hs, cs, ncharge, hcharge, body, cfg.PlanBytes)
	if err != nil {
		return nil, err
	}
	pcharge := ncharge + hcharge + extra
	allocate := func() cacheBuffer {
		return cacheBuffer{nodes: make([]nodeEntry, ns), hashes: make([]hashEntry, hs), body: make([]byte, int(body))}
	}
	c := &residentCache{cfg: cfg, current: allocate(), scratch: allocate(), observed: observations{entries: make([]observedEntry, cs), body: make([]byte, int(cb))}, policy: policy, policyCharge: policyCharge, nodeCharge: ncharge, hashCharge: hcharge, planCharge: pcharge, candidateCharge: ccharge, generation: 1}
	c.instanceCharge = page(uint64(unsafe.Sizeof(*c)))
	return c, nil
}
func pathEqual(stored [64]byte, n uint8, path []byte) bool {
	return int(n) == len(path) && bytes.Equal(stored[:n], path)
}
func (b *cacheBuffer) findNode(path []byte, h mpt.Hash) (int, bool) {
	for i := 0; i < b.n; i++ {
		e := &b.nodes[i]
		if e.Hash == h && pathEqual(e.Path, e.Length, path) {
			return i, true
		}
	}
	return 0, false
}
func (b *cacheBuffer) findHash(path []byte, h mpt.Hash) (int, bool) {
	for i := 0; i < b.h; i++ {
		e := &b.hashes[i]
		if e.Hash == h && pathEqual(e.Path, e.Length, path) {
			return i, true
		}
	}
	return 0, false
}
func (b *cacheBuffer) raw(i int) []byte {
	e := b.nodes[i]
	return b.body[int(e.Offset):int(e.Offset+e.Size)]
}

// lookupBody and lookupHash have different contracts. H can satisfy only the
// latter. The caller binds namespace, Store, root and Head; these primitives
// additionally reject stale generations. Returned bodies remain borrowed.
func (c *residentCache) lookupBody(path []byte, expected mpt.Hash, generation uint64) (int, bool) {
	if generation != c.generation {
		return 0, false
	}
	i, ok := c.current.findNode(path, expected)
	return i, ok && c.current.nodes[i].Generation == generation
}

func (c *residentCache) lookupHash(path []byte, generation uint64) (mpt.Hash, uint8, bool) {
	if generation != c.generation {
		return mpt.Hash{}, diskState, false
	}
	if i, ok := c.current.nodePosition(path); ok {
		e := c.current.nodes[i]
		if e.Generation == generation {
			return e.Hash, nodeState, true
		}
	}
	if i, ok := c.current.hashPosition(path); ok {
		e := c.current.hashes[i]
		if e.Generation == generation {
			return e.Hash, hashState, true
		}
	}
	return mpt.Hash{}, diskState, false
}
func (o *observations) reset() { o.n = 0; o.used = 0; o.dropped = 0 }
func (o *observations) add(path []byte, h mpt.Hash, raw []byte, tick uint64) {
	for i := 0; i < o.n; i++ {
		e := &o.entries[i]
		if e.Hash == h && pathEqual(e.Path, e.Length, path) {
			e.Tick = tick
			return
		}
	}
	if o.n == len(o.entries) || len(raw) > len(o.body)-o.used {
		o.dropped++
		return
	}
	e := observedEntry{Hash: h, Tick: tick, Offset: uint32(o.used), Size: uint32(len(raw)), Length: uint8(len(path))}
	copy(e.Path[:], path)
	copy(o.body[o.used:], raw)
	o.entries[o.n] = e
	o.n++
	o.used += len(raw)
}
func (c *residentCache) bodyFor(path []byte, h mpt.Hash) ([]byte, uint64, bool) {
	// New materialized versions supersede observations from the old state by hash.
	for i := c.observed.n - 1; i >= 0; i-- {
		e := c.observed.entries[i]
		if e.Hash == h && pathEqual(e.Path, e.Length, path) {
			return c.observed.body[int(e.Offset):int(e.Offset+e.Size)], e.Tick, true
		}
	}
	if i, ok := c.current.findNode(path, h); ok {
		return c.current.raw(i), c.current.nodes[i].Tick, true
	}
	return nil, 0, false
}
func (b *cacheBuffer) addNode(path []byte, h mpt.Hash, raw []byte, tick, gen uint64, parent uint32) (uint32, bool) {
	if b.n == len(b.nodes) || len(raw) > len(b.body)-b.used {
		return 0, false
	}
	e := nodeEntry{Hash: h, Tick: tick, Generation: gen, Parent: parent, Offset: uint32(b.used), Size: uint32(len(raw)), Length: uint8(len(path))}
	copy(e.Path[:], path)
	copy(b.body[b.used:], raw)
	b.nodes[b.n] = e
	b.n++
	b.used += len(raw)
	return uint32(b.n), true
}
func (b *cacheBuffer) addHash(p pendingEntry, gen uint64) (uint32, bool) {
	if b.h == len(b.hashes) {
		return 0, false
	}
	e := hashEntry{Path: p.Path, Hash: p.Hash, Generation: gen, Parent: p.Parent, Length: p.Length}
	b.hashes[b.h] = e
	b.h++
	return hashHandle | uint32(b.h), true
}

// layoutDelta compares the accepted plan with the currently published cache.
func (c *residentCache) layoutDelta() CacheStats {
	b := &c.scratch
	stats := CacheStats{PolicyMetrics: c.policy.metrics}
	for i := 0; i < c.current.n; i++ {
		e := c.current.nodes[i]
		path := e.Path[:e.Length]
		if _, ok := b.findNode(path, e.Hash); !ok {
			stats.NodeEvictions++
			if _, ok = b.findHash(path, e.Hash); ok {
				stats.Demotions++
			}
		}
	}
	for i := 0; i < c.current.h; i++ {
		e := c.current.hashes[i]
		if _, ok := b.findHash(e.Path[:e.Length], e.Hash); !ok {
			stats.HashEvictions++
		}
	}
	for i := 0; i < b.n; i++ {
		e := b.nodes[i]
		path := e.Path[:e.Length]
		if _, ok := c.current.findNode(path, e.Hash); !ok {
			stats.NodeAdmissions++
			if _, ok = c.current.findHash(path, e.Hash); ok {
				stats.Promotions++
			}
		}
	}
	for i := 0; i < b.h; i++ {
		e := b.hashes[i]
		if _, ok := c.current.findHash(e.Path[:e.Length], e.Hash); !ok {
			stats.HashAdmissions++
		}
	}
	stats.DroppedCandidates = c.observed.dropped
	return stats
}
func (c *residentCache) install() {
	c.current, c.scratch = c.scratch, c.current
	c.generation++
	c.observed.reset()
}
func (c *residentCache) layout(ctx NodeKey, eligible uint16) (uint16, error) {
	// Match by position: the plan was constructed from the final trusted root.
	b := &c.scratch
	for i := 0; i < b.n; i++ {
		e := b.nodes[i]
		if pathEqual(e.Path, e.Length, ctx.Path) {
			var mask uint16
			for slot, h := range e.Children {
				if h != 0 {
					mask |= 1 << slot
				}
			}
			return mask & eligible, nil
		}
	}
	return 0, nil
}
func (c *residentCache) stats() CacheStats {
	b := &c.current
	return CacheStats{PolicyStatsBudgetBytes: c.cfg.Policy.StatsBytes, PolicyAllocatedBytes: c.policyCharge, BodyAccessEvents: c.policy.heat.accessEvents, SketchResets: c.policy.heat.resets[0] + c.policy.heat.resets[1], NodeBudgetBytes: c.cfg.NodeBytes, HashBudgetBytes: c.cfg.HashBytes, PlanBudgetBytes: c.cfg.PlanBytes, CandidateBudgetBytes: c.cfg.CandidateBytes, NodeAllocatedBytes: c.nodeCharge, HashAllocatedBytes: c.hashCharge, PlanAllocatedBytes: c.planCharge, CandidateAllocatedBytes: c.candidateCharge, InstanceBytes: c.instanceCharge, NodeLiveBytes: uint64(b.n)*uint64(unsafe.Sizeof(nodeEntry{})) + uint64(b.used), HashLiveBytes: uint64(b.h) * uint64(unsafe.Sizeof(hashEntry{})), NodeEntries: uint64(b.n), HashEntries: uint64(b.h), NodeCapacity: uint64(len(b.nodes)), HashCapacity: uint64(len(b.hashes)), Generation: c.generation}
}
