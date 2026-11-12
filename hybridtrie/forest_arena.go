package hybridtrie

import (
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"unsafe"
)

type forestPosition struct {
	ID     uint64
	Path   [64]byte
	Length uint8
}

func position(id uint64, path []byte) forestPosition {
	p := forestPosition{ID: id, Length: uint8(len(path))}
	copy(p.Path[:], path)
	return p
}

type forestVersion struct {
	Height uint64
	Source ReferenceKind
	Known  bool
}
type forestN struct {
	Pos          forestPosition
	Hash         mpt.Hash
	A, U         frequency
	Allocation   uint64
	Version      forestVersion
	Offset, Size uint32
}
type forestH struct {
	Pos        forestPosition
	Hash       mpt.Hash
	Allocation uint64
	Version    forestVersion
}
type forestRoot struct {
	ID               uint64
	Hash             mpt.Hash
	WeightN, WeightH uint64
	Index            int32
}
type forestBank struct {
	Index         []int32
	Nodes         []forestN
	Hashes        []forestH
	Body          []byte
	Roots         []forestRoot
	N, H, Used, R int
}
type forestObservation struct {
	Pos            forestPosition
	Hash           mpt.Hash
	Version        forestVersion
	Offset, Size   uint32
	Updated, Reset bool
}
type forestObservations struct {
	Entries []forestObservation
	Body    []byte
	N, Used int
}
type forestEvent struct {
	Pos          forestPosition
	Used, Access bool
}
type forestHeat struct {
	sketch  [2]heatStats
	events  []forestEvent
	n       int
	height  uint64
	started bool
}

type ClassCacheStats struct{ NodeReserveBytes, HashReserveBytes, NodeLiveBytes, HashLiveBytes, NodeSharedBytes, HashSharedBytes, NodeHits, NodeMisses uint64 }
type SharedCacheStats struct {
	Account, Storage                                                                                                                             ClassCacheStats
	Generation, NodeBudgetBytes, HashBudgetBytes, MetadataAllocatedBytes, MetadataBudgetBytes                                                    uint64
	CandidateBudgetBytes, PlanBudgetBytes, NodeLiveBytes, HashLiveBytes, NodeEntries, HashEntries                                                uint64
	BodyAccessEvents, DroppedEvents, DroppedCandidates, Admissions, Evictions, Promotions, Demotions                                             uint64
	Samples, StaleCandidates, CascadeNodes, MaxCascade, ReservationReclaims, ScoreRejects, QuotaRejects, Rollbacks, WorkUnits, BudgetExhaustions uint64
	DescriptorRejects, RootRejects, PublicationFailures, ForcedRepairs, FreedTries, PoolResets                                                   uint64
	ExecutionResolution, PreparationResolution                                                                                                   ResolutionStats
	HeatNanos, RepairNanos, PolicyNanos, ProjectionNanos, WriteNanos, PublicationNanos                                                           uint64
}
type forestPlanNode struct {
	Pos            forestPosition
	Hash           mpt.Hash
	A, U           frequency
	Version        forestVersion
	Allocation     uint64
	Children       [16]int32
	Parent         int32
	RootSlot       int32
	Body           []byte
	State          uint8
	ScoreN, ScoreH float64
}
type forestUndo struct {
	Index int32
	State uint8
}
type forestShared struct {
	index                                                                                                 []int32
	cfg                                                                                                   ReservedSharedConfig
	current, scratch                                                                                      forestBank
	observed                                                                                              forestObservations
	heat                                                                                                  forestHeat
	graph                                                                                                 []forestPlanNode
	count                                                                                                 int
	undo                                                                                                  []forestUndo
	undoN                                                                                                 int
	roots, order                                                                                          []int32
	demotions                                                                                             []int32
	fenwick                                                                                               []uint64
	nUsed, hUsed                                                                                          [2]uint64
	nCount, hCount, bodyUsed, trieCount                                                                   int
	rootN                                                                                                 int
	remaining                                                                                             uint64
	random, allocation, generation                                                                        uint64
	head                                                                                                  Head
	uncertain                                                                                             bool
	stats                                                                                                 SharedCacheStats
	nodeAllocated, hashAllocated, candidateAllocated, metadataAllocated, planAllocated, instanceAllocated uint64
	// Optional fault hook used to prove the post-durable-write cold recovery path.
	beforePublish func() error
}

func newForestShared(c ReservedSharedConfig) (*forestShared, error) {
	if !c.valid() {
		return nil, ErrLimit
	}
	ns, body, nc := splitArena(c.NodeBudget.TotalBytes, uint64(unsafe.Sizeof(forestN{})))
	hs := int(c.HashBudget.TotalBytes / uint64(unsafe.Sizeof(forestH{})))
	cs, cb, cc := splitArena(c.CandidateBytes, uint64(unsafe.Sizeof(forestObservation{})))
	if ns == 0 || body == 0 {
		return nil, ErrLimit
	}
	hc := uint64(hs) * uint64(unsafe.Sizeof(forestH{}))
	indexCharge := uint64(2*(ns+hs)) * 4
	bank := nc + hc + uint64(c.MaxResidentTries)*uint64(unsafe.Sizeof(forestRoot{})) + indexCharge
	// At most 16 independent edges per available body. All graph/undo/sort
	// storage is one global allocation, never multiplied by contract count.
	count := 16*(ns+cs) + int(c.MaxResidentTries) + 1
	undoCount := int(c.WorkUnits/3) + 1
	// One trial changes each old resident at most once and admits at most a
	// 65-host ancestor chain. Unrelated per-block work must not inflate undo.
	if max := ns + hs + 65; undoCount > max {
		undoCount = max
	}
	basePlan := bank + uint64(undoCount)*(uint64(unsafe.Sizeof(forestUndo{}))+4)
	unit := uint64(unsafe.Sizeof(forestPlanNode{})) + 4*4 + 8*4
	if basePlan > c.PlanBytes {
		return nil, ErrLimit
	}
	maxGraph := int((c.PlanBytes - basePlan) / unit)
	if count > maxGraph {
		count = maxGraph
	}
	if count < ns+hs+int(c.MaxResidentTries) {
		return nil, ErrLimit
	}
	planCharge := basePlan + uint64(count)*unit
	meta := uint64(c.MaxResidentTries)*uint64(unsafe.Sizeof(forestRoot{})) + uint64(2*c.EventCapacity)*uint64(unsafe.Sizeof(forestEvent{})) + uint64(16*c.SketchWidth)*2 + indexCharge
	if planCharge > c.PlanBytes || meta > c.MetadataBytes {
		return nil, ErrLimit
	}
	makeBank := func() forestBank {
		return forestBank{Nodes: make([]forestN, ns), Hashes: make([]forestH, hs), Body: make([]byte, body), Roots: make([]forestRoot, c.MaxResidentTries), Index: make([]int32, 2*(ns+hs))}
	}
	f := &forestShared{cfg: c, current: makeBank(), scratch: makeBank(), graph: make([]forestPlanNode, count), undo: make([]forestUndo, undoCount), roots: make([]int32, count), order: make([]int32, count), fenwick: make([]uint64, 4*count), random: c.Seed, generation: 1}
	f.demotions = make([]int32, undoCount)
	f.index = make([]int32, 2*count)
	if f.random == 0 {
		f.random = 1
	}
	f.observed = forestObservations{Entries: make([]forestObservation, cs), Body: make([]byte, cb)}
	f.heat.events = make([]forestEvent, 2*c.EventCapacity)
	for i := range f.heat.sketch {
		cfg := DefaultPolicyConfig()
		cfg.HalfLife = c.HalfLife
		cfg.SketchPeriod = c.SketchPeriod
		cfg.SketchWidth = c.SketchWidth
		f.heat.sketch[i] = heatStats{cfg: cfg, counters: make([]uint16, 8*c.SketchWidth)}
	}
	f.nodeAllocated, f.hashAllocated, f.candidateAllocated, f.metadataAllocated, f.planAllocated = nc, hc, cc, meta, planCharge
	f.instanceAllocated = uint64(unsafe.Sizeof(*f)) + uint64(unsafe.Sizeof(CachedForest{})) + uint64(unsafe.Sizeof(ForestBlock{}))
	return f, nil
}
func (b *forestBank) reset() {
	clearForestNodes(b.Nodes[:b.N])
	for i := range b.Index {
		b.Index[i] = 0
	}
	b.N, b.H, b.Used, b.R = 0, 0, 0, 0
}
func clearForestNodes(nodes []forestN) {
	for i := range nodes {
		nodes[i] = forestN{}
	}
}
func (b *forestBank) node(pos forestPosition) int {
	if i := b.lookup(pos); i > 0 {
		return int(i - 1)
	}
	return -1
}
func (b *forestBank) hash(pos forestPosition) int {
	if i := b.lookup(pos); i < 0 {
		return int(-i - 1)
	}
	return -1
}
func (b *forestBank) lookup(pos forestPosition) int32 {
	n := len(b.Index)
	if n == 0 {
		return 0
	}
	start := int(positionHash(pos.ID, pos.Path[:pos.Length]) % uint64(n))
	for k := 0; k < n; k++ {
		h := b.Index[(start+k)%n]
		if h == 0 {
			return 0
		}
		if h > 0 && b.Nodes[h-1].Pos == pos || h < 0 && b.Hashes[-h-1].Pos == pos {
			return h
		}
	}
	return 0
}
func (b *forestBank) buildIndex() {
	for i := range b.Index {
		b.Index[i] = 0
	}
	put := func(pos forestPosition, h int32) {
		n := len(b.Index)
		start := int(positionHash(pos.ID, pos.Path[:pos.Length]) % uint64(n))
		for k := 0; k < n; k++ {
			j := (start + k) % n
			if b.Index[j] == 0 {
				b.Index[j] = h
				return
			}
		}
	}
	for i := 0; i < b.N; i++ {
		put(b.Nodes[i].Pos, int32(i+1))
	}
	for i := 0; i < b.H; i++ {
		put(b.Hashes[i].Pos, -int32(i+1))
	}
}
func (b *forestBank) raw(i int) []byte         { e := b.Nodes[i]; return b.Body[e.Offset : e.Offset+e.Size] }
func (f *forestShared) nextAllocation() uint64 { f.allocation++; return f.allocation }
func (f *forestShared) reset() {
	f.current.reset()
	f.scratch.reset()
	f.generation++
	f.stats.PoolResets++
}
func (f *forestShared) begin(height uint64) {
	f.observed.N, f.observed.Used = 0, 0
	f.heat.height = height
	for i := range f.heat.sketch {
		f.heat.sketch[i].begin(height)
	}
	for i := range f.heat.events {
		f.heat.events[i] = forestEvent{}
	}
	f.heat.n = 0
}
func (f *forestShared) event(pos forestPosition) bool {
	n := len(f.heat.events)
	h := positionHash(pos.ID, pos.Path[:pos.Length])
	for k := 0; k < n; k++ {
		e := &f.heat.events[(int(h%uint64(n))+k)%n]
		if e.Used {
			if e.Pos == pos {
				return false
			}
			continue
		}
		if f.heat.n >= int(f.cfg.EventCapacity) {
			break
		}
		*e = forestEvent{Pos: pos, Used: true, Access: true}
		f.heat.n++
		return true
	}
	f.stats.DroppedEvents++
	return false
}
func (f *forestShared) observe(pos forestPosition, hash mpt.Hash, raw []byte, v forestVersion) {
	o := &f.observed
	for i := 0; i < o.N; i++ {
		if o.Entries[i].Pos == pos && o.Entries[i].Hash == hash {
			if v.Known {
				o.Entries[i].Version = v
			}
			return
		}
	}
	if o.N == len(o.Entries) || len(raw) > len(o.Body)-o.Used {
		f.stats.DroppedCandidates++
		return
	}
	e := forestObservation{Pos: pos, Hash: hash, Version: v, Offset: uint32(o.Used), Size: uint32(len(raw))}
	copy(o.Body[o.Used:], raw)
	o.Used += len(raw)
	o.Entries[o.N] = e
	o.N++
}
func (f *forestShared) body(pos forestPosition, hash mpt.Hash) ([]byte, forestVersion) {
	for i := f.observed.N - 1; i >= 0; i-- {
		e := f.observed.Entries[i]
		if e.Pos == pos && e.Hash == hash {
			return f.observed.Body[e.Offset : e.Offset+e.Size], e.Version
		}
	}
	if i := f.current.node(pos); i >= 0 && f.current.Nodes[i].Hash == hash {
		return f.current.raw(i), f.current.Nodes[i].Version
	}
	return nil, forestVersion{}
}
