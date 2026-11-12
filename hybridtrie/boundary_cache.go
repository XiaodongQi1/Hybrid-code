package hybridtrie

import (
	"encoding/binary"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"time"
	"unsafe"
)

const PrefixBoundaryCache = "prefix-boundary"

// BoundaryConfig describes one N pool. There is no independent hash cache.
// Slots reserve a bounded parsed-field arena, so growing a resident node never
// forces an out-of-period eviction. Canonical RLP is operation-local; arena
// metadata and unused field capacity are fully billed.
type BoundaryConfig struct {
	ExecutionReads ExecutionReadConfig `json:",omitempty"`
	// MaxNodeBytes is either the general canonical limit or 532 for the
	// validated fixed-key Ethereum account/storage forest.
	MaxNodeBytes                                         uint32
	ResidentLayout, VictimEligibility, ReservationPolicy string `json:",omitempty"`
	AdmissionReadNodes, AdmissionReadBytes               uint64 `json:",omitempty"`
	HeatBucketCount, HeatSketchRows, HeatSketchWidth     uint32
	HeatBucketBlocks                                     uint64
	NodeBudget                                           ClassBudget
	PolicyIntervalBlocks, PolicyOriginHeight, HalfLife   uint64
	MaxResidentTries, WorkUnits                          uint32
	SampleSize                                           uint32 `json:",omitempty"` // zero selects the default of five
	HashUpdatedVictims                                   bool   `json:",omitempty"` // Deprecated: accepted for old configurations; evictions retain Prefix.
	SampleSeed                                           uint64 `json:",omitempty"`
	CandidateBytes, MetadataBytes, PlanBytes             uint64
}

func DefaultBoundaryConfig() BoundaryConfig {
	return BoundaryConfig{SampleSize: 5, MaxNodeBytes: mpt.MaxCanonicalNodeBytes, HeatBucketCount: 4, HeatBucketBlocks: 64, HeatSketchRows: 4, HeatSketchWidth: 16384, NodeBudget: ClassBudget{32 << 20, 8 << 20, 8 << 20}, PolicyIntervalBlocks: 16, HalfLife: 64, MaxResidentTries: 256, WorkUnits: 262144, CandidateBytes: 2 << 20, MetadataBytes: 4 << 20, PlanBytes: 40 << 20}
}
func DefaultPrefixBoundaryForestCacheConfig() ForestCacheConfig {
	return ForestCacheConfig{Mode: PrefixBoundaryCache, Boundary: DefaultBoundaryConfig()}
}

// The Ethereum preset enforces fixed 32-byte secure keys, empty branch values
// and bounded account/storage RLP. Its largest node is a 16-hash branch (532 B).
func DefaultEthereumPrefixBoundaryForestCacheConfig() ForestCacheConfig {
	c := DefaultPrefixBoundaryForestCacheConfig()
	c.Boundary.MaxNodeBytes = 532
	c.Boundary.NodeBudget = ClassBudget{256 << 10, 64 << 10, 64 << 10}
	c.Boundary.PlanBytes = 2 << 20
	return c
}

// Edge kinds are local only. Height zero is a real height, never a sentinel.
const (
	boundaryCold uint8 = iota
	boundaryPrefix
	boundaryHash
)

type boundaryEdge struct {
	Height [HeightBytes]byte
	Kind   uint8
}

func edgeAt(h uint64, kind uint8) boundaryEdge {
	e := boundaryEdge{Kind: kind}
	put48(e.Height[:], h)
	return e
}
func (e boundaryEdge) height() uint64 { return read48(e.Height[:]) }

// boundaryNode is a resident slot. Identity is derived from its root descriptor
// and links; complete paths exist only in block-local observations.
type boundaryNode struct {
	Link          boundaryLink
	Hash          mpt.Hash
	Image         mpt.NodeImage
	Version       forestVersion
	Heat          residentHeat
	Edges         [16]boundaryEdge
	Size          uint32
	Used, Updated bool
}

// Slot references are one-based; zero denotes no connection.
type boundaryLink struct {
	Parent, FirstChild, NextSibling int32
	Root                            uint16
	Edge                            uint8
}

type boundaryObservation struct {
	PreparationOnly bool
	Pos             forestPosition
	boundaryNode
}

type boundaryRoot struct {
	ID      uint64
	Hash    mpt.Hash
	Version forestVersion
	Used    bool
}
type boundaryCandidate struct {
	Node        boundaryObservation
	Offset      uint32
	CompactSize uint32 // physical Prefix payload in the block-local arena; Node.Size describes canonical bytes only
}
type boundaryBank struct {
	Pruned       uint64 // structural removals while replacing resident images
	Index        []int32
	Stride       int
	Nodes        []boundaryNode
	Headers      []boundaryHeader
	frames       boundaryFrames
	frontier     *boundarySelection
	usage        [2]uint64
	ResizeDrops  uint64
	Payload      []byte
	Roots        []boundaryRoot
	free, freeAt []int32
	counts       [2]uint64
	fieldBytes   uint64
	hintBytes    uint64
	changed      []int32
	marked       []uint64
	tracking     bool
	removed      func(forestPosition)
}

func (b *boundaryBank) root(id uint64) int {
	for i := range b.Roots {
		if b.Roots[i].Used && b.Roots[i].ID == id {
			return i
		}
	}
	return -1
}
func boundaryCharge() uint64 {
	return uint64(mpt.NodeImageMaxBytes(mpt.MaxCanonicalNodeBytes)) + uint64(unsafe.Sizeof(boundaryNode{}))
}

type BoundaryStats struct {
	ResidentLayout, VictimEligibility, ReservationPolicy                                                          string
	AllocationQuantumBytes, EffectiveNodeBudgetBytes                                                              uint64
	FrameCount, FrameMetadataBytes, HeaderBytes, StructuralResizeDrops                                            uint64
	FragmentationRejects, QuotaRejects, AdmissionReadLimitSkips, AdmissionReads, AdmissionBytes                   uint64
	ReserveAdmissions, ReserveReclaimedBytes, VictimGroupNodes                                                    uint64
	UnusedHeaderBytes, UnusedBodyBytes                                                                            uint64
	SampleAttempts, EligibleSum, SampleComparisons, EmptySamples, HeatRejects, SamplingNanos, SelectionIndexNanos uint64
	HeatSketchBytes, HeatSketchRows, HeatSketchWidth                                                              uint64
	HeatUpdates, HeatQueries, HeatParentClamps                                                                    uint64
	HeatRotations, HeatExpiredBuckets, HeatResetBytes, HeatResetNanos                                             uint64
	HeatHeight, HeatCoverageBlocks                                                                                uint64
	HeatBuckets                                                                                                   [boundaryHeatBucketCount]BoundaryHeatBucketStats
	MaterializedHits, TemporaryRLPEncodes                                                                         uint64
	NodeFieldBytes, NodeFieldSlotBytes, TemporaryRLPBytes                                                         uint64
	RetainedRLPBytes                                                                                              uint64
	SlotBytes, MaxNodeBytes, Slots                                                                                uint64
	Generation, PolicyRuns, PolicySkips, LastPolicyHeight, PolicyEvictions, StructuralRemovals                    uint64
	Admissions, AdmissionRejects, ProtectedRejects, CandidateDrops, WorkExhaustions                               uint64
	AccessEvents, NodeAccessEvents, NodeHits, NodeMisses                                                          uint64
	PrefixExactGets, PrefixSeeks, HashGets, ParentHashReuses, ColdResolutions                                     uint64
	PreparationExactGets, PreparationSeeks, PreparationHashGets                                                   uint64
	NodeEntries, RootEntries, NodeReservedBytes, NodeLiveBytes, BodySlackBytes, HashEntries                       uint64
	AccountReservedBytes, StorageReservedBytes, AccountReserveBytes, StorageReserveBytes                          uint64
	NodeAllocatedBytes, PlanAllocatedBytes, CandidateAllocatedBytes, MetadataAllocatedBytes                       uint64
	HeatNanos, PolicyNanos, PreparationNanos, ProjectionNanos, WriteNanos, PublicationNanos                       uint64
	TransientBodyHits, PreparationBodyReuses                                                                      uint64
	PublicationFailures, ForcedRecoveries                                                                         uint64
	ChangedNodes, IncrementalChecks                                                                               uint64
}
type boundaryForest struct {
	cfg                       BoundaryConfig
	selection                 boundarySelection
	current                   boundaryBank
	heat                      boundaryHeat
	candidates                []boundaryCandidate
	candidateIndex            []int32
	candidateBody             []byte
	candidateN, candidateUsed int
	candidateHighWater        int
	stats                     BoundaryStats
	head                      Head
	generation                uint64
	uncertain                 bool
	beforePublish             func() error
}

func (f *boundaryForest) charge() uint64 {
	return uint64(mpt.NodeImageMaxBytes(f.cfg.MaxNodeBytes)) + uint64(unsafe.Sizeof(boundaryNode{}))
}

type boundaryLayout struct {
	stride, slots, nc, candidateIndexSize, bodyBytes, indexSize int
	sketchCells, charge, meta, plan                             uint64
	frames                                                      int
	nodeBytes                                                   uint64
}

const maxBoundaryNodeBytes = 512 << 20

func planBoundary(c BoundaryConfig) (*boundaryLayout, error) {
	if !validBoundaryModes(c) || !c.ExecutionReads.valid() {
		return nil, ErrLimit
	}
	if c.SampleSize > 64 || c.HeatBucketCount != boundaryHeatBucketCount || c.HeatBucketBlocks != boundaryHeatBucketBlocks || c.HeatSketchRows == 0 || c.HeatSketchRows > 16 || c.HeatSketchWidth < 2 || c.HeatSketchWidth > 1<<20 || c.HeatSketchWidth%2 != 0 {
		return nil, ErrLimit
	}
	if (c.MaxNodeBytes != mpt.MaxCanonicalNodeBytes && c.MaxNodeBytes != 532) || c.NodeBudget.TotalBytes > maxBoundaryNodeBytes || !c.NodeBudget.valid() || c.PolicyIntervalBlocks == 0 || c.PolicyOriginHeight > MaxHeight || c.HalfLife == 0 || c.HalfLife > 1<<20 || c.MaxResidentTries == 0 || c.MaxResidentTries > 4096 || c.WorkUnits > 1<<24 || c.CandidateBytes < 4096 || c.CandidateBytes > 64<<20 || c.MetadataBytes > 64<<20 || c.PlanBytes > 128<<20 {
		return nil, ErrLimit
	}
	stride := int(mpt.NodeImageMaxBytes(c.MaxNodeBytes))
	charge := uint64(stride) + uint64(unsafe.Sizeof(boundaryNode{}))
	slots := int(c.NodeBudget.TotalBytes / charge)
	frames := 0
	nodeBytes := uint64(slots) * charge
	if c.ResidentLayout == BoundaryTypedLayout {
		frames = int(c.NodeBudget.TotalBytes / (typedFrameBytes + 4*uint64(unsafe.Sizeof(boundaryHeader{}))))
		slots = 4 * frames
		charge = 0
		nodeBytes = uint64(frames)*typedFrameBytes + uint64(slots)*uint64(unsafe.Sizeof(boundaryHeader{}))
		if c.NodeBudget.AccountReserveBytes < uint64(typedFrameBytes)+uint64(unsafe.Sizeof(boundaryHeader{})) {
			return nil, ErrLimit
		}
	}
	if slots == 0 {
		return nil, ErrLimit
	}
	nc := int(c.CandidateBytes / (3 * uint64(unsafe.Sizeof(boundaryCandidate{}))))
	if nc > 65536 {
		nc = 65536
	}
	candidateIndexSize := 1
	for candidateIndexSize < nc*4 {
		candidateIndexSize *= 2
	}
	bodyBytes := int(c.CandidateBytes) - nc*int(unsafe.Sizeof(boundaryCandidate{})) - 4*candidateIndexSize
	sketchCells := uint64(c.HeatBucketCount) * uint64(c.HeatSketchRows) * uint64(c.HeatSketchWidth)
	meta := sketchCells*8 + uint64(c.MaxResidentTries)*uint64(unsafe.Sizeof(boundaryRoot{})) + uint64(unsafe.Sizeof(boundaryForest{}))
	indexSize := 1
	for indexSize < slots*4 {
		indexSize *= 2
	}
	meta += uint64(frames) * uint64(unsafe.Sizeof(boundaryFrame{}))
	meta += uint64(indexSize)*4 + uint64(slots)*8 // committed free slots and reverse offsets
	plan := selectionBytes(slots, slots) + uint64(slots)*4 + uint64((slots+63)/64)*8

	if meta > c.MetadataBytes || plan > c.PlanBytes {
		return nil, ErrLimit
	}
	return &boundaryLayout{frames: frames, nodeBytes: nodeBytes, stride: stride, slots: slots, nc: nc, candidateIndexSize: candidateIndexSize, bodyBytes: bodyBytes, indexSize: indexSize, sketchCells: sketchCells, charge: charge, meta: meta, plan: plan}, nil
}
func newBoundaryForest(c BoundaryConfig) (*boundaryForest, error) {
	if c.SampleSize == 0 {
		c.SampleSize = 5
	}
	l, e := planBoundary(c)
	if e != nil {
		return nil, e
	}
	if l.frames > 0 {
		c.NodeBudget = effectiveTypedBudget(c.NodeBudget, l.nodeBytes)
	}
	stride, slots, nc, candidateIndexSize, bodyBytes, indexSize := l.stride, l.slots, l.nc, l.candidateIndexSize, l.bodyBytes, l.indexSize
	sketchCells, meta, plan := l.sketchCells, l.meta, l.plan
	current := boundaryBank{Index: make([]int32, indexSize), Stride: stride, Roots: make([]boundaryRoot, c.MaxResidentTries), free: make([]int32, slots), freeAt: make([]int32, slots)}
	if l.frames > 0 {
		current.Headers = make([]boundaryHeader, slots)
		current.Payload = make([]byte, l.frames*typedFrameBytes)
		current.frames.init(l.frames)
	} else {
		current.Nodes = make([]boundaryNode, slots)
		current.Payload = make([]byte, slots*stride)
	}
	for i := 0; i < slots; i++ {
		current.free[i] = int32(slots - 1 - i)
		current.freeAt[slots-1-i] = int32(i)
	}
	current.changed = make([]int32, 0, slots)
	current.marked = make([]uint64, (slots+63)/64)
	f := &boundaryForest{cfg: c, selection: newSelection(slots, slots), current: current, heat: boundaryHeat{Counters: make([]float64, int(sketchCells))}, candidates: make([]boundaryCandidate, nc), candidateIndex: make([]int32, candidateIndexSize), candidateBody: make([]byte, bodyBytes)}
	f.current.tracking = true
	if c.VictimEligibility == BoundaryFrontierVictims {
		f.current.frontier = &f.selection
	}
	f.stats.NodeAllocatedBytes = l.nodeBytes
	f.stats.PlanAllocatedBytes = plan
	f.stats.CandidateAllocatedBytes = uint64(nc)*uint64(unsafe.Sizeof(boundaryCandidate{})) + uint64(bodyBytes) + uint64(candidateIndexSize)*4
	f.stats.MetadataAllocatedBytes = meta
	if f.stats.MetadataAllocatedBytes > c.MetadataBytes {
		return nil, ErrLimit
	}
	return f, nil
}

// BoundaryCacheAllocation uses the same layout calculation as allocation,
// without constructing a probe cache.
func BoundaryCacheAllocation(c BoundaryConfig) (ForestCacheStats, error) {
	l, e := planBoundary(c)
	if e != nil {
		return ForestCacheStats{}, e
	}
	stats := BoundaryStats{Slots: uint64(l.slots), SlotBytes: l.charge, MaxNodeBytes: uint64(c.MaxNodeBytes), HeatSketchBytes: l.sketchCells * 8, EffectiveNodeBudgetBytes: l.nodeBytes, AllocationQuantumBytes: l.charge}
	stats.ResidentLayout, stats.VictimEligibility, stats.ReservationPolicy = boundaryModeNames(c)
	if l.frames > 0 {
		stats.HeaderBytes = uint64(unsafe.Sizeof(boundaryHeader{}))
		stats.FrameCount = uint64(l.frames)
		stats.FrameMetadataBytes = uint64(l.frames) * uint64(unsafe.Sizeof(boundaryFrame{}))
		stats.AllocationQuantumBytes = typedFrameBytes + 4*stats.HeaderBytes
	}
	return ForestCacheStats{ExecutionReadBudgetBytes: executionReadBudget(c.ExecutionReads), Mode: PrefixBoundaryCache, NodeAllocatedBytes: l.nodeBytes, PlanAllocatedBytes: l.plan, CandidateAllocatedBytes: uint64(l.nc)*uint64(unsafe.Sizeof(boundaryCandidate{})) + uint64(l.bodyBytes) + uint64(l.candidateIndexSize)*4, PolicyAllocatedBytes: l.meta, InstanceBytes: uint64(unsafe.Sizeof(CachedForest{})), Boundary: stats}, nil
}

func (s ForestCacheStats) AllocatedBytes() uint64 {
	return s.NodeAllocatedBytes + s.HashAllocatedBytes + s.PlanAllocatedBytes + s.CandidateAllocatedBytes + s.PolicyAllocatedBytes + s.InstanceBytes
}

// FitBoundaryCache maximizes resident slots under a total backing-allocation
// budget. It includes the committed pool, changed-slot indexes, indexes and sketches.
// Returned slack is explicit; it is not filled with unused padding allocations.
func FitBoundaryCache(total uint64, c BoundaryConfig) (BoundaryConfig, ForestCacheStats, error) {
	if _, e := planBoundary(c); e != nil {
		return c, ForestCacheStats{}, e
	}
	base := c.NodeBudget
	charge := uint64(mpt.NodeImageMaxBytes(c.MaxNodeBytes)) + uint64(unsafe.Sizeof(boundaryNode{}))
	if c.ResidentLayout == BoundaryTypedLayout {
		charge = typedFrameBytes + 4*uint64(unsafe.Sizeof(boundaryHeader{}))
	}
	lo, hi := uint64(1), uint64(maxBoundaryNodeBytes)/charge
	var best BoundaryConfig
	var stats ForestCacheStats
	for lo <= hi {
		mid := (lo + hi) / 2
		candidate := c
		candidate.NodeBudget = ClassBudget{mid * charge, mid * charge * base.AccountReserveBytes / base.TotalBytes, mid * charge * base.StorageReserveBytes / base.TotalBytes}
		candidate.PlanBytes = 128 << 20
		candidate.MetadataBytes = 64 << 20
		if c.ResidentLayout == BoundaryTypedLayout && candidate.NodeBudget.AccountReserveBytes < uint64(typedFrameBytes)+uint64(unsafe.Sizeof(boundaryHeader{})) {
			lo = mid + 1
			continue
		}
		s, e := BoundaryCacheAllocation(candidate)
		if e == nil && s.AllocatedBytes() <= total {
			best, stats = candidate, s
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	if stats.Boundary.Slots == 0 {
		return c, stats, ErrLimit
	}
	best.PlanBytes = stats.PlanAllocatedBytes
	best.MetadataBytes = stats.PolicyAllocatedBytes
	return best, stats, nil
}
func (f *boundaryForest) reset() {
	f.invalidateNodes()
	clearBoundary(f.heat.Counters)
	f.heat = boundaryHeat{Counters: f.heat.Counters}
}

// Discard invalid resident data after an exceptional error, but retain actual
// access observations. No undo copy of nodes is needed.
func (f *boundaryForest) invalidateNodes() {
	clearBoundary(f.current.Nodes)
	clearBoundary(f.current.Headers)
	if f.current.typed() {
		f.current.frames.reset()
	}
	f.current.usage = [2]uint64{}
	f.selection.resetPools()
	clearBoundary(f.current.Index)
	clearBoundary(f.current.Roots)
	f.current.counts = [2]uint64{}
	f.current.fieldBytes = 0
	f.current.hintBytes = 0
	f.current.free = f.current.free[:f.current.capacity()]
	for i := range f.current.free {
		slot := f.current.capacity() - 1 - i
		f.current.free[i] = int32(slot)
		f.current.freeAt[slot] = int32(i)
	}
	f.current.finishChanges()
	f.generation++
	f.stats.ForcedRecoveries++
}
func clearBoundary[T any](s []T) {
	var z T
	for i := range s {
		s[i] = z
	}
}
func (f *boundaryForest) begin(height uint64) error {
	t := time.Now()
	err := f.advanceHeat(height)
	f.stats.HeatNanos += uint64(time.Since(t))
	if err != nil {
		return err
	}
	f.candidateN, f.candidateUsed = 0, 0
	clearBoundary(f.candidateIndex)
	return nil
}
func (f *boundaryForest) policyDue(h uint64) bool {
	return h > f.cfg.PolicyOriginHeight && (h-f.cfg.PolicyOriginHeight)%f.cfg.PolicyIntervalBlocks == 0
}

func (f *boundaryForest) observe(n boundaryObservation, raw []byte) {
	f.observePayload(n, raw, false)
}

func (f *boundaryForest) observePayload(n boundaryObservation, raw []byte, compact bool) {
	if len(raw) > int(f.cfg.MaxNodeBytes) {
		if !compact {
			return
		}
		raw = nil // keep the exact locator if the compact body exceeds the cap
	}
	if i := f.findCandidate(n.Pos, n.Hash); i >= 0 {
		e := &f.candidates[i]
		if e.Node.Size == 0 && len(raw) > 0 && (!compact || e.CompactSize == 0) {
			// Upgrade a locator-only observation to a reusable canonical body.
			if len(raw) > len(f.candidateBody)-f.candidateUsed {
				f.stats.CandidateDrops++
				if n.Updated {
					e.Node.PreparationOnly = true
				}
				return
			}
			e.Offset = uint32(f.candidateUsed)
			if compact {
				e.CompactSize = uint32(len(raw))
			} else {
				e.CompactSize = 0
				e.Node.Size = uint32(len(raw))
				e.Node.Edges = n.Edges
			}
			copy(f.candidateBody[f.candidateUsed:], raw)
			f.candidateUsed += len(raw)
			if f.candidateUsed > f.candidateHighWater {
				f.candidateHighWater = f.candidateUsed
			}
		}
		if n.Updated {
			e.Node.Updated = true
			e.Node.Version = n.Version
		}
		if !n.PreparationOnly {
			e.Node.PreparationOnly = false
		}
		return
	}

	if f.candidateN == len(f.candidates) || len(raw) > len(f.candidateBody)-f.candidateUsed {
		f.stats.CandidateDrops++
		return
	}
	n.Used = true
	n.Size = uint32(len(raw))
	c := boundaryCandidate{Node: n, Offset: uint32(f.candidateUsed)}
	if compact {
		c.CompactSize, c.Node.Size = n.Size, 0
	}
	f.candidates[f.candidateN] = c
	f.candidateN++
	f.indexCandidate(f.candidateN - 1)
	copy(f.candidateBody[f.candidateUsed:], raw)
	f.candidateUsed += len(raw)
	if f.candidateUsed > f.candidateHighWater {
		f.candidateHighWater = f.candidateUsed
	}
}
func (f *boundaryForest) statsSnapshot() ForestCacheStats {
	s := f.stats
	s.Generation = f.generation
	s.HeatSketchBytes = uint64(len(f.heat.Counters)) * 8
	s.HeatSketchRows, s.HeatSketchWidth = uint64(f.cfg.HeatSketchRows), uint64(f.cfg.HeatSketchWidth)
	s.HeatHeight = f.heat.Height
	oldest := f.heat.Height
	for i, b := range f.heat.Buckets {
		s.HeatBuckets[i] = BoundaryHeatBucketStats{Epoch: b.Epoch, Anchor: b.Anchor, Valid: b.Valid}
		if b.Valid && b.Anchor < oldest {
			oldest = b.Anchor
		}
	}
	if f.heat.Started {
		s.HeatCoverageBlocks = f.heat.Height - oldest + 1
	}
	s.EffectiveNodeBudgetBytes = s.NodeAllocatedBytes
	s.AllocationQuantumBytes = f.charge()
	s.SlotBytes = f.charge()
	s.MaxNodeBytes = uint64(f.cfg.MaxNodeBytes)
	s.NodeFieldSlotBytes = uint64(f.current.Stride)
	s.TemporaryRLPBytes = uint64(f.candidateHighWater)
	s.Slots = uint64(f.current.capacity())
	s.NodeEntries = f.current.counts[0] + f.current.counts[1]
	s.NodeFieldBytes = f.current.fieldBytes
	s.NodeLiveBytes = s.NodeEntries*uint64(unsafe.Sizeof(boundaryNode{})) + s.NodeFieldBytes
	s.AccountReservedBytes = f.current.counts[0] * f.charge()
	s.StorageReservedBytes = f.current.counts[1] * f.charge()
	for _, r := range f.current.Roots {
		if r.Used {
			s.RootEntries++
		}
	}
	s.NodeReservedBytes = s.NodeEntries * f.charge()
	if f.current.typed() {
		s.SlotBytes = 0
		s.NodeFieldSlotBytes = 0
		s.HeaderBytes = uint64(unsafe.Sizeof(boundaryHeader{}))
		s.AllocationQuantumBytes = typedFrameBytes + 4*s.HeaderBytes
		s.FrameCount = uint64(len(f.current.frames.entries))
		s.FrameMetadataBytes = s.FrameCount * uint64(unsafe.Sizeof(boundaryFrame{}))
		s.AccountReservedBytes = f.current.usage[0]
		s.StorageReservedBytes = f.current.usage[1]
		s.NodeReservedBytes = s.AccountReservedBytes + s.StorageReservedBytes
		s.NodeLiveBytes = s.NodeEntries*s.HeaderBytes + s.NodeFieldBytes + f.current.hintBytes
		s.UnusedHeaderBytes = (s.Slots - s.NodeEntries) * s.HeaderBytes
		s.UnusedBodyBytes = uint64(len(f.current.Payload)) - (s.NodeReservedBytes - s.NodeEntries*s.HeaderBytes)
	}
	s.StructuralResizeDrops = f.current.ResizeDrops
	s.ResidentLayout, s.VictimEligibility, s.ReservationPolicy = boundaryModeNames(f.cfg)
	s.BodySlackBytes = s.NodeReservedBytes - s.NodeLiveBytes
	s.AccountReserveBytes = f.cfg.NodeBudget.AccountReserveBytes
	s.StorageReserveBytes = f.cfg.NodeBudget.StorageReserveBytes
	return ForestCacheStats{Mode: PrefixBoundaryCache, Boundary: s, Partitions: s.RootEntries, NodeAllocatedBytes: s.NodeAllocatedBytes, PlanAllocatedBytes: s.PlanAllocatedBytes, CandidateAllocatedBytes: s.CandidateAllocatedBytes, PolicyAllocatedBytes: s.MetadataAllocatedBytes, InstanceBytes: uint64(unsafe.Sizeof(CachedForest{})), NodeBodyHits: s.NodeHits, NodeBodyMisses: s.NodeMisses, Admissions: s.Admissions, Evictions: s.PolicyEvictions, DiskReaders: s.NodeMisses}
}

// validateNode makes the compact slot bound an explicit input contract, rather
// than silently evicting a node that grows beyond the reserved body capacity.
func (f *boundaryForest) validateNode(pos forestPosition, raw []byte) error {
	if len(raw) > int(f.cfg.MaxNodeBytes) {
		return ErrLimit
	}
	if f.cfg.MaxNodeBytes != 532 {
		return nil // the general slot contract requires only the size bound
	}
	d, err := mpt.DecodeLocalNodeFields(raw)
	if err != nil {
		return err
	}
	return f.validateFields(pos, len(raw), d)
}
func (f *boundaryForest) validateFields(pos forestPosition, size int, d mpt.CanonicalNode) error {
	if size > int(f.cfg.MaxNodeBytes) {
		return ErrLimit
	}
	if f.cfg.MaxNodeBytes != 532 {
		return nil
	}
	switch d.Kind {
	case mpt.CanonicalBranch:
		if len(d.Value) != 0 || pos.Length >= 64 {
			return ErrCorrupt
		}
	case mpt.CanonicalExtension:
		frag, _, e := mpt.DecodeCompactPath(d.Path)
		if e != nil || int(pos.Length)+len(frag) >= 64 {
			return ErrCorrupt
		}
	case mpt.CanonicalLeaf:
		frag, _, e := mpt.DecodeCompactPath(d.Path)
		limit := 110
		if pos.ID != 0 {
			limit = 33
		}
		if e != nil || int(pos.Length)+len(frag) != 64 || len(d.Value) > limit {
			return ErrCorrupt
		}
	}
	return nil
}

func (f *boundaryForest) candidateStart(pos forestPosition, h mpt.Hash) int {
	return int((positionHash(pos.ID, pos.Path[:pos.Length]) ^ binary.LittleEndian.Uint64(h[:8])) % uint64(len(f.candidateIndex)))
}
func (f *boundaryForest) findCandidate(pos forestPosition, h mpt.Hash) int {
	start := f.candidateStart(pos, h)
	for j := 0; j < len(f.candidateIndex); j++ {
		v := f.candidateIndex[(start+j)%len(f.candidateIndex)]
		if v == 0 {
			return -1
		}
		i := int(v - 1)
		if i < f.candidateN {
			n := f.candidates[i].Node
			if n.Pos == pos && n.Hash == h {
				return i
			}
		}
	}
	return -1
}
func (f *boundaryForest) indexCandidate(i int) {
	n := f.candidates[i].Node
	start := f.candidateStart(n.Pos, n.Hash)
	for j := 0; j < len(f.candidateIndex); j++ {
		p := (start + j) % len(f.candidateIndex)
		if f.candidateIndex[p] == 0 {
			f.candidateIndex[p] = int32(i + 1)
			return
		}
	}
}
func (f *boundaryForest) reindexCandidates() {
	clearBoundary(f.candidateIndex)
	for i := 0; i < f.candidateN; i++ {
		f.indexCandidate(i)
	}
}
