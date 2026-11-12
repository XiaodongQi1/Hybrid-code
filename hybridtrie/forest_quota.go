package hybridtrie

import "math"

const (
	PartitionedCache    = "partitioned"
	ReservedSharedCache = "reserved-shared"
)

// ClassBudget protects each class's unused reservation. Shared capacity is the
// difference, not a third set of entries or a permanent tag on individual nodes.
type ClassBudget struct {
	TotalBytes, AccountReserveBytes, StorageReserveBytes uint64
}

func (b ClassBudget) valid() bool {
	return b.AccountReserveBytes <= b.TotalBytes && b.StorageReserveBytes <= b.TotalBytes-b.AccountReserveBytes
}
func (b ClassBudget) reserve(class int) uint64 {
	if class == 0 {
		return b.AccountReserveBytes
	}
	return b.StorageReserveBytes
}
func excess(n, reserve uint64) uint64 {
	if n > reserve {
		return n - reserve
	}
	return 0
}
func (b ClassBudget) fits(u [2]uint64) bool {
	if !b.valid() || u[0] > b.TotalBytes || u[1] > b.TotalBytes-u[0] {
		return false
	}
	return excess(u[0], b.AccountReserveBytes)+excess(u[1], b.StorageReserveBytes) <= b.TotalBytes-b.AccountReserveBytes-b.StorageReserveBytes
}
func cacheClass(id uint64) int {
	if id == AccountTrieID {
		return 0
	}
	return 1
}

type ReservedSharedConfig struct {
	NodeBudget, HashBudget                   ClassBudget
	MaxResidentTries                         uint32
	MetadataBytes, CandidateBytes, PlanBytes uint64
	TreesPerAttempt, PathsPerTrie, WorkUnits uint32
	HalfLife, SketchPeriod                   uint64
	SketchWidth, EventCapacity               uint32
	Seed                                     uint64
	Epsilon, NodeWeight, HashWeight          float64
}

func DefaultReservedSharedConfig() ReservedSharedConfig {
	return ReservedSharedConfig{
		NodeBudget:       ClassBudget{256 << 10, 64 << 10, 64 << 10},
		HashBudget:       ClassBudget{64 << 10, 16 << 10, 16 << 10},
		MaxResidentTries: 256, MetadataBytes: 2 << 20, CandidateBytes: 1 << 20, PlanBytes: 16 << 20,
		TreesPerAttempt: 4, PathsPerTrie: 2, WorkUnits: 262144,
		HalfLife: 64, SketchPeriod: 256, SketchWidth: 16384, EventCapacity: 4096,
		Seed: 1, Epsilon: .1, NodeWeight: 1, HashWeight: 1,
	}
}
func DefaultReservedSharedForestCacheConfig() ForestCacheConfig {
	return ForestCacheConfig{Mode: ReservedSharedCache, ReservedShared: DefaultReservedSharedConfig()}
}
func (c ReservedSharedConfig) valid() bool {
	finite := func(x float64) bool { return !math.IsNaN(x) && !math.IsInf(x, 0) }
	return c.NodeBudget.TotalBytes <= 64<<20 && c.HashBudget.TotalBytes <= 64<<20 && c.NodeBudget.valid() && c.HashBudget.valid() && c.NodeBudget.TotalBytes >= 16<<10 &&
		c.MaxResidentTries > 0 && c.MaxResidentTries <= 4096 && c.MetadataBytes <= 128<<20 && c.PlanBytes <= 512<<20 && c.CandidateBytes <= 64<<20 &&
		c.TreesPerAttempt > 0 && c.TreesPerAttempt <= 64 && c.PathsPerTrie > 0 && c.PathsPerTrie <= 64 && c.WorkUnits <= 1<<20 &&
		c.HalfLife > 0 && c.HalfLife <= 1<<20 && c.SketchPeriod >= 2 && c.SketchPeriod <= 65534 && c.SketchPeriod%2 == 0 &&
		c.SketchWidth > 0 && c.SketchWidth <= 1<<20 && c.EventCapacity > 0 && c.EventCapacity <= 65536 &&
		finite(c.Epsilon) && c.Epsilon >= 0 && finite(c.NodeWeight) && c.NodeWeight > 0 && c.NodeWeight <= 1<<20 && finite(c.HashWeight) && c.HashWeight > 0 && c.HashWeight <= 1<<20
}

// planUsage checks both resource dimensions and the nonrequesting class floor.
// Topology repair/retirement is performed before this optional replacement step.
func planUsage(c ReservedSharedConfig, beforeN, beforeH, afterN, afterH [2]uint64, requester int) bool {
	if !c.NodeBudget.fits(afterN) || !c.HashBudget.fits(afterH) {
		return false
	}
	o := 1 - requester
	nFloor, hFloor := beforeN[o], beforeH[o]
	if nFloor > c.NodeBudget.reserve(o) {
		nFloor = c.NodeBudget.reserve(o)
	}
	if hFloor > c.HashBudget.reserve(o) {
		hFloor = c.HashBudget.reserve(o)
	}
	return afterN[o] >= nFloor && afterH[o] >= hFloor
}
