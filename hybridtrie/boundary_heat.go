package hybridtrie

import (
	"math"
	"time"
)

const (
	boundaryHeatBucketCount  = 4
	boundaryHeatBucketBlocks = 64
)

// Bucket components are temporary sketch estimates and global decay factors.
// Residents retain only a scalar value and its last heat-update height.
type boundaryFrequency [boundaryHeatBucketCount]float64

func (v boundaryFrequency) score(factors boundaryFrequency) (sum float64) {
	for i, x := range v {
		sum += x * factors[i]
	}
	return
}

type residentHeat struct {
	Value  float64
	Height uint64 // last heat update, independent of the durable node version
}

func (v residentHeat) score(height, halfLife uint64) float64 {
	if v.Value == 0 || height <= v.Height {
		return v.Value
	}
	return v.Value * math.Exp2(-float64(height-v.Height)/float64(halfLife))
}

func (v *residentHeat) event(height, halfLife uint64) {
	v.Value = v.score(height, halfLife) + 1
	v.Height = height
}

func (v residentHeat) boundedBy(parent residentHeat, height, halfLife uint64) bool {
	value, limit := v.score(height, halfLife), parent.score(height, halfLife)
	if value <= limit {
		return true
	}
	// Equal mathematical scores can differ by a few ULPs after decay from
	// different anchors. Do not reject a valid commit for that rounding alone.
	return value-limit <= 8*(math.Nextafter(limit, math.Inf(1))-limit)
}

func (v *residentHeat) clip(parent residentHeat, height, halfLife uint64) bool {
	limit := parent.score(height, halfLife)
	if v.score(height, halfLife) <= limit {
		return false
	}
	*v = residentHeat{Value: limit, Height: height}
	return true
}

type BoundaryHeatBucketStats struct {
	Epoch, Anchor uint64
	Valid         bool
}
type boundaryHeat struct {
	Buckets   [boundaryHeatBucketCount]BoundaryHeatBucketStats
	Counters  []float64
	Factors   boundaryFrequency
	Height    uint64
	Active    int
	Increment float64
	Started   bool
}

// advanceHeat observes an execution height, even for an empty block. Same-height
// retries retain visits; a backward clock is rejected before modifying state.
func (f *boundaryForest) advanceHeat(h uint64) error {
	s := &f.heat
	if h > MaxHeight || s.Started && h < s.Height {
		return ErrLimit
	}
	if s.Started && h == s.Height {
		return nil
	}
	epoch := h / boundaryHeatBucketBlocks
	size := int(f.cfg.HeatSketchRows) * int(f.cfg.HeatSketchWidth)
	for i, b := range s.Buckets {
		if !b.Valid || epoch-b.Epoch < boundaryHeatBucketCount {
			continue
		}
		t := time.Now()
		clearBoundary(s.Counters[i*size : (i+1)*size])
		s.Buckets[i] = BoundaryHeatBucketStats{}
		f.stats.HeatExpiredBuckets++
		f.stats.HeatResetBytes += uint64(size) * 8
		f.stats.HeatResetNanos += uint64(time.Since(t))
	}
	active := int(epoch % boundaryHeatBucketCount)
	if !s.Buckets[active].Valid {
		s.Buckets[active] = BoundaryHeatBucketStats{epoch, h, true}
		f.stats.HeatRotations++
	}
	s.Active, s.Height, s.Started = active, h, true
	s.Increment = math.Exp2(float64(h-s.Buckets[active].Anchor) / float64(f.cfg.HalfLife))
	for i, b := range s.Buckets {
		s.Factors[i] = 0
		if b.Valid {
			s.Factors[i] = math.Exp2(-float64(h-b.Anchor) / float64(f.cfg.HalfLife))
		}
	}
	return nil
}

// Columns are class-separated; namespace, nibble length and every path nibble
// participate in the hash. No per-key identity or linked collision chain exists.
func (f *boundaryForest) heatColumns(pos forestPosition) (cols [16]int) {
	half := uint64(f.cfg.HeatSketchWidth / 2)
	key := positionHash(pos.ID, pos.Path[:pos.Length])
	for row := 0; row < int(f.cfg.HeatSketchRows); row++ {
		col := mix(key+uint64(row+1)*0x9e3779b97f4a7c15) % half
		cols[row] = int(uint64(cacheClass(pos.ID))*half + col)
	}
	return
}
func (f *boundaryForest) heatColumn(pos forestPosition, row int) int { return f.heatColumns(pos)[row] }

func (f *boundaryForest) event(pos forestPosition, h uint64) {
	// Real observers are scoped to BeginBlock's height; this also permits direct
	// pure tests to feed a monotonically increasing trace without a durable store.
	if f.advanceHeat(h) != nil {
		return
	}
	f.stats.NodeAccessEvents++
	rows, width := int(f.cfg.HeatSketchRows), int(f.cfg.HeatSketchWidth)
	cols := f.heatColumns(pos)
	for row := 0; row < rows; row++ {
		i := (f.heat.Active*rows+row)*width + cols[row]
		f.heat.Counters[i] += f.heat.Increment
	}
	if i := f.current.find(pos); i >= 0 {
		f.current.eventHeat(i, h, f.cfg.HalfLife)
	}
	f.stats.HeatUpdates++
}
func (f *boundaryForest) sketchHeat(pos forestPosition) (v boundaryFrequency) {
	f.stats.HeatQueries++
	rows, width := int(f.cfg.HeatSketchRows), int(f.cfg.HeatSketchWidth)
	cols := f.heatColumns(pos)
	for b, meta := range f.heat.Buckets {
		if !meta.Valid {
			continue
		}
		best := math.Inf(1)
		for row := 0; row < rows; row++ {
			x := f.heat.Counters[(b*rows+row)*width+cols[row]]
			if x < best {
				best = x
			}
		}
		v[b] = best
	}
	return
}
func (f *boundaryForest) initialHeat(pos forestPosition) residentHeat {
	return residentHeat{Value: f.sketchHeat(pos).score(f.heat.Factors), Height: f.heat.Height}
}

func (f *boundaryForest) count(pos forestPosition, h uint64) float64 {
	// Pure lookahead does not mutate resident values or resurrect sketch buckets.
	if !f.heat.Started || h < f.heat.Height {
		return 0
	}
	if i := f.current.find(pos); i >= 0 {
		return f.current.heatAt(i).score(h, f.cfg.HalfLife)
	}
	factors := f.heat.Factors
	if h != f.heat.Height {
		for i, b := range f.heat.Buckets {
			factors[i] = 0
			if b.Valid && h/boundaryHeatBucketBlocks-b.Epoch < boundaryHeatBucketCount {
				factors[i] = math.Exp2(-float64(h-b.Anchor) / float64(f.cfg.HalfLife))
			}
		}
	}
	return f.sketchHeat(pos).score(factors)
}
