package hybridtrie

import (
	"bytes"
	"math"
)

// PolicyConfig follows the MPT-compatible implementation plan. Zero-valued
// Policy selects defaults; an explicit zero WorkUnits disables optional work.
type PolicyConfig struct {
	HalfLife, SketchPeriod                                          uint64
	SketchWidth, EventCapacity, NodePool, HashPool, PathsPerAttempt uint32
	WorkUnits                                                       uint32
	StatsBytes                                                      uint64
	Seed                                                            uint64
	Epsilon                                                         float64
}

func DefaultPolicyConfig() PolicyConfig {
	return PolicyConfig{64, 256, 16384, 4096, 16, 8, 4, 4096, 2 << 20, 1, 0}
}
func (p PolicyConfig) valid() bool {
	return p.HalfLife > 0 && p.HalfLife <= 1<<20 && p.SketchPeriod >= 2 && p.SketchPeriod <= 65534 && p.SketchPeriod%2 == 0 && p.SketchWidth > 0 && p.SketchWidth <= 1<<20 && p.EventCapacity > 0 && p.EventCapacity <= 65536 && p.NodePool > 0 && p.NodePool <= 256 && p.HashPool > 0 && p.HashPool <= 256 && p.PathsPerAttempt <= 64 && p.WorkUnits <= 1<<20 && p.StatsBytes <= 128<<20 && !math.IsNaN(p.Epsilon) && !math.IsInf(p.Epsilon, 0) && p.Epsilon >= 0
}

type frequency struct {
	Value  float64
	Height uint64
	Set    bool
}

func (f frequency) at(h, t uint64) float64 {
	if !f.Set {
		return 0
	}
	if h < f.Height {
		return f.Value
	}
	return f.Value * math.Exp2(-float64(h-f.Height)/float64(t))
}
func (f *frequency) event(h, t uint64) {
	if f.Set && f.Height == h {
		return
	}
	f.Value = f.at(h, t) + 1
	f.Height = h
	f.Set = true
}
func initialFrequency(v float64, h uint64) frequency { return frequency{v, h, true} }

type heatEvent struct {
	Path         [64]byte
	Length       uint8
	Access, Used bool
	Flags        uint8
}

const (
	eventUpdated uint8 = 1 << iota
	eventIdentityReset
	eventReplacementAccess
	eventIdentityChecked
)

type heatStats struct {
	cfg          PolicyConfig
	counters     []uint16
	events       []heatEvent
	used         int
	height       uint64
	started      bool
	start        [2]uint64
	resets       [2]uint64
	accessEvents uint64
}

func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	return x ^ (x >> 31)
}
func positionHash(id uint64, path []byte) uint64 {
	v := mix(id) ^ mix(uint64(len(path))+0x9e3779b97f4a7c15)
	for _, n := range path {
		v = mix(v ^ uint64(n+1))
	}
	return mix(v ^ 0x9e3779b97f4a7c15)
}
func (s *heatStats) begin(h uint64) {
	if s.started && s.height == h {
		return
	}
	for i := range s.events {
		s.events[i] = heatEvent{}
	}
	s.used = 0
	for j := 0; j < 2; j++ {
		phase := uint64(j) * s.cfg.SketchPeriod / 2
		reset := uint64(0)
		if h >= phase {
			reset = phase + (h-phase)/s.cfg.SketchPeriod*s.cfg.SketchPeriod
		}
		if !s.started || reset > s.start[j] {
			off := j * 4 * int(s.cfg.SketchWidth)
			for k := off; k < off+4*int(s.cfg.SketchWidth); k++ {
				s.counters[k] = 0
			}
			s.start[j] = h
			if s.started {
				s.start[j] = reset
			}
			s.resets[j]++
		}
	}
	s.height = h
	s.started = true
}
func (s *heatStats) event(path []byte) (*heatEvent, error) {
	key := positionHash(0, path)
	n := len(s.events)
	for k := 0; k < n; k++ {
		i := int((key + uint64(k)) % uint64(n))
		e := &s.events[i]
		if e.Used {
			if pathEqual(e.Path, e.Length, path) {
				return e, nil
			}
			continue
		}
		if s.used >= int(s.cfg.EventCapacity) {
			return nil, ErrLimit
		}
		*e = heatEvent{Used: true, Length: uint8(len(path))}
		copy(e.Path[:], path)
		s.used++
		return e, nil
	}
	return nil, ErrLimit
}
func (s *heatStats) find(path []byte) *heatEvent {
	key := positionHash(0, path)
	n := len(s.events)
	for k := 0; k < n; k++ {
		e := &s.events[int((key+uint64(k))%uint64(n))]
		if !e.Used {
			return nil
		}
		if pathEqual(e.Path, e.Length, path) {
			return e
		}
	}
	return nil
}
func (s *heatStats) increment(id uint64, path []byte) {
	key := positionHash(id, path)
	width := int(s.cfg.SketchWidth)
	for j := 0; j < 2; j++ {
		for row := 0; row < 4; row++ {
			col := int(mix(key+uint64(row+1)*0x9e3779b97f4a7c15) % uint64(width))
			i := (j*4+row)*width + col
			if s.counters[i] != 65535 {
				s.counters[i]++
			}
		}
	}
}
func (s *heatStats) estimate(id uint64, path []byte) float64 {
	key := positionHash(id, path)
	width := int(s.cfg.SketchWidth)
	best := float64(0)
	r := math.Exp2(-1 / float64(s.cfg.HalfLife))
	for j := 0; j < 2; j++ {
		n := uint16(65535)
		for row := 0; row < 4; row++ {
			i := (j*4+row)*width + int(mix(key+uint64(row+1)*0x9e3779b97f4a7c15)%uint64(width))
			if s.counters[i] < n {
				n = s.counters[i]
			}
		}
		age := s.height - s.start[j] + 1
		count := float64(n)
		if count > float64(age) {
			count = float64(age)
		}
		value := count * (-math.Expm1(float64(age) * math.Log(r))) / (float64(age) * (1 - r))
		if value > best {
			best = value
		}
	}
	return best
}
func (s *heatStats) clearUpdates() {
	for i := range s.events {
		// Final-state identity/update decisions are rebuilt on commit retry.
		// Real observations and their CMS deduplication survive that retry.
		s.events[i].Flags &^= eventUpdated | eventIdentityReset | eventIdentityChecked
	}
}
func (s *heatStats) updated(path []byte) bool {
	e := s.find(path)
	return e != nil && e.Flags&eventUpdated != 0
}
func (s *heatStats) identityReset(path []byte) bool {
	e := s.find(path)
	return e != nil && e.Flags&eventIdentityReset != 0
}
func (s *heatStats) identityChecked(path []byte) bool {
	e := s.find(path)
	return e != nil && e.Flags&eventIdentityChecked != 0
}
func lessPath(a [64]byte, an uint8, b [64]byte, bn uint8) bool {
	return bytes.Compare(a[:an], b[:bn]) < 0
}
