package hybridtrie

import (
	"bytes"
	"sort"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"unsafe"
)

func (f *forestShared) spend(n uint64) bool {
	if n > f.remaining {
		f.remaining = 0
		return false
	}
	f.remaining -= n
	f.stats.WorkUnits += n
	return true
}
func (f *forestShared) rng(n uint64) uint64 {
	x := f.random
	x ^= x << 13
	x ^= x >> 7
	x ^= x << 17
	f.random = x
	return x % n
}
func (f *forestShared) addRoot(id uint64, hash mpt.Hash) {
	if hash == mpt.EmptyRootHash {
		return
	}
	for j := 0; j < f.rootN; j++ {
		i := f.roots[j]
		if f.graph[i].Pos.ID == id {
			return
		}
	}
	if f.count == len(f.graph) {
		f.stats.DroppedCandidates++
		return
	}
	i := f.count
	f.count++
	raw, v := f.body(position(id, nil), hash)
	e := forestPlanNode{Pos: position(id, nil), Hash: hash, Body: raw, Version: v, Parent: -1, RootSlot: int32(f.rootN), Allocation: f.nextAllocation()}
	for j := range e.Children {
		e.Children[j] = -1
	}
	f.configure(&e)
	f.graph[i] = e
	f.roots[f.rootN] = int32(i)
	f.rootN++
}
func (f *forestShared) configure(e *forestPlanNode) {
	e.A = initialFrequency(f.heat.sketch[cacheClass(e.Pos.ID)].estimate(e.Pos.ID, e.Pos.Path[:e.Pos.Length]), f.heat.height)
	old := f.current.node(e.Pos)
	reset := false
	for i := f.observed.N - 1; i >= 0; i-- {
		o := f.observed.Entries[i]
		if o.Pos == e.Pos && o.Hash == e.Hash {
			reset = o.Reset
			if o.Updated {
				e.U = initialFrequency(1, f.heat.height)
			}
			break
		}
	}
	if old >= 0 && !reset {
		o := f.current.Nodes[old]
		e.A = o.A
		if e.U.Set {
			e.U = o.U
			e.U.event(f.heat.height, f.cfg.HalfLife)
		} else {
			e.U = o.U
		}
		if o.Hash == e.Hash {
			e.Allocation = o.Allocation
			e.State = nodeState
		}
		// A changed logical version can retain N residency and heat if its host
		// identity is stable. Its new body must have been observed normally.
		if len(e.Body) > 0 {
			a, ae := mpt.InspectNodeIdentity(f.current.raw(old))
			b, be := mpt.InspectNodeIdentity(e.Body)
			if ae == nil && be == nil && a == b {
				e.State = nodeState
			} else {
				e.State = diskState
				e.A = initialFrequency(1, f.heat.height)
				e.U = initialFrequency(1, f.heat.height)
			}
		}
	}
	if i := f.current.hash(e.Pos); i >= 0 && !reset && f.current.Hashes[i].Hash == e.Hash {
		e.State = hashState
		e.Allocation = f.current.Hashes[i].Allocation
		if !e.Version.Known {
			e.Version = f.current.Hashes[i].Version
		}
	}
	if len(e.Body) == 0 && e.State == nodeState {
		e.State = diskState
	}
}
func (f *forestShared) graphLookup(pos forestPosition) int {
	n := len(f.index)
	start := int(positionHash(pos.ID, pos.Path[:pos.Length]) % uint64(n))
	for k := 0; k < n; k++ {
		h := f.index[(start+k)%n]
		if h == 0 {
			return -1
		}
		if f.graph[h-1].Pos == pos {
			return int(h - 1)
		}
	}
	return -1
}
func (f *forestShared) indexGraph() {
	for i := range f.index {
		f.index[i] = 0
	}
	n := len(f.index)
	for i := 0; i < f.count; i++ {
		pos := f.graph[i].Pos
		start := int(positionHash(pos.ID, pos.Path[:pos.Length]) % uint64(n))
		for k := 0; k < n; k++ {
			j := (start + k) % n
			if f.index[j] == 0 {
				f.index[j] = int32(i + 1)
				break
			}
		}
	}
}
func (f *forestShared) expand() error {
	for i := 0; i < f.count; i++ {
		e := &f.graph[i]
		if len(e.Body) == 0 {
			continue
		}
		d, err := mpt.InspectCanonicalNode(e.Body)
		if err != nil {
			return err
		}
		for slot, ref := range d.Children {
			if !ref.Present || len(ref.Inline) != 0 {
				continue
			}
			var path [64]byte
			copy(path[:], e.Pos.Path[:e.Pos.Length])
			length := int(e.Pos.Length)
			if d.Kind == mpt.CanonicalExtension {
				frag, _, err := mpt.DecodeCompactPath(d.Path)
				if err != nil || length+len(frag) > 64 {
					return ErrCorrupt
				}
				copy(path[length:], frag)
				length += len(frag)
			} else {
				if length == 64 {
					return ErrCorrupt
				}
				path[length] = byte(slot)
				length++
			}
			if f.count == len(f.graph) {
				f.stats.DroppedCandidates++
				continue
			}
			pos := forestPosition{ID: e.Pos.ID, Path: path, Length: uint8(length)}
			raw, v := f.body(pos, ref.Hash)
			x := forestPlanNode{Pos: pos, Hash: ref.Hash, Body: raw, Version: v, Parent: int32(i), RootSlot: e.RootSlot, Allocation: f.nextAllocation()}
			for j := range x.Children {
				x.Children[j] = -1
			}
			f.configure(&x)
			// Only retained/observed canonical hosts are expanded; no disk prefetch.
			if e.State != nodeState {
				x.State = diskState
			}
			e.Children[slot] = int32(f.count)
			f.graph[f.count] = x
			f.count++
		}
	}
	return nil
}
func (f *forestShared) usage() (n, h [2]uint64, nc, hc, body, tries int) {
	return f.nUsed, f.hUsed, f.nCount, f.hCount, f.bodyUsed, f.trieCount
}
func (f *forestShared) value(i int) float64 {
	e := f.graph[i]
	switch e.State {
	case nodeState:
		return e.ScoreN
	case hashState:
		if e.Parent >= 0 {
			return e.ScoreH
		}
	}
	return 0
}
func (f *forestShared) frontier(i int) bool {
	e := f.graph[i]
	if e.State != nodeState || e.Pos.ID == 0 && e.Parent < 0 {
		return false
	}
	for _, ch := range e.Children {
		if ch >= 0 && f.graph[ch].State == nodeState {
			return false
		}
	}
	return true
}
func (f *forestShared) protected(v, x int) bool {
	a, b := f.graph[v].Pos, f.graph[x].Pos
	return a.ID == b.ID && bytes.HasPrefix(b.Path[:b.Length], a.Path[:a.Length])
}
func (f *forestShared) change(i int, state uint8) bool {
	if f.undoN == len(f.undo) || !f.spend(3) {
		return false
	}
	f.undo[f.undoN] = forestUndo{int32(i), f.graph[i].State}
	f.undoN++
	f.apply(i, state)
	return true
}
func (f *forestShared) rollback(start int) {
	for f.undoN > start {
		f.undoN--
		u := f.undo[f.undoN]
		f.apply(int(u.Index), u.State)
	}
	f.stats.Rollbacks++
}
func (f *forestShared) release(i int) (float64, bool) {
	e := &f.graph[i]
	loss := f.value(i)
	if e.State == nodeState {
		for _, ch := range e.Children {
			if ch >= 0 && f.graph[ch].State == hashState {
				loss += f.value(int(ch))
				if !f.change(int(ch), diskState) {
					return 0, false
				}
			}
		}
	}
	return loss, f.change(i, diskState)
}

// Four Fenwick indexes isolate N/H and account/storage. State changes and
// rollback update only one trie's weight in O(log number of root candidates).
func (f *forestShared) weightDelta(slot int, resource, class int, delta int64) {
	off := (resource*2 + class) * len(f.roots)
	for k := slot + 1; k <= f.rootN; k += k & -k {
		if delta < 0 {
			f.fenwick[off+k-1] -= uint64(-delta)
		} else {
			f.fenwick[off+k-1] += uint64(delta)
		}
	}
}
func (f *forestShared) weightTotal(resource, class int) uint64 {
	off := (resource*2 + class) * len(f.roots)
	var n uint64
	for k := f.rootN; k > 0; k -= k & -k {
		n += f.fenwick[off+k-1]
	}
	return n
}
func (f *forestShared) apply(i int, state uint8) {
	e := &f.graph[i]
	c := cacheClass(e.Pos.ID)
	ns := uint64(unsafe.Sizeof(forestN{})) + uint64(len(e.Body))
	hs := uint64(unsafe.Sizeof(forestH{}))
	switch e.State {
	case nodeState:
		f.nUsed[c] -= ns
		f.nCount--
		f.bodyUsed -= len(e.Body)
		if e.Parent < 0 {
			f.trieCount--
		}
		f.weightDelta(int(e.RootSlot), 0, c, -int64(ns))
	case hashState:
		f.hUsed[c] -= hs
		f.hCount--
		f.weightDelta(int(e.RootSlot), 1, c, -int64(hs))
	}
	e.State = state
	switch state {
	case nodeState:
		f.nUsed[c] += ns
		f.nCount++
		f.bodyUsed += len(e.Body)
		if e.Parent < 0 {
			f.trieCount++
		}
		f.weightDelta(int(e.RootSlot), 0, c, int64(ns))
	case hashState:
		f.hUsed[c] += hs
		f.hCount++
		f.weightDelta(int(e.RootSlot), 1, c, int64(hs))
	}
}
func (f *forestShared) initializeUsage() {
	f.nUsed, f.hUsed = [2]uint64{}, [2]uint64{}
	f.nCount, f.hCount, f.bodyUsed, f.trieCount = 0, 0, 0, 0
	for i := range f.fenwick {
		f.fenwick[i] = 0
	}
	for i := 0; i < f.count; i++ {
		f.graph[i].ScoreN = f.graph[i].A.at(f.heat.height, f.cfg.HalfLife) * f.cfg.NodeWeight
		if f.graph[i].Parent >= 0 {
			f.graph[i].ScoreH = f.graph[f.graph[i].Parent].U.at(f.heat.height, f.cfg.HalfLife) * f.cfg.HashWeight
		}
		state := f.graph[i].State
		f.graph[i].State = diskState
		f.apply(i, state)
	}
}
func (f *forestShared) pickRoot(resource, class int, total uint64) int {
	target := f.rng(total)
	idx := 0
	bit := 1
	off := (resource*2 + class) * len(f.roots)
	for bit<<1 <= f.rootN {
		bit <<= 1
	}
	for ; bit != 0; bit >>= 1 {
		next := idx + bit
		if next <= f.rootN && f.fenwick[off+next-1] <= target {
			target -= f.fenwick[off+next-1]
			idx = next
		}
	}
	return int(f.roots[idx])
}
func (f *forestShared) victim(node bool, x int, beforeN, beforeH [2]uint64) int {
	resource := 1
	if node {
		resource = 0
	}
	request := cacheClass(f.graph[x].Pos.ID)
	var totals [2]uint64
	for class := 0; class < 2; class++ {
		used, reserve := beforeH[class], f.cfg.HashBudget.reserve(class)
		if node {
			used, reserve = beforeN[class], f.cfg.NodeBudget.reserve(class)
		}
		if class == request || used > reserve {
			totals[class] = f.weightTotal(resource, class)
		}
	}
	total := totals[0] + totals[1]
	if total == 0 {
		return -1
	}
	best := -1
	density := 0.0
	consider := func(i int) {
		e := f.graph[i]
		if node {
			if !f.frontier(i) || f.protected(i, x) {
				return
			}
		} else if e.State != hashState {
			return
		}
		o := cacheClass(e.Pos.ID)
		if o != request {
			used, reserve := beforeH[o], f.cfg.HashBudget.reserve(o)
			if node {
				used, reserve = beforeN[o], f.cfg.NodeBudget.reserve(o)
			}
			if used <= reserve {
				return
			}
		}
		loss := f.value(i)
		size := float64(unsafe.Sizeof(forestH{}))
		if node {
			size = float64(uint64(unsafe.Sizeof(forestN{})) + uint64(len(e.Body)))
			for _, ch := range e.Children {
				if ch >= 0 && f.graph[ch].State == hashState {
					loss += f.value(int(ch))
				}
			}
		}
		d := loss / size
		if best < 0 || d < density || d == density && (e.Pos.ID < f.graph[best].Pos.ID || e.Pos.ID == f.graph[best].Pos.ID && lessPath(e.Pos.Path, e.Pos.Length, f.graph[best].Pos.Path, f.graph[best].Pos.Length)) {
			best, density = i, d
		}
	}
	for t := uint32(0); t < f.cfg.TreesPerAttempt; t++ {
		if !f.spend(16) {
			return best
		}
		class := 0
		if f.rng(total) >= totals[0] {
			class = 1
		}
		root := f.pickRoot(resource, class, totals[class])
		for k := uint32(0); k < f.cfg.PathsPerTrie; k++ {
			i := root
			for f.graph[i].State == nodeState {
				if !f.spend(17) {
					return best
				}
				f.stats.Samples++
				var ns, hs [16]int
				nn, hn := 0, 0
				for _, ch := range f.graph[i].Children {
					if ch < 0 {
						continue
					}
					switch f.graph[ch].State {
					case nodeState:
						ns[nn] = int(ch)
						nn++
					case hashState:
						hs[hn] = int(ch)
						hn++
					}
				}
				if !node && hn > 0 {
					consider(hs[f.rng(uint64(hn))])
				}
				if nn == 0 {
					if node {
						consider(i)
					}
					break
				}
				i = ns[f.rng(uint64(nn))]
			}
		}
	}
	return best
}
func (f *forestShared) try(i int, state uint8) bool {
	e := &f.graph[i]
	if e.State == state || state == hashState && e.State == nodeState {
		return true
	}
	if e.Parent < 0 {
		if state != nodeState || len(e.Body) == 0 {
			f.stats.RootRejects++
			return false
		}
	} else if state == hashState && f.graph[e.Parent].State != nodeState {
		return false
	}
	if state == nodeState && len(e.Body) == 0 {
		return false
	}
	beforeN, beforeH, _, _, _, _ := f.usage()
	request := cacheClass(e.Pos.ID)
	start := f.undoN
	old := e.State
	benefit := 0.0
	if state == nodeState {
		// Charge and admit the missing ancestor chain atomically. Comparing
		// only a cold root's own benefit would prevent a hot new contract from
		// entering a full descriptor set even when its entire path is useful.
		var chain [65]int
		count := 0
		for j := i; j >= 0 && f.graph[j].State != nodeState; j = int(f.graph[j].Parent) {
			if count == len(chain) || len(f.graph[j].Body) == 0 {
				return false
			}
			chain[count] = j
			count++
		}
		for j := count - 1; j >= 0; j-- {
			x := chain[j]
			lost := f.value(x)
			if !f.change(x, nodeState) {
				f.rollback(start)
				return false
			}
			benefit += f.value(x) - lost
		}
	} else {
		lost := f.value(i)
		if !f.change(i, state) {
			return false
		}
		benefit = f.value(i) - lost
	}
	loss := 0.0
	removed := uint64(0)
	reservation := beforeN[request] < f.cfg.NodeBudget.reserve(request)
	if state == hashState {
		reservation = beforeH[request] < f.cfg.HashBudget.reserve(request)
	}
	for {
		n, h, nc, hc, body, tries := f.usage()
		if planUsage(f.cfg, beforeN, beforeH, n, h, request) && nc <= len(f.scratch.Nodes) && hc <= len(f.scratch.Hashes) && body <= len(f.scratch.Body) && tries <= int(f.cfg.MaxResidentTries) {
			break
		}
		needN := !f.cfg.NodeBudget.fits(n) || nc > len(f.scratch.Nodes) || body > len(f.scratch.Body) || tries > int(f.cfg.MaxResidentTries)
		v := f.victim(needN, i, n, h)
		if v < 0 {
			if tries > int(f.cfg.MaxResidentTries) {
				f.stats.DescriptorRejects++
			}
			f.rollback(start)
			f.stats.QuotaRejects++
			return false
		}
		l, ok := f.release(v)
		if !ok {
			f.rollback(start)
			return false
		}
		loss += l
		removed++
		// Nonrequesting class floors apply to the entire N/H cascade.
		nn, hh, _, _, _, _ := f.usage()
		o := 1 - request
		floorN, floorH := beforeN[o], beforeH[o]
		if floorN > f.cfg.NodeBudget.reserve(o) {
			floorN = f.cfg.NodeBudget.reserve(o)
		}
		if floorH > f.cfg.HashBudget.reserve(o) {
			floorH = f.cfg.HashBudget.reserve(o)
		}
		if nn[o] < floorN || hh[o] < floorH {
			f.rollback(start)
			f.stats.QuotaRejects++
			return false
		}
	}
	afterN, afterH, _, _, _, _ := f.usage()
	// A request crossing its reservation also competes for its shared portion.
	if reservation {
		if state == nodeState {
			reservation = afterN[request] <= f.cfg.NodeBudget.reserve(request)
		} else {
			reservation = afterH[request] <= f.cfg.HashBudget.reserve(request)
		}
	}
	if loss > 0 && !reservation && benefit <= (1+f.cfg.Epsilon)*loss {
		f.rollback(start)
		f.stats.ScoreRejects++
		return false
	}
	demotions := 0
	if state == nodeState {
		for j := start; j < f.undoN; j++ {
			u := f.undo[j]
			if u.State == nodeState && f.graph[u.Index].State == diskState {
				f.demotions[demotions] = u.Index
				demotions++
			}
		}
	}
	f.undoN = start
	f.stats.Admissions++
	if state == nodeState && old == hashState {
		f.stats.Promotions++
	}
	f.stats.Evictions += removed
	f.stats.CascadeNodes += removed
	if removed > f.stats.MaxCascade {
		f.stats.MaxCascade = removed
	}
	if reservation && removed > 0 {
		f.stats.ReservationReclaims++
	}
	// Once the N replacement is accepted, optional H preservation competes
	// independently. It cannot roll back the successful N decision.
	for j := 0; j < demotions; j++ {
		v := int(f.demotions[j])
		if f.graph[v].Parent >= 0 && f.graph[f.graph[v].Parent].State == nodeState && f.try(v, hashState) {
			f.stats.Demotions++
		}
	}
	return true
}
func (f *forestShared) run() {
	// Repair keeps only valid root-connected old residents. Capacity expansion
	// from a larger host may require conservative cold fallback of that class.
	for i := 0; i < f.count; i++ {
		e := &f.graph[i]
		if e.Parent >= 0 && f.graph[e.Parent].State != nodeState || e.State == nodeState && len(e.Body) == 0 {
			if e.State != diskState {
				f.stats.ForcedRepairs++
			}
			e.State = diskState
		}
	}
	f.initializeUsage()
	n, h, nc, hc, body, tries := f.usage()
	if !f.cfg.NodeBudget.fits(n) || !f.cfg.HashBudget.fits(h) || nc > len(f.scratch.Nodes) || hc > len(f.scratch.Hashes) || body > len(f.scratch.Body) || tries > int(f.cfg.MaxResidentTries) {
		for i := 0; i < f.count; i++ {
			f.apply(i, diskState)
		}
		f.stats.ForcedRepairs++
	}
	for i := 0; i < f.count; i++ {
		f.order[i] = int32(i)
	}
	// Pay a conservative comparison bound before optional ordering. Budget
	// exhaustion never interrupts repair or leaves a partially accepted plan.
	log := uint64(1)
	for n := f.count; n > 1; n >>= 1 {
		log++
	}
	if !f.spend(4 * uint64(f.count) * log) {
		f.stats.BudgetExhaustions++
		return
	}
	sort.Slice(f.order[:f.count], func(a, b int) bool {
		x, y := f.graph[f.order[a]], f.graph[f.order[b]]
		if x.Pos.Length != y.Pos.Length {
			return x.Pos.Length < y.Pos.Length
		}
		if x.Pos.ID == 0 && y.Pos.ID != 0 {
			return true
		}
		if y.Pos.ID == 0 && x.Pos.ID != 0 {
			return false
		}
		sx, sy := x.ScoreN, y.ScoreN
		if sx != sy {
			return sx > sy
		}
		if x.Pos.ID != y.Pos.ID {
			return x.Pos.ID < y.Pos.ID
		}
		return lessPath(x.Pos.Path, x.Pos.Length, y.Pos.Path, y.Pos.Length)
	})
	for k := 0; k < f.count; k++ {
		if !f.spend(1) {
			break
		}
		i := int(f.order[k])
		if !f.try(i, nodeState) {
			f.try(i, hashState)
		}
	}
	if f.remaining == 0 {
		f.stats.BudgetExhaustions++
	}
}

func (f *forestShared) export() error {
	b := &f.scratch
	b.reset()
	for i := 0; i < f.count; i++ {
		e := &f.graph[i]
		switch e.State {
		case nodeState:
			if b.N == len(b.Nodes) || len(e.Body) > len(b.Body)-b.Used {
				return ErrLimit
			}
			v := forestN{Pos: e.Pos, Hash: e.Hash, A: e.A, U: e.U, Allocation: e.Allocation, Version: e.Version, Offset: uint32(b.Used), Size: uint32(len(e.Body))}
			copy(b.Body[b.Used:], e.Body)
			b.Used += len(e.Body)
			b.Nodes[b.N] = v
			b.N++
			if e.Parent < 0 {
				if b.R == len(b.Roots) {
					return ErrLimit
				}
				b.Roots[b.R] = forestRoot{ID: e.Pos.ID, Hash: e.Hash, Index: int32(b.N - 1)}
				b.R++
			}
		case hashState:
			if b.H == len(b.Hashes) {
				return ErrLimit
			}
			b.Hashes[b.H] = forestH{Pos: e.Pos, Hash: e.Hash, Allocation: e.Allocation, Version: e.Version}
			b.H++
		}
	}
	b.buildIndex()
	return nil
}
func (f *forestShared) layout(ctx NodeKey, eligible uint16) uint16 {
	i := f.graphLookup(position(ctx.TrieID, ctx.Path))
	if i < 0 || f.graph[i].State != nodeState {
		return 0
	}
	e := &f.graph[i]
	var mask uint16
	for slot, ch := range e.Children {
		if ch >= 0 && f.graph[ch].State != diskState {
			mask |= 1 << slot
		}
	}
	return mask & eligible
}
