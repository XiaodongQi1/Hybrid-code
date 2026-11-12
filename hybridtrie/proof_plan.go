package hybridtrie

import (
	"bytes"
	"context"
	"errors"
	"math/bits"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// ParallelProofConfig is a runtime query policy, not a database format option.
// Workers=0 uses the same planned walk with foreground sibling reads.
type ParallelProofConfig struct {
	Enabled                     bool
	Workers, MinBackgroundReads uint32
	MaxContextBytes             uint64
}

func DefaultParallelProofConfig() ParallelProofConfig {
	return ParallelProofConfig{true, 2, 4, 256 << 10}
}
func (c ParallelProofConfig) Validate() error {
	if !c.Enabled {
		return nil
	}
	if (c.Workers != 0 && c.Workers != 1 && c.Workers != 2 && c.Workers != 4) || c.MinBackgroundReads < 1 || c.MinBackgroundReads > 16 || c.MaxContextBytes < 4096 || c.MaxContextBytes > 128<<20 {
		return ErrLimit
	}
	return nil
}

// ParallelProofStats counts logical record reads, not device I/O. Context bytes
// include owned frames/results/queue buffers but exclude allocator bookkeeping,
// goroutine stacks, channel headers and native engine memory. TemporaryBytes
// is a foreground conversion/encoding peak; MaxReadBufferBytes is per reader.
type ParallelProofStats struct {
	FallbackPrefixReads                                                  uint64
	DirectoryReads, LifecycleReads                                       uint64
	PrefixReads, HashReads, SiblingHeaderReads, BodyDecodes              uint64
	LogicalReadBytes, CopiedReadBytes, ReadNodes                         uint64
	WorkerStarts, PrefixIteratorCreates, SubmittedGroups, DeferredGroups uint64
	BackgroundReads, ForegroundHeaderReads                               uint64
	EncodedNodes, ProofNodes, ProofBytes                                 uint64
	MainWalkNanos, ForegroundReadNanos, WaitNanos, AssembleNanos         uint64
	PeakContextBytes, PeakTemporaryBytes, MaxReadBufferBytes             uint64
	SynchronousFallback                                                  bool
}

type plannedProofItem struct {
	raw   []byte
	frame *plannedProofFrame
}
type plannedProof struct {
	id               uint64
	root             rootLocation
	key              [MaxDepth]byte
	keyLen           int
	walked           bool
	items            [MaxDepth + 1]plannedProofItem
	count            int
	value            []byte
	exists           bool
	nodes, readBytes atomic.Uint64
}
type plannedProofFrame struct {
	plan   *plannedProof
	fields mpt.CanonicalNode
	// Prefix before a branch slot, or complete extension-child prefix.
	base      [32]byte
	depth     uint8
	remaining uint16 // foreground-owned; submitted groups never read this field
}
type proofReadCounters struct {
	prefix, hash, headers, copied, logical, nodes, maxBuffer uint64
}
type proofReadWorker struct {
	session *ProofSession
	cursor  diskPredecessor
	native  bool
	key     [nodePositionHeaderBytes + 32 + HeightBytes]byte
	path    [MaxDepth]byte
	stats   proofReadCounters
}
type proofSiblingTask struct {
	frame *plannedProofFrame
	mask  uint16
}

// ProofSession has one foreground owner. Walk returns a terminal value before
// sibling completion. Finish returns owned compact proofs in Root/Walk order.
// Close must be deferred immediately; it joins readers before releasing View.
type ProofSession struct {
	view           *View
	ctx            context.Context
	cfg            ParallelProofConfig
	storeCfg       Config
	plans          [2]*plannedProof
	count          int
	main           proofReadWorker
	workers        [4]proofReadWorker
	started        int
	queue          chan proofSiblingTask
	wg             sync.WaitGroup
	failure        atomic.Pointer[proofSessionError]
	closed, joined bool
	pending        uint64
	retained       uint64
	stats          ParallelProofStats
}
type proofSessionError struct{ err error }

func (v *View) BeginProofSession(ctx context.Context, cfg ParallelProofConfig) (*ProofSession, error) {
	if ctx == nil || !cfg.Enabled {
		return nil, ErrConflict
	}
	if e := cfg.Validate(); e != nil {
		return nil, e
	}
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	v.mu.RLock()
	if v.closed {
		v.mu.RUnlock()
		return nil, ErrClosed
	}
	s := &ProofSession{view: v, ctx: ctx, cfg: cfg, storeCfg: v.store.cfg}
	s.main.session = s
	if _, ok := v.snapshot.(predecessorSource); !ok && cfg.Workers != 0 {
		s.cfg.Workers = 0
		s.stats.SynchronousFallback = true
	}
	if e := s.retain(uint64(unsafe.Sizeof(*s))); e != nil {
		v.mu.RUnlock()
		return nil, e
	}
	return s, nil
}
func (s *ProofSession) check() error {
	if s.closed {
		return ErrClosed
	}
	if f := s.failure.Load(); f != nil {
		return f.err
	}
	return s.ctx.Err()
}
func (s *ProofSession) fail(e error) error {
	if e != nil {
		s.failure.CompareAndSwap(nil, &proofSessionError{e})
	}
	return e
}
func (s *ProofSession) retain(n uint64) error {
	if n > s.cfg.MaxContextBytes || s.retained > s.cfg.MaxContextBytes-n {
		return s.fail(ErrLimit)
	}
	s.retained += n
	if s.retained > s.stats.PeakContextBytes {
		s.stats.PeakContextBytes = s.retained
	}
	return nil
}
func (s *ProofSession) temporary(n uint64) {
	if n > s.stats.PeakTemporaryBytes {
		s.stats.PeakTemporaryBytes = n
	}
}
func (s *ProofSession) plan(id uint64) (*plannedProof, error) {
	if e := s.check(); e != nil {
		return nil, e
	}
	if s.joined {
		return nil, ErrClosed
	}
	for i := 0; i < s.count; i++ {
		if s.plans[i].id == id {
			return s.plans[i], nil
		}
	}
	if s.count == len(s.plans) {
		return nil, s.fail(ErrLimit)
	}
	if e := s.retain(uint64(unsafe.Sizeof(plannedProof{}))); e != nil {
		return nil, e
	}
	s.stats.DirectoryReads++
	r, e := readRootLocation(s.view.snapshot, id, s.view.height)
	if e != nil {
		return nil, s.fail(e)
	}
	p := &plannedProof{id: id, root: r}
	s.plans[s.count] = p
	s.count++
	return p, nil
}

// Root reuses the same directory entry that Walk uses for source routing.
func (s *ProofSession) Root(id uint64) (mpt.Hash, error) {
	p, e := s.plan(id)
	if e != nil {
		return mpt.Hash{}, e
	}
	return p.root.Hash, nil
}
func (s *ProofSession) StorageTrieID(address [20]byte) (uint64, bool, error) {
	if e := s.check(); e != nil {
		return 0, false, e
	}
	s.stats.LifecycleReads++
	_, l, e := readStorageLifecycle(s.view.snapshot, address, s.view.height)
	if errors.Is(e, ErrNotFound) {
		return 0, false, nil
	}
	return l.TrieID, l.Active, s.fail(e)
}
func (s *ProofSession) Code(hash mpt.Hash) ([]byte, error) {
	if e := s.check(); e != nil {
		return nil, e
	}
	r, e := readCode(s.view.snapshot, hash)
	if e == nil {
		e = s.retain(uint64(cap(r)))
	}
	return r, s.fail(e)
}

// Walk never waits for siblings. The returned value is immutable and remains
// owned by this session until Finish transfers its result to the caller.
func (s *ProofSession) Walk(id uint64, expected mpt.Hash, key []byte) (result mpt.ProofResult, err error) {
	start := time.Now()
	defer func() {
		s.stats.MainWalkNanos += uint64(time.Since(start))
		if err != nil {
			s.fail(err)
		}
	}()
	p, e := s.plan(id)
	if e != nil {
		return result, e
	}
	if len(key) > 32 {
		return result, ErrLimit
	}
	if p.root.Hash != expected {
		return result, ErrHashMismatch
	}
	if p.walked {
		return result, ErrConflict
	}
	p.walked = true
	p.keyLen = 2 * len(key)
	for i, b := range key {
		p.key[2*i] = b >> 4
		p.key[2*i+1] = b & 15
	}
	if expected == mpt.EmptyRootHash {
		return result, nil
	}
	depth := 0
	source, want := p.root.Source, expected
	var inline []byte
	var parent *plannedProofFrame
	parentSlot := 0
	for steps := 0; steps <= MaxDepth; steps++ {
		if e := s.check(); e != nil {
			return result, e
		}
		ctx := NodeKey{TrieID: id, Path: p.key[:depth], Height: s.view.height}
		var fields mpt.CanonicalNode
		var prefixMask uint16
		var raw, fragment []byte
		var own mpt.Hash
		switch source {
		case Prefix:
			raw, e = s.main.prefix(p, ctx, false)
			if e != nil {
				return result, e
			}
			if uint64(len(raw)) > s.cfg.MaxContextBytes-s.retained {
				return result, ErrLimit
			}
			r, e := DecodeRecord(raw)
			if e != nil {
				return result, e
			}
			if e = r.validatePlacement(ctx); e != nil {
				return result, e
			}
			s.stats.BodyDecodes++
			own = r.Hash
			fragment = r.Fragment
			fields.Kind, fields.Value = r.Kind, r.Value
			if r.Kind != mpt.CanonicalBranch {
				fields.Path, e = mpt.EncodeCompactPath(r.Fragment, r.Kind == mpt.CanonicalLeaf)
				if e != nil {
					return result, e
				}
			}
			for slot, ref := range r.Children {
				switch ref.Kind {
				case Hash:
					fields.Children[slot] = mpt.CanonicalReference{Present: true, Hash: ref.Hash}
				case Embed:
					fields.Children[slot] = mpt.CanonicalReference{Present: true, Inline: ref.Inline}
				case Prefix:
					fields.Children[slot].Present = true
					prefixMask |= 1 << slot
				}
			}
			s.temporary(uint64(len(raw)) + recordPayload(r) + uint64(unsafe.Sizeof(r)))
			raw = nil
		case Hash:
			raw, e = s.main.hash(p, want)
			if e != nil {
				return result, e
			}
			if depth > 0 && len(raw) < 32 {
				return result, ErrCorrupt
			}
			if e = s.retain(uint64(cap(raw))); e != nil {
				return result, e
			}
			fields, e = mpt.DecodeLocalNodeFields(raw)
			if e != nil {
				return result, e
			}
			s.stats.BodyDecodes++
			own = want
		case Embed:
			fields, e = mpt.DecodeLocalNodeFields(inline)
			if e != nil {
				return result, e
			}
		default:
			return result, ErrCorrupt
		}
		if parent != nil {
			parent.fields.Children[parentSlot].Hash = own
			parent = nil
		}
		if source == Prefix && depth == 0 && own != expected {
			return result, ErrHashMismatch
		}
		if source != Prefix && fields.Kind != mpt.CanonicalBranch {
			fragment, _, e = mpt.DecodeCompactPath(fields.Path)
			if e != nil {
				return result, e
			}
		}
		if depth+len(fragment) > MaxDepth {
			return result, ErrCorrupt
		}
		slot, nextDepth := -1, depth
		terminal := false
		switch fields.Kind {
		case mpt.CanonicalBranch:
			if depth == MaxDepth {
				for _, c := range fields.Children {
					if c.Present {
						return result, ErrCorrupt
					}
				}
			}
			if depth == p.keyLen {
				p.value = fields.Value
				p.exists = len(p.value) != 0
				terminal = true
			} else {
				slot = int(p.key[depth])
				nextDepth = depth + 1
				if !fields.Children[slot].Present {
					terminal = true
					slot = -1
				}
			}
		case mpt.CanonicalExtension, mpt.CanonicalLeaf:
			if !bytes.HasPrefix(p.key[depth:p.keyLen], fragment) {
				terminal = true
			} else {
				nextDepth = depth + len(fragment)
				if fields.Kind == mpt.CanonicalLeaf {
					terminal = true
					if nextDepth == p.keyLen {
						p.value = fields.Value
						p.exists = true
					}
				} else {
					slot = 0
				}
			}
		default:
			return result, ErrCorrupt
		}
		var frame *plannedProofFrame
		if source == Prefix {
			n := uint64(unsafe.Sizeof(plannedProofFrame{})) + uint64(cap(fields.Path)+cap(fields.Value))
			for _, r := range fields.Children {
				n += uint64(cap(r.Inline))
			}
			if e = s.retain(n); e != nil {
				return result, e
			}
			frame = &plannedProofFrame{plan: p, fields: fields, remaining: prefixMask}
			base := ctx.Path
			var extended [MaxDepth]byte
			if fields.Kind == mpt.CanonicalExtension {
				copy(extended[:], base)
				copy(extended[len(base):], fragment)
				base = extended[:len(base)+len(fragment)]
			}
			frame.depth = uint8(len(base))
			for i, n := range base {
				frame.base[i/2] |= n << uint(4*(1-i%2))
			}
			if slot >= 0 {
				frame.remaining &^= 1 << slot
			}
		}
		if source != Embed {
			if p.count >= len(p.items) {
				return result, ErrLimit
			}
			p.items[p.count] = plannedProofItem{raw: raw, frame: frame}
			p.count++
		}
		// Save routing before publishing frame fields to concurrent writers.
		var next mpt.CanonicalReference
		if slot >= 0 {
			next = fields.Children[slot]
		}
		if frame != nil {
			if e = s.schedule(frame); e != nil {
				return result, e
			}
		}
		if terminal {
			return mpt.ProofResult{Value: p.value, Exists: p.exists}, nil
		}
		if slot < 0 || !next.Present {
			return result, ErrCorrupt
		}
		if prefixMask&(1<<slot) != 0 {
			source = Prefix
			parent, parentSlot = frame, slot
		} else if len(next.Inline) > 0 {
			source = Embed
			inline = next.Inline
		} else {
			source = Hash
			want = next.Hash
		}
		depth = nextDepth
	}
	return result, ErrLimit
}

func (s *ProofSession) Finish() (out []mpt.ProofResult, stats ParallelProofStats, err error) {
	defer func() {
		if err != nil {
			s.fail(err)
			out = nil
		}
		s.Close()
		stats = s.stats
	}()
	if e := s.check(); e != nil {
		return nil, stats, e
	}
	if s.joined {
		return nil, stats, ErrClosed
	}
	readStart := time.Now()
	for i := 0; i < s.count; i++ {
		p := s.plans[i]
		if !p.walked {
			return nil, stats, ErrConflict
		}
		for j := 0; j < p.count; j++ {
			if f := p.items[j].frame; f != nil && f.remaining != 0 {
				if e := s.main.siblings(proofSiblingTask{f, f.remaining}); e != nil {
					return nil, stats, e
				}
				f.remaining = 0
			}
		}
	}
	// Descent has finished, so the foreground can use its existing cursor for
	// queued siblings too. A channel receive transfers exclusive ownership of
	// the group; it never repeats work already claimed by a background reader.
	// Only the foreground closes the queue, in join below. An empty queue means
	// the remaining groups are already in flight, and must be joined normally.
drain:
	for {
		select {
		case task := <-s.queue:
			if e := s.main.siblings(task); e != nil {
				return nil, stats, e
			}
		default:
			break drain
		}
	}
	s.stats.ForegroundReadNanos += uint64(time.Since(readStart))
	s.join()
	if e := s.check(); e != nil {
		return nil, stats, e
	}
	start := time.Now()
	defer func() { s.stats.AssembleNanos += uint64(time.Since(start)) }()
	if e := s.retain(uint64(s.count) * uint64(unsafe.Sizeof(mpt.ProofResult{}))); e != nil {
		return nil, stats, e
	}
	out = make([]mpt.ProofResult, s.count)
	for i := 0; i < s.count; i++ {
		p := s.plans[i]
		if e := s.retain(uint64(p.count) * uint64(unsafe.Sizeof([]byte{}))); e != nil {
			return nil, stats, e
		}
		r := mpt.ProofResult{Nodes: make(mpt.Proof, 0, p.count), Value: p.value, Exists: p.exists}
		for j := 0; j < p.count; j++ {
			item := &p.items[j]
			raw := item.raw
			if item.frame != nil {
				n := item.frame.fields.LocalRLPSize()
				if n == 0 || n > mpt.MaxCanonicalNodeBytes || j > 0 && n < 32 {
					return nil, stats, ErrCorrupt
				}
				if e := s.retain(uint64(n)); e != nil {
					return nil, stats, e
				}
				var e error
				raw, e = item.frame.fields.LocalRLP()
				if e != nil {
					return nil, stats, e
				}
				s.temporary(uint64(n) * 2)
				s.stats.EncodedNodes++
			}
			r.Nodes = append(r.Nodes, raw)
			s.stats.ProofNodes++
			s.stats.ProofBytes += uint64(len(raw))
		}
		out[i] = r
	}
	return out, stats, nil
}

// Close is single-foreground-owner and idempotent; it also handles partial Walk.
func (s *ProofSession) Close() {
	if s.closed {
		return
	}
	if !s.joined {
		s.fail(context.Canceled)
	}
	s.join()
	for i := -1; i < s.started; i++ {
		w := &s.main
		if i >= 0 {
			w = &s.workers[i]
		}
		if w.cursor != nil {
			w.cursor.close()
			w.cursor = nil
			if w.native {
				s.stats.PrefixIteratorCreates++
			} else {
				s.stats.FallbackPrefixReads += w.stats.prefix
			}
		}
		c := w.stats
		s.stats.PrefixReads += c.prefix
		s.stats.HashReads += c.hash
		s.stats.SiblingHeaderReads += c.headers
		s.stats.ReadNodes += c.nodes
		s.stats.LogicalReadBytes += c.logical
		s.stats.CopiedReadBytes += c.copied
		if c.maxBuffer > s.stats.MaxReadBufferBytes {
			s.stats.MaxReadBufferBytes = c.maxBuffer
		}
		if i < 0 {
			s.stats.ForegroundHeaderReads = c.headers
		} else {
			s.stats.BackgroundReads += c.headers
		}
	}
	s.plans = [2]*plannedProof{}
	s.queue = nil
	s.closed = true
	s.view.mu.RUnlock()
}
func (s *ProofSession) Stats() ParallelProofStats { return s.stats } // after Finish/Close

func (p *plannedProof) reserve(limit uint64) error {
	for {
		n := p.nodes.Load()
		if n >= limit {
			return ErrLimit
		}
		if p.nodes.CompareAndSwap(n, n+1) {
			return nil
		}
	}
}
func (w *proofReadWorker) charge(p *plannedProof, keyBytes, size, copied int) error {
	if size < 0 || size > MaxRecordBytes {
		return ErrLimit
	}
	n := uint64(keyBytes + size)
	w.stats.nodes++
	w.stats.logical += n
	w.stats.copied += uint64(keyBytes + copied)
	if uint64(keyBytes+copied) > w.stats.maxBuffer {
		w.stats.maxBuffer = uint64(keyBytes + copied)
	}
	if p.readBytes.Add(n) > w.session.storeCfg.MaxReadBytes {
		return ErrLimit
	}
	return nil
}
func (w *proofReadWorker) prefix(p *plannedProof, ctx NodeKey, header bool) ([]byte, error) {
	s := w.session
	if e := s.check(); e != nil {
		return nil, e
	}
	if e := p.reserve(s.storeCfg.MaxReadNodes); e != nil {
		return nil, e
	}
	if w.cursor == nil {
		var e error
		w.cursor, w.native, e = openPredecessor(s.view.snapshot, prefixColumn)
		if e != nil {
			return nil, e
		}
	}
	target, e := appendNodeKey(w.key[:0], ctx)
	if e != nil {
		return nil, e
	}
	var found, raw []byte
	var size int
	w.stats.prefix++
	if header {
		found, raw, size, e = readLimited(w.cursor, target, recordHeaderBytes)
	} else {
		found, raw, e = w.cursor.prev(target)
		size = len(raw)
	}
	if e != nil {
		return nil, e
	}
	if e = w.charge(p, len(found), size, len(raw)); e != nil {
		return nil, e
	}
	if e = matchEncodedNodeKey(found, target); e != nil {
		return nil, ErrNotFound
	}
	if header {
		if len(raw) != recordHeaderBytes {
			return nil, ErrCorrupt
		}
		if _, e = recordKind(raw[0]); e != nil {
			return nil, e
		}
		w.stats.headers++
	}
	return raw, nil
}
func (w *proofReadWorker) hash(p *plannedProof, h mpt.Hash) ([]byte, error) {
	s := w.session
	if e := s.check(); e != nil {
		return nil, e
	}
	if e := p.reserve(s.storeCfg.MaxReadNodes); e != nil {
		return nil, e
	}
	w.stats.hash++
	raw, e := s.view.snapshot.get(hashColumn, h[:])
	if e != nil {
		return nil, e
	}
	if e = w.charge(p, 32, len(raw), len(raw)); e != nil {
		return nil, e
	}
	return raw, nil
}
func (w *proofReadWorker) siblings(t proofSiblingTask) error {
	f := t.frame
	n := int(f.depth)
	for i := 0; i < n; i++ {
		w.path[i] = (f.base[i/2] >> uint(4*(1-i%2))) & 15
	}
	for mask := t.mask; mask != 0; {
		slot := bits.TrailingZeros16(mask)
		mask &^= 1 << slot
		length := n
		if f.fields.Kind == mpt.CanonicalBranch {
			if n >= MaxDepth {
				return ErrCorrupt
			}
			w.path[n] = byte(slot)
			length++
		}
		raw, e := w.prefix(f.plan, NodeKey{f.plan.id, w.path[:length], w.session.view.height}, true)
		if e != nil {
			return e
		}
		copy(f.fields.Children[slot].Hash[:], raw[1:])
	}
	return nil
}
