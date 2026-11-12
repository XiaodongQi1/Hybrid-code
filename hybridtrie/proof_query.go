package hybridtrie

import (
	"bytes"
	"errors"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"unsafe"
)

// ProofStats describes one historical proof operation. Reads are logical store
// calls, never physical I/O. PeakContextBytes counts retained payload/Go headers
// in the path context, not allocator/native/RSS or the returned proof.
type ProofStats struct {
	ResolutionStats
	DirectoryReads, NodeRequests, PathReuses, NodesRebuilt         uint64
	PrefixIteratorCreates, FallbackPrefixReads                     uint64
	SiblingHeaderReads                                             uint64
	IteratorReuseEnabled                                           bool
	ReadNodes, ReadBytes, PeakContextBytes, ProofNodes, ProofBytes uint64
}

// ProveWithStats binds one root/height/namespace to one monotone MPT proof walk.
// The View lock keeps its snapshot alive through cursor cleanup. Whole-query
// read limits reuse Config.MaxReadNodes/MaxReadBytes; no latest cache is attached.
func (v *View) ProveWithStats(id uint64, trusted mpt.Hash, key []byte) (mpt.Proof, ProofStats, error) {
	r, s, e := v.ProveValueWithStats(id, trusted, key)
	return r.Nodes, s, e
}

// ProveValueWithStats decodes the requested terminal value during the bounded
// proof walk and retains the same authenticated root and read budget.
func (v *View) ProveValueWithStats(id uint64, trusted mpt.Hash, key []byte) (mpt.ProofResult, ProofStats, error) {
	return v.valueProof(id, trusted, key, mpt.ProveValueAt)
}

// AssembleValueProofWithStats serves a pinned local proof for client-side verification.
func (v *View) AssembleValueProofWithStats(id uint64, root mpt.Hash, key []byte) (mpt.ProofResult, ProofStats, error) {
	return v.valueProof(id, root, key, mpt.AssembleValueProofAt)
}

func (v *View) valueProof(id uint64, trusted mpt.Hash, key []byte, walk func(mpt.Hash, mpt.Store, []byte) (mpt.ProofResult, error)) (result mpt.ProofResult, stats ProofStats, err error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return mpt.ProofResult{}, stats, ErrClosed
	}
	if len(key) > 32 {
		return mpt.ProofResult{}, stats, ErrLimit
	}
	stats.DirectoryReads++
	location, err := readRootLocation(v.snapshot, id, v.height)
	if err != nil {
		return mpt.ProofResult{}, stats, err
	}
	if location.Hash != trusted {
		return mpt.ProofResult{}, stats, ErrHashMismatch
	}
	reader := &trieReader{snapshot: v.snapshot, cfg: v.store.cfg, id: id, height: v.height, root: location.Hash, rootSource: location.Source}
	op := &proofPath{reader: reader, stats: &stats}
	op.budget = readBudget{cfg: reader.cfg, stats: &stats.ResolutionStats}
	for _, b := range key {
		op.key = append(op.key, b>>4, b&15)
	}
	defer func() {
		stats.ReadNodes, stats.ReadBytes = op.budget.nodes, op.budget.bytes
		if op.err != nil {
			result = mpt.ProofResult{}
			err = op.err
		}
		op.close()
	}()
	result, err = walk(trusted, op, key)
	if errors.Is(err, mpt.ErrProofNodeHashMismatch) {
		return mpt.ProofResult{}, stats, ErrHashMismatch
	}
	if err != nil {
		return mpt.ProofResult{}, stats, err
	}
	proof := result.Nodes
	// Native MPT accepts optional inline path items. Standard compact proofs need
	// just the root and independently hash-addressed nodes; inline RLP is already
	// inside its host. The root may itself be shorter than 32 bytes.
	n := 0
	for i, raw := range proof {
		if i == 0 || len(raw) >= 32 {
			proof[n] = raw
			n++
		}
	}
	for i := n; i < len(proof); i++ {
		proof[i] = nil
	}
	proof = proof[:n]
	stats.ProofNodes = uint64(n)
	for _, raw := range proof {
		stats.ProofBytes += uint64(len(raw))
	}
	result.Nodes = proof
	return result, stats, nil
}

type proofFrame struct {
	ctx         NodeKey
	record      Record
	raw         []byte
	hash        mpt.Hash
	targetChild *Record
}
type proofPath struct {
	reader *trieReader
	budget readBudget
	stats  *ProofStats
	key    []byte
	frame  *proofFrame
	err    error
	closed bool
}

func (p *proofPath) Get([]byte) ([]byte, error) { return nil, ErrReadOnly }
func (p *proofPath) NewBatch() mpt.Batch        { return deniedBatch{} }
func (p *proofPath) close() {
	if p.budget.prefix != nil {
		p.budget.prefix.close()
		p.budget.prefix = nil
	}
	p.frame = nil
	p.key = nil
	p.closed = true
}
func (p *proofPath) preparePrefix() error {
	if p.budget.prefix == nil {
		var cursor diskPredecessor
		var native, created bool
		var err error
		if s, ok := p.reader.snapshot.(*proofReadSnapshot); ok {
			cursor, created, err = s.borrow(prefixColumn)
			native = true
		} else {
			cursor, native, err = openPredecessor(p.reader.snapshot, prefixColumn)
			created = native
		}
		if err != nil {
			return err
		}
		p.budget.prefix = cursor
		p.stats.IteratorReuseEnabled = native
		if created {
			p.stats.PrefixIteratorCreates++
		}
	}
	if !p.stats.IteratorReuseEnabled {
		p.stats.FallbackPrefixReads++
	}
	return nil
}

func (p *proofPath) prefix(ctx NodeKey) (Record, error) {
	if err := p.preparePrefix(); err != nil {
		return Record{}, err
	}
	return p.reader.prefix(ctx, &p.budget)
}

func (p *proofPath) siblingHash(ctx NodeKey) (mpt.Hash, error) {
	if err := p.preparePrefix(); err != nil {
		return mpt.Hash{}, err
	}
	_, header, err := p.reader.prefixBytes(ctx, &p.budget, true)
	if err != nil {
		return mpt.Hash{}, err
	}
	if len(header) != recordHeaderBytes {
		return mpt.Hash{}, ErrCorrupt
	}
	if _, err := recordKind(header[0]); err != nil {
		return mpt.Hash{}, err
	}
	p.stats.SiblingHeaderReads++
	return mpt.Hash(header[1:]), nil
}
func (p *proofPath) failure(err error) ([]byte, error) { p.err = err; return nil, err }
func (p *proofPath) GetNode(target []byte, expected mpt.Hash) ([]byte, error) {
	if p.closed {
		return nil, ErrClosed
	}
	if p.err != nil {
		return nil, p.err
	}
	p.stats.NodeRequests++
	if _, err := pack(target); err != nil {
		return p.failure(err)
	}
	if !bytes.HasPrefix(p.key, target) {
		return p.failure(ErrConflict)
	}
	if p.frame == nil {
		ctx := NodeKey{TrieID: p.reader.id, Height: p.reader.height}
		if p.reader.rootSource == Hash {
			if err := p.loadHash(ctx, p.reader.root); err != nil {
				return p.failure(err)
			}
		} else if p.reader.rootSource == Prefix {
			record, err := p.prefix(ctx)
			if err != nil {
				return p.failure(err)
			}
			if err = p.rebuild(ctx, record, p.reader.root); err != nil {
				return p.failure(err)
			}
		} else {
			return p.failure(ErrCorrupt)
		}
	} else if bytes.Equal(p.frame.ctx.Path, target) {
		if p.frame.hash != expected {
			return p.failure(ErrHashMismatch)
		}
		p.stats.PathReuses++
		return append([]byte(nil), p.frame.raw...), nil
	}
	for depth := 0; depth <= MaxDepth; depth++ {
		f := p.frame
		if bytes.Equal(f.ctx.Path, target) {
			if f.hash != expected {
				return p.failure(ErrHashMismatch)
			}
			return append([]byte(nil), f.raw...), nil
		}
		if len(target) <= len(f.ctx.Path) || !bytes.HasPrefix(target, f.ctx.Path) {
			return p.failure(ErrConflict)
		}
		slot := 0
		switch f.record.Kind {
		case mpt.CanonicalBranch:
			slot = int(target[len(f.ctx.Path)])
		case mpt.CanonicalExtension:
			if !bytes.HasPrefix(target[len(f.ctx.Path):], f.record.Fragment) {
				return p.failure(ErrNotFound)
			}
		default:
			return p.failure(ErrNotFound)
		}
		loc, err := locateChild(f.ctx, f.record, slot)
		if err != nil {
			return p.failure(err)
		}
		if len(loc.Target.Path) > len(target) {
			return p.failure(ErrNotFound)
		}
		switch loc.Kind {
		case Prefix:
			child := f.targetChild
			if child == nil {
				return p.failure(ErrNotFound)
			}
			if err = p.rebuild(loc.Target, *child, child.Hash); err != nil {
				return p.failure(err)
			}
		case Hash:
			if err = p.loadHash(loc.Target, loc.Hash); err != nil {
				return p.failure(err)
			}
		case Embed:
			return p.failure(ErrEmbeddedNode)
		default:
			return p.failure(ErrNotFound)
		}
	}
	return p.failure(ErrLimit)
}

func (p *proofPath) rebuild(ctx NodeKey, record Record, expected mpt.Hash) error {
	f := &proofFrame{ctx: ctx, record: record}
	if record.Hash != expected {
		return ErrHashMismatch
	}
	fields, err := canonicalFields(ctx, record, func(loc ChildLocator) (mpt.Hash, error) {
		if !bytes.HasPrefix(p.key, loc.Target.Path) {
			return p.siblingHash(loc.Target)
		}
		child, err := p.prefix(loc.Target)
		if err != nil {
			return mpt.Hash{}, err
		}
		f.targetChild = &child
		return child.Hash, nil
	})
	if err != nil {
		return err
	}
	raw, err := fields.LocalRLP()
	if err != nil {
		return err
	}
	if len(raw) > mpt.MaxCanonicalNodeBytes {
		return ErrLimit
	}
	if len(ctx.Path) > 0 && len(raw) < 32 {
		return ErrCorrupt
	}
	f.raw, f.hash = raw, expected
	p.frame = f
	p.stats.NodesRebuilt++
	p.measure()
	return nil
}
func recordPayload(r Record) uint64 {
	n := uint64(len(r.Fragment) + len(r.Value))
	for _, ref := range r.Children {
		n += uint64(len(ref.Inline))
	}
	return n
}
func (p *proofPath) measure() {
	f := p.frame
	n := uint64(unsafe.Sizeof(*p)+unsafe.Sizeof(*f)) + uint64(len(p.key)+len(f.ctx.Path)+len(f.raw)) + recordPayload(f.record)
	if f.targetChild != nil {
		n += uint64(unsafe.Sizeof(Record{})) + recordPayload(*f.targetChild)
	}
	if n > p.stats.PeakContextBytes {
		p.stats.PeakContextBytes = n
	}
}

func (p *proofPath) loadHash(ctx NodeKey, want mpt.Hash) error {
	// Both proof modes use these routing fields. Authentication belongs to
	// the checked walker or to the client verifying the assembled response.
	reader := *p.reader
	reader.local = true
	rec, raw, e := reader.hash(ctx, want, &p.budget)
	if e != nil {
		return e
	}
	p.frame = &proofFrame{ctx: ctx, record: rec, raw: raw, hash: want}
	p.stats.NodesRebuilt++
	p.measure()
	return nil
}
