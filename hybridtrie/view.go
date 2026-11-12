package hybridtrie

import (
	"bytes"
	"errors"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"sync"
)

type View struct {
	mu       sync.RWMutex
	store    *Store
	snapshot diskSnapshot
	height   uint64
	closed   bool
}

func (s *Store) ViewAt(height uint64) (*View, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return nil, e
	}
	if height > MaxHeight {
		return nil, ErrLimit
	}
	if !s.head.Initialized || height < s.cfg.InitialHeight || height > s.head.Height {
		return nil, ErrNotFound
	}
	snap := s.db.snapshot()
	key, _ := blockKey(height)
	value, e := snap.get(metaColumn, key)
	if e != nil || len(value) != 32 {
		snap.close()
		if e != nil {
			return nil, e
		}
		return nil, ErrCorrupt
	}
	s.views++
	return &View{store: s, snapshot: snap, height: height}, nil
}
func (v *View) Close() error {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil
	}
	v.closed = true
	v.snapshot.close()
	v.mu.Unlock()
	v.store.mu.Lock()
	v.store.views--
	v.store.mu.Unlock()
	return nil
}

func (v *View) Root(id uint64) (mpt.Hash, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return mpt.Hash{}, ErrClosed
	}
	return readRoot(v.snapshot, id, v.height)
}

// TrieReader binds the snapshot directory to the caller's trusted root. It only
// resolves nodes reachable from that root, never arbitrary orphan positions.
func (v *View) TrieReader(id uint64, trusted mpt.Hash) (mpt.Store, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if v.closed {
		return nil, ErrClosed
	}
	location, e := readRootLocation(v.snapshot, id, v.height)
	if e != nil {
		return nil, e
	}
	if location.Hash != trusted {
		return nil, ErrHashMismatch
	}
	return &trieReader{view: v, snapshot: v.snapshot, cfg: v.store.cfg, id: id, height: v.height, root: location.Hash, rootSource: location.Source}, nil
}
func (v *View) Prove(id uint64, trusted mpt.Hash, key []byte) (mpt.Proof, error) {
	proof, _, err := v.ProveWithStats(id, trusted, key)
	return proof, err
}

type trieReader struct {
	view       *View
	snapshot   diskSnapshot
	cfg        Config
	id         uint64
	height     uint64
	root       mpt.Hash
	rootSource ReferenceKind
	sibling    func(NodeKey, Record, []byte) // execution-only observation of an already-read Prefix body
	local      bool                          // pinned local execution or commit reader; proof readers stay strict
}

func (r *trieReader) Get(key []byte) ([]byte, error) { return nil, ErrReadOnly }
func (r *trieReader) NewBatch() mpt.Batch            { return deniedBatch{} }

type deniedBatch struct{}

func (deniedBatch) Put([]byte, []byte) error { return ErrReadOnly }
func (deniedBatch) Write() error             { return ErrReadOnly }

type readBudget struct {
	admission    *admissionReadBudget
	nodes, bytes uint64
	cfg          Config
	stats        *ResolutionStats
	prefix       diskPredecessor
	resolved     func(NodeKey, NodeKey)
}

// ResolutionStats counts logical resolver operations, not physical disk I/O.
// Hash-only use does not count as a body access or change cache heat.
type ResolutionStats struct {
	NodeHashHits, HashOnlyHits, HashMisses uint64
	PrefixReads, HashReads                 uint64
}

// summaryLookup is private to the pinned latest-state execution reader.
// Historical readers never attach it. Returned hashes still pass the normal
// parent reconstruction and trusted-root/expected-hash checks below.
type summaryLookup func(NodeKey) (mpt.Hash, uint8, bool)

type prefixResolution struct {
	hash   mpt.Hash
	record *Record
	ready  bool
}

func (b *readBudget) charge(n int) error {
	if err := b.admission.afterRead(uint64(n)); err != nil {
		return err
	}
	b.nodes++
	b.bytes += uint64(n)
	if b.nodes > b.cfg.MaxReadNodes || b.bytes > b.cfg.MaxReadBytes {
		return ErrLimit
	}
	return nil
}
func (r *trieReader) prefix(key NodeKey, b *readBudget) (Record, error) {
	return r.prefixObserved(key, b, nil)
}

func (r *trieReader) prefixObserved(key NodeKey, b *readBudget, observe func(NodeKey, Record, []byte)) (Record, error) {
	matched, raw, e := r.prefixBytes(key, b, false)
	if e != nil {
		return Record{}, e
	}
	rec, e := DecodeRecord(raw)
	if e == nil {
		e = rec.ValidateAt(key)
	}
	if e == nil && b.resolved != nil {
		b.resolved(key, matched)
	}
	if e == nil && observe != nil {
		observe(matched, rec, raw)
	}
	return rec, e
}

// Header reads still charge the full logical record and validate its position.
func (r *trieReader) prefixBytes(key NodeKey, b *readBudget, headerOnly bool) (NodeKey, []byte, error) {
	encoded, e := EncodeNodeKey(key)
	if e != nil {
		return NodeKey{}, nil, e
	}
	if b.stats != nil {
		b.stats.PrefixReads++
	}
	if e = b.beforeRead(); e != nil {
		return NodeKey{}, nil, e
	}
	var found, raw []byte
	var size int
	if headerOnly {
		found, raw, size, e = readLimited(b.prefix, encoded, recordHeaderBytes)
	} else if b.prefix != nil {
		found, raw, e = b.prefix.prev(encoded)
	} else {
		found, raw, e = r.snapshot.prev(prefixColumn, encoded)
	}
	if e != nil {
		return NodeKey{}, nil, e
	}
	if !headerOnly {
		size = len(raw)
	}
	if size > MaxRecordBytes {
		return NodeKey{}, nil, ErrLimit
	}
	if e = b.charge(size + len(found)); e != nil {
		return NodeKey{}, nil, e
	}
	matched, e := MatchPredecessor(found, key)
	if e != nil {
		if errors.Is(e, ErrPositionMismatch) {
			return NodeKey{}, nil, ErrNotFound
		}
		return NodeKey{}, nil, e
	}
	return matched, raw, nil
}

// GetNode uses a bounded authenticated root walk without a cross-call routing
// cache. Knowing a digest never changes a declared Prefix edge into Hash.
func (r *trieReader) GetNode(target []byte, expected mpt.Hash) ([]byte, error) {
	return r.getNode(target, expected, nil, nil)
}

func (r *trieReader) getNode(target []byte, expected mpt.Hash, lookup summaryLookup, stats *ResolutionStats) ([]byte, error) {
	return r.getNodeResolved(target, expected, lookup, stats, nil)
}
func (r *trieReader) getNodeResolved(target []byte, expected mpt.Hash, lookup summaryLookup, stats *ResolutionStats, resolved func(NodeKey, NodeKey)) ([]byte, error) {
	return r.getNodeObserved(target, expected, lookup, stats, resolved, nil)
}

// opened runs only after binding the reconstructed node to its authenticated parent.
func (r *trieReader) getNodeObserved(target []byte, expected mpt.Hash, lookup summaryLookup, stats *ResolutionStats, resolved func(NodeKey, NodeKey), opened func(NodeKey, ReferenceKind, Record, []byte)) ([]byte, error) {
	if r.view != nil {
		r.view.mu.RLock()
		defer r.view.mu.RUnlock()
		if r.view.closed {
			return nil, ErrClosed
		}
	}
	if _, e := pack(target); e != nil {
		return nil, e
	}
	if r.root == mpt.EmptyRootHash {
		return nil, ErrNotFound
	}
	b := &readBudget{cfg: r.cfg, stats: stats, resolved: resolved}
	ctx := NodeKey{r.id, nil, r.height}
	var record Record
	var hashRaw []byte
	var e error
	if r.rootSource == Hash {
		record, hashRaw, e = r.hash(ctx, r.root, b)
	} else if r.rootSource == Prefix {
		record, e = r.prefix(ctx, b)
	} else {
		return nil, ErrCorrupt
	}
	if e != nil {
		return nil, e
	}
	want := r.root
	source := r.rootSource
	for depth := 0; depth <= MaxDepth; depth++ {
		// A digest hit replaces only a summary read. If this edge is traversed,
		// load its body later from its declared source, preserving the digest
		// authenticated by this parent. Disk misses retain their record for reuse.
		var resolved [16]prefixResolution
		raw, h := hashRaw, record.Hash
		if source != Hash {
			raw, h, e = r.canonical(ctx, record, func(loc ChildLocator) (mpt.Hash, error) {
				slot := 0
				if record.Kind == mpt.CanonicalBranch {
					slot = int(loc.Target.Path[len(ctx.Path)])
				}
				if lookup != nil {
					if hash, state, hit := lookup(loc.Target); hit {
						resolved[slot] = prefixResolution{hash: hash, ready: true}
						if stats != nil {
							if state == nodeState {
								stats.NodeHashHits++
							} else {
								stats.HashOnlyHits++
							}
						}
						return hash, nil
					}
					if stats != nil {
						stats.HashMisses++
					}
				}
				child, e := r.prefixObserved(loc.Target, b, r.sibling)
				if e != nil {
					return mpt.Hash{}, e
				}
				resolved[slot] = prefixResolution{hash: child.Hash, record: &child, ready: true}
				return child.Hash, nil
			})
		}
		if e != nil {
			return nil, e
		}
		if h != want {
			return nil, ErrHashMismatch
		}
		if len(ctx.Path) != 0 && len(raw) < 32 {
			return nil, ErrCorrupt
		}
		if opened != nil {
			opened(ctx, source, record, raw)
		}
		if bytes.Equal(ctx.Path, target) {
			if h != expected {
				return nil, ErrHashMismatch
			}
			return raw, nil
		}
		if len(ctx.Path) >= len(target) || !bytes.HasPrefix(target, ctx.Path) {
			return nil, ErrNotFound
		}
		slot := 0
		switch record.Kind {
		case mpt.CanonicalBranch:
			slot = int(target[len(ctx.Path)])
		case mpt.CanonicalExtension:
			if !bytes.HasPrefix(target[len(ctx.Path):], record.Fragment) {
				return nil, ErrNotFound
			}
		default:
			return nil, ErrNotFound
		}
		loc, e := locateChild(ctx, record, slot)
		if e != nil {
			return nil, e
		}
		ctx = loc.Target
		source = loc.Kind
		switch loc.Kind {
		case Empty:
			return nil, ErrNotFound
		case Prefix:
			child := resolved[slot]
			if !child.ready {
				return nil, ErrNotFound
			}
			want = child.hash
			if child.record != nil {
				record = *child.record
			} else {
				record, e = r.prefix(ctx, b)
				if e != nil {
					return nil, e
				}
				if record.Hash != want {
					return nil, ErrHashMismatch
				}
			}
		case Hash:
			record, hashRaw, e = r.hash(ctx, loc.Hash, b)
			if e != nil {
				return nil, e
			}
			want = loc.Hash
		case Embed:
			// A sub-32-byte subtree cannot contain an independently hashed child.
			// mpt consumes its RLP inside the parent, so GetNode must never descend here.
			return nil, ErrEmbeddedNode
		}
	}
	return nil, ErrLimit
}

func (r *trieReader) canonical(ctx NodeKey, rec Record, resolve PrefixHashResolver) ([]byte, mpt.Hash, error) {
	if !r.local {
		return RebuildCanonical(ctx, rec, resolve)
	}
	d, err := canonicalFields(ctx, rec, resolve)
	if err != nil {
		return nil, mpt.Hash{}, err
	}
	raw, err := d.LocalRLP()
	return raw, rec.Hash, err
}

// hash resolves only a declared Hash source. A short root is valid; short
// non-root nodes belong inline to their host. There is no Prefix fallback.
func (r *trieReader) hash(ctx NodeKey, want mpt.Hash, b *readBudget) (Record, []byte, error) {
	if e := b.beforeRead(); e != nil {
		return Record{}, nil, e
	}
	if b.stats != nil {
		b.stats.HashReads++
	}
	raw, e := r.snapshot.get(hashColumn, want[:])
	if e != nil {
		return Record{}, nil, e
	}
	if e = b.charge(len(raw) + 32); e != nil {
		return Record{}, nil, e
	}
	if len(ctx.Path) > 0 && len(raw) < 32 {
		return Record{}, nil, ErrCorrupt
	}
	if r.local {
		d, e := mpt.DecodeLocalNodeFields(raw)
		if e != nil {
			return Record{}, nil, e
		}
		rec, e := recordFromFields(ctx, d, want, 0)
		return rec, raw, e
	}
	rec, e := FromCanonical(ctx, raw, 0)
	if e != nil {
		return Record{}, nil, e
	}
	if rec.Hash != want {
		return Record{}, nil, ErrHashMismatch
	}
	return rec, raw, nil
}
