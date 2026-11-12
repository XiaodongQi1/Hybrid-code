package hybridtrie

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"sync"
	"time"
	"unsafe"
)

var (
	ErrNotFound          = errors.New("hybridtrie: referenced data missing")
	ErrClosed            = errors.New("hybridtrie: closed")
	ErrBusy              = errors.New("hybridtrie: release read views before close")
	ErrReadOnly          = errors.New("hybridtrie: read only")
	ErrConflict          = errors.New("hybridtrie: stale or noncontinuous commit")
	ErrUncertain         = errors.New("hybridtrie: write outcome unknown; reopen required")
	ErrEngineUnavailable = errors.New("hybridtrie: requested KV engine unavailable")
)

type Config struct {
	Format, Scheme, CodecID, WorkloadID                           string
	InitialHeight                                                 uint64
	MaxBatchBytes, MaxReadBytes, MaxReadNodes, MaxProjectionNodes uint64
	Storage                                                       *kvstore.Options `json:",omitempty"`
}

func DefaultConfig(codec, workload string) Config {
	return Config{Format: StoreFormat, Scheme: LogicalScheme, CodecID: codec, WorkloadID: workload, MaxBatchBytes: 128 << 20, MaxReadBytes: 32 << 20, MaxReadNodes: 4096, MaxProjectionNodes: 1 << 20}
}
func (c Config) validate() error {
	if c.Format != StoreFormat || c.Scheme != LogicalScheme || c.InitialHeight > MaxHeight || len(c.WorkloadID) == 0 || len(c.WorkloadID) > 256 || c.MaxBatchBytes < 4096 || c.MaxBatchBytes > 128<<20 || c.MaxReadBytes < 1024 || c.MaxReadBytes > 128<<20 || c.MaxReadNodes == 0 || c.MaxReadNodes > 1<<20 || c.MaxProjectionNodes == 0 || c.MaxProjectionNodes > 1<<24 {
		return ErrCorrupt
	}
	if c.CodecID != "ethereum-mpt" && c.CodecID != "raw-bytes/v1" {
		return ErrCorrupt
	}
	if _, e := storageOptions(c.Storage); e != nil {
		return kvError(e)
	}
	return nil
}

type Head struct {
	Initialized bool
	Height      uint64
	CommitID    [32]byte
	NextTrieID  uint64
}
type Store struct {
	path                        string
	importPending, importActive bool
	mu                          sync.Mutex
	db                          diskEngine
	cfg                         Config
	head                        Head
	views                       int
	readOnly, closed, uncertain bool
	// write is replaceable only by in-package fault tests. Any write error fails stop.
	write func([]entry) error
}

var manifestKey = []byte("M/manifest")
var headKey = []byte("M/head")

func Open(path string, cfg Config, create, readOnly bool) (*Store, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if create && readOnly {
		return nil, ErrReadOnly
	}
	// Own mutable option slices; changing the caller's config cannot change an
	// open store's engine, namespace registry or persisted identity.
	if cfg.Storage != nil {
		o, e := storageOptions(cfg.Storage)
		if e != nil {
			return nil, e
		}
		cfg.Storage = &o
	}
	db, err := openConfiguredEngine(path, cfg.Storage, create, readOnly)
	if err != nil {
		return nil, err
	}
	s := &Store{path: path, db: db, cfg: cfg, readOnly: readOnly, head: Head{NextTrieID: 1}}
	s.write = db.write
	var active diskSnapshot
	defer func() {
		if active != nil {
			active.close()
		}
	}()
	fail := func(e error) (*Store, error) {
		if active != nil {
			active.close()
			active = nil
		}
		db.close()
		return nil, e
	}
	manifest, _ := json.Marshal(struct {
		Config      Config
		Engine      string
		Compression string
	}{cfg, db.(*kvEngine).store.Info().Version, string(db.(*kvEngine).store.Info().Compression)})
	if create {
		raw, _ := json.Marshal(s.head)
		if err = s.write([]entry{{metaColumn, manifestKey, manifest}, {metaColumn, headKey, raw}}); err != nil {
			return fail(fmt.Errorf("%w: %v", ErrUncertain, err))
		}
	} else {
		snap := db.snapshot()
		active = snap
		raw, e := snap.get(metaColumn, manifestKey)
		if e != nil {
			return fail(e)
		}
		if !bytes.Equal(raw, manifest) {
			return fail(fmt.Errorf("%w: manifest mismatch", ErrCorrupt))
		}
		raw, e = snap.get(metaColumn, headKey)
		if e != nil {
			return fail(e)
		}
		if e = json.Unmarshal(raw, &s.head); e != nil {
			return fail(ErrCorrupt)
		}
		canonical, _ := json.Marshal(s.head)
		if !bytes.Equal(raw, canonical) || s.head.NextTrieID < 1 || s.head.NextTrieID > MaxTrieID+1 || s.head.Height > MaxHeight || s.head.Initialized && (s.head.Height < cfg.InitialHeight || s.head.CommitID == ([32]byte{})) || !s.head.Initialized && (s.head.Height != 0 || s.head.CommitID != ([32]byte{}) || s.head.NextTrieID != 1) {
			return fail(ErrCorrupt)
		}
		if s.head.Initialized {
			b, _ := blockKey(s.head.Height)
			raw, e = snap.get(metaColumn, b)
			if e != nil || !bytes.Equal(raw, s.head.CommitID[:]) {
				return fail(ErrCorrupt)
			}
		}
	}
	if active == nil {
		active = db.snapshot()
	}
	if e := s.loadSnapshotMarker(active); e != nil {
		return fail(e)
	}
	return s, nil
}
func (s *Store) check() error {
	if s.closed {
		return ErrClosed
	}
	if s.uncertain {
		return ErrUncertain
	}
	return nil
}
func (s *Store) Head() (Head, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.head, s.check() }
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.views != 0 || s.importActive {
		return ErrBusy
	}
	if e := s.db.close(); e != nil {
		return e
	}
	s.closed = true
	return nil
}

// StorageOptions reports an owned, resolved configuration for constructing a
// scratch database with the same engine as the final store.
func (s *Store) StorageOptions() kvstore.Options { o, _ := storageOptions(s.cfg.Storage); return o }
func heightSuffix(h uint64) ([]byte, error) {
	k, e := EncodeNodeKey(NodeKey{Height: h})
	if e != nil {
		return nil, e
	}
	return k[nodePositionHeaderBytes:], nil
}
func blockKey(h uint64) ([]byte, error) { b, e := heightSuffix(h); return append([]byte{'B'}, b...), e }
func rootKey(id uint64, h uint64) ([]byte, error) {
	b, e := EncodeNodeKey(NodeKey{TrieID: id, Height: h})
	return append([]byte{'R'}, b...), e
}

// Root metadata accepts legacy Prefix hash32 and explicit Hash tag1+hash32.
// The source is local addressing information; CommitID still binds logical roots.
type rootLocation struct {
	Hash   mpt.Hash
	Source ReferenceKind
	Height uint64
}

func encodeRootLocation(root mpt.Hash, source ReferenceKind) ([]byte, error) {
	if source == Prefix {
		return append([]byte(nil), root[:]...), nil
	}
	if source == Hash {
		return append([]byte{byte(Hash)}, root[:]...), nil
	}
	return nil, ErrCorrupt
}
func readRootLocation(snap diskSnapshot, id, h uint64) (rootLocation, error) {
	var out rootLocation
	k, e := rootKey(id, h)
	if e != nil {
		return out, e
	}
	found, v, e := snap.prev(metaColumn, k)
	if e != nil {
		return out, e
	}
	if len(found) == 0 || found[0] != 'R' {
		return out, ErrNotFound
	}
	actual, e := MatchPredecessor(found[1:], NodeKey{TrieID: id, Height: h})
	if e != nil {
		if errors.Is(e, ErrPositionMismatch) {
			return out, ErrNotFound
		}
		return out, e
	}
	out.Source, out.Height = Prefix, actual.Height
	if len(v) == 33 && v[0] == byte(Hash) {
		out.Source = Hash
		v = v[1:]
	}
	if len(v) != 32 {
		return rootLocation{}, ErrCorrupt
	}
	copy(out.Hash[:], v)
	return out, nil
}
func readRoot(snap diskSnapshot, id, h uint64) (mpt.Hash, error) {
	r, e := readRootLocation(snap, id, h)
	return r.Hash, e
}

type TrieUpdate struct {
	TrieID     uint64
	Tree       *mpt.Trie
	PrefixMask uint16
}
type CommitStats struct {
	// One final encoding per published Prefix record, excluding temporary routing metadata.
	PrefixEncodes uint64
	// Conservative temporary parent staging charge, excluding the encoded batch and prepared emissions.
	ProjectionScratchPeakBytes                                       uint64
	InitialHashCommit                                                bool
	RetainedHashEdges                                                uint64
	ProjectionNanos, WriteNanos                                      uint64
	LifecycleRecords, LifecycleBytes, OwnerRecords, OwnerBytes       uint64
	PrefixRecords, HashRecords, PrefixBytes, HashBytes, ClosureReads uint64
	// Logical values returned by Hash Store probes, not physical disk read bytes.
	ClosureReadBytes uint64
	// Newly materialized Hash records outside this block's changed-node digests.
	// These expose unchanged-subtree backfill, separately from changed-node copies.
	ClosureBackfillRecords, ClosureBackfillBytes uint64
}

// LayoutSelector selects only among eligible non-embedded child slots. It is
// called under the commit lock, must not call Store methods or mutate the tree,
// and does not constitute N/H cache admission by itself.
type LayoutSelector func(NodeKey, uint16) (uint16, error)
type preparedChange struct {
	change mpt.LogicalChange
	fields mpt.CanonicalNode
}

type writePlan struct {
	entries map[string]entry
	bytes   uint64
	limit   uint64
	scratch uint64
	stats   CommitStats
}

func (p *writePlan) add(cf column, key, value []byte) error {
	ident := string(append([]byte{byte(cf)}, key...))
	if old, ok := p.entries[ident]; ok {
		if !bytes.Equal(old.value, value) {
			return ErrConflict
		}
		return nil
	}
	n := uint64(len(key) + len(value))
	if p.bytes+n > p.limit {
		return ErrLimit
	}
	if cf == metaColumn && len(key) > 0 {
		switch key[0] {
		case 'L':
			p.stats.LifecycleRecords++
			p.stats.LifecycleBytes += n
		case 'O':
			p.stats.OwnerRecords++
			p.stats.OwnerBytes += n
		}
	}
	p.entries[ident] = entry{cf, append([]byte(nil), key...), append([]byte(nil), value...)}
	p.bytes += n
	if cf == prefixColumn {
		p.stats.PrefixRecords++
		p.stats.PrefixBytes += n
	}
	if cf == hashColumn {
		p.stats.HashRecords++
		p.stats.HashBytes += n
	}
	return nil
}

// replacePrefix changes only an unpublished physical reference choice. Charge
// the final encoded size before replacing; the record count does not increase.
func (p *writePlan) replacePrefix(key, value []byte) error {
	ident := string(append([]byte{byte(prefixColumn)}, key...))
	old, ok := p.entries[ident]
	if !ok {
		return ErrNotFound
	}
	n := uint64(len(key) + len(value))
	oldN := uint64(len(old.key) + len(old.value))
	if p.bytes-oldN+n > p.limit {
		return ErrLimit
	}
	p.bytes = p.bytes - oldN + n
	p.stats.PrefixBytes = p.stats.PrefixBytes - oldN + n
	p.entries[ident] = entry{prefixColumn, append([]byte(nil), key...), append([]byte(nil), value...)}
	return nil
}
func (p *writePlan) lookup(cf column, key []byte) ([]byte, bool) {
	e, ok := p.entries[string(append([]byte{byte(cf)}, key...))]
	return e.value, ok
}

// Commit projects pending logical nodes and acknowledges them only after a
// durable write. Native hash-store staging remains independent. Every
// changed independent version uses Prefix by default; deferred cold updates
// and explicit BH edges use a closed canonical Hash representation. CommitInitialHash installs the initial forest
// without Prefix copies. Reference policy affects cost, never CommitID.
// Trees must not be mutated concurrently. Empty updates commit an empty block.
func (s *Store) Commit(height uint64, parent [32]byte, updates []TrieUpdate) (Head, CommitStats, error) {
	return s.CommitWithLayout(height, parent, updates, nil)
}

// CommitWithLayout permits a prepared per-node layout (e.g. a fixed connected
// cache frontier). A nil selector retains TrieUpdate.PrefixMask behavior.
func (s *Store) CommitWithLayout(height uint64, parent [32]byte, updates []TrieUpdate, selector LayoutSelector) (Head, CommitStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(height, parent, updates, selector, nil, false)
}

// CommitInitialHash atomically installs a complete initial forest as canonical
// Hash records, without Prefix copies. It cannot overwrite an initialized Head.
func (s *Store) CommitInitialHash(height uint64, parent [32]byte, updates []TrieUpdate) (Head, CommitStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitLocked(height, parent, updates, nil, nil, true)
}
func (s *Store) commitLocked(height uint64, parent [32]byte, updates []TrieUpdate, selector LayoutSelector, forest *ForestUpdate, initialHash bool) (Head, CommitStats, error) {
	started := time.Now()
	var zero CommitStats
	if e := s.check(); e != nil {
		return s.head, zero, e
	}
	if s.readOnly {
		return s.head, zero, ErrReadOnly
	}
	if s.importPending || s.importActive {
		return s.head, zero, ErrSnapshotPending
	}
	if initialHash && s.head.Initialized {
		return s.head, zero, ErrConflict
	}
	if height > MaxHeight {
		return s.head, zero, ErrLimit
	}
	expected := s.cfg.InitialHeight
	if s.head.Initialized {
		if s.head.Height == MaxHeight {
			return s.head, zero, ErrLimit
		}
		expected = s.head.Height + 1
	}
	if height != expected || parent != s.head.CommitID {
		return s.head, zero, ErrConflict
	}
	ordered := append([]TrieUpdate(nil), updates...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].TrieID < ordered[j].TrieID })
	nextID := s.head.NextTrieID
	for i, u := range ordered {
		if u.TrieID > MaxTrieID {
			return s.head, zero, ErrLimit
		}
		if u.TrieID >= nextID {
			nextID = u.TrieID + 1
		}
		if u.Tree == nil || i > 0 && ordered[i-1].TrieID == u.TrieID {
			return s.head, zero, ErrConflict
		}
	}
	if forest != nil && forest.nextID > nextID {
		nextID = forest.nextID
	}
	snap := s.db.snapshot()
	defer snap.close()
	for _, u := range ordered {
		if err := s.validateStorageUpdate(snap, u.TrieID, forest); err != nil {
			return s.head, zero, err
		}
	}
	p := &writePlan{entries: map[string]entry{}, limit: s.cfg.MaxBatchBytes}
	var cursor projectionCursor
	defer cursor.close()
	p.stats.InitialHashCommit = initialHash
	changedDigests := make(map[mpt.Hash]bool)
	identity := sha256.New()
	identity.Write([]byte(StoreFormat))
	identity.Write(parent[:])
	heightBytes, _ := heightSuffix(height)
	identity.Write(heightBytes)
	for _, u := range ordered {
		oldLocation := rootLocation{Hash: mpt.EmptyRootHash, Source: Prefix}
		if s.head.Initialized {
			r, e := readRootLocation(snap, u.TrieID, s.head.Height)
			if e == nil {
				oldLocation = r
			} else if !errors.Is(e, ErrNotFound) {
				return s.head, zero, e
			}
		}
		var oldReader *projectionReader
		if oldLocation.Hash != mpt.EmptyRootHash {
			reader := &trieReader{local: forest != nil, snapshot: snap, cfg: s.cfg, id: u.TrieID, height: s.head.Height, root: oldLocation.Hash, rootSource: oldLocation.Source}
			oldReader = &projectionReader{trieReader: reader, routes: make(map[string]projectionRoute), cursor: &cursor}
		}
		root, err := u.Tree.Hash()
		if err != nil {
			return s.head, zero, err
		}
		// Preserve the canonical child digests before the Prefix codec omits them.
		type prefixParent struct {
			key     NodeKey
			encoded []byte
			hashes  []mpt.Hash // Prefix edges in ascending slot order
			record  Record
			scratch uint64
		}
		var parents []prefixParent
		// Positions are block-local. Eligibility alone does not cause a write:
		// only a pending logical update converts a cold parent's child.
		coldChildren := make(map[forestPosition]bool)
		hashRoots := make(map[forestPosition]bool)
		hashCursor := u.Tree.CanonicalCursor()
		// PendingLogicalNodes emits a preorder walk. Finalize a parent as
		// soon as that walk leaves its subtree, keeping only O(depth) decoded
		// parents rather than O(changed nodes) large 16-child Records.
		finalizeParent := func() error {
			last := len(parents) - 1
			parent := parents[last]
			key := parent.key
			record := parent.record
			var e error
			var edges [16]boundaryEdge
			hashes := parent.hashes
			for i, ref := range record.Children {
				if ref.Kind != Prefix {
					continue
				}
				loc, e := LocateChild(key, record, i)
				if e != nil {
					return e
				}
				want := hashes[0]
				hashes = hashes[1:]
				childKey, e := EncodeNodeKey(loc.Target)
				if e != nil {
					return e
				}
				if raw, staged := p.lookup(prefixColumn, childKey); staged {
					if len(raw) < recordHeaderBytes || !bytes.Equal(raw[1:recordHeaderBytes], want[:]) {
						return ErrHashMismatch
					}
					edges[i] = edgeAt(height, boundaryPrefix)
					continue
				}
				if hashRoots[position(u.TrieID, loc.Target.Path)] {
					edges[i] = boundaryEdge{Kind: boundaryHash}
					record.Children[i] = Reference{Kind: Hash, Hash: want}
					continue
				}
				if oldReader == nil {
					return ErrNotFound
				}
				var route projectionRoute
				if forest != nil {
					route = forest.retainedRoutes[position(u.TrieID, loc.Target.Path)]
				}
				if route.hash != want || route.source != Prefix && route.source != Hash {
					route, e = oldReader.retainedPhysicalRoute(loc.Target.Path, want)
				}
				if e != nil {
					return e
				}
				if route.source == Hash {
					edges[i] = boundaryEdge{Kind: boundaryHash}
					record.Children[i] = Reference{Kind: Hash, Hash: want}
					// The old route already authenticated this existing Hash
					// body. Its declared closed source needs no second Get/backfill.
					p.stats.RetainedHashEdges++
				} else {
					edges[i] = edgeAt(route.height, boundaryPrefix)
				}
			}
			if forest != nil && forest.projectedEdges != nil {
				forest.projectedEdges(key, edges)
			}
			raw, e := EncodeRecord(record)
			if e != nil {
				return e
			}
			p.stats.PrefixEncodes++
			if e = p.replacePrefix(parent.encoded, raw); e != nil {
				return e
			}
			p.scratch -= parent.scratch
			parents[last] = prefixParent{}
			parents = parents[:last]
			return nil
		}
		wroteRoot := false
		emit := func(change mpt.LogicalChange, parsed *mpt.CanonicalNode) error {
			for len(parents) > 0 {
				path := parents[len(parents)-1].key.Path
				if len(change.Path) > len(path) && bytes.HasPrefix(change.Path, path) {
					break
				}
				if err := finalizeParent(); err != nil {
					return err
				}
			}
			if len(change.Path) > 0 && len(change.RLP) < 32 {
				return nil
			}
			if len(change.Path) == 0 {
				wroteRoot = true
			}
			changedDigests[change.Hash] = true
			if len(hashRoots) != 0 {
				for depth := len(change.Path); depth >= 0; depth-- {
					if hashRoots[position(u.TrieID, change.Path[:depth])] {
						return nil // ensureHash already staged the closed subtree
					}
				}
			}
			at := position(u.TrieID, change.Path)
			if coldChildren[at] {
				if e := s.ensureHashCursor(p, snap, hashCursor, change.Path, change.Hash, 0); e != nil {
					return e
				}
				hashRoots[at] = true
				return nil
			}
			if initialHash {
				if forest != nil && forest.initialNodeCheck != nil {
					if e := forest.initialNodeCheck(NodeKey{u.TrieID, change.Path, height}, change.RLP); e != nil {
						return e
					}
				}
				rec, e := FromCanonical(NodeKey{u.TrieID, change.Path, height}, change.RLP, 0)
				if e != nil {
					return e
				}
				if rec.Hash != change.Hash {
					return ErrHashMismatch
				}
				return p.add(hashColumn, change.Hash[:], change.RLP)
			}
			var desc mpt.CanonicalNode
			var e error
			if parsed != nil {
				desc = *parsed
			} else {
				desc, e = mpt.DecodeLocalNodeFields(change.RLP)
				if e != nil {
					return e
				}
			}
			var eligible uint16
			for i, ref := range desc.Children {
				if ref.Present && len(ref.Inline) == 0 {
					eligible |= 1 << i
				}
			}
			ctx := NodeKey{u.TrieID, change.Path, height}
			mask := u.PrefixMask & eligible
			if selector != nil {
				policyContext := ctx
				policyContext.Path = append([]byte(nil), ctx.Path...)
				mask, e = selector(policyContext, eligible)
				if e != nil {
					return e
				}
			}
			record, e := recordFromFields(ctx, desc, change.Hash, mask)
			if e != nil {
				return e
			}
			key, e := EncodeNodeKey(ctx)
			if e != nil {
				return e
			}
			var raw []byte
			var scratch uint64
			if mask == 0 {
				raw, e = EncodeRecord(record)
				if e != nil {
					return e
				}
				p.stats.PrefixEncodes++
			} else {
				// Keep only parents requiring edge resolution typed until final
				// encoding. Children can already refer to this staged digest.
				cost := 2*uint64(unsafe.Sizeof(prefixParent{})) + uint64(len(record.Fragment)+len(record.Value)+len(key)+len(ctx.Path))
				for _, ref := range record.Children {
					cost += uint64(len(ref.Inline))
					if ref.Kind == Prefix {
						cost += 64
					} // slice backing capacity
				}
				// MaxBatchBytes retains its persisted-byte meaning. Decoded
				// parents have a separate bounded O(depth) scratch allowance.
				scratchLimit := 2*p.limit + uint64(MaxDepth+1)*(2*uint64(unsafe.Sizeof(prefixParent{}))+16*64+256)
				if p.scratch+cost > scratchLimit || len(parents) >= MaxDepth+1 {
					return ErrLimit
				}
				p.scratch += cost
				if p.scratch > p.stats.ProjectionScratchPeakBytes {
					p.stats.ProjectionScratchPeakBytes = p.scratch
				}
				scratch = cost
				raw = make([]byte, recordHeaderBytes)
				raw[0] = byte(record.Kind)
				copy(raw[1:], record.Hash[:])
			}
			if e = p.add(prefixColumn, key, raw); e != nil {
				return e
			}
			if mask != 0 {
				parent := prefixParent{key: ctx, encoded: key, record: record, scratch: scratch}
				for i, ref := range record.Children {
					if ref.Kind == Prefix {
						parent.hashes = append(parent.hashes, desc.Children[i].Hash)
					}
				}
				parents = append(parents, parent)
				if s.head.Initialized && forest != nil && forest.cacheBoundary != nil &&
					forest.cacheBoundary.find(at) < 0 && !forest.retainedPrefix[at] && (forest.baseNodes == nil || forest.baseNodes.find(at) < 0) {
					for i, ref := range record.Children {
						if ref.Kind != Prefix {
							continue
						}
						loc, e := locateChild(ctx, record, i)
						if e != nil {
							return e
						}
						coldChildren[position(u.TrieID, loc.Target.Path)] = true
					}
				}
			}
			for i, ref := range record.Children {
				if ref.Kind == Hash {
					loc, e := LocateChild(ctx, record, i)
					if e != nil {
						return e
					}
					if e = s.ensureHashCursor(p, snap, hashCursor, loc.Target.Path, ref.Hash, 0); e != nil {
						return e
					}
				}
			}
			return nil
		}
		if root != oldLocation.Hash {
			if changes, ok := forestPending(forest, u.TrieID); ok {
				for i := range changes {
					if err = emit(changes[i].change, &changes[i].fields); err != nil {
						break
					}
				}
			} else {
				_, err = u.Tree.PendingLogicalNodes(oldLocation.Hash, s.cfg.MaxProjectionNodes, func(c mpt.LogicalChange) error { return emit(c, nil) })
			}
		}
		if err != nil {
			return s.head, zero, err
		}
		for len(parents) > 0 {
			if err = finalizeParent(); err != nil {
				return s.head, zero, err
			}
		}
		source := Prefix
		if initialHash {
			source = Hash
		} else if !wroteRoot {
			source = oldLocation.Source
		}
		rk, _ := rootKey(u.TrieID, height)
		rv, e := encodeRootLocation(root, source)
		if e != nil {
			return s.head, zero, e
		}
		if err = p.add(metaColumn, rk, rv); err != nil {
			return s.head, zero, err
		}
		identity.Write(rk)
		identity.Write(root[:])
	}
	if forest != nil {
		metadata, err := forest.stageMetadata(snap, p)
		if err != nil {
			return s.head, zero, err
		}
		for _, e := range metadata {
			identity.Write(e.key)
			identity.Write(e.value)
		}
	}
	var lastID [TrieIDBytes]byte
	put48(lastID[:], nextID-1)
	identity.Write(lastID[:])
	for _, item := range p.entries {
		if item.cf == hashColumn {
			var digest mpt.Hash
			copy(digest[:], item.key)
			if !initialHash && !changedDigests[digest] {
				p.stats.ClosureBackfillRecords++
				p.stats.ClosureBackfillBytes += uint64(len(item.key) + len(item.value))
			}
		}
	}
	next := Head{Initialized: true, Height: height, NextTrieID: nextID}
	copy(next.CommitID[:], identity.Sum(nil))
	raw, _ := json.Marshal(next)
	bk, _ := blockKey(height)
	if e := p.add(metaColumn, bk, next.CommitID[:]); e != nil {
		return s.head, zero, e
	}
	if e := p.add(metaColumn, headKey, raw); e != nil {
		return s.head, zero, e
	}
	entries := make([]entry, 0, len(p.entries))
	for _, e := range p.entries {
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].cf != entries[j].cf {
			return entries[i].cf < entries[j].cf
		}
		return bytes.Compare(entries[i].key, entries[j].key) < 0
	})
	p.stats.ProjectionNanos = uint64(time.Since(started))
	written := time.Now()
	if e := s.write(entries); e != nil {
		p.stats.WriteNanos = uint64(time.Since(written))
		s.uncertain = true
		return s.head, zero, fmt.Errorf("%w: %v", ErrUncertain, e)
	}
	p.stats.WriteNanos = uint64(time.Since(written))
	s.head = next
	for _, u := range ordered {
		root, _ := u.Tree.Hash() // hashing succeeded during staging; no mutation is allowed
		u.Tree.AcceptLogicalPersistence(root)
	}
	return next, p.stats, nil
}
func readPrefix(snap diskSnapshot, key NodeKey) (Record, error) {
	encoded, e := EncodeNodeKey(key)
	if e != nil {
		return Record{}, e
	}
	found, raw, e := snap.prev(prefixColumn, encoded)
	if e != nil {
		return Record{}, e
	}
	if _, e = MatchPredecessor(found, key); e != nil {
		if errors.Is(e, ErrPositionMismatch) {
			return Record{}, ErrNotFound
		}
		return Record{}, e
	}
	r, e := DecodeRecord(raw)
	if e == nil {
		e = r.ValidateAt(key)
	}
	return r, e
}
func (s *Store) ensureHash(p *writePlan, snap diskSnapshot, tree *mpt.Trie, path []byte, expected mpt.Hash, depth int) error {
	return s.ensureHashCursor(p, snap, tree.CanonicalCursor(), path, expected, depth)
}
func (s *Store) ensureHashCursor(p *writePlan, snap diskSnapshot, cursor *mpt.CanonicalCursor, path []byte, expected mpt.Hash, depth int) error {
	if depth > MaxDepth {
		return ErrLimit
	}
	if _, ok := p.lookup(hashColumn, expected[:]); ok {
		return nil
	}
	p.stats.ClosureReads++
	raw, e := snap.get(hashColumn, expected[:])
	p.stats.ClosureReadBytes += uint64(len(raw))
	if e == nil {
		_, e = canonicalHash(raw, expected)
		return e
	}
	if !errors.Is(e, ErrNotFound) {
		return e
	}
	h, raw, ok, e := cursor.AtPath(path)
	if e != nil {
		return e
	}
	if !ok {
		return ErrNotFound
	}
	if h != expected {
		return ErrHashMismatch
	}
	if len(path) != 0 && len(raw) < 32 {
		return ErrCorrupt
	}
	desc, e := mpt.InspectCanonicalNode(raw)
	if e != nil {
		return e
	}
	for i, ref := range desc.Children {
		if !ref.Present || len(ref.Inline) != 0 {
			continue
		}
		child := append(append([]byte(nil), path...), byte(i))
		if desc.Kind == mpt.CanonicalExtension {
			fragment, _, e := mpt.DecodeCompactPath(desc.Path)
			if e != nil {
				return e
			}
			child = append(append([]byte(nil), path...), fragment...)
		}
		if e = s.ensureHashCursor(p, snap, cursor, child, ref.Hash, depth+1); e != nil {
			return e
		}
	}
	return p.add(hashColumn, expected[:], raw)
}
func canonicalHash(raw []byte, expected mpt.Hash) (mpt.CanonicalNode, error) {
	d, h, e := mpt.InspectCanonicalNodeWithHash(raw)
	if e == nil && h != expected {
		e = ErrHashMismatch
	}
	return d, e
}

func forestPending(f *ForestUpdate, id uint64) ([]preparedChange, bool) {
	if f == nil {
		return nil, false
	}
	c, ok := f.pendingChanges[id]
	return c, ok
}
