package hybridtrie

import (
	"errors"
	"math/bits"
	"sort"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"sync"
)

// ForestCacheConfig selects a bounded latest-state execution policy. An empty
// Mode preserves the legacy Slots/PerTrie partitions (zero Slots disables them).
// ReservedSharedCache uses one global set of pools and class reservations;
// Slots and PerTrie must then be zero. PrefixBoundaryCache uses N bodies and
// parent-edge version hints without H. All modes leave historical Views separate.
type ForestCacheConfig struct {
	Mode           string
	Slots          uint32
	PerTrie        CacheConfig
	ReservedShared ReservedSharedConfig
	Boundary       BoundaryConfig
}

func DefaultForestCacheConfig() ForestCacheConfig {
	return ForestCacheConfig{Slots: 4, PerTrie: DefaultCacheConfig()}
}

type forestSession struct {
	trie *CachedTrie
	tick uint64
}
type CachedForest struct {
	readBacking                      *executionReadBacking
	readStats                        ExecutionReadStats
	mu                               sync.Mutex
	store                            *Store
	cfg                              ForestCacheConfig
	sessions                         map[uint64]forestSession
	active                           *ForestBlock
	tick                             uint64
	closed                           bool
	diskReads, admissions, evictions uint64
	retiredCounters                  ForestCacheStats
	shared                           *forestShared
	boundary                         *boundaryForest
}

// ForestBlock coordinates one durable height. Its readers are bound to the
// base height; the caller applies finalized transaction key writes to live MPTs
// and supplies their final trees here. Historical Views use persisted state.
type ForestBlock struct {
	reads           *executionReads
	owner           *CachedForest
	update          *ForestUpdate
	base            *View
	blocks          map[uint64]*CachedBlock
	retired         map[uint64]bool
	done            bool
	sharedReaders   map[uint64]*forestReader
	sharedPreparing bool
	boundaryReaders map[uint64]*boundaryReader
}
type ForestCacheStats struct {
	ExecutionReads                                               ExecutionReadStats
	ExecutionReadBudgetBytes                                     uint64
	Mode                                                         string
	Shared                                                       SharedCacheStats
	Boundary                                                     BoundaryStats
	Partitions                                                   uint64
	Slots                                                        uint32
	NodeAllocatedBytes, HashAllocatedBytes, PlanAllocatedBytes   uint64
	CandidateAllocatedBytes, PolicyAllocatedBytes, InstanceBytes uint64
	NodeBodyHits, NodeBodyMisses, HashOnlyHits, NodeHashHits     uint64
	DiskReaders, Admissions, Evictions                           uint64
}

func OpenCachedForest(store *Store, cfg ForestCacheConfig) (*CachedForest, error) {
	if store == nil {
		return nil, ErrClosed
	}
	if cfg.Mode == PrefixBoundaryCache {
		if cfg.Boundary.MaxNodeBytes == 532 && store.cfg.CodecID != "ethereum-mpt" {
			return nil, ErrConflict
		}
		if cfg.Slots != 0 || cfg.PerTrie != (CacheConfig{}) || cfg.ReservedShared != (ReservedSharedConfig{}) {
			return nil, ErrConflict
		}
		p, err := newBoundaryForest(cfg.Boundary)
		if err != nil {
			return nil, err
		}
		h, err := store.Head()
		if err != nil {
			return nil, err
		}
		p.head = h
		return &CachedForest{store: store, cfg: cfg, boundary: p}, nil
	}
	if cfg.Boundary != (BoundaryConfig{}) {
		return nil, ErrConflict
	}
	if cfg.Mode == ReservedSharedCache {
		if cfg.Slots != 0 || cfg.PerTrie != (CacheConfig{}) {
			return nil, ErrConflict
		}
		shared, err := newForestShared(cfg.ReservedShared)
		if err != nil {
			return nil, err
		}
		head, err := store.Head()
		if err != nil {
			return nil, err
		}
		shared.head = head
		return &CachedForest{store: store, cfg: cfg, shared: shared}, nil
	}
	if cfg.Mode != "" && cfg.Mode != PartitionedCache {
		return nil, ErrConflict
	}
	if cfg.ReservedShared != (ReservedSharedConfig{}) {
		return nil, ErrConflict
	}
	if cfg.Slots > 64 {
		return nil, ErrLimit
	}
	if cfg.Slots > 0 {
		// Validate without retaining a hidden extra partition.
		if _, err := newResidentCache(cfg.PerTrie); err != nil {
			return nil, err
		}
	}
	if _, err := store.Head(); err != nil {
		return nil, err
	}
	return &CachedForest{store: store, cfg: cfg, sessions: map[uint64]forestSession{}}, nil
}
func (f *CachedForest) BeginBlock(height uint64, parent [32]byte) (*ForestBlock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return nil, ErrClosed
	}
	if f.active != nil {
		return nil, ErrBusy
	}
	u, err := f.store.BeginForest(height, parent)
	if err != nil {
		return nil, err
	}
	var base *View
	if u.base.Initialized {
		base, err = f.store.ViewAt(u.base.Height)
		if err != nil {
			u.Abort()
			return nil, err
		}
	}
	// An external generic writer invalidates all cached Head bindings.
	for id, s := range f.sessions {
		if s.trie.head != u.base {
			f.drop(id)
		}
	}
	b := &ForestBlock{owner: f, update: u, base: base, blocks: map[uint64]*CachedBlock{}, retired: map[uint64]bool{}}
	if f.shared != nil {
		if f.shared.uncertain {
			u.Abort()
			if base != nil {
				base.Close()
			}
			return nil, ErrUncertain
		}
		if f.shared.head != u.base {
			f.shared.reset()
			f.shared.head = u.base
		}
		f.shared.begin(height)
		b.sharedReaders = map[uint64]*forestReader{}
	}
	if f.boundary != nil {
		if f.boundary.uncertain {
			u.Abort()
			if base != nil {
				base.Close()
			}
			return nil, ErrUncertain
		}
		if f.boundary.head != u.base {
			f.boundary.reset()
			f.boundary.head = u.base
		}
		// Initial snapshot construction contributes no visits or invented history.
		if u.base.Initialized {
			if err = f.boundary.begin(height); err != nil {
				u.Abort()
				if base != nil {
					base.Close()
				}
				return nil, err
			}
		}
		b.boundaryReaders = map[uint64]*boundaryReader{}
	}
	if base != nil && f.boundary != nil && f.cfg.Boundary.ExecutionReads.Enabled {
		b.reads = newExecutionReads(base.snapshot, u.base.Height, f.cfg.Boundary.ExecutionReads, f.readBacking)
		f.readBacking = nil
	}
	f.active = b
	return b, nil
}
func (b *ForestBlock) check() error {
	if b.done || b.owner.closed || b.owner.active != b {
		return ErrClosed
	}
	return nil
}

func (b *ForestBlock) reader(id uint64, root mpt.Hash) (mpt.Store, error) {
	if err := b.check(); err != nil {
		return nil, err
	}
	if b.base == nil {
		return nil, ErrNotFound
	}
	if b.owner.boundary != nil {
		return b.boundaryReader(id, root)
	}
	if b.owner.shared != nil {
		return b.sharedReader(id, root)
	}
	if old, ok := b.blocks[id]; ok {
		if old.owner.root != root {
			return nil, ErrHashMismatch
		}
		return forestCachedReader{old.reader}, nil
	}
	f := b.owner
	session, ok := f.sessions[id]
	if !ok && f.cfg.Slots > 0 {
		if len(f.sessions) >= int(f.cfg.Slots) {
			// Never evict a partition pinned by this block; account 0 stays pinned
			// across blocks when present. Overflow contracts remain disk-backed.
			var victim uint64
			found := false
			oldest := uint64(^uint64(0))
			for key, s := range f.sessions {
				if key == AccountTrieID || b.blocks[key] != nil {
					continue
				}
				if !found || s.tick < oldest || s.tick == oldest && key < victim {
					victim, oldest, found = key, s.tick, true
				}
			}
			if found {
				f.drop(victim)
			}
		}
		if len(f.sessions) < int(f.cfg.Slots) {
			c, err := OpenCachedTrie(f.store, id, root, f.cfg.PerTrie)
			if err != nil {
				return nil, err
			}
			session = forestSession{trie: c}
			f.admissions++
			ok = true
		}
	}
	if ok {
		if session.trie.root != root || session.trie.head != b.update.base {
			return nil, ErrConflict
		}
		block, err := session.trie.BeginBlock(b.update.height)
		if err != nil {
			return nil, err
		}
		f.tick++
		session.tick = f.tick
		f.sessions[id] = session
		b.blocks[id] = block
		return forestCachedReader{block.reader}, nil
	}
	f.diskReads++
	return b.base.TrieReader(id, root)
}

// MPT callers access this reader outside CachedBlock.Get's lock. Serialize
// resolution with per-partition statistics and Close. The block itself remains
// single-owner: execution may not race Commit, SetTrie or Abort.
type forestCachedReader struct{ inner *cachedReader }

func (r forestCachedReader) Get([]byte) ([]byte, error) { return nil, ErrReadOnly }
func (r forestCachedReader) NewBatch() mpt.Batch        { return deniedBatch{} }
func (r forestCachedReader) GetNode(path []byte, h mpt.Hash) ([]byte, error) {
	c := r.inner.block.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	return r.inner.GetNode(path, h)
}

func (f *CachedForest) drop(id uint64) {
	s := f.sessions[id]
	stats, _ := s.trie.Stats()
	f.retiredCounters.NodeBodyHits += stats.NodeBodyHits
	f.retiredCounters.NodeBodyMisses += stats.NodeBodyMisses
	f.retiredCounters.HashOnlyHits += stats.ExecutionResolution.HashOnlyHits
	f.retiredCounters.NodeHashHits += stats.ExecutionResolution.NodeHashHits
	s.trie.Close()
	delete(f.sessions, id)
	f.evictions++
}

func (b *ForestBlock) TrieReader(id uint64, root mpt.Hash) (mpt.Store, error) {
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()
	return b.reader(id, root)
}
func (b *ForestBlock) BaseView() *View { return b.base }
func (b *ForestBlock) StorageTrieID(a [20]byte) (uint64, bool, error) {
	if err := b.check(); err != nil {
		return 0, false, err
	}
	return b.update.StorageTrieID(a)
}
func (b *ForestBlock) CreateStorageTrie(a [20]byte) (uint64, error) {
	if err := b.check(); err != nil {
		return 0, err
	}
	return b.update.CreateStorageTrie(a)
}
func (b *ForestBlock) DeleteStorageTrie(a [20]byte) error {
	if err := b.check(); err != nil {
		return err
	}
	id, active, err := b.update.StorageTrieID(a)
	if err != nil {
		return err
	}
	if err = b.update.DeleteStorageTrie(a); err == nil && active {
		b.retired[id] = true
	}
	return err
}
func (b *ForestBlock) SetBlockID(id [32]byte) error {
	if err := b.check(); err != nil {
		return err
	}
	return b.update.SetBlockID(id)
}
func (b *ForestBlock) PutCode(h mpt.Hash, code []byte) error {
	if err := b.check(); err != nil {
		return err
	}
	return b.update.PutCode(h, code)
}
func (b *ForestBlock) SetTrie(id uint64, tree *mpt.Trie) error {
	f := b.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := b.check(); err != nil {
		return err
	}
	if tree == nil {
		return ErrConflict
	}
	if f.boundary != nil {
		return b.update.SetTrie(id, tree, 0)
	}
	if f.shared != nil {
		return b.update.SetTrie(id, tree, 0)
	}
	if b.blocks[id] == nil && b.base != nil {
		// New lifetimes have no base root or resident state to inherit.
		if _, created := b.update.created[id]; !created {
			root, err := b.base.Root(id)
			if err != nil {
				return err
			}
			if _, err = b.reader(id, root); err != nil {
				return err
			}
		}
	}
	if err := b.update.SetTrie(id, tree, 0); err != nil {
		return err
	}
	if block := b.blocks[id]; block != nil {
		block.tree = tree
	}
	return nil
}

func (b *ForestBlock) Commit() (Head, CommitStats, error) {
	b.FinishReads()
	f := b.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := b.check(); err != nil {
		return Head{}, CommitStats{}, err
	}
	if f.boundary != nil {
		return b.commitBoundary()
	}
	if f.shared != nil {
		return b.commitShared()
	}
	for _, block := range b.blocks {
		block.reader.observing = false
	}
	defer func() {
		for _, block := range b.blocks {
			if block.reader != nil {
				block.reader.observing = true
			}
		}
	}()
	ids := make([]uint64, 0, len(b.blocks))
	for id := range b.blocks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	deltas := map[uint64]CacheStats{}
	roots := map[uint64]mpt.Hash{}
	for _, id := range ids {
		if b.retired[id] {
			continue
		}
		block := b.blocks[id]
		root, delta, err := block.prepare()
		if err != nil {
			return b.update.base, CommitStats{}, err
		}
		roots[id], deltas[id] = root, delta
	}
	selected := map[uint64]uint64{}
	head, stats, err := b.update.CommitWithLayout(func(key NodeKey, eligible uint16) (uint16, error) {
		block := b.blocks[key.TrieID]
		if block == nil || b.retired[key.TrieID] {
			return 0, nil
		}
		mask, err := block.owner.cache.layout(key, eligible)
		selected[key.TrieID] += uint64(bits.OnesCount16(eligible &^ mask))
		return mask, err
	})
	if err != nil {
		if errors.Is(err, ErrUncertain) {
			for _, s := range f.sessions {
				s.trie.uncertain = true
			}
			b.release()
		}
		return head, stats, err
	}
	for _, id := range ids {
		if b.retired[id] {
			continue
		}
		c := b.blocks[id].owner
		delta := deltas[id]
		delta.HashReferenceSelections = selected[id]
		c.addStats(delta)
		c.cache.install()
		c.root = roots[id]
	}
	for id, s := range f.sessions {
		if b.retired[id] {
			f.drop(id)
			continue
		}
		s.trie.head = head
	}
	b.release()
	return head, stats, nil
}
func (b *ForestBlock) release() {
	if b.reads != nil {
		b.owner.readBacking = b.reads.close()
		b.owner.readStats.add(b.reads.snapshotStats())
	}
	for _, block := range b.blocks {
		block.Abort()
	}
	if b.base != nil {
		b.base.Close()
	}
	b.update.Abort()
	b.base = nil
	b.blocks = nil
	b.sharedReaders = nil
	b.boundaryReaders = nil
	if f := b.owner.boundary; f != nil {
		clearBoundary(f.candidateBody[:f.candidateHighWater])
		f.candidateHighWater = 0
		f.candidateN, f.candidateUsed = 0, 0
		clearBoundary(f.candidateIndex)
	}
	if b.owner.shared != nil {
		b.owner.shared.observed.N, b.owner.shared.observed.Used = 0, 0
		for i := 0; i < b.owner.shared.count; i++ {
			b.owner.shared.graph[i].Body = nil
		}
		b.owner.shared.count, b.owner.shared.rootN = 0, 0
	}
	b.done = true
	if b.owner.active == b {
		b.owner.active = nil
	}
}
func (b *ForestBlock) Abort() {
	b.FinishReads()
	f := b.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if !b.done {
		b.release()
	}
}
func (f *CachedForest) Close() error {
	f.mu.Lock()
	active := f.active
	f.mu.Unlock()
	if active != nil {
		active.FinishReads()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.active != nil {
		f.active.release()
	}
	for _, s := range f.sessions {
		s.trie.Close()
	}
	f.sessions = nil
	f.readBacking = nil
	f.shared = nil
	f.boundary = nil
	f.closed = true
	return nil
}
func (f *CachedForest) Stats() (ForestCacheStats, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ForestCacheStats{}, ErrClosed
	}
	if f.boundary != nil {
		out := f.boundary.statsSnapshot()
		out.ExecutionReads = f.readStats
		if f.active != nil && f.active.reads != nil {
			out.ExecutionReads.add(f.active.reads.snapshotStats())
		}
		out.ExecutionReadBudgetBytes = executionReadBudget(f.cfg.Boundary.ExecutionReads)
		return out, nil
	}
	if f.shared != nil {
		return f.shared.cacheStats(), nil
	}
	out := ForestCacheStats{Mode: PartitionedCache, Slots: f.cfg.Slots, Partitions: uint64(len(f.sessions)), DiskReaders: f.diskReads, Admissions: f.admissions, Evictions: f.evictions}
	out.NodeBodyHits, out.NodeBodyMisses = f.retiredCounters.NodeBodyHits, f.retiredCounters.NodeBodyMisses
	out.HashOnlyHits, out.NodeHashHits = f.retiredCounters.HashOnlyHits, f.retiredCounters.NodeHashHits
	for _, p := range f.sessions {
		s, _ := p.trie.Stats()
		out.NodeAllocatedBytes += s.NodeAllocatedBytes
		out.HashAllocatedBytes += s.HashAllocatedBytes
		out.PlanAllocatedBytes += s.PlanAllocatedBytes
		out.CandidateAllocatedBytes += s.CandidateAllocatedBytes
		out.PolicyAllocatedBytes += s.PolicyAllocatedBytes
		out.InstanceBytes += s.InstanceBytes
		out.NodeBodyHits += s.NodeBodyHits
		out.NodeBodyMisses += s.NodeBodyMisses
		out.HashOnlyHits += s.ExecutionResolution.HashOnlyHits
		out.NodeHashHits += s.ExecutionResolution.NodeHashHits
	}
	return out, nil
}

// CommitInitialHash installs the initial forest in one atomic write and skips
// cache planning/admission. Construction must not prewarm execution statistics.
func (b *ForestBlock) CommitInitialHash() (Head, CommitStats, error) {
	f := b.owner
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := b.check(); err != nil {
		return Head{}, CommitStats{}, err
	}
	if b.update.base.Initialized {
		return b.update.base, CommitStats{}, ErrConflict
	}
	if f.boundary != nil {
		b.update.initialNodeCheck = func(k NodeKey, raw []byte) error { return f.boundary.validateNode(position(k.TrieID, k.Path), raw) }
	}
	head, stats, err := b.update.CommitInitialHash()
	if err != nil {
		if errors.Is(err, ErrUncertain) {
			if f.boundary != nil {
				f.boundary.uncertain = true
			}
			if f.shared != nil {
				f.shared.uncertain = true
			}
			for _, s := range f.sessions {
				s.trie.uncertain = true
			}
			b.release()
		}
		return head, stats, err
	}
	if f.boundary != nil {
		f.boundary.head = head
		f.boundary.generation++
	}
	if f.shared != nil {
		f.shared.head = head
		f.shared.generation++
	}
	b.release()
	return head, stats, nil
}

// FinishReads seals optional prefetch and joins raw I/O before final hashing.
// Mandatory synchronous reads remain available until this block is released.
func (b *ForestBlock) FinishReads() {
	if b.reads != nil {
		b.reads.sealAndDrain()
	}
}
