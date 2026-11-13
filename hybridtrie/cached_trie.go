package hybridtrie

import (
	"errors"
	"math/bits"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"sync"
	"unsafe"
)

// CachedTrie owns one latest-state execution cache, not the Store. Historical
// Views remain independent. Only one block may be active; all public methods
// are serialized. A closed/committed block retains no native trie or read view.
type CachedTrie struct {
	mu                sync.Mutex
	store             *Store
	id                uint64
	head              Head
	root              mpt.Hash
	cache             *residentCache
	active            *CachedBlock
	tick              uint64
	totals            CacheStats
	closed, uncertain bool
}

type CachedBlock struct {
	owner      *CachedTrie
	height     uint64
	tree       *mpt.Trie
	view       *View
	reader     *cachedReader
	operations uint32
	charged    uint64
}

type cachedReader struct {
	block      *CachedBlock
	base       *trieReader
	head       Head
	generation uint64
	observing  bool
}

// OpenCachedTrie attaches to an already imported snapshot at the Store head.
// Initialization is outside the execution-cache quota. The supplied root must
// match its persisted directory; it is not silently replaced by local metadata.
func OpenCachedTrie(store *Store, id uint64, trusted mpt.Hash, cfg CacheConfig) (*CachedTrie, error) {
	if store == nil {
		return nil, ErrClosed
	}
	cache, err := newResidentCache(cfg)
	if err != nil {
		return nil, err
	}
	head, err := store.Head()
	if err != nil {
		return nil, err
	}
	if !head.Initialized {
		return nil, ErrNotFound
	}
	view, err := store.ViewAt(head.Height)
	if err != nil {
		return nil, err
	}
	defer view.Close()
	reader, err := view.TrieReader(id, trusted)
	if err != nil {
		return nil, err
	}
	var raw []byte
	if trusted != mpt.EmptyRootHash {
		raw, err = reader.(mpt.PathStore).GetNode(nil, trusted)
		if err != nil {
			return nil, err
		}
	}
	cache.trieID = id
	c := &CachedTrie{store: store, id: id, head: head, root: trusted, cache: cache, tick: 1}
	cache.instanceCharge = page(uint64(unsafe.Sizeof(*cache) + unsafe.Sizeof(*c) + unsafe.Sizeof(CachedBlock{}) + unsafe.Sizeof(cachedReader{}) + unsafe.Sizeof(cachePolicy{})))
	stats, err := cache.prepare(trusted, raw, head.Height)
	if err != nil {
		return nil, err
	}
	cache.install()
	c.addStats(stats)
	c.totals.PolicyMetrics.add(stats.PolicyMetrics)
	return c, nil
}

func (c *CachedTrie) check() error {
	if c.closed {
		return ErrClosed
	}
	if c.uncertain {
		return ErrUncertain
	}
	return nil
}

// BeginBlock pins the base snapshot. An external writer invalidates this
// session's head; open a new session rather than mixing cache generations.
func (c *CachedTrie) BeginBlock(height uint64) (*CachedBlock, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.check(); err != nil {
		return nil, err
	}
	if c.active != nil {
		return nil, ErrBusy
	}
	head, err := c.store.Head()
	if err != nil {
		return nil, err
	}
	if head != c.head || c.head.Height == MaxHeight || height != c.head.Height+1 {
		return nil, ErrConflict
	}
	view, err := c.store.ViewAt(c.head.Height)
	if err != nil {
		return nil, err
	}
	reader, err := view.TrieReader(c.id, c.root)
	if err != nil {
		view.Close()
		return nil, err
	}
	c.cache.observed.reset()
	c.cache.policy.heat.begin(height)
	b := &CachedBlock{owner: c, height: height, view: view}
	b.reader = &cachedReader{block: b, base: reader.(*trieReader), head: c.head, generation: c.cache.generation, observing: false}
	b.tree, err = mpt.Open(c.root, b.reader)
	if err != nil {
		b.release()
		return nil, err
	}
	b.reader.observing = true
	c.active = b
	return b, nil
}

func (c *CachedTrie) nextTick() uint64 {
	// An unreachable-in-practice overflow must not reverse recency order.
	if c.tick != ^uint64(0) {
		c.tick++
	}
	return c.tick
}

func (b *CachedBlock) check() error {
	if err := b.owner.check(); err != nil {
		return err
	}
	if b.owner.active != b || b.tree == nil {
		return ErrClosed
	}
	return nil
}
func (b *CachedBlock) charge(n uint64) error {
	if n > b.owner.cache.cfg.MaxBlockCharge-b.charged {
		return ErrLimit
	}
	b.charged += n
	if b.charged > b.owner.totals.MaxBlockChargedBytes {
		b.owner.totals.MaxBlockChargedBytes = b.charged
	}
	return nil
}
func (b *CachedBlock) operation(key []byte, valueBytes int, write bool) error {
	if err := b.check(); err != nil {
		return err
	}
	if len(key) > 32 || b.operations >= b.owner.cache.cfg.MaxOperations {
		return ErrLimit
	}
	n := uint64(4096)
	if write {
		if valueBytes > mpt.MaxCanonicalNodeBytes {
			return ErrLimit
		}
		n += 65*4096 + 8*uint64(valueBytes)
	}
	if err := b.charge(n); err != nil {
		return err
	}
	// Open now retains its decoded root. Logical root access must not depend
	// on forcing a second physical read; the legacy heat table deduplicates
	// this event within the block and still enforces its bounded budget.
	if b.owner.root != mpt.EmptyRootHash {
		if err := b.owner.cache.observeBody(nil, b.height); err != nil {
			return err
		}
	}
	b.operations++
	return nil
}

func (b *CachedBlock) Get(key []byte) ([]byte, error) {
	c := b.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := b.operation(key, 0, false); err != nil {
		return nil, err
	}
	return b.tree.Get(key)
}
func (b *CachedBlock) Update(key, value []byte) error {
	c := b.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := b.operation(key, len(value), true); err != nil {
		return err
	}
	return b.tree.Update(key, value)
}
func (b *CachedBlock) Delete(key []byte) error { return b.Update(key, nil) }
func (b *CachedBlock) Root() (mpt.Hash, error) {
	c := b.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := b.check(); err != nil {
		return mpt.Hash{}, err
	}
	return b.tree.Hash()
}

func (r *cachedReader) Get([]byte) ([]byte, error) { return nil, ErrReadOnly }
func (r *cachedReader) NewBatch() mpt.Batch        { return deniedBatch{} }

func (r *cachedReader) cacheScope() bool {
	c, v := r.block.owner, r.block.view
	return v != nil && c.cache != nil && !c.closed && !c.uncertain &&
		r.head == c.head && r.generation == c.cache.generation &&
		r.base.view == v && v.store == c.store && r.base.id == c.id &&
		c.cache.trieID == c.id && r.base.height == r.head.Height &&
		r.base.root == c.root
}

func (r *cachedReader) lookupHash(ctx NodeKey) (mpt.Hash, uint8, bool) {
	if !r.cacheScope() || ctx.TrieID != r.base.id || ctx.Height != r.base.height {
		return mpt.Hash{}, diskState, false
	}
	return r.block.owner.cache.lookupHash(ctx.Path, r.generation)
}

func (r *cachedReader) GetNode(path []byte, expected mpt.Hash) ([]byte, error) {
	b, c := r.block, r.block.owner
	if b.view == nil || c.closed || c.uncertain {
		return nil, ErrClosed
	}
	var raw []byte
	i, hit := 0, false
	if r.cacheScope() {
		i, hit = c.cache.lookupBody(path, expected, r.generation)
	}
	if hit {
		raw = c.cache.current.raw(i)
		if r.observing {
			c.totals.NodeBodyHits++
			c.cache.current.nodes[i].Tick = c.nextTick()
		} else {
			c.totals.PreparationHits++
		}
	} else {
		if r.observing {
			c.totals.NodeBodyMisses++
		} else {
			c.totals.PreparationMisses++
		}
		var err error
		stats := &c.totals.PreparationResolution
		if r.observing {
			stats = &c.totals.ExecutionResolution
		}
		raw, err = r.base.getNode(path, expected, r.lookupHash, stats)
		if err != nil {
			return nil, err
		}
	}
	if r.observing {
		if err := c.cache.observeBody(path, b.height); err != nil {
			return nil, err
		}
		if err := b.charge(4096 + 8*uint64(len(raw))); err != nil {
			return nil, err
		}
		if !hit {
			c.cache.observed.add(path, expected, raw, c.nextTick())
		}
	}
	// Never expose an arena slice to native decoding or a caller.
	return append([]byte(nil), raw...), nil
}

// prepare finalizes one shadow cache without publishing it. Forest commits reuse
// this exact policy/identity path and install every cache only after one write.
func (b *CachedBlock) prepare() (mpt.Hash, CacheStats, error) {
	c := b.owner
	root, err := b.tree.Hash()
	if err != nil {
		return mpt.Hash{}, CacheStats{}, err
	}
	var raw []byte
	if root != mpt.EmptyRootHash {
		var got mpt.Hash
		var exists bool
		got, raw, exists, err = b.tree.CanonicalNodeAtPath(nil)
		if err != nil {
			return mpt.Hash{}, CacheStats{}, err
		}
		if !exists || got != root {
			return mpt.Hash{}, CacheStats{}, ErrHashMismatch
		}
	}
	c.cache.policy.heat.clearUpdates()
	before, err := mpt.Open(c.root, b.reader)
	if err != nil {
		return mpt.Hash{}, CacheStats{}, err
	}
	beforeCursor := before.CanonicalCursor()
	_, err = b.tree.PendingLogicalNodes(c.root, uint64(c.cache.cfg.MaxOperations)*65+1, func(change mpt.LogicalChange) error {
		oldHash, oldRaw, found, err := beforeCursor.AtPath(change.Path)
		if err != nil {
			return err
		}
		if !found || oldHash != change.Hash {
			if err = c.cache.observeUpdate(change.Path); err != nil {
				return err
			}
		}
		newHost := !found || (len(change.Path) > 0 && len(oldRaw) < 32)
		if found && oldHash != change.Hash && !newHost {
			oldIdentity, err := mpt.InspectNodeIdentity(oldRaw)
			if err != nil {
				return err
			}
			newIdentity, err := mpt.InspectNodeIdentity(change.RLP)
			if err != nil {
				return err
			}
			newHost = oldIdentity != newIdentity
			// observeUpdate above already reserved this event slot. Reuse the
			// classification rather than parsing these identities again in plan.
			c.cache.policy.heat.find(change.Path).Flags |= eventIdentityChecked
		}
		// Normal value/digest updates retain statistics. New structural hosts
		// supply a separate candidate observation, even at an old N position.
		if newHost {
			if err = c.cache.observeNewHost(change.Path, b.height); err != nil {
				return err
			}
		}
		c.cache.observed.add(change.Path, change.Hash, change.RLP, c.nextTick())
		return nil
	})
	if err != nil {
		return mpt.Hash{}, CacheStats{}, err
	}
	delta, err := c.cache.prepare(root, raw, b.height)
	c.totals.PolicyMetrics.add(c.cache.policy.metrics)
	if err != nil {
		return mpt.Hash{}, CacheStats{}, err
	}
	return root, delta, nil
}

// Commit prepares a shadow cache without prefetch, chooses new physical
// references, and installs the layout only after the synchronous atomic write.
// A pre-write error keeps the active working trie and old resident layout.
func (b *CachedBlock) Commit() (Head, CommitStats, error) {
	c := b.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := b.check(); err != nil {
		return c.head, CommitStats{}, err
	}
	// Suppress observations through physical projection as well
	// as policy preparation. These reads must not become execution heat.
	b.reader.observing = false
	defer func() {
		if b.reader != nil {
			b.reader.observing = true
		}
	}()
	root, delta, err := b.prepare()
	if err != nil {
		return c.head, CommitStats{}, err
	}
	var selections uint64
	head, stats, err := c.store.CommitWithLayout(b.height, c.head.CommitID, []TrieUpdate{{TrieID: c.id, Tree: b.tree}}, func(key NodeKey, eligible uint16) (uint16, error) {
		if key.TrieID != c.id {
			return 0, ErrConflict
		}
		mask, err := c.cache.layout(key, eligible)
		selections += uint64(bits.OnesCount16(eligible &^ mask))
		return mask, err
	})
	if err != nil {
		if errors.Is(err, ErrUncertain) {
			c.uncertain = true
			b.release()
		}
		return c.head, stats, err
	}
	delta.HashReferenceSelections = selections
	c.addStats(delta)
	c.cache.install()
	c.head, c.root = head, root
	b.release()
	return head, stats, nil
}

func (b *CachedBlock) release() {
	if b.view != nil {
		b.view.Close()
	}
	b.view, b.tree, b.reader = nil, nil, nil
	if b.owner.active == b {
		b.owner.active = nil
	}
}
func (b *CachedBlock) Abort() error {
	c := b.owner
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active != b {
		return nil
	}
	c.cache.observed.reset()
	b.release()
	return nil
}
func (c *CachedTrie) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.active != nil {
		c.active.release()
	}
	c.closed = true
	c.cache = nil
	return nil
}
func (c *CachedTrie) addStats(s CacheStats) {
	c.totals.NodeAdmissions += s.NodeAdmissions
	c.totals.NodeEvictions += s.NodeEvictions
	c.totals.HashAdmissions += s.HashAdmissions
	c.totals.HashEvictions += s.HashEvictions
	c.totals.Promotions += s.Promotions
	c.totals.Demotions += s.Demotions
	c.totals.DroppedCandidates += s.DroppedCandidates
	c.totals.HashReferenceSelections += s.HashReferenceSelections
}

// Stats reports rounded-up backing-array allocation charges, actual live entry
// bytes and execution versus commit-preparation reads. It is not process RSS.
func (c *CachedTrie) Stats() (CacheStats, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return CacheStats{}, ErrClosed
	}
	s := c.cache.stats()
	s.PolicyMetrics = c.totals.PolicyMetrics
	s.ExecutionResolution = c.totals.ExecutionResolution
	s.PreparationResolution = c.totals.PreparationResolution
	t := c.totals
	s.NodeBodyHits, s.NodeBodyMisses = t.NodeBodyHits, t.NodeBodyMisses
	s.PreparationHits, s.PreparationMisses = t.PreparationHits, t.PreparationMisses
	s.HashReferenceSelections = t.HashReferenceSelections
	s.NodeAdmissions, s.NodeEvictions = t.NodeAdmissions, t.NodeEvictions
	s.HashAdmissions, s.HashEvictions = t.HashAdmissions, t.HashEvictions
	s.Promotions, s.Demotions, s.DroppedCandidates = t.Promotions, t.Demotions, t.DroppedCandidates
	s.MaxBlockChargedBytes = t.MaxBlockChargedBytes
	return s, nil
}
