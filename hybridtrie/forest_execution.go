package hybridtrie

import (
	"bytes"
	"errors"
	"sort"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"time"
	"unsafe"
)

type forestReader struct {
	block      *ForestBlock
	base       *trieReader
	head       Head
	generation uint64
}

func (r *forestReader) Get([]byte) ([]byte, error) { return nil, ErrReadOnly }
func (r *forestReader) NewBatch() mpt.Batch        { return deniedBatch{} }
func (b *ForestBlock) sharedReader(id uint64, root mpt.Hash) (mpt.Store, error) {
	if r := b.sharedReaders[id]; r != nil {
		if r.base.root != root {
			return nil, ErrHashMismatch
		}
		return r, nil
	}
	r, err := b.base.TrieReader(id, root)
	if err != nil {
		return nil, err
	}
	x := &forestReader{block: b, base: r.(*trieReader), head: b.update.base, generation: b.owner.shared.generation}
	b.sharedReaders[id] = x
	return x, nil
}
func (r *forestReader) scope() bool {
	b := r.block
	f := b.owner.shared
	return !b.done && b.owner.active == b && !b.owner.closed && f != nil && !f.uncertain && f.head == r.head && f.generation == r.generation && b.base == r.base.view && r.head == b.update.base
}
func (r *forestReader) lookup(ctx NodeKey) (mpt.Hash, uint8, bool) {
	if !r.scope() || ctx.TrieID != r.base.id || ctx.Height != r.head.Height {
		return mpt.Hash{}, diskState, false
	}
	f := r.block.owner.shared
	pos := position(ctx.TrieID, ctx.Path)
	if i := f.current.node(pos); i >= 0 {
		return f.current.Nodes[i].Hash, nodeState, true
	}
	if i := f.current.hash(pos); i >= 0 {
		return f.current.Hashes[i].Hash, hashState, true
	}
	return mpt.Hash{}, diskState, false
}
func (r *forestReader) GetNode(path []byte, expected mpt.Hash) ([]byte, error) {
	// Forest commit already owns this lock. Store projection may resolve a
	// lazy edge through the original EVM tree's reader while persisting it.
	// Execution/Commit are single-owner; Stats/Close remain serialized by mu.
	if r.block.sharedPreparing {
		return r.getNode(path, expected)
	}
	r.block.owner.mu.Lock()
	defer r.block.owner.mu.Unlock()
	return r.getNode(path, expected)
}

type forestPreparationReader struct{ r *forestReader }

func (r forestPreparationReader) Get([]byte) ([]byte, error) { return nil, ErrReadOnly }
func (r forestPreparationReader) NewBatch() mpt.Batch        { return deniedBatch{} }
func (r forestPreparationReader) GetNode(p []byte, h mpt.Hash) ([]byte, error) {
	return r.r.getNode(p, h)
}
func (r *forestReader) getNode(path []byte, expected mpt.Hash) ([]byte, error) {
	if !r.scope() {
		return nil, ErrClosed
	}
	if _, err := pack(path); err != nil {
		return nil, err
	}
	b := r.block
	f := b.owner.shared
	pos := position(r.base.id, path)
	stats := &f.stats.PreparationResolution
	observing := !b.sharedPreparing
	if observing {
		stats = &f.stats.ExecutionResolution
	}
	var raw []byte
	v := forestVersion{Source: Hash}
	i := f.current.node(pos)
	hit := i >= 0 && f.current.Nodes[i].Hash == expected
	if hit {
		raw = f.current.raw(i)
		v = f.current.Nodes[i].Version
	} else {
		var err error
		raw, err = r.base.getNodeResolved(path, expected, r.lookup, stats, func(query, actual NodeKey) {
			if bytes.Equal(query.Path, path) {
				v = forestVersion{Height: actual.Height, Source: Prefix, Known: true}
			}
		})
		if err != nil {
			return nil, err
		}
	}
	if observing {
		cs := &f.stats.Account
		if pos.ID != 0 {
			cs = &f.stats.Storage
		}
		if hit {
			cs.NodeHits++
		} else {
			cs.NodeMisses++
		}
		t := time.Now()
		if f.event(pos) {
			f.stats.BodyAccessEvents++
			if hit {
				f.current.Nodes[i].A.event(b.update.height, f.cfg.HalfLife)
			} else {
				f.heat.sketch[cacheClass(pos.ID)].increment(pos.ID, path)
			}
		}
		f.stats.HeatNanos += uint64(time.Since(t))
		if !hit {
			f.observe(pos, expected, raw, v)
		}
	}
	return append([]byte(nil), raw...), nil
}

var errCacheCandidatesFull = errors.New("optional forest candidates exhausted")

func (b *ForestBlock) prepareShared() error {
	f := b.owner.shared
	f.scratch.reset()
	for i := 0; i < f.count; i++ {
		f.graph[i].Body = nil
	}
	f.count, f.rootN, f.undoN = 0, 0, 0
	t := time.Now()
	ids := make([]uint64, 0, len(b.update.updates))
	for id := range b.update.updates {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if b.retired[id] {
			continue
		}
		tree := b.update.updates[id].Tree
		root, err := tree.Hash()
		if err != nil {
			return err
		}
		if root == mpt.EmptyRootHash {
			continue
		}
		var before *mpt.Trie
		if b.base != nil {
			oldRoot, err := b.base.Root(id)
			if err == nil {
				reader, err := b.sharedReader(id, oldRoot)
				if err != nil {
					return err
				}
				before, err = mpt.Open(oldRoot, forestPreparationReader{reader.(*forestReader)})
				if err != nil {
					return err
				}
			} else if !errors.Is(err, ErrNotFound) {
				return err
			}
		}
		var beforeCursor *mpt.CanonicalCursor
		if before != nil {
			beforeCursor = before.CanonicalCursor()
		}
		baseRoot := mpt.EmptyRootHash
		if before != nil {
			baseRoot, err = before.Hash()
			if err != nil {
				return err
			}
		}
		_, err = tree.PendingLogicalNodes(baseRoot, b.owner.store.cfg.MaxProjectionNodes, func(change mpt.LogicalChange) error {
			if f.observed.N == len(f.observed.Entries) || len(change.RLP) > len(f.observed.Body)-f.observed.Used {
				f.stats.DroppedCandidates++
				return errCacheCandidatesFull
			}
			var oldHash mpt.Hash
			var oldRaw []byte
			found := false
			if before != nil {
				var err error
				oldHash, oldRaw, found, err = beforeCursor.AtPath(change.Path)
				if err != nil {
					return err
				}
			}
			updated := !found || oldHash != change.Hash
			reset := !found || len(change.Path) > 0 && len(oldRaw) < 32
			if found && updated {
				a, err := mpt.InspectNodeIdentity(oldRaw)
				if err != nil {
					return err
				}
				c, err := mpt.InspectNodeIdentity(change.RLP)
				if err != nil {
					return err
				}
				reset = a != c
			}
			v := forestVersion{}
			if updated {
				v = forestVersion{Height: b.update.height, Source: Prefix, Known: true}
			} else {
				_, v = f.body(position(id, change.Path), change.Hash)
			}
			f.observe(position(id, change.Path), change.Hash, change.RLP, v)
			for j := f.observed.N - 1; j >= 0; j-- {
				o := &f.observed.Entries[j]
				if o.Pos == position(id, change.Path) && o.Hash == change.Hash {
					o.Updated, o.Reset = updated, reset
					break
				}
			}
			return nil
		})
		if err != nil && !errors.Is(err, errCacheCandidatesFull) {
			return err
		}
	}
	add := func(id uint64, root mpt.Hash) error {
		if b.retired[id] {
			return nil
		}
		if u, ok := b.update.updates[id]; ok {
			var err error
			root, err = u.Tree.Hash()
			if err != nil {
				return err
			}
		}
		f.addRoot(id, root)
		return nil
	}
	for i := 0; i < f.current.R; i++ {
		r := f.current.Roots[i]
		if err := add(r.ID, r.Hash); err != nil {
			return err
		}
	}
	for i := 0; i < f.observed.N; i++ {
		o := f.observed.Entries[i]
		if o.Pos.Length == 0 {
			if err := add(o.Pos.ID, o.Hash); err != nil {
				return err
			}
		}
	}
	if err := f.expand(); err != nil {
		return err
	}
	f.indexGraph()
	f.stats.RepairNanos += uint64(time.Since(t))
	f.remaining = uint64(f.cfg.WorkUnits)
	t = time.Now()
	f.run()
	f.stats.PolicyNanos += uint64(time.Since(t))
	return f.export()
}
func (b *ForestBlock) commitShared() (Head, CommitStats, error) {
	f := b.owner.shared
	b.sharedPreparing = true
	defer func() { b.sharedPreparing = false }()
	if err := b.prepareShared(); err != nil {
		return b.update.base, CommitStats{}, err
	}
	t := time.Now()
	head, stats, err := b.update.CommitWithLayout(func(key NodeKey, eligible uint16) (uint16, error) { return f.layout(key, eligible), nil })
	f.stats.ProjectionNanos += stats.ProjectionNanos
	f.stats.WriteNanos += stats.WriteNanos
	if err != nil {
		if errors.Is(err, ErrUncertain) {
			f.uncertain = true
			b.release()
		}
		return head, stats, err
	}
	t = time.Now()
	if f.beforePublish != nil {
		if err := f.beforePublish(); err != nil {
			f.stats.PublicationFailures++
			f.reset()
			f.head = head
			b.release()
			return head, stats, nil
		}
	}
	for i := 0; i < f.current.R; i++ {
		old := f.current.Roots[i]
		found := false
		for j := 0; j < f.scratch.R; j++ {
			if f.scratch.Roots[j].ID == old.ID {
				found = true
				break
			}
		}
		if !found {
			f.stats.FreedTries++
		}
	}
	f.current, f.scratch = f.scratch, f.current
	f.generation++
	f.head = head
	f.stats.PublicationNanos += uint64(time.Since(t))
	b.release()
	return head, stats, nil
}
func (f *forestShared) cacheStats() ForestCacheStats {
	s := f.stats
	s.Generation = f.generation
	s.NodeBudgetBytes = f.cfg.NodeBudget.TotalBytes
	s.HashBudgetBytes = f.cfg.HashBudget.TotalBytes
	s.MetadataAllocatedBytes = f.metadataAllocated
	s.MetadataBudgetBytes = f.cfg.MetadataBytes
	s.PlanBudgetBytes = f.cfg.PlanBytes
	s.CandidateBudgetBytes = f.cfg.CandidateBytes
	s.Account.NodeReserveBytes = f.cfg.NodeBudget.AccountReserveBytes
	s.Account.HashReserveBytes = f.cfg.HashBudget.AccountReserveBytes
	s.Storage.NodeReserveBytes = f.cfg.NodeBudget.StorageReserveBytes
	s.Storage.HashReserveBytes = f.cfg.HashBudget.StorageReserveBytes
	for i := 0; i < f.current.N; i++ {
		e := f.current.Nodes[i]
		cs := &s.Account
		if e.Pos.ID != 0 {
			cs = &s.Storage
		}
		cs.NodeLiveBytes += uint64(unsafe.Sizeof(forestN{})) + uint64(e.Size)
	}
	for i := 0; i < f.current.H; i++ {
		e := f.current.Hashes[i]
		cs := &s.Account
		if e.Pos.ID != 0 {
			cs = &s.Storage
		}
		cs.HashLiveBytes += uint64(unsafe.Sizeof(forestH{}))
	}
	s.Account.NodeSharedBytes = excess(s.Account.NodeLiveBytes, s.Account.NodeReserveBytes)
	s.Account.HashSharedBytes = excess(s.Account.HashLiveBytes, s.Account.HashReserveBytes)
	s.Storage.NodeSharedBytes = excess(s.Storage.NodeLiveBytes, s.Storage.NodeReserveBytes)
	s.Storage.HashSharedBytes = excess(s.Storage.HashLiveBytes, s.Storage.HashReserveBytes)
	s.NodeLiveBytes = s.Account.NodeLiveBytes + s.Storage.NodeLiveBytes
	s.HashLiveBytes = s.Account.HashLiveBytes + s.Storage.HashLiveBytes
	s.NodeEntries = uint64(f.current.N)
	s.HashEntries = uint64(f.current.H)
	return ForestCacheStats{Mode: ReservedSharedCache, Shared: s, Partitions: uint64(f.current.R), NodeAllocatedBytes: f.nodeAllocated, HashAllocatedBytes: f.hashAllocated, PlanAllocatedBytes: f.planAllocated, CandidateAllocatedBytes: f.candidateAllocated, PolicyAllocatedBytes: f.metadataAllocated, InstanceBytes: f.instanceAllocated, NodeBodyHits: s.Account.NodeHits + s.Storage.NodeHits, NodeBodyMisses: s.Account.NodeMisses + s.Storage.NodeMisses, HashOnlyHits: s.ExecutionResolution.HashOnlyHits, NodeHashHits: s.ExecutionResolution.NodeHashHits, Admissions: s.Admissions, Evictions: s.Evictions, DiskReaders: s.Account.NodeMisses + s.Storage.NodeMisses}
}
