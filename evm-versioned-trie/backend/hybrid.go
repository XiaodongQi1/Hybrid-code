package backend

import (
	"errors"
	"fmt"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
)

// HybridConfig preserves the v6 physical codec. Cache bounds retained execution
// partitions; historical reads are independent. The live adapter uses native
// Ethereum account/storage RLP exclusively; the old backend keeps both codecs.
const (
	InitializationHashCold     = "hash-cold"
	InitializationHashColdBulk = "hash-cold-bulk"
	InitializationPrefix       = "prefix"
)

type HybridConfig struct {
	ParallelProof  ht.ParallelProofConfig `json:",omitempty,omitzero"`
	Initialization string
	Store          ht.Config
	Cache          ht.ForestCacheConfig
	Snapshot       *SnapshotConfig
}

func DefaultHybridConfig(workload string) HybridConfig {
	return HybridConfig{Store: ht.DefaultConfig(string(codec.Ethereum), workload), Cache: ht.DefaultForestCacheConfig()}
}
func OpenHybrid(path string, cfg HybridConfig, create, readOnly bool) (*Backend, error) {
	if e := cfg.ParallelProof.Validate(); e != nil {
		return nil, hybridError(e)
	}
	if cfg.Store.CodecID != string(codec.Ethereum) {
		return nil, fmt.Errorf("hybrid EVM requires ethereum-mpt codec")
	}
	if cfg.Initialization != "" && cfg.Initialization != InitializationPrefix && cfg.Initialization != InitializationHashCold && cfg.Initialization != InitializationHashColdBulk {
		return nil, fmt.Errorf("unknown hybrid initialization: %s", cfg.Initialization)
	}
	if cfg.Snapshot != nil && cfg.Initialization == InitializationHashCold {
		return nil, fmt.Errorf("hash-cold initialization conflicts with streaming HybridSnapshot")
	}
	if cfg.Initialization == InitializationHashColdBulk && cfg.Snapshot == nil {
		c := DefaultHashColdBulkSnapshotConfig()
		cfg.Snapshot = &c
	}
	if cfg.Snapshot != nil {
		if e := cfg.Snapshot.validate(); e != nil {
			return nil, e
		}
	}
	s, err := ht.Open(path, cfg.Store, create, readOnly)
	if err != nil {
		return nil, hybridError(err)
	}
	adapter := &hybridStore{store: s, path: path, sourceID: cfg.Store.WorkloadID, snapshot: cfg.Snapshot, initialHash: cfg.Initialization == InitializationHashCold || cfg.Initialization == InitializationHashColdBulk, storageConfigured: cfg.Store.Storage != nil, parallelProof: cfg.ParallelProof}
	if _, err = adapter.Head(); err != nil {
		s.Close()
		return nil, err
	}
	if !readOnly {
		adapter.cache, err = ht.OpenCachedForest(s, cfg.Cache)
		if err != nil {
			s.Close()
			return nil, hybridError(err)
		}
	}
	return &Backend{store: adapter, initialHeight: cfg.Store.InitialHeight, maxHeight: ht.MaxHeight, mode: codec.Ethereum, readOnly: readOnly}, nil
}

// Keep backend error classes stable across storage adapters.
func hybridError(err error) error {
	if err == nil {
		return nil
	}
	for _, pair := range [][2]error{{ht.ErrReadOnly, vt.ErrReadOnly}, {ht.ErrLimit, vt.ErrLimit}, {ht.ErrConflict, vt.ErrConflict}, {ht.ErrNotFound, vt.ErrNotFound}, {ht.ErrClosed, vt.ErrClosed}, {ht.ErrBusy, vt.ErrBusy}, {ht.ErrSnapshotPending, vt.ErrBusy}, {ht.ErrUncertain, vt.ErrUncertain}, {ht.ErrCorrupt, vt.ErrCorrupt}, {ht.ErrHashMismatch, vt.ErrCorrupt}} {
		if errors.Is(err, pair[0]) {
			return fmt.Errorf("%w: %v", pair[1], err)
		}
	}
	return err
}

type hybridStore struct {
	parallelProof     ht.ParallelProofConfig
	storageConfigured bool
	initialHash       bool
	lastCommit        ht.CommitStats
	path, sourceID    string
	snapshot          *SnapshotConfig
	store             *ht.Store
	cache             *ht.CachedForest
	work              *ht.ForestBlock
	workHeight        uint64
}

func (s *hybridStore) Head() (Head, error) {
	h, err := s.store.Head()
	if err != nil {
		return Head{}, hybridError(err)
	}
	out := Head{Initialized: h.Initialized, Height: h.Height, Root: mpt.EmptyRootHash, NextTrieID: h.NextTrieID}
	if !h.Initialized {
		return out, nil
	}
	v, err := s.store.ViewAt(h.Height)
	if err != nil {
		return Head{}, hybridError(err)
	}
	defer v.Close()
	out.Root, err = v.Root(ht.AccountTrieID)
	if err != nil {
		return Head{}, hybridError(err)
	}
	out.CommitID, err = v.BlockID()
	if err != nil {
		return Head{}, hybridError(err)
	}
	return out, nil
}
func (s *hybridStore) Close() error {
	// A busy Close must leave an otherwise usable execution cache intact.
	if err := s.store.Close(); err != nil {
		return hybridError(err)
	}
	if s.cache != nil {
		s.cache.Close()
	}
	s.work = nil
	return nil
}
func (s *hybridStore) ViewAt(h uint64) (stateSnapshot, error) {
	return wrapHistoricalView(s.store.ViewAt(h))
}
func (s *hybridStore) proofViewAt(h uint64) (stateSnapshot, error) {
	return wrapHistoricalView(s.store.ProofViewAt(h))
}
func wrapHistoricalView(v *ht.View, err error) (stateSnapshot, error) {
	if err != nil {
		return nil, hybridError(err)
	}
	out, err := newHybridSnapshot(v, nil)
	if err != nil {
		v.Close()
	}
	return out, err
}
func (s *hybridStore) start(height uint64) (*ht.ForestBlock, error) {
	if s.work != nil {
		return nil, vt.ErrBusy
	}
	if s.cache == nil {
		return nil, vt.ErrReadOnly
	}
	h, err := s.store.Head()
	if err != nil {
		return nil, hybridError(err)
	}
	w, err := s.cache.BeginBlock(height, h.CommitID)
	if err != nil {
		return nil, hybridError(err)
	}
	s.work, s.workHeight = w, height
	return w, nil
}
func (s *hybridStore) finish(w *ht.ForestBlock) {
	w.Abort()
	if s.work == w {
		s.work = nil
	}
}
func (s *hybridStore) ExecutionView(height uint64) (stateSnapshot, func(), error) {
	w, err := s.start(height)
	if err != nil {
		return nil, nil, err
	}
	abort := func() { s.finish(w) }
	if w.BaseView() == nil {
		return nil, abort, nil
	}
	v, err := newHybridSnapshot(w.BaseView(), w)
	if err != nil {
		abort()
		return nil, nil, err
	}
	return v, abort, nil
}
func (s *hybridStore) Begin(height uint64, root mpt.Hash, id [32]byte) (stateUpdate, error) {
	head, err := s.Head()
	if err != nil {
		return nil, err
	}
	w := s.work
	if w == nil {
		w, err = s.start(height)
		if err != nil {
			return nil, err
		}
	}
	if s.workHeight != height {
		return nil, vt.ErrConflict
	}
	if err = w.SetBlockID(id); err != nil {
		s.finish(w)
		return nil, hybridError(err)
	}
	return &hybridUpdate{store: s, work: w, before: head.Root, root: root, initialHash: !head.Initialized && s.initialHash}, nil
}

type hybridSnapshot struct {
	view *ht.View
	work *ht.ForestBlock
	root mpt.Hash
	id   [32]byte
}

func newHybridSnapshot(v *ht.View, w *ht.ForestBlock) (*hybridSnapshot, error) {
	r, err := v.Root(ht.AccountTrieID)
	if err != nil {
		return nil, hybridError(err)
	}
	id, err := v.BlockID()
	if err != nil {
		return nil, hybridError(err)
	}
	return &hybridSnapshot{v, w, r, id}, nil
}
func (v *hybridSnapshot) Root() mpt.Hash     { return v.root }
func (v *hybridSnapshot) CommitID() [32]byte { return v.id }
func (v *hybridSnapshot) Close() error {
	if v.work != nil {
		return nil
	} // The forest owns the execution snapshot.
	return hybridError(v.view.Close())
}
func (v *hybridSnapshot) Reader(id uint64, root mpt.Hash) (mpt.Store, error) {
	if v.work != nil {
		r, e := v.work.TrieReader(id, root)
		return r, hybridError(e)
	}
	r, e := v.view.TrieReader(id, root)
	return r, hybridError(e)
}
func (v *hybridSnapshot) StorageID(a common.Address) (uint64, bool, error) {
	id, active, e := v.view.StorageTrieID(a)
	return id, active, hybridError(e)
}
func (v *hybridSnapshot) Code(h mpt.Hash) ([]byte, error) {
	raw, e := v.view.Code(h)
	return raw, hybridError(e)
}
func (v *hybridSnapshot) HistoricalValueProof(id uint64, root mpt.Hash, key []byte, serving bool) (mpt.ProofResult, error) {
	prove := v.view.ProveValueWithStats
	if serving {
		prove = v.view.AssembleValueProofWithStats
	}
	p, _, e := prove(id, root, key)
	return p, hybridError(e)
}

type hybridUpdate struct {
	initialHash  bool
	store        *hybridStore
	work         *ht.ForestBlock
	before, root mpt.Hash
}

func (u *hybridUpdate) PutCode(h mpt.Hash, code []byte) error {
	return hybridError(u.work.PutCode(h, code))
}
func (u *hybridUpdate) Storage(p storagePlan) error {
	id := p.oldID
	if p.active && p.reset {
		if err := u.work.DeleteStorageTrie(p.address); err != nil {
			return hybridError(err)
		}
	}
	if (!p.active || p.reset) && p.needed {
		var err error
		id, err = u.work.CreateStorageTrie(p.address)
		if err != nil {
			return hybridError(err)
		}
	}
	if p.tree != nil && (p.needed || p.active && !p.reset) {
		return hybridError(u.work.SetTrie(id, p.tree))
	}
	return nil
}
func (u *hybridUpdate) Accounts(before mpt.Hash, t *mpt.Trie) error {
	if before != u.before {
		return vt.ErrConflict
	}
	r, err := t.Hash()
	if err != nil {
		return err
	}
	if r != u.root {
		return vt.ErrCorrupt
	}
	return hybridError(u.work.SetTrie(ht.AccountTrieID, t))
}
func (u *hybridUpdate) Commit() error {
	var stats ht.CommitStats
	var e error
	if u.initialHash {
		_, stats, e = u.work.CommitInitialHash()
	} else {
		_, stats, e = u.work.Commit()
	}
	if e == nil {
		u.store.lastCommit = stats
	}
	return hybridError(e)
}
func (u *hybridUpdate) Abort() { u.store.finish(u.work) }

func (b *Backend) CacheStats() (ht.ForestCacheStats, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.store.(*hybridStore); ok && s.cache != nil {
		stats, err := s.cache.Stats()
		return stats, hybridError(err)
	}
	return ht.ForestCacheStats{}, nil
}

func (s *hybridStore) FinishExecutionReads() {
	if s.work != nil {
		s.work.FinishReads()
	}
}
