package hybridtrie

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

var builderManifestKey = []byte("S/manifest")
var builderProgressKey = []byte("S/progress")

// SnapshotBuildConfig identifies an owned, disposable construction database.
// ChunkEntries bounds materialized COW updates; BatchBytes bounds each native
// write. Hash-addressed construction nodes never enter the final Hash CF.
type SnapshotBuildConfig struct {
	RecordAccounts bool   `json:",omitempty"`
	Provenance     []byte `json:",omitempty"`
	SourceID       string
	Height         uint64
	BlockID        [32]byte
	BatchBytes     uint64
	ChunkEntries   uint32
	Storage        *kvstore.Options `json:",omitempty"`
}
type SnapshotProgress struct {
	Cursor, NextTrieID uint64
	Digest             [32]byte
	LastAddress        [20]byte
	Ready              bool
}
type SnapshotBuildStats struct{ Batches, WrittenBytes, PeakBatchBytes, Flushes, Updates uint64 }
type SnapshotBuilder struct {
	path                       string
	cfg                        SnapshotBuildConfig
	db                         diskEngine
	tree                       *mpt.Trie
	active                     uint64
	count                      uint32
	inputBytes                 uint64
	hasTree, closed, uncertain bool
	rootWritten                bool
	pending                    []entry
	accounts                   []entry
	accountBytes               uint64
	progress                   SnapshotProgress
	stats                      SnapshotBuildStats
}

func OpenSnapshotBuilder(path string, cfg SnapshotBuildConfig) (*SnapshotBuilder, error) {
	var pathErr error
	path, pathErr = filepath.Abs(path)
	if pathErr != nil {
		return nil, pathErr
	}
	if len(cfg.SourceID) == 0 || len(cfg.SourceID) > 256 || cfg.Height > MaxHeight || cfg.BlockID == ([32]byte{}) || cfg.BatchBytes < 4096 || cfg.BatchBytes > 128<<20 || cfg.ChunkEntries == 0 || cfg.ChunkEntries > 4096 || len(cfg.Provenance) > 65536 {
		return nil, ErrLimit
	}
	cfg.Provenance = bytes.Clone(cfg.Provenance)
	if cfg.Storage != nil {
		o, e := storageOptions(cfg.Storage)
		if e != nil {
			return nil, kvError(e)
		}
		cfg.Storage = &o
	}
	manifest, e := json.Marshal(cfg)
	if e != nil {
		return nil, e
	}
	_, e = os.Stat(path)
	create := os.IsNotExist(e)
	if e != nil && !create {
		return nil, e
	}
	db, e := openConfiguredEngine(path, cfg.Storage, create, false)
	if e != nil {
		return nil, e
	}
	b := &SnapshotBuilder{path: path, cfg: cfg, db: db, progress: SnapshotProgress{NextTrieID: 1}}
	opened := false
	defer func() {
		if !opened {
			db.close()
		}
	}()
	fail := func(e error) (*SnapshotBuilder, error) { return nil, e }
	if create {
		raw, _ := json.Marshal(b.progress)
		if e = b.write([]entry{{metaColumn, builderManifestKey, manifest}, {metaColumn, builderProgressKey, raw}}); e != nil {
			return fail(e)
		}
	} else {
		snap := db.snapshot()
		defer snap.close()
		raw, e := snap.get(metaColumn, builderManifestKey)
		if e != nil {
			return fail(e)
		}
		if !bytes.Equal(raw, manifest) {
			return fail(ErrConflict)
		}
		raw, e = snap.get(metaColumn, builderProgressKey)
		if e != nil {
			return fail(e)
		}
		if e = json.Unmarshal(raw, &b.progress); e != nil {
			return fail(ErrCorrupt)
		}
		canon, _ := json.Marshal(b.progress)
		if !bytes.Equal(raw, canon) || b.progress.NextTrieID < 1 || b.progress.NextTrieID > MaxTrieID+1 {
			return fail(ErrCorrupt)
		}
	}
	opened = true
	return b, nil
}
func (b *SnapshotBuilder) check() error {
	if b.closed {
		return ErrClosed
	}
	if b.uncertain {
		return ErrUncertain
	}
	return nil
}
func (b *SnapshotBuilder) write(es []entry) error {
	var n uint64
	for _, e := range es {
		n += uint64(len(e.key) + len(e.value))
	}
	if n > b.cfg.BatchBytes {
		return ErrLimit
	}
	if e := b.db.write(es); e != nil {
		b.uncertain = true
		return fmt.Errorf("%w: construction write: %v", ErrUncertain, e)
	}
	b.stats.Batches++
	b.stats.WrittenBytes += n
	if n > b.stats.PeakBatchBytes {
		b.stats.PeakBatchBytes = n
	}
	return nil
}
func (b *SnapshotBuilder) Progress() SnapshotProgress { return b.progress }
func (b *SnapshotBuilder) Stats() SnapshotBuildStats  { return b.stats }
func (b *SnapshotBuilder) Close() error {
	if !b.closed {
		if e := b.db.close(); e != nil {
			return e
		}
		b.closed = true
		b.tree = nil
	}
	return nil
}

// Remove deletes only this successfully opened, closed builder's owned path.
func (b *SnapshotBuilder) Remove() error {
	if !b.closed {
		return ErrBusy
	}
	return os.RemoveAll(b.path)
}
func (b *SnapshotBuilder) Get(key []byte) ([]byte, error) {
	if e := b.check(); e != nil {
		return nil, e
	}
	s := b.db.snapshot()
	defer s.close()
	return s.get(hashColumn, key)
}
func (b *SnapshotBuilder) NewBatch() mpt.Batch { return &snapshotHashBatch{builder: b} }

type snapshotHashBatch struct {
	builder *SnapshotBuilder
	entries []entry
	bytes   uint64
}

func (b *snapshotHashBatch) Put(k, v []byte) error {
	if e := b.builder.check(); e != nil {
		return e
	}
	n := uint64(len(k) + len(v))
	if n > b.builder.cfg.BatchBytes {
		return ErrLimit
	}
	if b.bytes+n > b.builder.cfg.BatchBytes {
		if e := b.builder.write(b.entries); e != nil {
			return e
		}
		b.entries = nil
		b.bytes = 0
	}
	b.entries = append(b.entries, entry{hashColumn, bytes.Clone(k), bytes.Clone(v)})
	b.bytes += n
	return nil
}
func (b *snapshotHashBatch) Write() error {
	// Nodes may be flushed before the construction root, but old saved roots
	// remain valid. Only this final WAL+Sync publishes the new progress/root.
	if e := b.builder.check(); e != nil {
		return e
	}
	for _, e := range b.builder.pending {
		n := uint64(len(e.key) + len(e.value))
		if n > b.builder.cfg.BatchBytes {
			return ErrLimit
		}
		if b.bytes+n > b.builder.cfg.BatchBytes {
			if err := b.builder.write(b.entries); err != nil {
				return err
			}
			b.entries = nil
			b.bytes = 0
		}
		b.entries = append(b.entries, e)
		b.bytes += n
	}
	if e := b.builder.write(b.entries); e != nil {
		return e
	}
	b.builder.rootWritten = true
	b.entries = nil
	b.bytes = 0
	return nil
}
func (b *SnapshotBuilder) selectTrie(id uint64) error {
	if e := b.check(); e != nil {
		return e
	}
	if b.progress.Ready {
		return ErrConflict
	}
	if id > MaxTrieID {
		return ErrLimit
	}
	if b.hasTree && b.active == id {
		return nil
	}
	if e := b.Flush(); e != nil {
		return e
	}
	s := b.db.snapshot()
	root, e := readRoot(s, id, b.cfg.Height)
	s.close()
	if errors.Is(e, ErrNotFound) {
		root = mpt.EmptyRootHash
	} else if e != nil {
		return e
	}
	b.tree, e = mpt.Open(root, b)
	if e != nil {
		return e
	}
	b.active = id
	b.hasTree = true
	return nil
}
func (b *SnapshotBuilder) Put(id uint64, key, value []byte) error {
	if len(key) > 32 || len(value) > mpt.MaxCanonicalNodeBytes-128 {
		return ErrLimit
	}
	n := uint64(len(key) + len(value))
	if n > b.cfg.BatchBytes {
		return ErrLimit
	}
	if b.hasTree && b.active == id && b.inputBytes+n > b.cfg.BatchBytes {
		if e := b.Flush(); e != nil {
			return e
		}
	}
	if e := b.selectTrie(id); e != nil {
		return e
	}
	if e := b.tree.Update(key, value); e != nil {
		return e
	}
	b.count++
	b.inputBytes += n
	b.stats.Updates++
	if b.count >= b.cfg.ChunkEntries {
		return b.Flush()
	}
	return nil
}
func (b *SnapshotBuilder) Flush() error {
	if e := b.check(); e != nil {
		return e
	}
	if !b.hasTree {
		return nil
	}
	h, e := b.tree.Hash()
	if e != nil {
		return e
	}
	key, _ := rootKey(b.active, b.cfg.Height)
	b.pending = []entry{{metaColumn, key, h[:]}}
	b.rootWritten = false
	if b.count == 0 {
		e = b.write(b.pending)
	} else {
		_, e = b.tree.Commit()
		if e == nil && !b.rootWritten {
			e = b.write(b.pending)
		}
	}
	b.pending = nil
	if e != nil {
		return e
	}
	b.hasTree = false
	b.tree = nil
	b.count = 0
	b.inputBytes = 0
	b.stats.Flushes++
	return nil
}
func (b *SnapshotBuilder) Root(id uint64) (mpt.Hash, error) {
	if e := b.Flush(); e != nil {
		return mpt.Hash{}, e
	}
	s := b.db.snapshot()
	defer s.close()
	r, e := readRoot(s, id, b.cfg.Height)
	if errors.Is(e, ErrNotFound) {
		return mpt.EmptyRootHash, nil
	}
	return r, e
}
func (b *SnapshotBuilder) BindStorage(id uint64, address [20]byte) error {
	if e := b.check(); e != nil {
		return e
	}
	if b.progress.Ready || id == 0 || id > MaxTrieID {
		return ErrConflict
	}
	snap := b.db.snapshot()
	old, exists, e := readStorageOwner(snap, id)
	snap.close()
	if e != nil {
		return e
	}
	if exists && old != address {
		return ErrConflict
	}
	life, _ := encodeStorageLifecycle(StorageLifecycle{true, id})
	return b.write([]entry{{metaColumn, storageOwnerKey(id), address[:]}, {metaColumn, lifecycleKey(address, b.cfg.Height), life}})
}
func (b *SnapshotBuilder) PutCode(hash mpt.Hash, code []byte) error {
	if e := b.check(); e != nil {
		return e
	}
	if b.progress.Ready || codeDigest(code) != hash {
		return ErrCorrupt
	}
	if hash == codeDigest(nil) {
		return nil
	}
	snap := b.db.snapshot()
	old, e := snap.get(metaColumn, codeKey(hash))
	snap.close()
	if e == nil {
		if !bytes.Equal(old, code) {
			return ErrConflict
		}
		return nil
	}
	if !errors.Is(e, ErrNotFound) {
		return e
	}
	return b.write([]entry{{metaColumn, codeKey(hash), code}})
}

// Save checkpoints whole input records, not partial account/storage updates.
// Replaying a record after interruption is idempotent in the immutable scratch
// hash store. The supplied digest binds the already consumed source prefix.
func (b *SnapshotBuilder) Save(p SnapshotProgress) error {
	if p.Ready {
		return ErrConflict // Only Finish can publish a ready construction source.
	}
	return b.save(p)
}
func (b *SnapshotBuilder) save(p SnapshotProgress) error {
	if e := b.flushAccounts(); e != nil {
		return e
	}
	if e := b.Flush(); e != nil {
		return e
	}
	if p.Cursor < b.progress.Cursor || p.NextTrieID < b.progress.NextTrieID || p.NextTrieID < 1 || p.NextTrieID > MaxTrieID+1 || b.progress.Ready {
		return ErrConflict
	}
	raw, _ := json.Marshal(p)
	if e := b.write([]entry{{metaColumn, builderProgressKey, raw}}); e != nil {
		return e
	}
	b.progress = p
	return nil
}

// RecordAccount retains the original bounded source row outside final state
// databases. EVM-aware callers validate its JSON/address and rolling digest.
func (b *SnapshotBuilder) RecordAccount(address [20]byte, raw []byte) error {
	if e := b.check(); e != nil {
		return e
	}
	if !b.cfg.RecordAccounts || b.progress.Ready {
		return ErrConflict
	}
	k := append([]byte{'A'}, address[:]...)
	size := uint64(len(k) + len(raw))
	if size > b.cfg.BatchBytes {
		return ErrLimit
	}
	if b.accountBytes+size > b.cfg.BatchBytes {
		if e := b.flushAccounts(); e != nil {
			return e
		}
	}
	b.accounts = append(b.accounts, entry{metaColumn, k, bytes.Clone(raw)})
	b.accountBytes += size
	return nil
}
func (b *SnapshotBuilder) flushAccounts() error {
	if e := b.check(); e != nil {
		return e
	}
	if len(b.accounts) == 0 {
		return nil
	}
	if e := b.write(b.accounts); e != nil {
		return e
	}
	b.accounts = nil
	b.accountBytes = 0
	return nil
}
func (b *SnapshotBuilder) Finish(p SnapshotProgress) error {
	if e := b.check(); e != nil {
		return e
	}
	if b.progress.Ready {
		return nil
	}
	if e := b.selectTrie(0); e != nil {
		return e
	}
	if e := b.Flush(); e != nil {
		return e
	}
	p.Ready = true
	key := evmBlockKey(b.cfg.Height)
	if e := b.write([]entry{{metaColumn, key, b.cfg.BlockID[:]}}); e != nil {
		return e
	}
	return b.save(p)
}
func (b *SnapshotBuilder) ownedTarget(path string) bool {
	a, _ := filepath.Abs(b.path)
	t, _ := filepath.Abs(path)
	rel, e := filepath.Rel(a, t)
	if e != nil || rel == "." || !bytes.HasPrefix([]byte(rel), []byte(".."+string(os.PathSeparator))) && rel != ".." {
		return false
	}
	rel, e = filepath.Rel(t, a)
	return e == nil && (rel == ".." || bytes.HasPrefix([]byte(rel), []byte(".."+string(os.PathSeparator))))
}
