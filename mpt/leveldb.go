package mpt

import (
	"errors"
	"fmt"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// LevelDBStore persists trie nodes in a LevelDB database. Each committed node
// is stored under its Keccak-256 hash, so every returned root remains usable
// until its referenced nodes are removed by an external pruning policy.
type LevelDBStore struct {
	db       *leveldb.DB
	readOnly bool
}

var ErrReadOnly = errors.New("storage: read-only archive")

// DBOptions are per-handle budgets; application LRU capacity is independent.
type DBOptions struct {
	BlockCacheBytes int `json:"block_cache_bytes,omitempty"`
	OpenFiles       int `json:"open_files,omitempty"`
}

func ReadOnlyOptions(o DBOptions) *opt.Options {
	return &opt.Options{ReadOnly: true, ErrorIfMissing: true, BlockCacheCapacity: o.BlockCacheBytes, OpenFilesCacheCapacity: o.OpenFiles}
}

func OpenLevelDBReadOnly(path string, options DBOptions) (*LevelDBStore, error) {
	db, err := leveldb.OpenFile(path, ReadOnlyOptions(options))
	if err != nil {
		return nil, fmt.Errorf("mpt: open read-only leveldb: %w", err)
	}
	return &LevelDBStore{db: db, readOnly: true}, nil
}

// OpenLevelDB opens or creates a LevelDB database at path.
func OpenLevelDB(path string) (*LevelDBStore, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, fmt.Errorf("mpt: open leveldb: %w", err)
	}
	return &LevelDBStore{db: db}, nil
}

// OpenLevelDBCached opens LevelDB and wraps it with an entry-bounded LRU cache.
func OpenLevelDBCached(path string, cacheEntries int) (*CachedStore, error) {
	store, err := OpenLevelDB(path)
	if err != nil {
		return nil, err
	}
	cached, err := NewCachedStore(store, cacheEntries)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return cached, nil
}

func (s *LevelDBStore) Get(key []byte) ([]byte, error) {
	value, err := s.db.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("mpt: leveldb get: %w", err)
	}
	return cloneBytes(value), nil
}

func (s *LevelDBStore) NewBatch() Batch {
	return &levelDBBatch{store: s, batch: new(leveldb.Batch)}
}

// Close flushes and closes the underlying LevelDB database.
func (s *LevelDBStore) Close() error {
	return s.db.Close()
}

type levelDBBatch struct {
	store *LevelDBStore
	batch *leveldb.Batch
}

func (b *levelDBBatch) Put(key, value []byte) error {
	if b.store.readOnly {
		return ErrReadOnly
	}
	b.batch.Put(key, value)
	return nil
}

func (b *levelDBBatch) Write() error {
	if b.store.readOnly {
		return ErrReadOnly
	}
	if err := b.store.db.Write(b.batch, &opt.WriteOptions{Sync: true}); err != nil {
		return fmt.Errorf("mpt: leveldb write: %w", err)
	}
	return nil
}

func (s *LevelDBStore) Compact() error {
	if s.readOnly {
		return ErrReadOnly
	}
	return s.db.CompactRange(util.Range{})
}
