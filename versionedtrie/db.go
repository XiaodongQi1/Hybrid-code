package versionedtrie

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math"
	"github.com/XiaodongQi1/Hybrid-code/internal/kvcache"
	"github.com/XiaodongQi1/Hybrid-code/internal/logicalio"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"sync"
)

const Format = "versioned-trie/v1"

var manifestKey = []byte("M/config")
var headKey = []byte("M/head")

type Config struct {
	Format, CodecID, WorkloadID, Layout string
	TrieIDBytes, HeightBytes            uint8
	InitialHeight                       uint32
	MaxBatchBytes                       uint64
	Storage                             kvcache.Options
}

func DefaultConfig(codec, workload string) Config {
	return Config{Format: Format, CodecID: codec, WorkloadID: workload, Layout: "single", TrieIDBytes: 4, HeightBytes: 4, MaxBatchBytes: 8 << 20, Storage: kvcache.LegacyOptions()}
}
func (c Config) validate() error {
	if c.Format != Format || (c.CodecID != "legacy-raw105" && c.CodecID != "ethereum-mpt" && c.CodecID != "raw-bytes/v1") || len(c.WorkloadID) == 0 || len(c.WorkloadID) > 256 || c.Layout != "single" || c.TrieIDBytes != 4 || c.HeightBytes != 4 || c.MaxBatchBytes < 4096 || c.MaxBatchBytes > 128<<20 {
		return ErrCorrupt
	}
	return c.Storage.Validate()
}

type Head struct {
	Initialized bool
	Height      uint32
	Root        mpt.Hash
	CommitID    [32]byte
	NextTrieID  uint64
}
type Store struct {
	mu                          sync.Mutex
	db                          *kvcache.DB
	cfg                         Config
	head                        Head
	epoch                       uint64
	views                       int
	readOnly, closed, uncertain bool
	write                       func(*leveldb.Batch) error
	logical                     logicalio.Stats
}

// Open creates a new database or opens a matching existing one. It never
// migrates formats or repairs incomplete data by overwriting it.
func Open(path string, cfg Config, create, readOnly bool) (*Store, error) {
	if e := cfg.validate(); e != nil {
		return nil, e
	}
	if create && readOnly {
		return nil, ErrReadOnly
	}
	tuning := cfg.Storage
	db, e := kvcache.OpenFile(path, &opt.Options{ReadOnly: readOnly, ErrorIfMissing: !create, ErrorIfExist: create, Compression: opt.SnappyCompression, BlockCacheCapacity: tuning.BlockCacheBytes, WriteBuffer: tuning.WriteBufferBytes, OpenFilesCacheCapacity: tuning.OpenFiles}, tuning.CacheBytes)
	if e != nil {
		return nil, e
	}
	s := &Store{db: db, cfg: cfg, head: Head{NextTrieID: 1}, readOnly: readOnly, logical: logicalio.Stats{}}
	fail := func(e error) (*Store, error) { db.Close(); return nil, e }
	s.write = func(b *leveldb.Batch) error { return db.Write(b, &opt.WriteOptions{Sync: true}) }
	manifest, _ := json.Marshal(cfg)
	if create {
		b := new(leveldb.Batch)
		b.Put(manifestKey, manifest)
		h, _ := json.Marshal(s.head)
		b.Put(headKey, h)
		if e = s.write(b); e != nil {
			return fail(fmt.Errorf("%w: %v", ErrUncertain, e))
		}
	} else {
		raw, e := db.Get(manifestKey, nil)
		if e != nil {
			return fail(e)
		}
		if !bytes.Equal(raw, manifest) {
			return fail(fmt.Errorf("%w: manifest mismatch", ErrCorrupt))
		}
		raw, e = db.Get(headKey, nil)
		if e != nil {
			return fail(e)
		}
		if e = json.Unmarshal(raw, &s.head); e != nil {
			return fail(ErrCorrupt)
		}
		canonical, _ := json.Marshal(s.head)
		if !bytes.Equal(canonical, raw) || s.head.NextTrieID < 1 || s.head.NextTrieID > uint64(math.MaxUint32)+1 {
			return fail(ErrCorrupt)
		}
		h := s.head
		if h.Initialized {
			if h.Height < cfg.InitialHeight || h.CommitID == ([32]byte{}) || h.Root == (mpt.Hash{}) {
				return fail(ErrCorrupt)
			}
			raw, e = db.Get(rootKey(h.Height), nil)
			if e != nil {
				return fail(e)
			}
			if !bytes.Equal(raw, encodeRoot(h.Root, h.CommitID)) {
				return fail(ErrCorrupt)
			}
		} else if h.NextTrieID != 1 || h.Height != 0 || h.Root != (mpt.Hash{}) || h.CommitID != ([32]byte{}) {
			return fail(ErrCorrupt)
		}
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
func (s *Store) LogicalWrites() (logicalio.Stats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logical.Clone(), s.check()
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.views != 0 {
		return ErrBusy
	}
	s.closed = true
	return s.db.Close()
}
func encodeRoot(root mpt.Hash, id [32]byte) []byte {
	out := make([]byte, 64)
	copy(out, root[:])
	copy(out[32:], id[:])
	return out
}
