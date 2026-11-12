package versionedbench

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"io"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
	"strings"
)

type archiveStore struct {
	db       *leveldb.DB
	readOnly bool
}
type archiveBatch struct {
	s *archiveStore
	b *leveldb.Batch
}

func (s *archiveStore) Get(k []byte) ([]byte, error) {
	v, e := s.db.Get(k, nil)
	if errors.Is(e, leveldb.ErrNotFound) {
		return nil, mpt.ErrNotFound
	}
	return v, e
}
func (s *archiveStore) NewBatch() mpt.Batch { return &archiveBatch{s, new(leveldb.Batch)} }
func (b *archiveBatch) Put(k, v []byte) error {
	if b.s.readOnly {
		return mpt.ErrReadOnly
	}
	b.b.Put(k, v)
	return nil
}
func (b *archiveBatch) Write() error {
	if b.s.readOnly {
		return mpt.ErrReadOnly
	}
	return b.s.db.Write(b.b, &opt.WriteOptions{Sync: true})
}
func options(cfg vt.Config, create, ro bool) *opt.Options {
	return &opt.Options{ErrorIfMissing: !create, ErrorIfExist: create, ReadOnly: ro, Compression: opt.SnappyCompression, BlockCacheCapacity: cfg.Storage.BlockCacheBytes, WriteBuffer: cfg.Storage.WriteBufferBytes, OpenFilesCacheCapacity: cfg.Storage.OpenFiles}
}
func openPlain(path, format string, cfg vt.Config, create, ro bool) (*leveldb.DB, error) {
	db, e := leveldb.OpenFile(path, options(cfg, create, ro))
	if e != nil {
		return nil, e
	}
	if create {
		wire, _ := json.Marshal(struct {
			Format string
			Config vt.Config
		}{format, cfg})
		b := new(leveldb.Batch)
		b.Put([]byte("M/config"), wire)
		head, _ := json.Marshal(vt.Head{NextTrieID: 1})
		b.Put([]byte("M/head"), head)
		if e = db.Write(b, &opt.WriteOptions{Sync: true}); e != nil {
			db.Close()
			return nil, e
		}
	}
	return db, nil
}
func meta(b *leveldb.Batch, h uint32, root mpt.Hash, id [32]byte) {
	key := []byte{'R', byte(h >> 24), byte(h >> 16), byte(h >> 8), byte(h)}
	value := append(append([]byte(nil), root[:]...), id[:]...)
	b.Put(key, value)
	wire, _ := json.Marshal(vt.Head{Initialized: true, Height: h, Root: root, CommitID: id, NextTrieID: 1})
	b.Put([]byte("M/head"), wire)
}

// fullReader uses the same version keys as the compact store, with full RLP as
// Values. It is a controlled representation baseline, not a Geth Path engine.
type fullReader struct {
	db     *leveldb.DB
	height uint32
}

func (r fullReader) Get([]byte) ([]byte, error) { return nil, vt.ErrPathRequired }
func (r fullReader) NewBatch() mpt.Batch        { return denied{} }

type denied struct{}

func (denied) Put([]byte, []byte) error { return mpt.ErrReadOnly }
func (denied) Write() error             { return mpt.ErrReadOnly }
func (r fullReader) GetNode(path []byte, hash mpt.Hash) ([]byte, error) {
	target, e := vt.EncodeNodeKey(0, path, uint64(r.height))
	if e != nil {
		return nil, e
	}
	head := target[:len(target)-4]
	it := r.db.NewIterator(util.BytesPrefix(head), nil)
	defer it.Release()
	if it.Seek(target) {
		if !bytes.Equal(it.Key(), target) {
			it.Prev()
		}
	} else {
		if e = it.Error(); e != nil {
			return nil, e
		}
		it.Last()
	}
	if e = it.Error(); e != nil {
		return nil, e
	}
	if !it.Valid() {
		return nil, mpt.ErrMissingNode
	}
	k, e := vt.DecodeNodeKey(it.Key())
	if e != nil || k.TrieID != 0 || !bytes.Equal(k.Path, path) || k.Height > r.height {
		return nil, vt.ErrCorrupt
	}
	return append([]byte(nil), it.Value()...), nil
}

type Counts struct{ Entries, KeyBytes, ValueBytes int64 }

func (c Counts) Bytes() int64 { return c.KeyBytes + c.ValueBytes }

type Inventory struct {
	Total, Nodes             Counts
	Tables                   map[string]Counts
	SSTBytes, DirectoryBytes int64
	OnDiskSSTBytes           int64
	LevelSizes               []int64
	TableLayout              string
	Fingerprint              string
	NodeKeysSHA256           string
}

// SealInventory applies the shared offline LevelDB compaction/cleanup policy.
// It measures a database, not cumulative Put traffic.
func SealInventory(path, method string, cfg vt.Config) (Inventory, error) {
	return inventory(path, method, cfg)
}

func DirectoryFingerprint(path string) (string, error) { return directoryFingerprint(path) }

func inventory(path, method string, cfg vt.Config) (Inventory, error) {
	var inv Inventory
	inv.Tables = map[string]Counts{}
	nodeKeys := sha256.New()
	// Identical offline compaction policy at initialization and final sealing.
	db, e := leveldb.OpenFile(path, options(cfg, false, false))
	if e != nil {
		return inv, e
	}
	if e = db.CompactRange(util.Range{}); e != nil {
		db.Close()
		return inv, e
	}
	if e = db.Close(); e != nil {
		return inv, e
	}
	// A normal writable reopen runs LevelDB's synchronous obsolete-file janitor.
	// No application writes occur here; all three methods use this same policy.
	db, e = leveldb.OpenFile(path, options(cfg, false, false))
	if e != nil {
		return inv, e
	}
	if e = db.Close(); e != nil {
		return inv, e
	}
	db, e = leveldb.OpenFile(path, options(cfg, false, true))
	if e != nil {
		return inv, e
	}
	var stats leveldb.DBStats
	if e = db.Stats(&stats); e != nil {
		db.Close()
		return inv, e
	}
	inv.SSTBytes = stats.LevelSizes.Sum()
	inv.LevelSizes = append([]int64(nil), stats.LevelSizes...)
	inv.TableLayout, e = db.GetProperty("leveldb.sstables")
	if e != nil {
		db.Close()
		return inv, e
	}
	it := db.NewIterator(nil, nil)
	for it.Next() {
		k, v := it.Key(), it.Value()
		class := "metadata"
		node := false
		if method == "HashArchive" && len(k) == 32 {
			class = "nodes"
			node = true
		} else if len(k) > 0 {
			switch k[0] {
			case 'N':
				class = "nodes"
				node = true
			case 'R':
				class = "roots"
			case 'M':
				class = "metadata"
			}
		}
		c := inv.Tables[class]
		c.Entries++
		c.KeyBytes += int64(len(k))
		c.ValueBytes += int64(len(v))
		inv.Tables[class] = c
		inv.Total.Entries++
		inv.Total.KeyBytes += int64(len(k))
		inv.Total.ValueBytes += int64(len(v))
		if node {
			fmt.Fprintf(nodeKeys, "%d:", len(k))
			nodeKeys.Write(k)
			inv.Nodes.Entries++
			inv.Nodes.KeyBytes += int64(len(k))
			inv.Nodes.ValueBytes += int64(len(v))
		}
	}
	inv.NodeKeysSHA256 = hex.EncodeToString(nodeKeys.Sum(nil))
	e = it.Error()
	it.Release()
	closeErr := db.Close()
	if e != nil {
		return inv, e
	}
	if closeErr != nil {
		return inv, closeErr
	}
	e = filepath.WalkDir(path, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		inv.DirectoryBytes += info.Size()
		if strings.HasSuffix(p, ".ldb") || strings.HasSuffix(p, ".sst") {
			inv.OnDiskSSTBytes += info.Size()
		}
		return nil
	})
	if e != nil {
		return inv, e
	}
	if inv.OnDiskSSTBytes != inv.SSTBytes {
		return inv, fmt.Errorf("obsolete SST files remain after sealing: live=%d on_disk=%d", inv.SSTBytes, inv.OnDiskSSTBytes)
	}
	inv.Fingerprint, e = directoryFingerprint(path)
	return inv, e
}

// directoryFingerprint checks content, not only byte counts, after read-only queries.
func directoryFingerprint(path string) (string, error) {
	h := sha256.New()
	err := filepath.WalkDir(path, func(p string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			return nil
		}
		rel, e := filepath.Rel(path, p)
		if e != nil {
			return e
		}
		info, e := d.Info()
		if e != nil {
			return e
		}
		fmt.Fprintf(h, "%s\x00%d\x00", rel, info.Size())
		f, e := os.Open(p)
		if e != nil {
			return e
		}
		_, e = io.Copy(h, f)
		closeErr := f.Close()
		if e != nil {
			return e
		}
		return closeErr
	})
	return hex.EncodeToString(h.Sum(nil)), err
}
