package kvstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/syndtr/goleveldb/leveldb"
	ldberrors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Zero-length column names are forbidden, reserving this physical key for the
// namespace registry. All user keys, including default-column keys, are scoped.
var levelRegistryKey = []byte{0}

type levelDriver struct {
	lastBatchBytes uint64
	db             *leveldb.DB
	options        Options
}
type levelSnapshot struct{ snapshot *leveldb.Snapshot }
type levelIterator struct {
	iterator.Iterator
	prefix []byte
}

func levelError(e error) error {
	if errors.Is(e, leveldb.ErrNotFound) {
		return ErrNotFound
	}
	if ldberrors.IsCorrupted(e) {
		return fmt.Errorf("%w: %v", ErrCorrupt, e)
	}
	return e
}
func levelPrefix(column string) []byte          { return append([]byte{byte(len(column))}, []byte(column)...) }
func levelKey(column string, key []byte) []byte { return append(levelPrefix(column), key...) }

func openLevelDB(path string, o Options, create, readOnly bool) (driver, error) {
	compression := opt.NoCompression
	if o.Compression == Snappy {
		compression = opt.SnappyCompression
	}
	db, e := leveldb.OpenFile(path, &opt.Options{ReadOnly: readOnly, ErrorIfMissing: !create, ErrorIfExist: create, Compression: compression, BlockCacheCapacity: int(o.BlockCacheBytes), WriteBuffer: int(o.WriteBufferBytes), OpenFilesCacheCapacity: o.OpenFiles})
	if e != nil {
		return nil, levelError(e)
	}
	names := append([]string(nil), o.Columns...)
	sort.Strings(names)
	wire, _ := json.Marshal(struct {
		Format  string
		Columns []string
	}{"kvstore-leveldb/v1", names})
	if create {
		e = db.Put(levelRegistryKey, wire, &opt.WriteOptions{Sync: true})
	} else {
		var stored []byte
		stored, e = db.Get(levelRegistryKey, nil)
		if e == nil && !bytes.Equal(stored, wire) {
			e = fmt.Errorf("%w: column registry mismatch", ErrCorrupt)
		}
		if errors.Is(e, leveldb.ErrNotFound) {
			e = fmt.Errorf("%w: missing kvstore namespace registry", ErrCorrupt)
		}
	}
	if e != nil {
		db.Close()
		return nil, levelError(e)
	}
	return &levelDriver{db: db, options: o}, nil
}
func (*levelDriver) version() string { return "goleveldb/2ae1ddf74ef7" }
func (d *levelDriver) registryBytes() (uint64, error) {
	v, e := d.db.Get(levelRegistryKey, nil)
	if e != nil {
		return 0, levelError(e)
	}
	return uint64(len(levelRegistryKey) + len(v)), nil
}
func (d *levelDriver) get(cf string, key []byte) ([]byte, error) {
	v, e := d.db.Get(levelKey(cf, key), nil)
	if len(v) > MaxValueBytes {
		return nil, ErrLimit
	}
	return v, levelError(e)
}
func (d *levelDriver) snapshot() (driverSnapshot, error) {
	s, e := d.db.GetSnapshot()
	if e != nil {
		return nil, e
	}
	return &levelSnapshot{s}, nil
}
func (d *levelDriver) write(es []operation) error {
	b := new(leveldb.Batch)
	for _, op := range es {
		k := levelKey(op.column, op.key)
		if op.deleted {
			b.Delete(k)
		} else {
			b.Put(k, op.value)
		}
	}
	size := uint64(12 + len(b.Dump()))
	if e := prepareWrite(d.options.BeforeWrite, es, LevelDB, size); e != nil {
		return e
	}
	if e := d.db.Write(b, &opt.WriteOptions{Sync: true}); e != nil {
		return e
	}
	d.lastBatchBytes = size
	return nil
}
func (d *levelDriver) lastWritePayloadBytes() uint64         { return d.lastBatchBytes }
func (d *levelDriver) compact() error                        { return d.db.CompactRange(util.Range{}) }
func (*levelDriver) property(string, string) (uint64, error) { return 0, ErrUnsupported }
func (d *levelDriver) close() error                          { return d.db.Close() }
func (d *levelDriver) statistics() (EngineStatistics, error) {
	var s leveldb.DBStats
	if e := d.db.Stats(&s); e != nil {
		return EngineStatistics{}, levelError(e)
	}
	cache := uint64(s.BlockCacheSize)
	var sst uint64
	for _, size := range s.LevelSizes {
		if size < 0 {
			return EngineStatistics{}, ErrCorrupt
		}
		sst += uint64(size)
	}
	out := EngineStatistics{BlockCacheBytes: &cache, LiveSSTBytes: &sst}
	if d.options.EnableStatistics {
		out.FileWriteBytes = &s.IOWrite
	}
	// GoLevelDB exposes file-write traffic, but no separate WAL/flush counters
	// or current memtable-allocation property. Those fields intentionally stay nil.
	return out, nil
}
func (s *levelSnapshot) get(cf string, key []byte) ([]byte, error) {
	v, e := s.snapshot.Get(levelKey(cf, key), nil)
	if len(v) > MaxValueBytes {
		return nil, ErrLimit
	}
	return v, levelError(e)
}
func (s *levelSnapshot) iterator(cf string, bounds Bounds) (driverIterator, error) {
	prefix := levelPrefix(cf)
	r := util.BytesPrefix(prefix)
	if bounds.Start != nil {
		r.Start = append(bytes.Clone(prefix), bounds.Start...)
	}
	if bounds.Limit != nil {
		r.Limit = append(bytes.Clone(prefix), bounds.Limit...)
	}
	return &levelIterator{Iterator: s.snapshot.NewIterator(r, nil), prefix: prefix}, nil
}
func (s *levelSnapshot) close()       { s.snapshot.Release() }
func (it *levelIterator) first() bool { return it.First() }
func (it *levelIterator) last() bool  { return it.Last() }
func (it *levelIterator) seek(key []byte) bool {
	return it.Seek(append(bytes.Clone(it.prefix), key...))
}
func (it *levelIterator) seekForPrev(key []byte) bool {
	target := append(bytes.Clone(it.prefix), key...)
	if it.Seek(target) {
		if bytes.Compare(it.Key(), target) > 0 {
			return it.Prev()
		}
		return true
	}
	if it.Error() != nil {
		return false
	}
	return it.Last()
}
func (it *levelIterator) next() bool { return it.Next() }
func (it *levelIterator) prev() bool { return it.Prev() }
func (it *levelIterator) key() []byte {
	k := it.Key()
	if !bytes.HasPrefix(k, it.prefix) {
		return nil
	}
	return k[len(it.prefix):]
}
func (it *levelIterator) value() []byte { return it.Value() }
func (it *levelIterator) err() error {
	if e := it.Error(); e != nil {
		return levelError(e)
	}
	if it.Valid() && !bytes.HasPrefix(it.Key(), it.prefix) {
		return ErrCorrupt
	}
	if it.Valid() && (len(it.Key())-len(it.prefix) > MaxKeyBytes || len(it.Value()) > MaxValueBytes) {
		return ErrLimit
	}
	return nil
}
func (it *levelIterator) close() { it.Release() }
