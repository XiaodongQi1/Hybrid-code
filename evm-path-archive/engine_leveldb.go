package patharchive

import (
	"errors"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/XiaodongQi1/HybridTrie/internal/kvcache"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
)

// The compatibility adapter alone knows the old LevelDB batch/range/errors.
// Its key encoding and default compression/cache settings remain unchanged.
type legacyEngine struct{ db *kvcache.DB }
type legacySnapshot struct{ view *kvcache.Snapshot }
type legacyIterator struct{ iterator }

func legacyError(e error) error {
	if errors.Is(e, leveldb.ErrNotFound) {
		return kvstore.ErrNotFound
	}
	return e
}
func legacyRange(r *kvstore.Bounds) *util.Range {
	if r == nil {
		return nil
	}
	return &util.Range{Start: r.Start, Limit: r.Limit}
}
func openLegacyEngine(path string, create, ro bool, o kvcache.Options) (diskEngine, error) {
	db, e := kvcache.OpenFile(path, &opt.Options{ReadOnly: ro, ErrorIfMissing: !create, ErrorIfExist: create, Compression: opt.SnappyCompression, BlockCacheCapacity: o.BlockCacheBytes, WriteBuffer: o.WriteBufferBytes, OpenFilesCacheCapacity: o.OpenFiles}, o.CacheBytes)
	if e != nil {
		return nil, e
	}
	return &legacyEngine{db}, nil
}
func (d *legacyEngine) Get(k []byte) ([]byte, error) {
	v, e := d.db.Get(k, nil)
	return v, legacyError(e)
}
func (d *legacyEngine) NewIterator(r *kvstore.Bounds) iterator {
	return &legacyIterator{iterator: d.db.NewIterator(legacyRange(r), nil)}
}
func (d *legacyEngine) GetSnapshot() (diskSnapshot, error) {
	s, e := d.db.GetSnapshot()
	if e != nil {
		return nil, e
	}
	return &legacySnapshot{s}, nil
}
func (d *legacyEngine) Close() error          { return d.db.Close() }
func (d *legacyEngine) Put(k, v []byte) error { b := newDiskBatch(); b.Put(k, v); return d.Write(b) }

type legacyBatch struct{ batch *leveldb.Batch }

func (b legacyBatch) Put(_ string, k, v []byte) error { b.batch.Put(k, v); return nil }
func (b legacyBatch) Delete(_ string, k []byte) error { b.batch.Delete(k); return nil }
func (d *legacyEngine) Write(b *diskBatch) error {
	native := new(leveldb.Batch)
	if e := b.raw.Replay(legacyBatch{native}); e != nil {
		return e
	}
	return d.db.Write(native, &opt.WriteOptions{Sync: true})
}
func (s *legacySnapshot) Get(k []byte) ([]byte, error) {
	v, e := s.view.Get(k, nil)
	return v, legacyError(e)
}
func (s *legacySnapshot) NewIterator(r *kvstore.Bounds) iterator {
	return &legacyIterator{iterator: s.view.NewIterator(legacyRange(r), nil)}
}
func (s *legacySnapshot) Release()      { s.view.Release() }
func (it *legacyIterator) Error() error { return legacyError(it.iterator.Error()) }
