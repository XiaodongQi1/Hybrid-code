// Package kvcache provides a per-database, byte-bounded cross-block KV LRU.
// It does not cache logical accounts or bypass application integrity checks.
package kvcache

import (
	"bytes"
	"container/list"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"sync"
)

type Options struct {
	CacheBytes                                   uint64
	BlockCacheBytes, WriteBufferBytes, OpenFiles int
}

func LegacyOptions() Options {
	return Options{BlockCacheBytes: 8 << 20, WriteBufferBytes: 4 << 20, OpenFiles: 128}
}
func DefaultOptions() Options {
	return Options{CacheBytes: 2 << 30, BlockCacheBytes: 1 << 30, WriteBufferBytes: 64 << 20, OpenFiles: 512}
}
func (o Options) Validate() error {
	if o.CacheBytes > 16<<30 || o.BlockCacheBytes < 0 || o.BlockCacheBytes > 8<<30 || o.WriteBufferBytes < 64<<10 || o.WriteBufferBytes > 256<<20 || o.OpenFiles < 16 || o.OpenFiles > 4096 {
		return fmt.Errorf("invalid KV cache options")
	}
	return nil
}

// Charge includes key/value lengths and a fixed bookkeeping allowance. It is
// deterministic accounting, not a Go heap/RSS hard limit.
const EntryOverhead uint64 = 128

type Stats struct {
	Capacity, ChargedBytes, Hits, Misses, Evictions uint64
	Entries                                         int
}
type entry struct {
	key    string
	value  []byte
	charge uint64
}
type DB struct {
	*leveldb.DB
	gate    sync.RWMutex // excludes writes/close while allowing concurrent reads
	mu      sync.Mutex
	epoch   uint64
	closed  bool
	stats   Stats
	entries map[string]*list.Element
	order   *list.List
}

func OpenFile(path string, o *opt.Options, capacity uint64) (*DB, error) {
	db, e := leveldb.OpenFile(path, o)
	if e != nil {
		return nil, e
	}
	return &DB{DB: db, stats: Stats{Capacity: capacity}, entries: map[string]*list.Element{}, order: list.New()}, nil
}
func (d *DB) remove(k string) {
	if el := d.entries[k]; el != nil {
		d.stats.ChargedBytes -= el.Value.(entry).charge
		delete(d.entries, k)
		d.order.Remove(el)
	}
}
func (d *DB) put(k, v []byte) {
	if d.stats.Capacity == 0 {
		return
	}
	key := string(k)
	n := uint64(len(k)+len(v)) + EntryOverhead
	if n > d.stats.Capacity {
		d.remove(key)
		return
	}
	if el := d.entries[key]; el != nil {
		old := el.Value.(entry)
		d.stats.ChargedBytes -= old.charge
		el.Value = entry{old.key, bytes.Clone(v), n}
		d.stats.ChargedBytes += n
		d.order.MoveToFront(el)
	} else {
		d.entries[key] = d.order.PushFront(entry{key, bytes.Clone(v), n})
		d.stats.ChargedBytes += n
	}
	for d.stats.ChargedBytes > d.stats.Capacity {
		el := d.order.Back()
		d.remove(el.Value.(entry).key)
		d.stats.Evictions++
	}
}
func (d *DB) get(k []byte, o *opt.ReadOptions, load func([]byte, *opt.ReadOptions) ([]byte, error)) ([]byte, error) {
	if d.closed {
		return nil, leveldb.ErrClosed
	}
	d.mu.Lock()
	if el := d.entries[string(k)]; el != nil {
		d.stats.Hits++
		d.order.MoveToFront(el)
		v := bytes.Clone(el.Value.(entry).value)
		d.mu.Unlock()
		return v, nil
	}
	d.stats.Misses++
	d.mu.Unlock()
	v, e := load(k, o)
	if e == nil {
		d.mu.Lock()
		d.put(k, v)
		d.mu.Unlock()
	}
	return v, e
}
func (d *DB) Get(k []byte, o *opt.ReadOptions) ([]byte, error) {
	d.gate.RLock()
	defer d.gate.RUnlock()
	return d.get(k, o, d.DB.Get)
}

type publish struct{ d *DB }

func (p publish) Put(k, v []byte) { p.d.put(k, v) }
func (p publish) Delete(k []byte) { p.d.remove(string(k)) }
func (d *DB) clear() {
	d.entries = map[string]*list.Element{}
	d.order.Init()
	d.stats.ChargedBytes = 0
}
func (d *DB) Write(b *leveldb.Batch, o *opt.WriteOptions) error {
	d.gate.Lock()
	defer d.gate.Unlock()
	e := d.DB.Write(b, o)
	d.mu.Lock()
	defer d.mu.Unlock()
	d.epoch++
	if e != nil {
		d.clear()
		return e
	}
	if d.stats.Capacity == 0 {
		return nil
	}
	return b.Replay(publish{d})
}
func (d *DB) Put(k, v []byte, o *opt.WriteOptions) error {
	b := new(leveldb.Batch)
	b.Put(k, v)
	return d.Write(b, o)
}
func (d *DB) Delete(k []byte, o *opt.WriteOptions) error {
	b := new(leveldb.Batch)
	b.Delete(k)
	return d.Write(b, o)
}
func (d *DB) CacheStats() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.stats
	s.Entries = len(d.entries)
	return s
}
func (d *DB) Close() error {
	d.gate.Lock()
	defer d.gate.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	d.clear()
	return d.DB.Close()
}

// A snapshot shares the LRU only while its generation is still current. After
// any write it reads its pinned LevelDB snapshot, never the newer cached values.
type Snapshot struct {
	*leveldb.Snapshot
	owner    *DB
	epoch    uint64
	released bool
}

func (d *DB) GetSnapshot() (*Snapshot, error) {
	d.gate.RLock()
	defer d.gate.RUnlock()
	s, e := d.DB.GetSnapshot()
	if e != nil {
		return nil, e
	}
	return &Snapshot{Snapshot: s, owner: d, epoch: d.epoch}, nil
}
func (s *Snapshot) Get(k []byte, o *opt.ReadOptions) ([]byte, error) {
	d := s.owner
	d.gate.RLock()
	defer d.gate.RUnlock()
	if s.released {
		return nil, leveldb.ErrSnapshotReleased
	}
	if d.closed {
		return nil, leveldb.ErrClosed
	}
	if d.epoch != s.epoch {
		return s.Snapshot.Get(k, o)
	}
	return d.get(k, o, s.Snapshot.Get)
}
func (s *Snapshot) Release() {
	d := s.owner
	d.gate.Lock()
	defer d.gate.Unlock()
	s.released = true
	s.Snapshot.Release()
}
