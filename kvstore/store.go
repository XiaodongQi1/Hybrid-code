// Package kvstore provides bytewise ordered storage over RocksDB or GoLevelDB.
// Batches publish synchronously and atomically across logical columns. Values
// returned by Get/Key/Value are owned copies. Iterators are short-lived and may
// be repositioned repeatedly with Seek or SeekForPrev on the same snapshot.
package kvstore

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
)

type Engine string
type Compression string

const (
	RocksDB       Engine      = "rocksdb"
	LevelDB       Engine      = "leveldb"
	None          Compression = "none"
	Snappy        Compression = "snappy"
	DefaultColumn             = "default"
	// Guard native-to-Go conversions and accidental unbounded record allocations.
	MaxKeyBytes   = 1 << 20
	MaxValueBytes = 64 << 20
)

var (
	ErrNotFound    = errors.New("kvstore: key not found")
	ErrClosed      = errors.New("kvstore: closed")
	ErrBusy        = errors.New("kvstore: release iterators and snapshots before close")
	ErrReadOnly    = errors.New("kvstore: read only")
	ErrUnavailable = errors.New("kvstore: requested engine unavailable")
	ErrInvalid     = errors.New("kvstore: invalid options, column or bounds")
	ErrCorrupt     = errors.New("kvstore: corrupt data or incompatible store")
	ErrLimit       = errors.New("kvstore: record exceeds size limit")
	ErrUnsupported = errors.New("kvstore: operation not supported by engine")
	ErrUncertain   = errors.New("kvstore: write outcome unknown; reopen required")
)

type Options struct {
	// BeforeWrite is an optional process-local observer for checkpointing measured
	// imports. It runs before the synchronous engine write, under the DB lock.
	// An error prevents submission. It must not re-enter the DB.
	BeforeWrite func(WriteCounters) error `json:"-"`
	Engine      Engine
	Columns     []string    `json:",omitempty"`
	Compression Compression `json:",omitempty"`
	// Zero retains the engine's default block-cache policy. WriteBufferBytes is
	// per memtable (per CF on RocksDB), not a whole-process memory guarantee.
	BlockCacheBytes  int64 `json:",omitempty"`
	WriteBufferBytes int64 `json:",omitempty"`
	OpenFiles        int   `json:",omitempty"`
	// GlobalWriteBufferBytes bounds the whole database's memtable target rather
	// than multiplying WriteBufferBytes by its column count. Zero is legacy mode.
	GlobalWriteBufferBytes int64 `json:",omitempty"`
	// ApplicationCacheBytes is a fixed-allocation point-read LRU. Hybrid's
	// execution cache has its own accounting and normally leaves this at zero.
	ApplicationCacheBytes uint64 `json:",omitempty"`
	ExecutionCacheOnly    bool   `json:",omitempty"`
	BackgroundJobs        int    `json:",omitempty"`
	EnableStatistics      bool   `json:",omitempty"`
}

func DefaultOptions(engine Engine) Options {
	return Options{Engine: engine, Compression: None, BlockCacheBytes: 8 << 20, WriteBufferBytes: 8 << 20, OpenFiles: 128}
}

func (o Options) Resolved() (Options, error) {
	if o.Engine != RocksDB && o.Engine != LevelDB {
		return o, ErrInvalid
	}
	if o.Compression == "" {
		o.Compression = None
	}
	if o.Compression != None && o.Compression != Snappy {
		return o, ErrInvalid
	}
	if o.WriteBufferBytes == 0 {
		o.WriteBufferBytes = 8 << 20
	}
	if o.OpenFiles == 0 {
		o.OpenFiles = 128
	}
	if o.BlockCacheBytes < 0 || o.BlockCacheBytes > 8<<30 || o.WriteBufferBytes < 64<<10 || o.WriteBufferBytes > 256<<20 || o.OpenFiles < 16 || o.OpenFiles > 4096 {
		return o, ErrInvalid
	}
	if o.GlobalWriteBufferBytes < 0 || o.GlobalWriteBufferBytes > 512<<20 || o.GlobalWriteBufferBytes > 0 && o.GlobalWriteBufferBytes < 128<<10 || o.ApplicationCacheBytes > 1<<30 || o.BackgroundJobs < 0 || o.BackgroundJobs > 64 {
		return o, ErrInvalid
	}
	if o.GlobalWriteBufferBytes > 0 && o.WriteBufferBytes > o.GlobalWriteBufferBytes/2 {
		o.WriteBufferBytes = o.GlobalWriteBufferBytes / 2
	}
	if _, e := PlanReadCache(o.ApplicationCacheBytes); e != nil {
		return o, e
	}
	if o.Engine == LevelDB && o.BackgroundJobs != 0 {
		return o, fmt.Errorf("%w: GoLevelDB uses its fixed background workers", ErrUnsupported)
	}
	columns := append([]string(nil), o.Columns...)
	seen := map[string]bool{}
	for _, name := range columns {
		if len(name) == 0 || len(name) > 63 || bytes.IndexByte([]byte(name), 0) >= 0 || seen[name] {
			return o, ErrInvalid
		}
		seen[name] = true
	}
	if !seen[DefaultColumn] {
		columns = append([]string{DefaultColumn}, columns...)
	}
	if len(columns) > 64 {
		return o, ErrInvalid
	}
	// Native RocksDB requires its default column at open; names, not ordinal
	// positions, identify namespaces on both engines.
	sort.SliceStable(columns, func(i, j int) bool { return columns[i] == DefaultColumn && columns[j] != DefaultColumn })
	o.Columns = columns
	return o, nil
}

type Info struct {
	Engine      Engine
	Version     string
	Compression Compression
	Columns     []string
	// LevelDB emulates columns using a one-byte name length and the name.
	// These bytes are physical storage overhead, not part of application keys.
	ColumnKeyOverhead map[string]int
	Memory            MemoryConfiguration
}

// Bounds is [Start, Limit). Nil endpoints are unbounded within one column.
type Bounds struct{ Start, Limit []byte }

// PrefixBounds owns the half-open range containing exactly the keys beginning
// with prefix. An empty prefix selects the whole column.
func PrefixBounds(prefix []byte) Bounds {
	b := Bounds{Start: bytes.Clone(prefix)}
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 255 {
			b.Limit = bytes.Clone(prefix[:i+1])
			b.Limit[i]++
			break
		}
	}
	return b
}

type operation struct {
	column     string
	key, value []byte
	deleted    bool
}
type Batch struct{ entries []operation }

func NewBatch() *Batch { return &Batch{} }
func (b *Batch) Put(column string, key, value []byte) {
	b.PutBorrowed(column, bytes.Clone(key), bytes.Clone(value))
}

// PutBorrowed is for an already-owned immutable write plan. The caller must
// keep both slices alive and unchanged until Write/Replay returns, and while
// reusing the batch. DB and its drivers do not retain them after a write.
// Use Put when the caller may subsequently mutate its input.
func (b *Batch) PutBorrowed(column string, key, value []byte) {
	b.entries = append(b.entries, operation{column: column, key: key, value: value})
}
func (b *Batch) Delete(column string, key []byte) {
	b.entries = append(b.entries, operation{column: column, key: bytes.Clone(key), deleted: true})
}
func (b *Batch) Len() int { return len(b.entries) }
func (b *Batch) Reset()   { clearOperations(b.entries); b.entries = nil }

// BatchWriter consumes read-only operation slices during Replay. It must not
// retain or mutate those slices. Replay preserves operation order and stops at
// the first error without modifying the batch.
type BatchWriter interface {
	Put(column string, key, value []byte) error
	Delete(column string, key []byte) error
}

func (b *Batch) Replay(w BatchWriter) error {
	if b == nil || w == nil {
		return ErrInvalid
	}
	for _, op := range b.entries {
		var err error
		if op.deleted {
			err = w.Delete(op.column, op.key)
		} else {
			err = w.Put(op.column, op.key, op.value)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func clearOperations(es []operation) {
	for i := range es {
		es[i] = operation{}
	}
}

type driver interface {
	get(string, []byte) ([]byte, error)
	snapshot() (driverSnapshot, error)
	write([]operation) error
	compact() error
	property(string, string) (uint64, error)
	close() error
	version() string
}
type driverSnapshot interface {
	get(string, []byte) ([]byte, error)
	iterator(string, Bounds) (driverIterator, error)
	close()
}

// key/value borrow the driver's current position. Only the public wrapper
// copies them, so a C/Go boundary does not introduce a second record copy.
type driverIterator interface {
	first() bool
	last() bool
	seek([]byte) bool
	seekForPrev([]byte) bool
	next() bool
	prev() bool
	key() []byte
	value() []byte
	err() error
	close()
}

// Optional fused positioning for engines with a foreign-function boundary.
// Slices borrow the current position and must be copied before another move.
type driverPredecessorReader interface {
	readForPrev([]byte) (bool, []byte, []byte)
}

type DB struct {
	mu                          sync.RWMutex
	raw                         driver
	options                     Options
	columns                     map[string]bool
	views                       int
	closed, readOnly, uncertain bool
	path                        string
	cache                       *pointCache
	epoch                       uint64
	writes                      WriteCounters
	scope                       CounterScope
}

func Open(path string, options Options, create, readOnly bool) (*DB, error) {
	o, e := options.Resolved()
	if e != nil {
		return nil, e
	}
	if path == "" {
		return nil, ErrInvalid
	}
	path, e = filepath.Abs(path)
	if e != nil {
		return nil, e
	}
	if create && readOnly {
		return nil, ErrReadOnly
	}
	scope, e := newCounterScope(create)
	if e != nil {
		return nil, e
	}
	if e = checkEngineMarker(path, o, create); e != nil {
		return nil, e
	}
	var raw driver
	switch o.Engine {
	case LevelDB:
		raw, e = openLevelDB(path, o, create, readOnly)
	case RocksDB:
		raw, e = openRocksDB(path, o, create, readOnly)
	}
	if e != nil {
		return nil, e
	}
	if create {
		if e = writeEngineMarker(path, o); e != nil {
			raw.close()
			return nil, e
		}
	}
	d := &DB{raw: raw, options: o, path: path, readOnly: readOnly, columns: map[string]bool{}, scope: scope, writes: WriteCounters{Columns: map[string]WriteCount{}, BatchBytesComplete: true}}
	if !readOnly || !o.ExecutionCacheOnly {
		d.cache = newPointCache(o.ApplicationCacheBytes)
	}
	for _, name := range o.Columns {
		d.columns[name] = true
	}
	return d, nil
}

func (d *DB) Info() Info {
	d.mu.RLock()
	defer d.mu.RUnlock()
	i := d.info()
	return i
}

// Path is the owned absolute database path used by offline inventory and to
// reject source/target nesting during bulk construction.
func (d *DB) Path() string { return d.path }
func (d *DB) info() Info {
	i := Info{Engine: d.options.Engine, Version: d.raw.version(), Compression: d.options.Compression, Columns: append([]string(nil), d.options.Columns...), ColumnKeyOverhead: map[string]int{}, Memory: memoryConfiguration(d.options)}
	if i.Engine == LevelDB {
		for _, name := range i.Columns {
			i.ColumnKeyOverhead[name] = 1 + len(name)
		}
	}
	return i
}
func (d *DB) check() error {
	if d.closed {
		return ErrClosed
	}
	if d.uncertain {
		return ErrUncertain
	}
	return nil
}
func (d *DB) column(name string) (string, error) {
	if name == "" {
		name = DefaultColumn
	}
	if !d.columns[name] {
		return "", ErrInvalid
	}
	return name, nil
}

func (d *DB) Get(column string, key []byte) ([]byte, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.check(); e != nil {
		return nil, e
	}
	name, e := d.column(column)
	if e != nil {
		return nil, e
	}
	if len(key) > MaxKeyBytes {
		return nil, ErrLimit
	}
	return d.read(name, key, func() ([]byte, error) { return d.raw.get(name, key) })
}

// Write publishes the entire batch using one WAL-backed synchronous commit.
// A native write error fails stop because the durable outcome may be unknown.
// Validation errors occur before the backend is called and do not poison DB.
func (d *DB) Write(b *Batch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if d.readOnly {
		return ErrReadOnly
	}
	if b == nil {
		return ErrInvalid
	}
	entries := b.entries
	detached := false
	for i, op := range b.entries {
		name, e := d.column(op.column)
		if e != nil {
			return e
		}
		if len(op.key) > MaxKeyBytes || len(op.value) > MaxValueBytes {
			return ErrLimit
		}
		if name != op.column {
			if !detached {
				entries = append([]operation(nil), entries...)
				detached = true
			}
			op.column = name
			entries[i] = op
		}
	}
	if len(entries) == 0 {
		return nil
	}
	if e := d.raw.write(entries); e != nil {
		d.uncertain = true
		d.cache = nil
		return fmt.Errorf("%w: %v", ErrUncertain, e)
	}
	d.epoch++
	d.writes.add(entries, d.options.Engine)
	if p, ok := d.raw.(interface{ lastWritePayloadBytes() uint64 }); ok {
		d.writes.BatchBytes += p.lastWritePayloadBytes()
	} else {
		d.writes.BatchBytesComplete = false
	}
	for _, op := range entries {
		if d.cache != nil {
			d.cache.invalidate(op.column, op.key)
		}
	}
	return nil
}

func (d *DB) NewSnapshot() (*Snapshot, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return nil, e
	}
	raw, e := d.raw.snapshot()
	if e != nil {
		return nil, e
	}
	d.views++
	return &Snapshot{db: d, raw: raw, epoch: d.epoch}, nil
}

// NewIterator owns a snapshot; closing it releases that snapshot automatically.
func (d *DB) NewIterator(column string, bounds Bounds) (*Iterator, error) {
	s, e := d.NewSnapshot()
	if e != nil {
		return nil, e
	}
	it, e := s.NewIterator(column, bounds)
	if e != nil {
		s.Close()
		return nil, e
	}
	it.ownSnapshot = true
	return it, nil
}

// Compact flushes current writes and compacts the logical columns. It is an
// explicit maintenance operation, not part of normal reads or block timing.
func (d *DB) Compact() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e := d.check(); e != nil {
		return e
	}
	if d.readOnly {
		return ErrReadOnly
	}
	return d.raw.compact()
}

// IntProperty exposes engine-specific diagnostics. Unsupported properties must
// return ErrUnsupported, rather than fabricated zero measurements.
func (d *DB) IntProperty(column, name string) (uint64, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if e := d.check(); e != nil {
		return 0, e
	}
	cf, e := d.column(column)
	if e != nil {
		return 0, e
	}
	return d.raw.property(cf, name)
}
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	if d.views != 0 {
		return ErrBusy
	}
	if e := d.raw.close(); e != nil {
		return e
	}
	d.closed = true
	d.cache = nil
	return nil
}

type Snapshot struct {
	mu        sync.RWMutex
	db        *DB
	raw       driverSnapshot
	iterators int
	closed    bool
	epoch     uint64
}

func (s *Snapshot) Get(column string, key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	cf, e := s.db.column(column)
	if e != nil {
		return nil, e
	}
	if len(key) > MaxKeyBytes {
		return nil, ErrLimit
	}
	s.db.mu.RLock()
	defer s.db.mu.RUnlock()
	if s.db.cache != nil && s.epoch == s.db.epoch {
		return s.db.read(cf, key, func() ([]byte, error) { return s.raw.get(cf, key) })
	}
	return s.raw.get(cf, key)
}
func (s *Snapshot) NewIterator(column string, bounds Bounds) (*Iterator, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	cf, e := s.db.column(column)
	if e != nil {
		return nil, e
	}
	if len(bounds.Start) > MaxKeyBytes || len(bounds.Limit) > MaxKeyBytes || bounds.Limit != nil && bytes.Compare(bounds.Start, bounds.Limit) > 0 {
		return nil, ErrInvalid
	}
	bounds = Bounds{bytes.Clone(bounds.Start), bytes.Clone(bounds.Limit)}
	raw, e := s.raw.iterator(cf, bounds)
	if e != nil {
		return nil, e
	}
	s.iterators++
	return &Iterator{snapshot: s, raw: raw, bounds: bounds}, nil
}
func (s *Snapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if s.iterators != 0 {
		return ErrBusy
	}
	s.raw.close()
	s.closed = true
	s.db.mu.Lock()
	s.db.views--
	s.db.mu.Unlock()
	return nil
}

// Iterator is single-owner. Returned Key/Value bytes survive the next seek and
// may be modified by the caller. After Error returns non-nil, close the iterator.
type Iterator struct {
	mu                         sync.Mutex
	snapshot                   *Snapshot
	raw                        driverIterator
	bounds                     Bounds
	valid, closed, ownSnapshot bool
	failure                    error
}

func (it *Iterator) move(f func() bool) bool {
	if it.closed {
		it.failure = ErrClosed
		return false
	}
	if it.failure != nil {
		return false
	}
	it.valid = f()
	if e := it.raw.err(); e != nil {
		it.failure = e
		it.valid = false
	}
	if it.valid && (it.bounds.Start != nil || it.bounds.Limit != nil) {
		k := it.raw.key()
		if e := it.raw.err(); e != nil {
			it.failure = e
			it.valid = false
		}
		if bytes.Compare(k, it.bounds.Start) < 0 || it.bounds.Limit != nil && bytes.Compare(k, it.bounds.Limit) >= 0 {
			it.valid = false
		}
	}
	return it.valid
}
func (it *Iterator) First() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.move(func() bool {
		if it.bounds.Start != nil {
			return it.raw.seek(it.bounds.Start)
		}
		return it.raw.first()
	})
}
func (it *Iterator) Last() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.move(func() bool {
		if it.bounds.Limit == nil {
			return it.raw.last()
		}
		if !it.raw.seekForPrev(it.bounds.Limit) {
			return false
		}
		if bytes.Equal(it.raw.key(), it.bounds.Limit) {
			return it.raw.prev()
		}
		return true
	})
}
func (it *Iterator) Seek(key []byte) bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if len(key) > MaxKeyBytes {
		it.failure = ErrLimit
		it.valid = false
		return false
	}
	return it.move(func() bool {
		if bytes.Compare(key, it.bounds.Start) < 0 {
			key = it.bounds.Start
		}
		if it.bounds.Limit != nil && bytes.Compare(key, it.bounds.Limit) >= 0 {
			return false
		}
		return it.raw.seek(key)
	})
}
func (it *Iterator) SeekForPrev(key []byte) bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if len(key) > MaxKeyBytes {
		it.failure = ErrLimit
		it.valid = false
		return false
	}
	return it.seekForPrev(key)
}
func (it *Iterator) seekForPrev(key []byte) bool {
	return it.move(func() bool {
		if bytes.Compare(key, it.bounds.Start) < 0 {
			return false
		}
		if it.bounds.Limit != nil && bytes.Compare(key, it.bounds.Limit) >= 0 {
			if !it.raw.seekForPrev(it.bounds.Limit) {
				return false
			}
			if bytes.Equal(it.raw.key(), it.bounds.Limit) {
				return it.raw.prev()
			}
			return true
		}
		return it.raw.seekForPrev(key)
	})
}

// ReadForPrev positions at the greatest key <= key within the iterator bounds.
// It returns owned key/value-prefix bytes and the full value length. A missing
// predecessor returns ErrNotFound without poisoning subsequent seeks. Like
// SeekForPrev, all other failures are sticky until the iterator is closed.
func (it *Iterator) ReadForPrev(key []byte, limit int) ([]byte, []byte, int, error) {
	it.mu.Lock()
	defer it.mu.Unlock()
	switch {
	case it.closed:
		it.failure = ErrClosed
	case it.failure != nil:
	case len(key) > MaxKeyBytes:
		it.failure = ErrLimit
	case limit < 0:
		it.failure = ErrInvalid
	}
	if it.failure != nil {
		it.valid = false
		return nil, nil, 0, it.failure
	}
	var k, v []byte
	if fast, ok := it.raw.(driverPredecessorReader); ok && it.bounds.Start == nil && it.bounds.Limit == nil {
		it.valid, k, v = fast.readForPrev(key)
	} else if it.seekForPrev(key) {
		k, v = it.raw.key(), it.raw.value()
	}
	if e := it.raw.err(); e != nil {
		it.failure = e
	}
	if len(k) > MaxKeyBytes || len(v) > MaxValueBytes {
		it.failure = ErrLimit
	}
	if it.failure != nil {
		it.valid = false
		return nil, nil, 0, it.failure
	}
	if !it.valid {
		return nil, nil, 0, ErrNotFound
	}
	size := len(v)
	if size > limit {
		v = v[:limit]
	}
	return bytes.Clone(k), bytes.Clone(v), size, nil
}

func (it *Iterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.move(func() bool { return it.valid && it.raw.next() })
}
func (it *Iterator) Prev() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.move(func() bool { return it.valid && it.raw.prev() })
}
func (it *Iterator) Valid() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	return !it.closed && it.valid && it.failure == nil
}
func (it *Iterator) Error() error { it.mu.Lock(); defer it.mu.Unlock(); return it.failure }
func (it *Iterator) Key() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed || !it.valid {
		return nil
	}
	k := it.raw.key()
	if e := it.raw.err(); e != nil {
		it.failure = e
		it.valid = false
		return nil
	}
	if len(k) > MaxKeyBytes {
		it.failure = ErrLimit
		it.valid = false
		return nil
	}
	return bytes.Clone(k)
}
func (it *Iterator) Value() []byte {
	v, _ := it.ValuePrefix(MaxValueBytes)
	return v
}

// ValuePrefix copies at most limit bytes of the current value and also returns
// its full length, so callers can enforce their usual record and read budgets.
func (it *Iterator) ValuePrefix(limit int) ([]byte, int) {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.closed || !it.valid {
		return nil, 0
	}
	if limit < 0 {
		it.failure = ErrInvalid
		it.valid = false
		return nil, 0
	}
	v := it.raw.value()
	if e := it.raw.err(); e != nil {
		it.failure = e
		it.valid = false
		return nil, 0
	}
	size := len(v)
	if size > limit {
		v = v[:limit]
	}
	return bytes.Clone(v), size
}
func (it *Iterator) Close() error {
	it.mu.Lock()
	if it.closed {
		it.mu.Unlock()
		return nil
	}
	it.raw.close()
	it.closed = true
	it.valid = false
	it.snapshot.mu.Lock()
	it.snapshot.iterators--
	it.snapshot.mu.Unlock()
	own, s := it.ownSnapshot, it.snapshot
	it.mu.Unlock()
	if own {
		return s.Close()
	}
	return nil
}
