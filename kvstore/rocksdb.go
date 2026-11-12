//go:build rocksdb && cgo

package kvstore

/*
#cgo LDFLAGS: -lrocksdb
#cgo darwin LDFLAGS: -lc++
#cgo linux LDFLAGS: -lstdc++
#include <stdlib.h>
#include <rocksdb/c.h>
int kvstore_rocksdb_version_ok(void);
uint32_t kvstore_rocksdb_ticker(int index);

typedef struct {
 const char *key, *value;
 size_t key_n, value_n;
 unsigned char valid;
 char *error;
} kvstore_predecessor;

// Return borrowed pointers; Go copies them while holding the iterator lock.
static kvstore_predecessor kvstore_read_for_prev(rocksdb_iterator_t *it, const char *key, size_t n) {
 kvstore_predecessor out = {0};
 rocksdb_iter_seek_for_prev(it, key, n);
 out.valid = rocksdb_iter_valid(it);
 if (out.valid) {
  out.key = rocksdb_iter_key(it, &out.key_n);
  out.value = rocksdb_iter_value(it, &out.value_n);
 } else {
  rocksdb_iter_get_error(it, &out.error);
 }
 return out;
}
*/
import "C"
import (
	"fmt"
	"sort"
	"unsafe"
)

type rocksDriver struct {
	beforeWrite       func(WriteCounters) error
	lastBatchBytes    uint64
	db                *C.rocksdb_t
	columns           map[string]*C.rocksdb_column_family_handle_t
	read              *C.rocksdb_readoptions_t
	cache             *C.rocksdb_cache_t
	writeBuffer       *C.rocksdb_write_buffer_manager_t
	statisticsOptions *C.rocksdb_options_t
}
type rocksSnapshot struct {
	db       *rocksDriver
	snapshot *C.rocksdb_snapshot_t
	read     *C.rocksdb_readoptions_t
}
type rocksIterator struct {
	iterator *C.rocksdb_iterator_t
	failure  error
}

func cbytes(b []byte) *C.char {
	if len(b) == 0 {
		return nil
	}
	return (*C.char)(unsafe.Pointer(&b[0]))
}
func cerror(e *C.char) error {
	if e == nil {
		return nil
	}
	defer C.rocksdb_free(unsafe.Pointer(e))
	return fmt.Errorf("rocksdb: %s", C.GoString(e))
}

func openRocksDB(path string, o Options, create, readOnly bool) (driver, error) {
	if C.kvstore_rocksdb_version_ok() != 1 {
		return nil, fmt.Errorf("%w: RocksDB 9.10.0 required", ErrUnavailable)
	}
	opts := C.rocksdb_options_create()
	defer C.rocksdb_options_destroy(opts)
	var flag C.uchar
	if create {
		flag = 1
	}
	C.rocksdb_options_set_create_if_missing(opts, flag)
	C.rocksdb_options_set_create_missing_column_families(opts, flag)
	C.rocksdb_options_set_error_if_exists(opts, flag)
	compression := C.int(C.rocksdb_no_compression)
	if o.Compression == Snappy {
		compression = C.int(C.rocksdb_snappy_compression)
	}
	C.rocksdb_options_set_compression(opts, compression)
	C.rocksdb_options_set_write_buffer_size(opts, C.size_t(o.WriteBufferBytes))
	C.rocksdb_options_set_max_write_buffer_number(opts, 2)
	C.rocksdb_options_set_max_open_files(opts, C.int(o.OpenFiles))
	r := &rocksDriver{beforeWrite: o.BeforeWrite, columns: map[string]*C.rocksdb_column_family_handle_t{}}
	if o.BackgroundJobs > 0 {
		C.rocksdb_options_set_max_background_jobs(opts, C.int(o.BackgroundJobs))
	}
	if o.GlobalWriteBufferBytes > 0 {
		r.writeBuffer = C.rocksdb_write_buffer_manager_create(C.size_t(o.GlobalWriteBufferBytes), C.bool(true))
		if r.writeBuffer == nil {
			return nil, ErrLimit
		}
		C.rocksdb_options_set_write_buffer_manager(opts, r.writeBuffer)
	}
	if o.EnableStatistics {
		C.rocksdb_options_enable_statistics(opts)
		C.rocksdb_options_set_statistics_level(opts, C.rocksdb_statistics_level_except_histogram_or_timers)
	}
	if o.BlockCacheBytes > 0 {
		r.cache = C.rocksdb_cache_create_lru(C.size_t(o.BlockCacheBytes))
		if r.cache == nil {
			r.close()
			return nil, ErrLimit
		}
		table := C.rocksdb_block_based_options_create()
		C.rocksdb_block_based_options_set_block_cache(table, r.cache)
		C.rocksdb_options_set_block_based_table_factory(opts, table)
		C.rocksdb_block_based_options_destroy(table)
	}
	// Pointer arrays live in C memory. One resource policy is shared by the
	// column families; native handles are indexed by column name after open.
	n := len(o.Columns)
	names := C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof(uintptr(0))))
	options := C.malloc(C.size_t(n) * C.size_t(unsafe.Sizeof(uintptr(0))))
	handles := C.calloc(C.size_t(n), C.size_t(unsafe.Sizeof(uintptr(0))))
	if names == nil || options == nil || handles == nil {
		C.free(names)
		C.free(options)
		C.free(handles)
		r.close()
		return nil, ErrLimit
	}
	defer C.free(names)
	defer C.free(options)
	defer C.free(handles)
	ns := unsafe.Slice((**C.char)(names), n)
	os := unsafe.Slice((**C.rocksdb_options_t)(options), n)
	hs := unsafe.Slice((**C.rocksdb_column_family_handle_t)(handles), n)
	for i, name := range o.Columns {
		ns[i] = C.CString(name)
		os[i] = opts
		defer C.free(unsafe.Pointer(ns[i]))
	}
	name := C.CString(path)
	defer C.free(unsafe.Pointer(name))
	var err *C.char
	if readOnly {
		r.db = C.rocksdb_open_for_read_only_column_families(opts, name, C.int(n), (**C.char)(names), (**C.rocksdb_options_t)(options), (**C.rocksdb_column_family_handle_t)(handles), 0, &err)
	} else {
		r.db = C.rocksdb_open_column_families(opts, name, C.int(n), (**C.char)(names), (**C.rocksdb_options_t)(options), (**C.rocksdb_column_family_handle_t)(handles), &err)
	}
	for i, name := range o.Columns {
		if hs[i] != nil {
			r.columns[name] = hs[i]
		}
	}
	if e := cerror(err); e != nil {
		r.close()
		return nil, e
	}
	if r.db == nil {
		r.close()
		return nil, ErrCorrupt
	}
	r.read = C.rocksdb_readoptions_create()
	if o.EnableStatistics {
		r.statisticsOptions = C.rocksdb_options_create_copy(opts)
	}
	if r.read == nil || o.EnableStatistics && r.statisticsOptions == nil {
		r.close()
		return nil, ErrLimit
	}
	return r, nil
}
func (*rocksDriver) version() string { return "rocksdb/9.10.0" }
func (r *rocksDriver) close() error {
	if r.read != nil {
		C.rocksdb_readoptions_destroy(r.read)
		r.read = nil
	}
	for name, cf := range r.columns {
		C.rocksdb_column_family_handle_destroy(cf)
		delete(r.columns, name)
	}
	if r.db != nil {
		C.rocksdb_close(r.db)
		r.db = nil
	}
	if r.cache != nil {
		C.rocksdb_cache_destroy(r.cache)
		r.cache = nil
	}
	if r.statisticsOptions != nil {
		C.rocksdb_options_destroy(r.statisticsOptions)
		r.statisticsOptions = nil
	}
	if r.writeBuffer != nil {
		C.rocksdb_write_buffer_manager_destroy(r.writeBuffer)
		r.writeBuffer = nil
	}
	return nil
}

func (r *rocksDriver) statistics() (EngineStatistics, error) {
	var out EngineStatistics
	value := func(v uint64) *uint64 { return &v }
	if r.cache != nil {
		out.BlockCacheBytes = value(uint64(C.rocksdb_cache_get_usage(r.cache)))
		out.BlockCacheCapacityBytes = value(uint64(C.rocksdb_cache_get_capacity(r.cache)))
		out.BlockCachePinnedBytes = value(uint64(C.rocksdb_cache_get_pinned_usage(r.cache)))
	}
	if r.writeBuffer != nil {
		out.WriteBufferCapacityBytes = value(uint64(C.rocksdb_write_buffer_manager_buffer_size(r.writeBuffer)))
		out.MemtableBytes = value(uint64(C.rocksdb_write_buffer_manager_memory_usage(r.writeBuffer)))
		out.MutableMemtableBytes = value(uint64(C.rocksdb_write_buffer_manager_mutable_memtable_memory_usage(r.writeBuffer)))
	} else {
		var total uint64
		for column := range r.columns {
			v, e := r.property(column, "rocksdb.size-all-mem-tables")
			if e != nil {
				return out, e
			}
			total += v
		}
		out.MemtableBytes = value(total)
	}
	var sst uint64
	for column := range r.columns {
		v, e := r.property(column, "rocksdb.live-sst-files-size")
		if e != nil {
			return out, e
		}
		sst += v
	}
	out.LiveSSTBytes = value(sst)
	if r.statisticsOptions != nil {
		out.WALWriteBytes = value(uint64(C.rocksdb_options_statistics_get_ticker_count(r.statisticsOptions, C.kvstore_rocksdb_ticker(0))))
		out.FlushWriteBytes = value(uint64(C.rocksdb_options_statistics_get_ticker_count(r.statisticsOptions, C.kvstore_rocksdb_ticker(1))))
		out.CompactionWriteBytes = value(uint64(C.rocksdb_options_statistics_get_ticker_count(r.statisticsOptions, C.kvstore_rocksdb_ticker(2))))
	}
	return out, nil
}
func (r *rocksDriver) readValue(options *C.rocksdb_readoptions_t, cf string, key []byte) ([]byte, error) {
	var n C.size_t
	var err *C.char
	v := C.rocksdb_get_cf(r.db, options, r.columns[cf], cbytes(key), C.size_t(len(key)), &n, &err)
	if v != nil {
		defer C.rocksdb_free(unsafe.Pointer(v))
	}
	if e := cerror(err); e != nil {
		return nil, e
	}
	if v == nil {
		return nil, ErrNotFound
	}
	if uint64(n) > MaxValueBytes {
		return nil, ErrLimit
	}
	return C.GoBytes(unsafe.Pointer(v), C.int(n)), nil
}
func (r *rocksDriver) get(cf string, key []byte) ([]byte, error) { return r.readValue(r.read, cf, key) }
func (r *rocksDriver) snapshot() (driverSnapshot, error) {
	s := &rocksSnapshot{db: r, snapshot: C.rocksdb_create_snapshot(r.db), read: C.rocksdb_readoptions_create()}
	if s.snapshot == nil || s.read == nil {
		s.close()
		return nil, ErrLimit
	}
	C.rocksdb_readoptions_set_snapshot(s.read, s.snapshot)
	C.rocksdb_readoptions_set_total_order_seek(s.read, 1)
	return s, nil
}
func (s *rocksSnapshot) get(cf string, key []byte) ([]byte, error) {
	return s.db.readValue(s.read, cf, key)
}
func (s *rocksSnapshot) close() {
	if s.read != nil {
		C.rocksdb_readoptions_destroy(s.read)
		s.read = nil
	}
	if s.snapshot != nil {
		C.rocksdb_release_snapshot(s.db.db, s.snapshot)
		s.snapshot = nil
	}
}
func (s *rocksSnapshot) iterator(cf string, _ Bounds) (driverIterator, error) {
	iter := C.rocksdb_create_iterator_cf(s.db.db, s.read, s.db.columns[cf])
	if iter == nil {
		return nil, ErrLimit
	}
	return &rocksIterator{iterator: iter}, nil
}
func (r *rocksDriver) write(es []operation) error {
	b := C.rocksdb_writebatch_create()
	defer C.rocksdb_writebatch_destroy(b)
	for _, op := range es {
		if op.deleted {
			C.rocksdb_writebatch_delete_cf(b, r.columns[op.column], cbytes(op.key), C.size_t(len(op.key)))
		} else {
			C.rocksdb_writebatch_put_cf(b, r.columns[op.column], cbytes(op.key), C.size_t(len(op.key)), cbytes(op.value), C.size_t(len(op.value)))
		}
	}
	o := C.rocksdb_writeoptions_create()
	defer C.rocksdb_writeoptions_destroy(o)
	C.rocksdb_writeoptions_set_sync(o, 1)
	var err *C.char
	var size C.size_t
	C.rocksdb_writebatch_data(b, &size)
	if e := prepareWrite(r.beforeWrite, es, RocksDB, uint64(size)); e != nil {
		return e
	}
	C.rocksdb_write(r.db, o, b, &err)
	if e := cerror(err); e != nil {
		return e
	}
	r.lastBatchBytes = uint64(size)
	return nil
}
func (r *rocksDriver) lastWritePayloadBytes() uint64 { return r.lastBatchBytes }
func (r *rocksDriver) property(cf, name string) (uint64, error) {
	k := C.CString(name)
	defer C.free(unsafe.Pointer(k))
	var value C.uint64_t
	if C.rocksdb_property_int_cf(r.db, r.columns[cf], k, &value) != 0 {
		return 0, fmt.Errorf("%w: %s", ErrUnsupported, name)
	}
	return uint64(value), nil
}
func (r *rocksDriver) compact() error {
	o := C.rocksdb_flushoptions_create()
	defer C.rocksdb_flushoptions_destroy(o)
	C.rocksdb_flushoptions_set_wait(o, 1)
	names := make([]string, 0, len(r.columns))
	for name := range r.columns {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		cf := r.columns[name]
		var err *C.char
		C.rocksdb_flush_cf(r.db, o, cf, &err)
		if e := cerror(err); e != nil {
			return e
		}
		C.rocksdb_compact_range_cf(r.db, cf, nil, 0, nil, 0)
		n, e := r.property(name, "rocksdb.background-errors")
		if e != nil {
			return e
		}
		if n != 0 {
			return fmt.Errorf("rocksdb: background error during compaction")
		}
	}
	var err *C.char
	C.rocksdb_flush_wal(r.db, 1, &err)
	return cerror(err)
}
func (it *rocksIterator) valid() bool {
	valid := C.rocksdb_iter_valid(it.iterator) != 0
	// RocksDB guarantees OK status for a valid position. Capture an error only
	// when positioning fails, rather than crossing the C boundary on every getter.
	if !valid {
		var e *C.char
		C.rocksdb_iter_get_error(it.iterator, &e)
		if err := cerror(e); err != nil {
			it.failure = err
		}
	}
	return valid
}
func (it *rocksIterator) first() bool { C.rocksdb_iter_seek_to_first(it.iterator); return it.valid() }
func (it *rocksIterator) last() bool  { C.rocksdb_iter_seek_to_last(it.iterator); return it.valid() }
func (it *rocksIterator) seek(k []byte) bool {
	C.rocksdb_iter_seek(it.iterator, cbytes(k), C.size_t(len(k)))
	return it.valid()
}
func (it *rocksIterator) seekForPrev(k []byte) bool {
	C.rocksdb_iter_seek_for_prev(it.iterator, cbytes(k), C.size_t(len(k)))
	return it.valid()
}

func (it *rocksIterator) readForPrev(k []byte) (bool, []byte, []byte) {
	out := C.kvstore_read_for_prev(it.iterator, cbytes(k), C.size_t(len(k)))
	if out.valid == 0 {
		if e := cerror(out.error); e != nil {
			it.failure = e
		}
		return false, nil, nil
	}
	if uint64(out.key_n) > MaxKeyBytes || uint64(out.value_n) > MaxValueBytes {
		it.failure = ErrLimit
		return false, nil, nil
	}
	return true, unsafe.Slice((*byte)(unsafe.Pointer(out.key)), int(out.key_n)),
		unsafe.Slice((*byte)(unsafe.Pointer(out.value)), int(out.value_n))
}

func (it *rocksIterator) next() bool { C.rocksdb_iter_next(it.iterator); return it.valid() }
func (it *rocksIterator) prev() bool { C.rocksdb_iter_prev(it.iterator); return it.valid() }
func (it *rocksIterator) key() []byte {
	var n C.size_t
	p := C.rocksdb_iter_key(it.iterator, &n)
	if uint64(n) > MaxKeyBytes {
		it.failure = ErrLimit
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))
}
func (it *rocksIterator) value() []byte {
	var n C.size_t
	p := C.rocksdb_iter_value(it.iterator, &n)
	if uint64(n) > MaxValueBytes {
		it.failure = ErrLimit
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(p)), int(n))
}
func (it *rocksIterator) err() error {
	return it.failure
}
func (it *rocksIterator) close() { C.rocksdb_iter_destroy(it.iterator); it.iterator = nil }
