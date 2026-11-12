package hybridtrie

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

var familyNames = []string{"default", "prefix", "hash"}

type kvEngine struct{ store *kvstore.DB }

// Existing native diagnostic fixtures retain this private name. Production
// reads, cursors and batches all use kvEngine, irrespective of selected engine.
type rocksEngine = kvEngine
type kvSnapshot struct {
	view    *kvstore.Snapshot
	failure error
}
type kvPredecessor struct{ iterator *kvstore.Iterator }

func storageOptions(request *kvstore.Options) (kvstore.Options, error) {
	// Retain the previous RocksDB options when the new field is omitted.
	o := kvstore.Options{Engine: kvstore.RocksDB, Compression: kvstore.None, WriteBufferBytes: 8 << 20, OpenFiles: 128}
	if request != nil {
		o = *request
	}
	if len(o.Columns) != 0 {
		seen := map[string]bool{}
		for _, name := range o.Columns {
			seen[name] = true
		}
		if len(o.Columns) != 3 || len(seen) != 3 || !seen["default"] || !seen["prefix"] || !seen["hash"] {
			return o, ErrCorrupt
		}
	}
	o.Columns = append([]string(nil), familyNames...)
	return o.Resolved()
}
func kvError(e error) error {
	if e == nil {
		return nil
	}
	for _, pair := range [][2]error{{kvstore.ErrNotFound, ErrNotFound}, {kvstore.ErrClosed, ErrClosed}, {kvstore.ErrBusy, ErrBusy}, {kvstore.ErrReadOnly, ErrReadOnly}, {kvstore.ErrUnavailable, ErrEngineUnavailable}, {kvstore.ErrInvalid, ErrCorrupt}, {kvstore.ErrCorrupt, ErrCorrupt}, {kvstore.ErrLimit, ErrLimit}, {kvstore.ErrUncertain, ErrUncertain}} {
		if errors.Is(e, pair[0]) {
			return fmt.Errorf("%w: %v", pair[1], e)
		}
	}
	return e
}
func family(cf column) (string, error) {
	if cf > hashColumn {
		return "", ErrCorrupt
	}
	return familyNames[cf], nil
}

func openEngine(path string, create, readOnly bool) (diskEngine, error) {
	return openConfiguredEngine(path, nil, create, readOnly)
}
func openConfiguredEngine(path string, options *kvstore.Options, create, readOnly bool) (diskEngine, error) {
	o, e := storageOptions(options)
	if e != nil {
		return nil, kvError(e)
	}
	db, e := kvstore.Open(path, o, create, readOnly)
	if e != nil {
		return nil, kvError(e)
	}
	return &kvEngine{store: db}, nil
}
func (r *kvEngine) close() error { return kvError(r.store.Close()) }
func (r *kvEngine) snapshot() diskSnapshot {
	s, e := r.store.NewSnapshot()
	return &kvSnapshot{view: s, failure: kvError(e)}
}
func (r *kvEngine) write(es []entry) error {
	b := kvstore.NewBatch()
	for _, e := range es {
		cf, err := family(e.cf)
		if err != nil {
			return err
		}
		b.PutBorrowed(cf, e.key, e.value)
	}
	return kvError(r.store.Write(b))
}
func (s *kvSnapshot) close() {
	if s.view != nil {
		s.view.Close()
	}
}
func (s *kvSnapshot) get(cf column, key []byte) ([]byte, error) {
	if s.failure != nil {
		return nil, s.failure
	}
	name, e := family(cf)
	if e != nil {
		return nil, e
	}
	v, e := s.view.Get(name, key)
	if len(v) > MaxRecordBytes+4096 {
		return nil, ErrLimit
	}
	return v, kvError(e)
}
func (s *kvSnapshot) newPredecessor(cf column) (diskPredecessor, error) {
	if s.failure != nil {
		return nil, s.failure
	}
	name, e := family(cf)
	if e != nil {
		return nil, e
	}
	it, e := s.view.NewIterator(name, kvstore.Bounds{})
	if e != nil {
		return nil, kvError(e)
	}
	return &kvPredecessor{iterator: it}, nil
}
func (s *kvSnapshot) prev(cf column, key []byte) ([]byte, []byte, error) {
	p, e := s.newPredecessor(cf)
	if e != nil {
		return nil, nil, e
	}
	defer p.close()
	return p.prev(key)
}
func (p *kvPredecessor) close() { p.iterator.Close() }
func (p *kvPredecessor) prev(key []byte) ([]byte, []byte, error) {
	k, v, _, err := p.prevLimited(key, MaxRecordBytes+4096)
	return k, v, err
}

func (p *kvPredecessor) prevLimited(key []byte, limit int) ([]byte, []byte, int, error) {
	k, v, size, e := p.iterator.ReadForPrev(key, limit)
	if e != nil {
		return nil, nil, 0, kvError(e)
	}
	if len(k) > 128 || size > MaxRecordBytes+4096 {
		return nil, nil, 0, ErrLimit
	}
	return k, v, size, nil
}
func (s *kvSnapshot) scan(cf column, start, end []byte, visit func([]byte, []byte) error) error {
	if s.failure != nil {
		return s.failure
	}
	name, e := family(cf)
	if e != nil {
		return e
	}
	it, e := s.view.NewIterator(name, kvstore.Bounds{Start: start, Limit: end})
	if e != nil {
		return kvError(e)
	}
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
		key, value := it.Key(), it.Value()
		if e := it.Error(); e != nil {
			return kvError(e)
		}
		if len(key) > 4096 || len(value) > MaxRecordBytes+4096 {
			return ErrLimit
		}
		if len(end) > 0 && bytes.Compare(key, end) >= 0 {
			break
		}
		if e := visit(key, value); e != nil {
			return e
		}
	}
	return kvError(it.Error())
}
