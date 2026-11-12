package patharchive

import "github.com/XiaodongQi1/HybridTrie/kvstore"

type sharedEngine struct{ db *kvstore.DB }
type sharedSnapshot struct{ view *kvstore.Snapshot }
type sharedIterator struct {
	it         *kvstore.Iterator
	failure    error
	positioned bool
}

func pathStorageOptions(request kvstore.Options) (kvstore.Options, error) {
	if len(request.Columns) > 0 && (len(request.Columns) != 1 || request.Columns[0] != kvstore.DefaultColumn) {
		return request, kvstore.ErrInvalid
	}
	request.Columns = []string{kvstore.DefaultColumn}
	return request.Resolved()
}
func openSharedEngine(path string, o kvstore.Options, create, ro bool) (diskEngine, error) {
	d, e := kvstore.Open(path, o, create, ro)
	if e != nil {
		return nil, e
	}
	return &sharedEngine{d}, nil
}
func (d *sharedEngine) Get(k []byte) ([]byte, error) { return d.db.Get(kvstore.DefaultColumn, k) }
func (d *sharedEngine) Write(b *diskBatch) error     { return d.db.Write(b.raw) }
func (d *sharedEngine) Put(k, v []byte) error        { b := newDiskBatch(); b.Put(k, v); return d.Write(b) }
func (d *sharedEngine) Close() error                 { return d.db.Close() }
func sharedBounds(r *kvstore.Bounds) kvstore.Bounds {
	if r == nil {
		return kvstore.Bounds{}
	}
	return *r
}
func (d *sharedEngine) NewIterator(r *kvstore.Bounds) iterator {
	it, e := d.db.NewIterator(kvstore.DefaultColumn, sharedBounds(r))
	return &sharedIterator{it: it, failure: e}
}
func (d *sharedEngine) GetSnapshot() (diskSnapshot, error) {
	s, e := d.db.NewSnapshot()
	if e != nil {
		return nil, e
	}
	return &sharedSnapshot{s}, nil
}
func (s *sharedSnapshot) Get(k []byte) ([]byte, error) { return s.view.Get(kvstore.DefaultColumn, k) }
func (s *sharedSnapshot) NewIterator(r *kvstore.Bounds) iterator {
	it, e := s.view.NewIterator(kvstore.DefaultColumn, sharedBounds(r))
	return &sharedIterator{it: it, failure: e}
}
func (s *sharedSnapshot) Release() { s.view.Close() }
func (it *sharedIterator) First() bool {
	it.positioned = true
	return it.failure == nil && it.it.First()
}
func (it *sharedIterator) Seek(k []byte) bool {
	it.positioned = true
	return it.failure == nil && it.it.Seek(k)
}
func (it *sharedIterator) Next() bool {
	if !it.positioned {
		return it.First()
	}
	return it.failure == nil && it.it.Next()
}
func (it *sharedIterator) Key() []byte {
	if it.failure != nil {
		return nil
	}
	return it.it.Key()
}
func (it *sharedIterator) Value() []byte {
	if it.failure != nil {
		return nil
	}
	return it.it.Value()
}
func (it *sharedIterator) Error() error {
	if it.failure != nil {
		return it.failure
	}
	return it.it.Error()
}
func (it *sharedIterator) Release() {
	if it.it != nil {
		it.it.Close()
	}
}
