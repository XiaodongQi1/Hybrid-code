package patharchive

import "github.com/XiaodongQi1/Hybrid-code/kvstore"

// These private contracts keep history algorithms independent of an LSM API.
// A nil range selects the whole default column. Iterators retain the legacy
// positioning convention: Next on a new iterator positions it at First.
type diskReader interface {
	Get([]byte) ([]byte, error)
	NewIterator(*kvstore.Bounds) iterator
}
type diskSnapshot interface {
	diskReader
	Release()
}
type diskEngine interface {
	diskReader
	GetSnapshot() (diskSnapshot, error)
	Write(*diskBatch) error
	Put([]byte, []byte) error
	Close() error
}
type diskBatch struct{ raw *kvstore.Batch }

func newDiskBatch() *diskBatch       { return &diskBatch{kvstore.NewBatch()} }
func (b *diskBatch) Put(k, v []byte) { b.raw.Put(kvstore.DefaultColumn, k, v) }
func (b *diskBatch) Delete(k []byte) { b.raw.Delete(kvstore.DefaultColumn, k) }
func (b *diskBatch) Len() int        { return b.raw.Len() }

func (b *diskBatch) Reset() { b.raw.Reset() }

func prefixRange(prefix []byte) *kvstore.Bounds {
	r := kvstore.PrefixBounds(prefix)
	return &r
}
