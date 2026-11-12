// Package logicalio counts successful application KV writes, before backend
// compression, WAL framing or compaction. Repeated puts count repeatedly.
package logicalio

import (
	"encoding/hex"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
)

type Count struct{ Puts, Deletes, KeyBytes, ValueBytes, DeleteKeyBytes uint64 }
type Stats map[string]Count

func prefix(k []byte) string {
	if len(k) == 0 {
		return "empty"
	}
	return hex.EncodeToString(k[:1])
}

type classified struct {
	stats    Stats
	classify func([]byte) string
}

func (r classified) Put(k, v []byte) {
	p := r.classify(k)
	n := r.stats[p]
	n.Puts++
	n.KeyBytes += uint64(len(k))
	n.ValueBytes += uint64(len(v))
	r.stats[p] = n
}
func (r classified) Delete(k []byte) {
	p := r.classify(k)
	n := r.stats[p]
	n.Deletes++
	n.DeleteKeyBytes += uint64(len(k))
	r.stats[p] = n
}
func (s Stats) AddBatch(b *leveldb.Batch) { s.AddClassified(b, prefix) }

type kvClassified struct{ classified }

func (r kvClassified) Put(_ string, k, v []byte) error { r.classified.Put(k, v); return nil }
func (r kvClassified) Delete(_ string, k []byte) error { r.classified.Delete(k); return nil }

func (s Stats) AddKVBatch(b *kvstore.Batch) {
	if e := b.Replay(kvClassified{classified{s, prefix}}); e != nil {
		panic(e)
	}
}
func (s Stats) AddClassified(b *leveldb.Batch, c func([]byte) string) {
	if e := b.Replay(classified{s, c}); e != nil {
		panic(e)
	}
}
func (s Stats) Clone() Stats {
	r := Stats{}
	for k, v := range s {
		r[k] = v
	}
	return r
}
func Difference(before, after Stats) Stats {
	r := Stats{}
	for k, b := range after {
		a := before[k]
		r[k] = Count{b.Puts - a.Puts, b.Deletes - a.Deletes, b.KeyBytes - a.KeyBytes, b.ValueBytes - a.ValueBytes, b.DeleteKeyBytes - a.DeleteKeyBytes}
	}
	return r
}
func (s Stats) Total() Count {
	var r Count
	for _, n := range s {
		r.Puts += n.Puts
		r.Deletes += n.Deletes
		r.KeyBytes += n.KeyBytes
		r.ValueBytes += n.ValueBytes
		r.DeleteKeyBytes += n.DeleteKeyBytes
	}
	return r
}
