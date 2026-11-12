package logicalio

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"testing"
)

func TestCountsRepeatedPutsDeletesAndSnapshots(t *testing.T) {
	s := Stats{}
	b := new(leveldb.Batch)
	b.Put([]byte("Na"), []byte("abc"))
	b.Put([]byte("Na"), []byte("x"))
	b.Delete([]byte("Na"))
	s.AddBatch(b)
	before := s.Clone()
	s.AddBatch(b)
	d := Difference(before, s).Total()
	if d != (Count{Puts: 2, Deletes: 1, KeyBytes: 4, ValueBytes: 4, DeleteKeyBytes: 2}) {
		t.Fatal(d)
	}
	if before.Total() != d {
		t.Fatal("snapshot mutated")
	}
}

func TestCommonBatchMatchesLegacyLogicalCounts(t *testing.T) {
	legacy := new(leveldb.Batch)
	common := kvstore.NewBatch()
	legacy.Put([]byte("Pa"), []byte("value"))
	common.Put("default", []byte("Pa"), []byte("value"))
	legacy.Delete([]byte("Pb"))
	common.Delete("default", []byte("Pb"))
	a, b := Stats{}, Stats{}
	a.AddBatch(legacy)
	b.AddKVBatch(common)
	if a.Total() != b.Total() || a["50"] != b["50"] {
		t.Fatal(a, b)
	}
}
