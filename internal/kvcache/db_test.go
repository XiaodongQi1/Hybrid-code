package kvcache

import (
	"bytes"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"path/filepath"
	"sync"
	"testing"
)

func TestCacheAtomicSnapshotsAndBounds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	d, e := OpenFile(path, nil, 300)
	if e != nil {
		t.Fatal(e)
	}
	key := []byte("key")
	old := []byte("old")
	if e = d.Put(key, old, &opt.WriteOptions{Sync: true}); e != nil {
		t.Fatal(e)
	}
	old[0] = 'X'
	v, e := d.Get(key, nil)
	if e != nil || string(v) != "old" {
		t.Fatal(v, e)
	}
	v[0] = 'Y'
	snap, e := d.GetSnapshot()
	if e != nil {
		t.Fatal(e)
	}
	b := new(leveldb.Batch)
	b.Put(key, []byte("new"))
	b.Put([]byte("tmp"), []byte("x"))
	b.Delete([]byte("tmp"))
	if e = d.Write(b, &opt.WriteOptions{Sync: true}); e != nil {
		t.Fatal(e)
	}
	v, e = snap.Get(key, nil)
	if e != nil || string(v) != "old" {
		t.Fatal("snapshot saw future", string(v), e)
	}
	v, e = d.Get(key, nil)
	if e != nil || string(v) != "new" {
		t.Fatal(v, e)
	}
	if _, e = d.Get([]byte("tmp"), nil); e != leveldb.ErrNotFound {
		t.Fatal(e)
	}
	if e = d.Delete(key, nil); e != nil {
		t.Fatal(e)
	}
	if _, e = d.Get(key, nil); e != leveldb.ErrNotFound {
		t.Fatal(e)
	}
	for i := byte(0); i < 10; i++ {
		if e = d.Put([]byte{i}, []byte{i}, nil); e != nil {
			t.Fatal(e)
		}
	}
	s := d.CacheStats()
	if s.ChargedBytes > s.Capacity || s.Evictions == 0 || s.Hits == 0 {
		t.Fatal(s)
	}
	huge := bytes.Repeat([]byte{1}, 400)
	if e = d.Put([]byte("large"), huge, nil); e != nil {
		t.Fatal(e)
	}
	if d.CacheStats().ChargedBytes > 300 {
		t.Fatal("budget")
	}
	snap.Release()
	if _, e = snap.Get(key, nil); e != leveldb.ErrSnapshotReleased {
		t.Fatal(e)
	}
	if e = d.Close(); e != nil {
		t.Fatal(e)
	}
	d, e = OpenFile(path, &opt.Options{ReadOnly: true}, 300)
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	v, e = d.Get([]byte("large"), nil)
	if e != nil || !bytes.Equal(v, huge) {
		t.Fatal(e)
	}
	if e = d.Put([]byte("large"), []byte("bad"), nil); e == nil {
		t.Fatal("readonly write accepted")
	}
	v, e = d.Get([]byte("large"), nil)
	if e != nil || !bytes.Equal(v, huge) {
		t.Fatal("failed write published", e)
	}
}
func TestConcurrentCacheAndDisabled(t *testing.T) {
	for _, capacity := range []uint64{0, 4096} {
		d, e := OpenFile(filepath.Join(t.TempDir(), "db"), nil, capacity)
		if e != nil {
			t.Fatal(e)
		}
		var wg sync.WaitGroup
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				key := []byte{byte(i)}
				for n := 0; n < 30; n++ {
					if e := d.Put(key, []byte{byte(n)}, nil); e != nil {
						t.Error(e)
					}
					if _, e := d.Get(key, nil); e != nil {
						t.Error(e)
					}
				}
			}(i)
		}
		wg.Wait()
		if d.CacheStats().ChargedBytes > capacity {
			t.Fatal("over budget")
		}
		d.Close()
	}
}
