package kvstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func testOptions(engine Engine) Options {
	o := DefaultOptions(engine)
	o.Columns = []string{"default", "prefix", "hash"}
	o.WriteBufferBytes = 1 << 20
	o.BlockCacheBytes = 1 << 20
	return o
}
func must(t *testing.T, e error) {
	t.Helper()
	if e != nil {
		t.Fatal(e)
	}
}

func TestEnginesAtomicColumnsSnapshotReopenAndReadOnly(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			o := testOptions(engine)
			db, e := Open(path, o, true, false)
			must(t, e)
			defer func() { db.Close() }()
			b := NewBatch()
			b.Put("prefix", []byte("same"), []byte("prefix"))
			b.Put("hash", []byte("same"), []byte("hash"))
			b.Put("", nil, nil)
			must(t, db.Write(b))
			s, e := db.NewSnapshot()
			must(t, e)
			b.Reset()
			b.Put("prefix", []byte("same"), []byte("new"))
			b.Delete("hash", []byte("same"))
			b.Put("default", []byte("new"), []byte("value"))
			must(t, db.Write(b))
			v, e := s.Get("prefix", []byte("same"))
			must(t, e)
			if string(v) != "prefix" {
				t.Fatal("snapshot changed")
			}
			v[0] = 'x'
			v, e = s.Get("prefix", []byte("same"))
			must(t, e)
			if string(v) != "prefix" {
				t.Fatal("Get returned mutable storage memory")
			}
			v, e = s.Get("hash", []byte("same"))
			must(t, e)
			if string(v) != "hash" {
				t.Fatal("column collision")
			}
			if _, e = s.Get("default", []byte("new")); !errors.Is(e, ErrNotFound) {
				t.Fatal("snapshot saw new write", e)
			}
			if _, e = db.Get("hash", []byte("same")); !errors.Is(e, ErrNotFound) {
				t.Fatal("delete failed", e)
			}
			if _, e = db.Get("default", nil); e != nil {
				t.Fatal("empty key/value lost", e)
			}
			if e = db.Close(); !errors.Is(e, ErrBusy) {
				t.Fatal("closed with live snapshot", e)
			}
			must(t, s.Close())
			must(t, db.Compact())
			must(t, db.Close())
			db, e = Open(path, o, false, true)
			must(t, e)
			v, e = db.Get("prefix", []byte("same"))
			must(t, e)
			if string(v) != "new" {
				t.Fatal("reopen lost commit")
			}
			if e = db.Write(b); !errors.Is(e, ErrReadOnly) {
				t.Fatal("read-only write", e)
			}
			if e = db.Compact(); !errors.Is(e, ErrReadOnly) {
				t.Fatal("read-only compaction", e)
			}
			must(t, db.Close())
			if _, e = db.Get("prefix", nil); !errors.Is(e, ErrClosed) {
				t.Fatal("read after close", e)
			}
		})
	}
}

func TestSeekForPrevMatchesSortedModelAndReusesIterator(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			db, e := Open(filepath.Join(t.TempDir(), "db"), testOptions(engine), true, false)
			must(t, e)
			defer db.Close()
			var keys [][]byte
			b := NewBatch()
			for _, n := range []uint16{0, 1, 3, 20, 21, 30, 100, 255, 1024, 65535} {
				var k [2]byte
				binary.BigEndian.PutUint16(k[:], n)
				key := bytes.Clone(k[:])
				keys = append(keys, key)
				b.Put("prefix", key, []byte{byte(n)})
			}
			b.Put("hash", []byte{0xff, 0xff}, []byte("unrelated"))
			must(t, db.Write(b))
			for _, bounds := range []Bounds{{}, {Start: []byte{0, 3}, Limit: []byte{0, 30}}, {Start: []byte{0, 3}}, {Limit: []byte{0, 30}}, {Start: []byte{0, 3}, Limit: []byte{0, 3}}} {
				var model [][]byte
				for _, k := range keys {
					if bytes.Compare(k, bounds.Start) >= 0 && (bounds.Limit == nil || bytes.Compare(k, bounds.Limit) < 0) {
						model = append(model, k)
					}
				}
				it, e := db.NewIterator("prefix", bounds)
				must(t, e)
				var forward, reverse [][]byte
				for ok := it.First(); ok; ok = it.Next() {
					forward = append(forward, it.Key())
				}
				must(t, it.Error())
				for ok := it.Last(); ok; ok = it.Prev() {
					reverse = append(reverse, it.Key())
				}
				must(t, it.Error())
				if len(forward) != len(model) || len(reverse) != len(model) {
					t.Fatal("range count", bounds, forward, reverse)
				}
				for i, k := range model {
					if !bytes.Equal(k, forward[i]) || !bytes.Equal(k, reverse[len(model)-1-i]) {
						t.Fatal("range order")
					}
				}
				rng := rand.New(rand.NewSource(91))
				for i := 0; i < 300; i++ {
					var key [2]byte
					binary.BigEndian.PutUint16(key[:], uint16(rng.Intn(65536)))
					index := sort.Search(len(model), func(i int) bool { return bytes.Compare(model[i], key[:]) > 0 }) - 1
					found, value, size, err := it.ReadForPrev(key[:], 1)
					if index < 0 {
						if !errors.Is(err, ErrNotFound) || it.Error() != nil {
							t.Fatal("combined predecessor absence", err, it.Error())
						}
					} else if err != nil || !bytes.Equal(found, model[index]) || size != 1 || !bytes.Equal(value, model[index][1:]) {
						t.Fatal("combined predecessor disagrees with sorted model", found, value, size, err)
					}
					ok := it.SeekForPrev(key[:])
					if ok != (index >= 0) {
						t.Fatal("predecessor existence", engine, key, index, it.Error())
					}
					if ok && !bytes.Equal(it.Key(), model[index]) {
						t.Fatal("wrong predecessor", engine, key, it.Key(), model[index])
					}
					index = sort.Search(len(model), func(i int) bool { return bytes.Compare(model[i], key[:]) >= 0 })
					ok = it.Seek(key[:])
					if ok != (index < len(model)) {
						t.Fatal("successor existence", engine, key, index, it.Error())
					}
					if ok && !bytes.Equal(it.Key(), model[index]) {
						t.Fatal("wrong successor")
					}
					must(t, it.Error())
				}
				must(t, it.Close())
			}
		})
	}
}

func TestReadForPrevOwnershipAndErrors(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "db"), testOptions(engine), true, false)
			must(t, err)
			defer db.Close()
			b := NewBatch()
			body := bytes.Repeat([]byte{0xaa}, 4096)
			b.Put("prefix", []byte("a"), body)
			b.Put("prefix", []byte("b"), nil)
			must(t, db.Write(b))
			it, err := db.NewIterator("prefix", Bounds{})
			must(t, err)
			defer it.Close()
			if _, _, _, err = it.ReadForPrev(nil, 33); !errors.Is(err, ErrNotFound) {
				t.Fatal("missing predecessor", err)
			}
			k, v, size, err := it.ReadForPrev([]byte("a"), 33)
			if err != nil || string(k) != "a" || size != len(body) || !bytes.Equal(v, body[:33]) {
				t.Fatal("wrong prefix or size", err, size)
			}
			// Mixing combined reads and ordinary moves must not retain stale pointers.
			if !it.Next() || string(it.Key()) != "b" || !it.Prev() || !it.Last() || !it.First() {
				t.Fatal("mixed movement failed", it.Error())
			}
			if string(k) != "a" || !bytes.Equal(v, body[:33]) {
				t.Fatal("returned data changed with cursor")
			}
			k[0], v[0] = 'z', 0
			k2, v2, size, err := it.ReadForPrev([]byte("a"), MaxValueBytes)
			if err != nil || string(k2) != "a" || size != len(body) || !bytes.Equal(v2, body) {
				t.Fatal("caller modified native data", err)
			}
			for _, key := range []string{"a", "b"} {
				_, v, size, err = it.ReadForPrev([]byte(key), 0)
				want := 0
				if key == "a" {
					want = len(body)
				}
				if err != nil || len(v) != 0 || size != want {
					t.Fatal("zero prefix or empty value", err, size)
				}
			}
			if _, _, _, err = it.ReadForPrev([]byte("a"), -1); !errors.Is(err, ErrInvalid) || it.Valid() {
				t.Fatal("negative limit", err)
			}
			if _, _, _, err = it.ReadForPrev([]byte("a"), 33); !errors.Is(err, ErrInvalid) {
				t.Fatal("failure not sticky", err)
			}
			must(t, it.Close())
			if _, _, _, err = it.ReadForPrev([]byte("a"), 33); !errors.Is(err, ErrClosed) {
				t.Fatal("closed iterator", err)
			}
		})
	}
}

func TestIteratorOwnershipColumnsAndValidation(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			o := testOptions(engine)
			o.Columns = append(o.Columns, "prefix-other")
			db, e := Open(filepath.Join(t.TempDir(), "db"), o, true, false)
			must(t, e)
			defer db.Close()
			b := NewBatch()
			b.Put("prefix", []byte("a"), []byte("one"))
			b.Put("prefix", []byte("b"), []byte("two"))
			b.Put("prefix-other", []byte("a"), []byte("other"))
			must(t, db.Write(b))
			s, e := db.NewSnapshot()
			must(t, e)
			it, e := s.NewIterator("prefix", Bounds{})
			must(t, e)
			if !it.First() {
				t.Fatal(it.Error())
			}
			key, value := it.Key(), it.Value()
			if !it.Next() || string(key) != "a" || string(value) != "one" {
				t.Fatal("returned slices changed on Next")
			}
			key[0] = 'z'
			value[0] = 'x'
			if !it.Seek([]byte("a")) || string(it.Value()) != "one" {
				t.Fatal("caller mutated native data")
			}
			if !errors.Is(s.Close(), ErrBusy) {
				t.Fatal("snapshot closed with iterator")
			}
			must(t, it.Close())
			must(t, s.Close())
			if it.Seek(nil) || !errors.Is(it.Error(), ErrClosed) {
				t.Fatal("closed iterator usable")
			}
			b.Reset()
			b.Put("prefix", []byte("c"), []byte("bad"))
			b.Put("missing", []byte("d"), nil)
			if e = db.Write(b); !errors.Is(e, ErrInvalid) {
				t.Fatal("unknown column accepted", e)
			}
			if _, e = db.Get("prefix", []byte("c")); !errors.Is(e, ErrNotFound) {
				t.Fatal("invalid batch partially published", e)
			}
			b.Reset()
			b.Put("prefix", []byte("c"), []byte("good"))
			must(t, db.Write(b))
			if _, e = db.NewIterator("missing", Bounds{}); !errors.Is(e, ErrInvalid) {
				t.Fatal(e)
			}
			if _, e = db.NewIterator("prefix", Bounds{Start: []byte("z"), Limit: []byte("a")}); !errors.Is(e, ErrInvalid) {
				t.Fatal(e)
			}
		})
	}
}

func TestSnapshotsSeeWholeCrossColumnBatches(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			db, e := Open(filepath.Join(t.TempDir(), "db"), testOptions(engine), true, false)
			must(t, e)
			defer db.Close()
			b := NewBatch()
			b.Put("prefix", []byte("x"), []byte{0})
			b.Put("hash", []byte("x"), []byte{0})
			must(t, db.Write(b))
			var wg sync.WaitGroup
			done := make(chan struct{})
			fail := make(chan error, 1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(done)
				for i := 1; i <= 40; i++ {
					b := NewBatch()
					b.Put("prefix", []byte("x"), []byte{byte(i)})
					b.Put("hash", []byte("x"), []byte{byte(i)})
					if e := db.Write(b); e != nil {
						fail <- e
						return
					}
				}
			}()
			for n := 0; n < 100; n++ {
				s, e := db.NewSnapshot()
				must(t, e)
				a, e := s.Get("prefix", []byte("x"))
				must(t, e)
				b, e := s.Get("hash", []byte("x"))
				must(t, e)
				must(t, s.Close())
				if !bytes.Equal(a, b) {
					t.Fatal("torn cross-column batch")
				}
			}
			wg.Wait()
			<-done
			select {
			case e := <-fail:
				t.Fatal(e)
			default:
			}
		})
	}
}

type uncertainDriver struct{ driver }

func (d uncertainDriver) write(es []operation) error {
	if e := d.driver.write(es); e != nil {
		return e
	}
	return errors.New("injected error after durable publication")
}

func TestUncertainWriteFailsStopAndReopenDeterminesOutcome(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			options := testOptions(engine)
			db, e := Open(path, options, true, false)
			must(t, e)
			db.raw = uncertainDriver{db.raw}
			b := NewBatch()
			b.Put("prefix", []byte("x"), []byte("committed"))
			b.Put("hash", []byte("x"), []byte("committed"))
			if e = db.Write(b); !errors.Is(e, ErrUncertain) {
				t.Fatal(e)
			}
			if _, e = db.Get("prefix", []byte("x")); !errors.Is(e, ErrUncertain) {
				t.Fatal("uncertain handle remained readable", e)
			}
			if _, e = db.NewSnapshot(); !errors.Is(e, ErrUncertain) {
				t.Fatal(e)
			}
			must(t, db.Close())
			db, e = Open(path, options, false, true)
			must(t, e)
			defer db.Close()
			for _, cf := range []string{"prefix", "hash"} {
				v, e := db.Get(cf, []byte("x"))
				must(t, e)
				if string(v) != "committed" {
					t.Fatal("durable batch was lost")
				}
			}
		})
	}
}

func TestIteratorValuePrefixReturnsOwnedBytesAndFullSize(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "db"), testOptions(engine), true, false)
			must(t, err)
			defer db.Close()
			b := NewBatch()
			body := bytes.Repeat([]byte{0xaa}, 4096)
			b.Put("prefix", []byte("a"), body)
			b.Put("prefix", []byte("b"), []byte{1})
			must(t, db.Write(b))
			it, err := db.NewIterator("prefix", Bounds{})
			must(t, err)
			defer it.Close()
			if !it.First() {
				t.Fatal("missing first record")
			}
			header, size := it.ValuePrefix(33)
			if size != len(body) || !bytes.Equal(header, body[:33]) {
				t.Fatal("wrong header or length")
			}
			if !it.Next() {
				t.Fatal("missing second record")
			}
			short, size := it.ValuePrefix(33)
			if size != 1 || !bytes.Equal(short, []byte{1}) || !bytes.Equal(header, body[:33]) {
				t.Fatal("cursor movement corrupted returned data")
			}
		})
	}
}
