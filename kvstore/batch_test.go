package kvstore

import (
	"bytes"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
)

type batchRecorder struct {
	entries []string
	failAt  int
}

func TestBorrowedBatchWriteDoesNotRetainInput(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "db"), testOptions(engine), true, false)
			must(t, err)
			defer db.Close()
			key, value := []byte("key"), []byte("value")
			b := NewBatch()
			b.PutBorrowed("", key, value)
			b.PutBorrowed("hash", key, value)
			must(t, db.Write(b))
			if b.entries[0].column != "" {
				t.Fatal("write mutated caller batch")
			}
			b.Reset() // the borrowed lifetime ends before the slices are reused
			key[0], value[0] = 'x', 'x'
			for _, cf := range []string{"default", "hash"} {
				got, err := db.Get(cf, []byte("key"))
				if err != nil || string(got) != "value" {
					t.Fatal("driver retained borrowed memory", err)
				}
			}
		})
	}
}

var replayFailure = errors.New("replay failure")

func (r *batchRecorder) Put(c string, k, v []byte) error {
	r.entries = append(r.entries, c+":"+string(k)+"="+string(v))
	return r.check()
}
func (r *batchRecorder) Delete(c string, k []byte) error {
	r.entries = append(r.entries, c+":"+string(k)+" deleted")
	return r.check()
}
func (r *batchRecorder) check() error {
	if r.failAt == len(r.entries) {
		return replayFailure
	}
	return nil
}
func TestBatchReplayOwnsInputPreservesOrderAndStops(t *testing.T) {
	b := NewBatch()
	key, value := []byte("key"), []byte("value")
	b.Put("default", key, value)
	b.Delete("hash", key)
	b.Put("default", key, []byte("last"))
	key[0] = 'x'
	value[0] = 'x'
	r := &batchRecorder{failAt: 2}
	if e := b.Replay(r); !errors.Is(e, replayFailure) || len(r.entries) != 2 {
		t.Fatal(r, e)
	}
	r = &batchRecorder{}
	if e := b.Replay(r); e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(r.entries, []string{"default:key=value", "hash:key deleted", "default:key=last"}) || b.Len() != 3 {
		t.Fatal(r.entries)
	}
}
func TestPrefixBoundsMatchesPrefixIncludingMaxByte(t *testing.T) {
	keys := [][]byte{nil, {0}, {1}, {1, 0}, {1, 255}, {2}, {255}, {255, 0}, {255, 255}}
	for _, p := range [][]byte{nil, {0}, {1}, {1, 255}, {255}, {255, 255}} {
		b := PrefixBounds(p)
		for _, k := range keys {
			inside := bytes.Compare(k, b.Start) >= 0 && (b.Limit == nil || bytes.Compare(k, b.Limit) < 0)
			if inside != bytes.HasPrefix(k, p) {
				t.Fatal(p, k, b)
			}
		}
		if len(p) > 0 {
			original := bytes.Clone(b.Start)
			p[0] ^= 127
			if !bytes.Equal(b.Start, original) {
				t.Fatal("borrowed bound")
			}
			p[0] ^= 127
		}
	}
}
