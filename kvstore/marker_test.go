package kvstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestEngineAndColumnIdentityCheckedBeforeOpen(t *testing.T) {
	for _, engine := range testEngines() {
		t.Run(string(engine), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			options := testOptions(engine)
			db, e := Open(path, options, true, false)
			must(t, e)
			b := NewBatch()
			b.Put("prefix", []byte("key"), []byte("value"))
			must(t, db.Write(b))
			must(t, db.Close())
			var paths []string
			before := map[string][]byte{}
			es, e := os.ReadDir(path)
			must(t, e)
			for _, ent := range es {
				if ent.IsDir() {
					continue
				}
				p := filepath.Join(path, ent.Name())
				raw, e := os.ReadFile(p)
				must(t, e)
				before[p] = raw
				paths = append(paths, p)
			}
			wrong := options
			if engine == RocksDB {
				wrong.Engine = LevelDB
			} else {
				wrong.Engine = RocksDB
			}
			if other, e := Open(path, wrong, false, false); other != nil || !errors.Is(e, ErrCorrupt) {
				if other != nil {
					other.Close()
				}
				t.Fatal("engine mismatch not rejected", e)
			}
			wrong = options
			wrong.Columns = []string{"different"}
			if other, e := Open(path, wrong, false, false); other != nil || !errors.Is(e, ErrCorrupt) {
				if other != nil {
					other.Close()
				}
				t.Fatal("namespace mismatch not rejected", e)
			}
			for _, p := range paths {
				after, e := os.ReadFile(p)
				must(t, e)
				if !bytes.Equal(before[p], after) {
					t.Fatal("failed open modified", p)
				}
			}
			// Column order is immaterial because both adapters identify columns by name.
			options.Columns = []string{"hash", "prefix", "default"}
			db, e = Open(path, options, false, true)
			must(t, e)
			defer db.Close()
			v, e := db.Get("prefix", []byte("key"))
			must(t, e)
			if string(v) != "value" {
				t.Fatal("columns depend on declaration order")
			}
		})
	}
}

func TestOptionsValidationDoesNotCreateData(t *testing.T) {
	for _, o := range []Options{{Engine: "unknown"}, {Engine: LevelDB, Columns: []string{"x", "x"}}, {Engine: LevelDB, Columns: []string{"bad\x00name"}}, {Engine: LevelDB, OpenFiles: 1}, {Engine: LevelDB, WriteBufferBytes: 1}, {Engine: LevelDB, Compression: "unknown"}} {
		path := filepath.Join(t.TempDir(), "db")
		if db, e := Open(path, o, true, false); db != nil || !errors.Is(e, ErrInvalid) {
			t.Fatal("invalid options accepted", e)
		}
		if _, e := os.Stat(path); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("invalid options created a store", e)
		}
	}
}
