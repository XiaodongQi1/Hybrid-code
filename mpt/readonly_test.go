package mpt

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestLevelDBSharedReadersAndWriteRejection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state")
	writer, err := OpenLevelDB(path)
	if err != nil {
		t.Fatal(err)
	}
	trie, _ := NewEmpty(writer)
	if err = trie.Update([]byte("a"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	root, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if reader, err := OpenLevelDBReadOnly(path, DBOptions{}); err == nil {
		_ = reader.Close()
		t.Fatal("reader opened with writer active")
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	var readers []*LevelDBStore
	for i := 0; i < 3; i++ {
		reader, err := OpenLevelDBReadOnly(path, DBOptions{BlockCacheBytes: 1 << 20, OpenFiles: 16})
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, reader)
		trie, err := Open(root, reader)
		if err != nil {
			t.Fatal(err)
		}
		got, err := trie.Get([]byte("a"))
		if err != nil || string(got) != "value" {
			t.Fatal("read failure")
		}
		batch := reader.NewBatch()
		if err = batch.Put([]byte("x"), []byte("y")); !errors.Is(err, ErrReadOnly) {
			t.Fatal(err)
		}
		if err = batch.Write(); !errors.Is(err, ErrReadOnly) {
			t.Fatal(err)
		}
		if err = reader.Compact(); !errors.Is(err, ErrReadOnly) {
			t.Fatal(err)
		}
	}
	if writer, err := OpenLevelDB(path); err == nil {
		_ = writer.Close()
		t.Fatal("writer opened with readers active")
	}
	for _, reader := range readers {
		if err = reader.Close(); err != nil {
			t.Fatal(err)
		}
	}
	writer, err = OpenLevelDB(path)
	if err != nil {
		t.Fatal("reader locks remained", err)
	}
	_ = writer.Close()
	missing := filepath.Join(t.TempDir(), "missing")
	if reader, err := OpenLevelDBReadOnly(missing, DBOptions{}); err == nil {
		_ = reader.Close()
		t.Fatal("missing readonly DB created")
	}
}
