//go:build !rocksdb || !cgo

package kvstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func testEngines() []Engine { return []Engine{LevelDB} }
func TestRocksDBUnavailableDoesNotFallBackOrCreateDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, e := Open(path, DefaultOptions(RocksDB), true, false)
	if db != nil || !errors.Is(e, ErrUnavailable) {
		t.Fatal("silent fallback", db, e)
	}
	if _, e = os.Stat(path); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("failed open changed path", e)
	}
}
