//go:build rocksdb && cgo

package kvstore

import (
	"os"
	"path/filepath"
	"testing"
)

func testEngines() []Engine { return []Engine{LevelDB, RocksDB} }

func TestExistingRocksDBColumnFamiliesNeedNoMigration(t *testing.T) {
	options, e := testOptions(RocksDB).Resolved()
	must(t, e)
	path := filepath.Join(t.TempDir(), "db")
	// The previous HybridTrie engine wrote directly into these native CFs,
	// without a generic wrapper marker or key namespace bytes.
	raw, e := openRocksDB(path, options, true, false)
	must(t, e)
	must(t, raw.write([]operation{{column: "prefix", key: []byte("position"), value: []byte("old-value")}}))
	must(t, raw.close())
	db, e := Open(path, options, false, true)
	must(t, e)
	defer db.Close()
	v, e := db.Get("prefix", []byte("position"))
	must(t, e)
	if string(v) != "old-value" {
		t.Fatal("native keys were changed")
	}
	if _, e = os.Stat(filepath.Join(path, engineMarker)); !os.IsNotExist(e) {
		t.Fatal("read-only open migrated old store", e)
	}
}
