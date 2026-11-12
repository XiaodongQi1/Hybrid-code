package kvstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const engineMarker = "KVSTORE.json"

func markerBytes(o Options) []byte {
	columns := append([]string(nil), o.Columns...)
	sort.Strings(columns)
	b, _ := json.Marshal(struct {
		Schema  string
		Engine  Engine
		Columns []string
	}{"kvstore/v1", o.Engine, columns})
	return append(b, '\n')
}
func checkEngineMarker(path string, o Options, create bool) error {
	b, e := os.ReadFile(filepath.Join(path, engineMarker))
	if e == nil {
		if create {
			return os.ErrExist
		}
		if !bytes.Equal(b, markerBytes(o)) {
			return fmt.Errorf("%w: engine or columns differ from KVSTORE.json", ErrCorrupt)
		}
		return nil
	}
	if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	// Existing native RocksDB stores predate this wrapper and retain their CF
	// layout and application manifest. They can be opened without migration.
	if !create && o.Engine == LevelDB {
		if _, e := os.Stat(path); e == nil {
			return fmt.Errorf("%w: LevelDB kvstore marker missing", ErrCorrupt)
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
	}
	return nil
}
func writeEngineMarker(path string, o Options) error {
	f, e := os.OpenFile(filepath.Join(path, engineMarker), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	_, e = f.Write(markerBytes(o))
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	d, e := os.Open(path)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
