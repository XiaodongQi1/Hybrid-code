//go:build !rocksdb || !cgo

package hybridtrie

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestOpenWithoutNativeEngineNeverFallsBack(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	if s, e := Open(path, DefaultConfig("raw-bytes/v1", "no-fallback"), true, false); s != nil || !errors.Is(e, ErrEngineUnavailable) {
		t.Fatal(s, e)
	}
	if _, e := os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("unavailable engine created a database", e)
	}
}
