//go:build rocksdb && cgo

package backend

import "testing"

func TestHybridLogicalTraceImportMatchesEVM(t *testing.T) {
	runLogicalTraceImport(t, hashInitialOpener(t))
}
