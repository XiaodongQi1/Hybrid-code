//go:build rocksdb && cgo

package backend

import (
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"testing"
)

func TestParallelProofEVMRocksDB(t *testing.T) { runParallelProofEVM(t, kvstore.RocksDB) }
