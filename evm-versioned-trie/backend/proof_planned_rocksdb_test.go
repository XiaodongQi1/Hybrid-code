//go:build rocksdb && cgo

package backend

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestParallelProofEVMRocksDB(t *testing.T) { runParallelProofEVM(t, kvstore.RocksDB) }
