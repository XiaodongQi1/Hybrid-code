//go:build rocksdb && cgo

package hybridtrie

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestPlannedProofRocksDB(t *testing.T) { runPlannedProofs(t, kvstore.RocksDB) }
