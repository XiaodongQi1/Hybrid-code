//go:build rocksdb && cgo

package snapshot

import (
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"testing"
)

func TestSourceRocksDBResumeAndBoundInputs(t *testing.T) { runSourceResume(t, kvstore.RocksDB) }
