//go:build rocksdb && cgo

package patharchive

import (
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"testing"
)

func TestSharedRocksDBStateOnlyArtifacts(t *testing.T) { runStateOnlyArtifacts(t, kvstore.RocksDB) }
