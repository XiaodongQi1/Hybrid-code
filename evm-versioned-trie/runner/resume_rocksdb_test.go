//go:build rocksdb && cgo

package runner

import (
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/backend"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
)

func hybridResumeConfig(mode string) Config {
	c := resumeConfig()
	c.Backend = "hybrid-rocksdb"
	c.HybridInitialization = mode
	c.BlockCacheBytes, c.WriteBufferBytes, c.OpenFiles = 0, 0, 0
	c.MaxBatchBytes = 8 << 20
	cache := ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	cache.Boundary.PolicyIntervalBlocks = 2
	c.HybridCache = &cache
	if mode == "streaming" {
		c.HybridInitialization = "prefix"
		snapshot := backend.DefaultSnapshotConfig()
		c.HybridSnapshot = &snapshot
	}
	return c
}

func TestHybridResumeAtCommitBoundaries(t *testing.T) {
	for _, mode := range []string{"hash-cold", "prefix", "streaming"} {
		t.Run(mode, func(t *testing.T) { exerciseResume(t, hybridResumeConfig(mode)) })
	}
}

func TestHybridResumeProcessCrash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := jsonFile(path, hybridResumeConfig("hash-cold")); err != nil {
		t.Fatal(err)
	}
	want, err := Run(path, filepath.Join(dir, "reference"))
	if err != nil {
		t.Fatal(err)
	}
	log := contents(t, filepath.Join(dir, "reference/blocks.jsonl"))
	exerciseCrashResume(t, path, want, log)
}
