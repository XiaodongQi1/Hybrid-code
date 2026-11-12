// Package storagebench compares actual state encodings using one EVM execution
// stream. Input/receipt ledgers are excluded from comparable state measurements.
package storagebench

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	q "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/workload"
	"github.com/XiaodongQi1/HybridTrie/internal/kvcache"
	vb "github.com/XiaodongQi1/HybridTrie/internal/versionedbench"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
)

var Methods = []string{"HashArchive", "PathV7", "HashVersioned"}

type Config struct {
	Schema        string
	Workload      workload.Options
	Storage       kvcache.Options
	MaxBatchBytes uint64
}

func (c Config) validate() error {
	if c.Schema != "versioned-evm-storage/v1" || c.Storage.CacheBytes != 0 || c.MaxBatchBytes < 4096 || c.MaxBatchBytes > 128<<20 {
		return fmt.Errorf("invalid storage comparison configuration")
	}
	return c.Storage.Validate()
}
func (c Config) versionConfig(id string) vt.Config {
	v := vt.DefaultConfig("legacy-raw105", id)
	v.Storage = c.Storage
	v.MaxBatchBytes = c.MaxBatchBytes
	return v
}

type rootRow struct {
	Height uint64
	ID     common.Hash
	Root   mpt.Hash
}
type sample struct {
	Request  q.Request
	Expected q.Verified
}
type executionRecord struct {
	rootRow
	Evaluated block.EvaluatedBlock
	Samples   []sample
}
type Measurement struct {
	StateBytes, CanonicalRootBytes, SourceAllKVBytes int64
	Components                                       map[string]vb.Counts
	StateKVHash                                      string
	Projected                                        vb.Inventory
	SourceDirectoryBytes                             int64
}
type Result struct {
	Method, WorkloadID, Codec, MeasurementVersion           string
	Initial, Final                                          Measurement
	LogicalGrowth, SSTGrowth, ProjectedDirectoryGrowth      int64
	ExecutedEVMTransactions, VerifiedRoots, VerifiedQueries int
	ReadOnlyUnchanged, Verified                             bool
	ElapsedSeconds                                          float64
}
type Summary struct {
	Schema, WorkloadID                                             string
	Configuration                                                  Config
	Blocks, Transactions, BackgroundAccounts, TotalInitialAccounts int
	Methods                                                        []Result
	Notes                                                          []string
}
type status struct {
	Stage, Method                                  string
	CompletedBlocks, TotalBlocks, CompletedMethods int
	Error                                          string `json:",omitempty"`
}

func writeJSON(path string, v any) error {
	raw, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	tmp := path + ".tmp"
	if e = os.WriteFile(tmp, append(raw, '\n'), 0600); e != nil {
		return e
	}
	return os.Rename(tmp, path)
}
func rootPath(out, method string) string { return filepath.Join(out, method, "db") }
