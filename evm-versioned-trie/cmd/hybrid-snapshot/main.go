// hybrid-snapshot imports an address-ordered stream of native EVM allocations.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/backend"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"strings"
	"syscall"
	"time"
)

type config struct {
	Initialization                   string `json:",omitempty"`
	Schema, SourceFile, ExpectedRoot string
	Header                           block.InputBlock
	HybridCache                      *ht.ForestCacheConfig
	Snapshot                         backend.SnapshotConfig
	MaxBatchBytes                    uint64
	HybridStorage                    *kvstore.Options `json:",omitempty"`
}
type importPlan struct {
	Initialization                               string `json:",omitempty"`
	Schema, SourceFile, SourceSHA256, WorkloadID string
	SourceBytes                                  int64
	Header                                       block.InputBlock
	ExpectedRoot                                 string
	HybridCache                                  ht.ForestCacheConfig
	Snapshot                                     backend.SnapshotConfig
	MaxBatchBytes                                uint64
	HybridStorage                                *kvstore.Options `json:",omitempty"`
}
type report struct {
	Schema, WorkloadID, SourceSHA256 string
	SourceBytes                      int64
	ElapsedSeconds                   float64
	Snapshot                         backend.SnapshotReport
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}
func decodeOne(r io.Reader, out any) error {
	d := json.NewDecoder(r)
	d.DisallowUnknownFields()
	if e := d.Decode(out); e != nil {
		return e
	}
	var extra any
	if e := d.Decode(&extra); e != io.EOF {
		return errors.New("expected one configuration object")
	}
	return nil
}
func writeJSON(path string, value any) error {
	raw, e := json.MarshalIndent(value, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".snapshot-json-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if _, e = f.Write(append(raw, '\n')); e == nil {
		e = f.Sync()
	}
	if closeErr := f.Close(); e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	return os.Rename(f.Name(), path)
}
func run(ctx context.Context, configPath, output string, resume bool) (out report, err error) {
	started := time.Now()
	cfg := config{Snapshot: backend.DefaultSnapshotConfig(), MaxBatchBytes: 8 << 20}
	raw, e := os.ReadFile(configPath)
	if e != nil {
		return out, e
	}
	if len(raw) > 1<<20 {
		return out, errors.New("configuration exceeds 1 MiB")
	}
	if e = decodeOne(bytes.NewReader(raw), &cfg); e != nil {
		return out, e
	}
	if cfg.Schema != "hybrid-snapshot/v1" || cfg.SourceFile == "" || cfg.Header.Height > ht.MaxHeight || len(cfg.Header.Allocations) != 0 || len(cfg.Header.Transactions) != 0 {
		return out, errors.New("invalid snapshot configuration")
	}
	if cfg.Initialization != "" && cfg.Initialization != backend.InitializationPrefix && cfg.Initialization != backend.InitializationHashColdBulk {
		return out, errors.New("snapshot Initialization must be prefix or hash-cold-bulk")
	}
	var expected *mpt.Hash
	if cfg.ExpectedRoot != "" {
		value, e := hex.DecodeString(strings.TrimPrefix(cfg.ExpectedRoot, "0x"))
		if e != nil || len(value) != 32 {
			return out, errors.New("ExpectedRoot must be a 32-byte hex root")
		}
		var h mpt.Hash
		copy(h[:], value)
		expected = &h
	}
	source := cfg.SourceFile
	if !filepath.IsAbs(source) {
		source = filepath.Join(filepath.Dir(configPath), source)
	}
	source, e = filepath.Abs(source)
	if e != nil {
		return out, e
	}
	f, e := os.Open(source)
	if e != nil {
		return out, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil {
		return out, e
	}
	if !info.Mode().IsRegular() {
		return out, errors.New("SourceFile must be a regular JSONL file")
	}
	h := sha256.New()
	n, e := io.Copy(h, contextReader{ctx, f})
	if e != nil {
		return out, e
	}
	sum := h.Sum(nil)
	identity, _ := json.Marshal(struct {
		Schema string
		Header block.InputBlock
		Source string
	}{cfg.Schema, cfg.Header, hex.EncodeToString(sum)})
	id := sha256.Sum256(identity)
	cache := ht.DefaultEthereumPrefixBoundaryForestCacheConfig()
	if cfg.HybridCache != nil {
		cache = *cfg.HybridCache
	}
	output, e = filepath.Abs(output)
	if e != nil {
		return out, e
	}
	// Scratch remains separate from the published database. Relative paths are
	// resolved against the output directory and recorded before any import writes.
	if cfg.Snapshot.ScratchPath != "" && !filepath.IsAbs(cfg.Snapshot.ScratchPath) {
		cfg.Snapshot.ScratchPath = filepath.Join(output, cfg.Snapshot.ScratchPath)
	}
	plan := importPlan{Schema: cfg.Schema, SourceFile: source, SourceSHA256: hex.EncodeToString(sum), WorkloadID: hex.EncodeToString(id[:]), SourceBytes: n, Header: cfg.Header, ExpectedRoot: cfg.ExpectedRoot, HybridCache: cache, Snapshot: cfg.Snapshot, MaxBatchBytes: cfg.MaxBatchBytes}
	plan.HybridStorage = cfg.HybridStorage
	plan.Initialization = cfg.Initialization
	planRaw, _ := json.Marshal(plan)
	planPath := filepath.Join(output, "import.json")
	if resume {
		saved, e := os.ReadFile(planPath)
		if e != nil {
			return out, e
		}
		var old importPlan
		if e = decodeOne(bytes.NewReader(saved), &old); e != nil {
			return out, e
		}
		canonical, _ := json.Marshal(old)
		if !bytes.Equal(canonical, planRaw) {
			return out, errors.New("resume configuration or source content changed")
		}
	} else {
		if e = os.MkdirAll(filepath.Dir(output), 0755); e != nil {
			return out, e
		}
		if e = os.Mkdir(output, 0700); e != nil {
			return out, e
		}
		if e = writeJSON(planPath, plan); e != nil {
			return out, e
		}
	}
	dbPath := filepath.Join(output, "db")
	_, e = os.Stat(dbPath)
	create := os.IsNotExist(e)
	if e != nil && !create {
		return out, e
	}
	store := backend.DefaultHybridConfig(plan.WorkloadID)
	store.Store.Storage = cfg.HybridStorage
	store.Store.InitialHeight = cfg.Header.Height
	store.Store.MaxBatchBytes = cfg.MaxBatchBytes
	store.Cache = cache
	store.Snapshot = &cfg.Snapshot
	store.Initialization = cfg.Initialization
	b, e := backend.OpenHybrid(dbPath, store, create, false)
	if e != nil {
		return out, e
	}
	defer b.Close()
	if _, e = f.Seek(0, io.SeekStart); e != nil {
		return out, e
	}
	// Check the second pass too: a file changed during import is never published.
	h.Reset()
	d := json.NewDecoder(io.TeeReader(contextReader{ctx, f}, h))
	d.DisallowUnknownFields()
	next := func() (block.Allocation, error) {
		var a block.Allocation
		e := d.Decode(&a)
		if e == io.EOF && !bytes.Equal(h.Sum(nil), sum) {
			return a, errors.New("source changed while importing")
		}
		return a, e
	}
	_, snapshot, e := b.ImportSnapshotContext(ctx, cfg.Header, next, expected)
	if e != nil {
		return out, e
	}
	if e = b.Close(); e != nil {
		return out, e
	}
	out = report{Schema: cfg.Schema, WorkloadID: plan.WorkloadID, SourceSHA256: plan.SourceSHA256, SourceBytes: n, ElapsedSeconds: time.Since(started).Seconds(), Snapshot: snapshot}
	if e = writeJSON(filepath.Join(output, "report.json"), out); e != nil {
		return out, e
	}
	return out, nil
}
func main() {
	configPath := flag.String("config", "", "snapshot configuration JSON")
	output := flag.String("out", "", "output directory (new unless -resume)")
	resume := flag.Bool("resume", false, "resume the same source and configuration")
	base := flag.String("base-dir", "", "base for relative command-line paths")
	flag.Parse()
	if *configPath == "" || *output == "" {
		flag.Usage()
		os.Exit(2)
	}
	if *base != "" {
		if !filepath.IsAbs(*configPath) {
			*configPath = filepath.Join(*base, *configPath)
		}
		if !filepath.IsAbs(*output) {
			*output = filepath.Join(*base, *output)
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	r, e := run(ctx, *configPath, *output, *resume)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
	if e = json.NewEncoder(os.Stdout).Encode(r); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
