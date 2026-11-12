// Package runner executes frozen workloads against the live versioned backend.
package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/backend"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
)

type Config struct {
	ParallelProof        ht.ParallelProofConfig `json:",omitempty,omitzero"`
	HybridInitialization string
	Schema               string
	Codec                codec.Mode
	Backend              string
	HybridCache          *ht.ForestCacheConfig
	HybridSnapshot       *backend.SnapshotConfig
	// Choose exactly one input. WorkloadFile is relative to the config file.
	WorkloadFile                                 string
	Synthetic                                    *workload.Options
	BlockCacheBytes, WriteBufferBytes, OpenFiles int
	CacheBytes                                   uint64
	MaxBatchBytes                                uint64
	HybridStorage                                *kvstore.Options `json:",omitempty"`
}
type Report struct {
	QueryRequests, QueryWorkersStarted, QueryBackgroundReads uint64 `json:",omitempty"`
	Schema, Codec, WorkloadID                                string
	Backend                                                  string
	Cache                                                    ht.ForestCacheStats
	Snapshot                                                 backend.SnapshotReport
	InitialHeight, FinalHeight                               uint64
	ExecutedBlocks, Transactions, FailedTransactions         int
	FinalRoot                                                mpt.Hash
	AccountProofs, StorageProofs                             int
	ReadOnlyReopen                                           bool
	Resumed, CacheSessionOnly, InitializationStatsMissing    bool
	ReusedBlocks, ExecutedBlocksThisRun                      int
	KVEngine                                                 kvstore.Engine `json:",omitempty"`
}

func jsonFile(path string, v any) error {
	raw, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return writeAtomic(path, append(raw, '\n'))
}

// Run creates a fresh run. Use RunWithOptions with Resume to continue it.
func Run(configPath, output string) (report Report, err error) {
	return RunWithOptions(configPath, output, Options{})
}

// RunWithOptions resumes only the same frozen workload/configuration. Committed
// state and outcomes are retained; unfinished blocks execute again from scratch.
func RunWithOptions(configPath, output string, opts Options) (Report, error) {
	return run(configPath, output, opts, nil)
}

// hook exposes crash boundaries to in-package tests; production always uses nil.
func run(configPath, output string, opts Options, hook func(string, int) error) (report Report, err error) {
	output, err = filepath.Abs(output)
	if err != nil {
		return report, err
	}
	raw, e := os.ReadFile(configPath)
	if e != nil {
		return report, e
	}
	var cfg Config
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if e = d.Decode(&cfg); e != nil {
		return report, e
	}
	if e = d.Decode(new(any)); e != io.EOF {
		return report, fmt.Errorf("trailing configuration content")
	}
	if cfg.Schema != "versioned-evm-run/v1" || (cfg.WorkloadFile == "") == (cfg.Synthetic == nil) {
		return report, fmt.Errorf("select exactly one workload source")
	}
	if e = cfg.Codec.Validate(); e != nil {
		return report, e
	}
	if cfg.Backend == "" {
		cfg.Backend = "versioned"
	}
	if cfg.Backend != "versioned" && cfg.Backend != "hybrid-rocksdb" && cfg.Backend != "hybrid" {
		return report, fmt.Errorf("unknown backend %q", cfg.Backend)
	}
	if cfg.Backend == "versioned" && (cfg.HybridCache != nil || cfg.HybridSnapshot != nil || cfg.HybridInitialization != "" || cfg.HybridStorage != nil) {
		return report, fmt.Errorf("Hybrid settings require a hybrid backend")
	}
	if e = cfg.ParallelProof.Validate(); e != nil {
		return report, e
	}
	if cfg.ParallelProof.Enabled && cfg.Backend == "versioned" {
		return report, fmt.Errorf("ParallelProof requires a hybrid backend")
	}
	isHybrid := cfg.Backend == "hybrid-rocksdb" || cfg.Backend == "hybrid"
	if isHybrid && (cfg.Codec != codec.Ethereum || cfg.CacheBytes != 0 || cfg.BlockCacheBytes != 0 || cfg.WriteBufferBytes != 0 || cfg.OpenFiles != 0) {
		return report, fmt.Errorf("hybrid requires ethereum-mpt; configure engine buffers under HybridStorage and omit legacy storage/cache fields")
	}
	if cfg.Backend == "hybrid-rocksdb" && cfg.HybridStorage != nil && cfg.HybridStorage.Engine != kvstore.RocksDB {
		return report, fmt.Errorf("hybrid-rocksdb requires rocksdb; use hybrid with HybridStorage.Engine for engine selection")
	}
	if cfg.HybridStorage != nil {
		if _, e := cfg.HybridStorage.Resolved(); e != nil {
			return report, e
		}
	}
	var w block.Workload
	if cfg.Synthetic != nil {
		w, e = workload.Synthetic(*cfg.Synthetic)
	} else {
		path := cfg.WorkloadFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(filepath.Dir(configPath), path)
		}
		wire, readErr := os.ReadFile(path)
		if readErr != nil {
			return report, readErr
		}
		e = json.Unmarshal(wire, &w)
	}
	if e != nil {
		return report, e
	}
	workloadID, wire, e := w.Freeze()
	if e != nil {
		return report, e
	}
	var dbCfg any
	var open func(bool, bool) (*backend.Backend, error)
	if isHybrid {
		c := backend.DefaultHybridConfig(workloadID)
		c.ParallelProof = cfg.ParallelProof
		c.Store.Storage = cfg.HybridStorage
		c.Initialization = cfg.HybridInitialization
		c.Store.InitialHeight = w.InitialHeight()
		c.Store.MaxBatchBytes = cfg.MaxBatchBytes
		if cfg.HybridCache != nil {
			c.Cache = *cfg.HybridCache
		}
		if cfg.HybridSnapshot != nil {
			copy := *cfg.HybridSnapshot
			if copy.ScratchPath != "" && !filepath.IsAbs(copy.ScratchPath) {
				copy.ScratchPath = filepath.Join(output, copy.ScratchPath)
			}
			c.Snapshot = &copy
		}
		if c.Store.InitialHeight > ht.MaxHeight {
			return report, ht.ErrLimit
		}
		dbCfg = c
		open = func(create, ro bool) (*backend.Backend, error) {
			return backend.OpenHybrid(filepath.Join(output, "db"), c, create, ro)
		}
	} else {
		c := vt.DefaultConfig(string(cfg.Codec), workloadID)
		if w.InitialHeight() > uint64(^uint32(0)) {
			return report, vt.ErrLimit
		}
		c.InitialHeight = uint32(w.InitialHeight())
		c.Storage.CacheBytes = cfg.CacheBytes
		c.Storage.BlockCacheBytes = cfg.BlockCacheBytes
		c.Storage.WriteBufferBytes = cfg.WriteBufferBytes
		c.Storage.OpenFiles = cfg.OpenFiles
		c.MaxBatchBytes = cfg.MaxBatchBytes
		if e = c.Storage.Validate(); e != nil {
			return report, e
		}
		dbCfg = c
		open = func(create, ro bool) (*backend.Backend, error) {
			return backend.Open(filepath.Join(output, "db"), c, create, ro)
		}
	}
	lock, create, e := prepareRun(output, cfg, dbCfg, wire, opts.Resume)
	if e != nil {
		return report, e
	}
	defer unlockRun(lock)
	b, e := open(create, false)
	if e != nil {
		return report, e
	}
	defer func() {
		if b != nil {
			_ = b.Close()
		}
	}()
	report = Report{Schema: "versioned-evm-report/v1", Backend: cfg.Backend, Codec: string(cfg.Codec), WorkloadID: workloadID, InitialHeight: w.InitialHeight(), Resumed: opts.Resume}
	if isHybrid {
		report.KVEngine = kvstore.RocksDB
		if cfg.HybridStorage != nil {
			report.KVEngine = cfg.HybridStorage.Engine
		}
	}
	addresses := []common.Address{common.HexToAddress("0xdead")}
	seen := map[common.Address]bool{addresses[0]: true}
	addAddress := func(a common.Address) {
		if !seen[a] && len(addresses) < 32 {
			seen[a] = true
			addresses = append(addresses, a)
		}
	}
	// Prefer initialized contracts so the bounded historical sample includes
	// owner-scoped storage as well as account proofs and logical absence.
	for _, a := range w.Blocks[0].Allocations {
		if len(a.Code) > 0 || len(a.Storage) > 0 {
			addAddress(a.Address)
		}
	}
	for _, a := range w.Blocks[0].Allocations {
		addAddress(a.Address)
	}
	var roots []mpt.Hash
	accept := func(i int, r backend.Result) {
		input := w.Blocks[i]
		roots = append(roots, r.Root)
		if i > 0 {
			report.ExecutedBlocks++
		}
		report.Transactions += len(input.Transactions)
		report.FinalHeight = input.Height
		report.FinalRoot = r.Root
		for _, o := range r.Outcomes {
			if o.VMError != "" {
				report.FailedTransactions++
			}
			if o.Receipt.ContractAddress != (common.Address{}) {
				addAddress(o.Receipt.ContractAddress)
			}
		}
	}
	journal, e := inspectJournal(output, workloadID, w, b, accept)
	if e != nil {
		return report, e
	}
	if journal.Recover != nil {
		accept(journal.Count, *journal.Recover)
	}
	var sealed Report
	e = readJSON(filepath.Join(output, "report.json"), &sealed)
	if e == nil {
		if len(roots) != len(w.Blocks) || journal.Recover != nil ||
			sealed.KVEngine != "" && sealed.KVEngine != report.KVEngine ||
			sealed.Schema != report.Schema || sealed.Backend != report.Backend || sealed.Codec != report.Codec ||
			sealed.WorkloadID != workloadID || sealed.InitialHeight != report.InitialHeight ||
			sealed.FinalHeight != report.FinalHeight || sealed.FinalRoot != report.FinalRoot ||
			sealed.Transactions != report.Transactions || sealed.ExecutedBlocks != report.ExecutedBlocks ||
			sealed.FailedTransactions != report.FailedTransactions || !sealed.ReadOnlyReopen ||
			sealed.AccountProofs != len(addresses)*len(roots) || sealed.StorageProofs != 4*sealed.AccountProofs {
			return report, fmt.Errorf("saved report does not match committed results")
		}
		if e = clearPending(output); e != nil {
			return report, e
		}
		return sealed, nil
	}
	if !errors.Is(e, os.ErrNotExist) {
		return report, e
	}
	e = readJSON(filepath.Join(output, "initialization.json"), &report.Snapshot)
	if e != nil && !errors.Is(e, os.ErrNotExist) {
		return report, e
	}
	report.InitializationStatsMissing = errors.Is(e, os.ErrNotExist) && len(roots) > 0
	if e == nil && (len(roots) == 0 || report.Snapshot.Root != (mpt.Hash{}) && report.Snapshot.Root != roots[0]) {
		return report, fmt.Errorf("initialization statistics conflict with committed snapshot")
	}
	defer func() {
		if err != nil {
			_ = jsonFile(filepath.Join(output, "failure.json"), struct{ Error string }{err.Error()})
		}
	}()
	log, e := os.OpenFile(filepath.Join(output, "blocks.jsonl"), os.O_RDWR|os.O_CREATE, 0600)
	if e != nil {
		return report, e
	}
	defer log.Close()
	if journal.Torn {
		if e = log.Truncate(journal.Offset); e != nil {
			return report, e
		}
	}
	if _, e = log.Seek(journal.Offset, io.SeekStart); e != nil {
		return report, e
	}
	if journal.Recover != nil {
		if e = appendResult(log, *journal.Recover); e != nil {
			return report, e
		}
		journal.Count++
	}
	if journal.Pending {
		if e = clearPending(output); e != nil {
			return report, e
		}
	}
	report.ReusedBlocks = journal.Count
	report.CacheSessionOnly = opts.Resume && journal.Count > 0
	checkpoint := func(stage string, i int) error {
		if hook != nil {
			return hook(stage, i)
		}
		return nil
	}
	for i := journal.Count; i < len(w.Blocks); i++ {
		if e = checkpoint("before-block", i); e != nil {
			return report, e
		}
		r, e := b.ApplyWithRecorder(w.Blocks[i], nil, func(r backend.Result) error {
			p := pendingBlock{Schema: "versioned-evm-pending/v1", WorkloadID: workloadID, Index: i, Result: r}
			if e := jsonFile(filepath.Join(output, "pending-block.json"), p); e != nil {
				return e
			}
			return checkpoint("prepared", i)
		})
		if e != nil {
			return report, e
		}
		if e = checkpoint("committed", i); e != nil {
			return report, e
		}
		if i == 0 {
			report.Snapshot = b.SnapshotStats()
			report.InitializationStatsMissing = false
			if e = jsonFile(filepath.Join(output, "initialization.json"), report.Snapshot); e != nil {
				return report, e
			}
		}
		if e = appendResult(log, *r); e != nil {
			return report, e
		}
		if e = checkpoint("logged", i); e != nil {
			return report, e
		}
		if e = clearPending(output); e != nil {
			return report, e
		}
		accept(i, *r)
		if i > 0 {
			report.ExecutedBlocksThisRun++
		}
	}
	if e = log.Close(); e != nil {
		return report, e
	}
	if report.Cache, e = b.CacheStats(); e != nil {
		return report, e
	}
	if e = b.Close(); e != nil {
		return report, e
	}
	b, e = open(false, true)
	if e != nil {
		return report, e
	}
	for i, input := range w.Blocks {
		if cfg.ParallelProof.Enabled {
			if e = verifyConfiguredQueries(b, input.Height, roots[i], addresses, &report); e != nil {
				return report, e
			}
			continue
		}
		v, e := b.ViewAt(input.Height)
		if e != nil {
			return report, e
		}
		e = func() error {
			defer v.Close()
			if v.Root() != roots[i] {
				return fmt.Errorf("reopened root mismatch")
			}
			for _, addr := range addresses {
				a, exists, e := v.Account(addr)
				if e != nil {
					return e
				}
				p, e := v.AccountProof(addr)
				if e != nil {
					return e
				}
				got, ok, e := mpt.VerifyProof(v.Root(), state.AccountKey(addr), p)
				if e != nil {
					return e
				}
				var want []byte
				if exists {
					want, e = cfg.Codec.EncodeAccount(a)
					if e != nil {
						return e
					}
				}
				if ok != exists || !bytes.Equal(got, want) {
					return fmt.Errorf("historical account mismatch")
				}
				report.AccountProofs++
				for n := byte(0); n < 4; n++ {
					var k common.Hash
					k[31] = n
					root, p, e := v.StorageProof(addr, k)
					if e != nil {
						return e
					}
					raw, exists, e := mpt.VerifyProof(root, state.SlotKey(k), p)
					if e != nil {
						return e
					}
					word, e := cfg.Codec.DecodeStorage(raw)
					if e != nil {
						return e
					}
					want := common.Hash{}
					if a.StorageRoot != (mpt.Hash{}) {
						want, e = v.SlotAt(addr, a.StorageRoot, k)
						if e != nil {
							return e
						}
					}
					if word != want || exists != (want != (common.Hash{})) {
						return fmt.Errorf("historical storage mismatch")
					}
					report.StorageProofs++
				}
			}
			return nil
		}()
		if e != nil {
			return report, e
		}
	}
	if e = b.Close(); e != nil {
		return report, e
	}
	report.ReadOnlyReopen = true
	if e = checkpoint("before-report", len(w.Blocks)); e != nil {
		return report, e
	}
	if e = jsonFile(filepath.Join(output, "report.json"), report); e != nil {
		return report, e
	}
	return report, nil
}
