package backend

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	snap "github.com/XiaodongQi1/Hybrid-code/evm-state/snapshot"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
)

// SnapshotConfig is opt-in for allocation-only initialization. The iterator
// avoids a full allocation journal and account trie in RAM. Each input row's
// Storage map remains caller-owned; JSONL sources retain one account at a time.
type SnapshotConfig struct {
	ChunkEntries, CheckpointAccounts uint32
	BatchBytes                       uint64
	ScratchPath                      string
	KeepScratch                      bool
}

func DefaultSnapshotConfig() SnapshotConfig {
	return SnapshotConfig{ChunkEntries: 256, CheckpointAccounts: 256, BatchBytes: 1 << 20}
}
func DefaultHashColdBulkSnapshotConfig() SnapshotConfig {
	c := DefaultSnapshotConfig()
	c.KeepScratch = true
	return c
}
func (c SnapshotConfig) validate() error {
	if c.ChunkEntries == 0 || c.ChunkEntries > 4096 || c.CheckpointAccounts == 0 || c.CheckpointAccounts > 65536 || c.BatchBytes < 4096 || c.BatchBytes > 128<<20 {
		return vt.ErrLimit
	}
	return nil
}

type SnapshotReport struct {
	PreparedSource, BuildSessionOnly bool `json:",omitempty"`
	Mode                             string
	AtomicCommit                     ht.CommitStats
	ColdCache                        bool
	Accounts, NextTrieID             uint64
	Root                             mpt.Hash
	InputDigest                      [32]byte
	ResumedBuild, AlreadyComplete    bool
	Build                            ht.SnapshotBuildStats
	Import                           ht.SnapshotImportStats
}

// ImportSnapshot reads strictly ascending addresses. WorkloadID identifies the
// source across resume. The callback starts at row zero; saved prefixes are
// rehashed before unfinished rows are built. A ready scratch accepts next=nil.
// A supplied expected root is checked before publication.
func (b *Backend) ImportSnapshot(header block.InputBlock, next func() (block.Allocation, error), expected *mpt.Hash) (*Result, SnapshotReport, error) {
	return b.ImportSnapshotContext(context.Background(), header, next, expected)
}

func (b *Backend) ImportSnapshotContext(ctx context.Context, header block.InputBlock, next func() (block.Allocation, error), expected *mpt.Hash) (*Result, SnapshotReport, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, report, e := b.importSnapshotWithContext(ctx, header, next, expected)
	b.lastSnapshot = report
	return r, report, e
}
func (b *Backend) importSnapshotWithContext(ctx context.Context, header block.InputBlock, next func() (block.Allocation, error), expected *mpt.Hash) (result *Result, report SnapshotReport, err error) {
	return b.importSnapshotRecorded(ctx, header, next, expected, nil)
}

func (b *Backend) importSnapshotRecorded(ctx context.Context, header block.InputBlock, next func() (block.Allocation, error), expected *mpt.Hash, record func(Result) error) (result *Result, report SnapshotReport, err error) {
	if e := ctx.Err(); e != nil {
		return nil, report, e
	}
	if b.readOnly {
		return nil, report, vt.ErrReadOnly
	}
	s, ok := b.store.(*hybridStore)
	if !ok || s.snapshot == nil {
		return nil, report, fmt.Errorf("streaming snapshot requires configured HybridTrie")
	}
	report.Mode = "streaming-prefix"
	if s.initialHash {
		report.Mode = InitializationHashColdBulk
	}
	report.ColdCache = true
	report.BuildSessionOnly = true
	cfg := *s.snapshot
	if e := cfg.validate(); e != nil {
		return nil, report, e
	}
	if header.Height != b.initialHeight || header.ID == (common.Hash{}) || header.ParentID != (common.Hash{}) || header.Context.GasLimit == 0 || len(header.Transactions) > 0 || len(header.Allocations) > 0 {
		return nil, report, vt.ErrConflict
	}
	if s.work != nil {
		return nil, report, vt.ErrBusy
	}
	buildCfg := ht.SnapshotBuildConfig{SourceID: s.sourceID, Height: header.Height, BlockID: [32]byte(header.ID), BatchBytes: cfg.BatchBytes, ChunkEntries: cfg.ChunkEntries}
	if s.storageConfigured {
		o := s.store.StorageOptions()
		buildCfg.Storage = &o
	}
	sourceCfg := snap.Config{SourceID: s.sourceID, Header: header, BatchBytes: cfg.BatchBytes, ChunkEntries: cfg.ChunkEntries, CheckpointAccounts: cfg.CheckpointAccounts, Storage: buildCfg.Storage}
	if s.initialHash {
		var e error
		buildCfg, e = snap.BuildConfig(sourceCfg)
		if e != nil {
			return nil, report, e
		}
	}
	path := cfg.ScratchPath
	if path == "" {
		path = s.path + ".snapshot-build"
	}
	target, _ := filepath.Abs(s.path)
	scratch, _ := filepath.Abs(path)
	independent := func(a, z string) bool {
		r, e := filepath.Rel(a, z)
		return e == nil && (r == ".." || len(r) > 3 && r[:3] == ".."+string(filepath.Separator))
	}
	if !independent(target, scratch) || !independent(scratch, target) {
		return nil, report, fmt.Errorf("scratch must be independent of target database")
	}
	head, e := s.Head()
	if e != nil {
		return nil, report, e
	}
	if head.Initialized {
		kind, binding, e := s.store.SnapshotImportBinding()
		if e != nil {
			return nil, report, e
		}
		want := ""
		if s.initialHash {
			want = InitializationHashColdBulk
			sum := sha256.Sum256(buildCfg.Provenance)
			if binding != hex.EncodeToString(sum[:]) {
				return nil, report, vt.ErrConflict
			}
		}
		if kind != want {
			return nil, report, vt.ErrConflict
		}
		stats, complete, e := s.store.SnapshotImportStatus()
		if e != nil {
			return nil, report, hybridError(e)
		}
		if !complete || head.Height != header.Height || head.CommitID != [32]byte(header.ID) {
			return nil, report, vt.ErrConflict
		}
		if expected != nil && head.Root != *expected {
			return nil, report, state.ErrCorrupt
		}
		report.Root = head.Root
		report.NextTrieID = head.NextTrieID
		report.Import = stats
		report.Accounts, report.InputDigest = stats.SourceRecords, stats.SourceDigest
		report.AlreadyComplete = true
		result = &Result{Height: head.Height, ID: header.ID, Root: head.Root}
		// Publication can survive a process exit immediately before cleanup.
		// A retry only removes a scratch with the same verified owner/config.
		if !cfg.KeepScratch {
			if _, statErr := os.Stat(path); statErr == nil {
				builder, e := ht.OpenSnapshotBuilder(path, buildCfg)
				if e != nil {
					return result, report, fmt.Errorf("snapshot published; scratch cleanup: %w", e)
				}
				p := builder.Progress()
				r, e := builder.Root(0)
				builder.Close()
				if e != nil || !p.Ready || r != head.Root || p.Cursor != stats.SourceRecords || p.Digest != stats.SourceDigest {
					return result, report, fmt.Errorf("snapshot published; scratch identity mismatch")
				}
				if e = builder.Remove(); e != nil {
					return result, report, fmt.Errorf("snapshot published; scratch cleanup: %w", e)
				}
			} else if !os.IsNotExist(statErr) {
				return result, report, statErr
			}
		}
		return result, report, nil
	}
	var builder *ht.SnapshotBuilder
	if s.initialHash {
		_, statErr := os.Stat(path)
		report.ResumedBuild = statErr == nil
		builder, e = snap.Build(ctx, path, sourceCfg, next)
	} else {
		builder, e = ht.OpenSnapshotBuilder(path, buildCfg)
	}
	if e != nil {
		return nil, report, hybridError(e)
	}
	defer builder.Close()
	progress := builder.Progress()
	if !s.initialHash {
		report.ResumedBuild = progress.Cursor > 0 || progress.Ready
	}
	if !s.initialHash && (!progress.Ready || next != nil) {
		if next == nil {
			return nil, report, fmt.Errorf("snapshot input callback required")
		}
		var cursor uint64
		var digest [32]byte
		var last common.Address
		haveLast := false
		nextID := progress.NextTrieID
		for {
			if e = ctx.Err(); e != nil {
				return nil, report, e
			}
			a, e := next()
			if e == io.EOF {
				break
			}
			if e != nil {
				return nil, report, e
			}
			if a.Balance == nil || a.Balance.Sign() < 0 || a.Balance.BitLen() > 256 || haveLast && bytes.Compare(a.Address[:], last[:]) <= 0 {
				return nil, report, fmt.Errorf("invalid or unordered snapshot allocation")
			}
			raw, e := json.Marshal(a)
			if e != nil {
				return nil, report, e
			}
			h := sha256.New()
			h.Write(digest[:])
			h.Write(raw)
			copy(digest[:], h.Sum(nil))
			cursor++
			last = a.Address
			haveLast = true
			if cursor <= progress.Cursor {
				if cursor == progress.Cursor && (digest != progress.Digest || [20]byte(last) != progress.LastAddress) {
					return nil, report, vt.ErrConflict
				}
				continue
			}
			if progress.Ready {
				return nil, report, vt.ErrConflict
			}
			value := state.EmptyAccount()
			value.Nonce = a.Nonce
			value.Balance.SetFromBig(a.Balance)
			value.CodeHash = crypto.Keccak256Hash(a.Code)
			keys := make([]common.Hash, 0, len(a.Storage))
			for k, v := range a.Storage {
				if v != (common.Hash{}) {
					keys = append(keys, k)
				}
			}
			sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
			if len(a.Code) > 0 || len(keys) > 0 {
				id := nextID
				nextID++
				if id > ht.MaxTrieID {
					return nil, report, vt.ErrLimit
				}
				if e = builder.BindStorage(id, [20]byte(a.Address)); e != nil {
					return nil, report, hybridError(e)
				}
				for _, k := range keys {
					raw, e := b.mode.EncodeStorage(a.Storage[k])
					if e != nil {
						return nil, report, e
					}
					if e = builder.Put(id, state.SlotKey(k), raw); e != nil {
						return nil, report, hybridError(e)
					}
				}
				value.StorageRoot, e = builder.Root(id)
				if e != nil {
					return nil, report, hybridError(e)
				}
			}
			if e = builder.PutCode(mpt.Hash(value.CodeHash), a.Code); e != nil {
				return nil, report, hybridError(e)
			}
			encoded, e := b.mode.EncodeAccount(value)
			if e != nil {
				return nil, report, e
			}
			if e = builder.Put(0, state.AccountKey(a.Address), encoded); e != nil {
				return nil, report, hybridError(e)
			}
			if cursor%uint64(cfg.CheckpointAccounts) == 0 {
				p := ht.SnapshotProgress{Cursor: cursor, NextTrieID: nextID, Digest: digest, LastAddress: [20]byte(last)}
				if e = builder.Save(p); e != nil {
					return nil, report, hybridError(e)
				}
			}
		}
		if cursor < progress.Cursor {
			return nil, report, vt.ErrConflict
		}
		if !progress.Ready {
			p := ht.SnapshotProgress{Cursor: cursor, NextTrieID: nextID, Digest: digest, LastAddress: [20]byte(last)}
			if e = builder.Finish(p); e != nil {
				return nil, report, hybridError(e)
			}
		}
	}
	progress = builder.Progress()
	root, e := builder.Root(0)
	if e != nil {
		return nil, report, hybridError(e)
	}
	if expected != nil && root != *expected {
		return nil, report, state.ErrCorrupt
	}
	if s.initialHash {
		source, e := builder.OpenSource()
		if e != nil {
			return nil, report, e
		}
		_, e = snap.Verify(ctx, source, header)
		source.Close()
		if e != nil {
			return nil, report, e
		}
	}
	report.Accounts, report.NextTrieID, report.Root, report.InputDigest = progress.Cursor, progress.NextTrieID, root, progress.Digest
	report.Build = builder.Stats()
	if record != nil {
		if e = record(Result{Height: header.Height, ID: header.ID, Root: root}); e != nil {
			return nil, report, e
		}
	}
	if s.initialHash {
		_, report.Import, e = s.store.ImportHashSnapshotContext(ctx, builder)
	} else {
		_, report.Import, e = s.store.ImportSnapshotContext(ctx, builder)
	}
	if e != nil {
		return nil, report, hybridError(e)
	}
	result = &Result{Height: header.Height, ID: header.ID, Root: root}
	if e = builder.Close(); e != nil {
		return nil, report, e
	}
	if !cfg.KeepScratch {
		if e = builder.Remove(); e != nil {
			return result, report, fmt.Errorf("snapshot published; scratch cleanup: %w", e)
		}
	}
	return result, report, nil
}
func (b *Backend) SnapshotStats() SnapshotReport {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastSnapshot
}
