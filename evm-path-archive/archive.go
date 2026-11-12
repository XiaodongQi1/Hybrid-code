package patharchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/holiman/uint256"
	rt "github.com/XiaodongQi1/HybridTrie/evm-runtime"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/blockhashcache"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/params"
	"github.com/XiaodongQi1/HybridTrie/evm-state/artifact"
	da "github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-state/forest"
	dq "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	snap "github.com/XiaodongQi1/HybridTrie/evm-state/snapshot"
	ds "github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/internal/kvcache"
	"github.com/XiaodongQi1/HybridTrie/internal/logicalio"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type Config struct {
	ExecutionNodeCacheBytes uint64 `json:",omitempty"`
	StateOnly               bool   `json:",omitempty"`
	// Empty preserves the persisted legacy config and its raw105/raw32 leaves.
	Codec                                        codec.Mode     `json:",omitempty"`
	HistoryLayout                                *HistoryLayout `json:",omitempty"`
	HistoryGroupEntries                          uint64         `json:",omitempty"`
	HistoryIndexSpan                             uint64         `json:",omitempty"`
	Format, WorkloadID                           string
	HistoryNodeCheckpointInterval                uint64 `json:",omitempty"`
	MaxHistoryDecodeBytes                        uint64 `json:",omitempty"`
	ZeroGenesis                                  bool
	Chain                                        *params.ChainConfig
	MaxBatchBytes, MaxStagingBytes, MaxWalkNodes uint64
	Storage                                      *kvstore.Options `json:",omitempty"`
}

func (c Config) codec() codec.Mode {
	if c.Codec == "" {
		return codec.Legacy
	}
	return c.Codec
}

// P15: explicit bulk-import resource ceilings for large supplied snapshots.
// Defaults and all persisted state/history encodings remain unchanged.
const MaxWriteBudgetBytes uint64 = 4 << 30

func DefaultConfig(id string) Config {
	return Config{Format: DifferentialFormat, HistoryGroupEntries: 16, HistoryIndexSpan: 64, HistoryNodeCheckpointInterval: 8, MaxHistoryDecodeBytes: 64 << 20, WorkloadID: id, Chain: rt.DefaultChainConfig(), MaxBatchBytes: 128 << 20, MaxStagingBytes: 128 << 20, MaxWalkNodes: 1 << 24}
}
func (c Config) initial() uint64 {
	if c.ZeroGenesis {
		return 0
	}
	return 1
}

type Record struct {
	Artifacts  *ArtifactRefs `json:",omitempty"`
	StateID    uint64        `json:",omitempty"`
	Height     uint64
	ID, Digest common.Hash
	Root       mpt.Hash
	Outcomes   []da.Outcome
	Changes    Changes
	Input      json.RawMessage `json:"-"`
}
type ArtifactRefs struct {
	SnapshotManifest                  string `json:",omitempty"`
	Input, Outcomes, Changes, Logical common.Hash
}

type Archive struct {
	s            *Store
	hashes       blockhashcache.Cache
	queryHeaders map[uint64]queryHeader
}

type queryHeader struct {
	Height, StateID uint64
	Root            mpt.Hash
}

// PrepareQueryHeaders makes immutable canonical metadata available before
// benchmark timing. It does not load trie nodes, proofs, flat values or answers.
func (a *Archive) PrepareQueryHeaders(ctx context.Context) error {
	s := a.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	if !s.readOnly {
		return errors.New("query header preparation requires read-only archive")
	}
	headers := make(map[uint64]queryHeader)
	for h := s.cfg.initial(); h <= s.head.Height; h++ {
		if e := ctx.Err(); e != nil {
			return e
		}
		header, e := s.queryHeader(h)
		if e != nil {
			return e
		}
		headers[h] = header
	}
	a.queryHeaders = headers
	return nil
}

func (s *Store) queryHeader(height uint64) (queryHeader, error) {
	var h queryHeader
	b, e := s.db.Get(heightKey('B', height))
	if e != nil {
		return h, e
	}
	if e = json.Unmarshal(b, &h); e != nil {
		return h, e
	}
	if h.Height != height {
		return h, ErrCorrupt
	}
	return h, nil
}

func Open(path string, c Config, create, readOnly bool) (*Archive, error) {
	return OpenWithOptions(path, c, create, readOnly, kvcache.LegacyOptions())
}
func OpenWithOptions(path string, c Config, create, readOnly bool, tuning kvcache.Options) (*Archive, error) {
	return openArchive(path, c, create, readOnly, tuning, nil)
}

// OpenWithArtifacts uses a caller-owned shared artifact store. StateOnly mode
// persists compact headers instead of transaction inputs, receipts and metrics.
// Read-only queries can omit artifacts; Result/Audit still require the source.
func OpenWithArtifacts(path string, c Config, create, readOnly bool, artifacts *artifact.Store) (*Archive, error) {
	return openArchive(path, c, create, readOnly, kvcache.LegacyOptions(), artifacts)
}
func openArchive(path string, c Config, create, readOnly bool, tuning kvcache.Options, artifacts *artifact.Store) (*Archive, error) {
	if e := c.codec().Validate(); e != nil {
		return nil, e
	}
	if c.StateOnly {
		if c.Storage == nil || c.codec() != codec.Ethereum || !readOnly && artifacts == nil {
			return nil, errors.New("state-only Path requires shared KV, ethereum-mpt and artifacts for writing")
		}
		if artifacts != nil && !artifacts.Matches(c.WorkloadID, c.codec()) {
			return nil, artifact.ErrCorrupt
		}
	} else if artifacts != nil {
		return nil, errors.New("artifact source requires StateOnly")
	}
	if (!IsIndexedFormat(c.Format) && c.Format != Format && c.Format != DifferentialFormat && c.Format != FullHistoryFormat && c.Format != CompactFormat && c.Format != LegacyFormat) || c.WorkloadID == "" || c.Chain == nil || c.MaxBatchBytes < 1024 || c.MaxBatchBytes > MaxWriteBudgetBytes || c.MaxStagingBytes < 1024 || c.MaxStagingBytes > MaxWriteBudgetBytes || c.MaxWalkNodes == 0 {
		return nil, errors.New("invalid path archive config")
	}
	if isDifferential(c.Format) {
		if c.HistoryNodeCheckpointInterval == 0 || c.HistoryNodeCheckpointInterval > 65536 || c.MaxHistoryDecodeBytes < 1024 || c.MaxHistoryDecodeBytes > 1<<30 {
			return nil, errors.New("invalid path history limits")
		}
	} else {
		c.HistoryNodeCheckpointInterval = 0
		c.MaxHistoryDecodeBytes = 0
	}
	if c.Format == Format {
		if c.HistoryGroupEntries == 0 || c.HistoryGroupEntries > 256 || c.HistoryIndexSpan == 0 || c.HistoryIndexSpan > 4096 {
			return nil, errors.New("invalid history grouping limits")
		}
	} else {
		c.HistoryGroupEntries = 0
		c.HistoryIndexSpan = 0
	}
	if IsIndexedFormat(c.Format) {
		if c.HistoryLayout == nil {
			l := DefaultHistoryLayout()
			c.HistoryLayout = &l
		}
		l := c.HistoryLayout.ForFormat(c.Format)
		c.HistoryLayout = &l
		if !l.valid() || (c.Format == IndexedFormat && l.RestartEntries != 0) {
			return nil, errors.New("invalid indexed history layout")
		}
	} else {
		c.HistoryLayout = nil
	}
	mem, _ := rt.NewMemoryState()
	if _, e := rt.New(mem, c.Chain, rt.Block{Number: 1, GasLimit: 1}); e != nil {
		return nil, e
	}
	if c.Storage != nil {
		o, e := pathStorageOptions(*c.Storage)
		if e != nil {
			return nil, e
		}
		c.Storage = &o
	}
	wire, _ := json.Marshal(c)
	var frozen Config
	if e := json.Unmarshal(wire, &frozen); e != nil {
		return nil, e
	}
	if frozen.Storage != nil {
		frozen.Storage.BeforeWrite = c.Storage.BeforeWrite
	}
	s, e := openStoreExternal(path, frozen, create, readOnly, tuning, artifacts)
	if e != nil {
		return nil, e
	}
	return &Archive{s: s, hashes: blockhashcache.Cache{}}, nil
}
func (a *Archive) Close() error { return a.s.Close() }
func (a *Archive) Result(h uint64) (Record, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	if e := a.s.check(); e != nil {
		return Record{}, e
	}
	return a.s.record(h)
}
func (a *Archive) ExecuteBlock(ctx context.Context, b da.InputBlock) (Record, error) {
	return a.executeBlock(ctx, b, nil, nil, nil)
}

func (a *Archive) ExecuteMeasured(ctx context.Context, b da.InputBlock, timing *da.ExecutionTiming) (Record, error) {
	return a.executeBlock(ctx, b, nil, nil, timing)
}

// ApplyEvaluatedBlock is a trusted experiment path; it executes no EVM instructions.
func (a *Archive) ApplyEvaluatedBlock(ctx context.Context, b da.InputBlock, e da.EvaluatedBlock) (Record, error) {
	// This compatibility artifact contains raw105 account payloads. New native
	// comparisons must use the typed logical v2 importer instead.
	if a.s.cfg.codec() != codec.Legacy {
		return Record{}, ErrCorrupt
	}
	return a.executeBlock(ctx, b, &e, nil, nil)
}

func (a *Archive) ApplyLogicalBlock(ctx context.Context, b da.InputBlock, trace da.LogicalBlock) (Record, error) {
	if e := trace.Validate(b, a.s.cfg.codec()); e != nil {
		return Record{}, e
	}
	return a.executeBlock(ctx, b, nil, &trace, nil)
}

func (a *Archive) executeBlock(ctx context.Context, b da.InputBlock, evaluated *da.EvaluatedBlock, logical *da.LogicalBlock, timing *da.ExecutionTiming) (Record, error) {
	s := a.s
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return Record{}, e
	}
	if s.readOnly {
		return Record{}, ErrReadOnly
	}
	if e := ctx.Err(); e != nil {
		return Record{}, e
	}
	if b.ID == (common.Hash{}) || b.Context.GasLimit == 0 || b.Context.BaseFee == nil || b.Context.BlobBaseFee == nil || b.Context.BaseFee.Sign() < 0 || b.Context.BlobBaseFee.Sign() < 0 {
		return Record{}, errors.New("invalid block context")
	}
	wire, e := json.Marshal(b)
	if e != nil {
		return Record{}, e
	}
	var previous common.Hash
	if s.head.ID != (common.Hash{}) && b.Height == s.head.Height {
		prior, e := s.input(b.Height)
		if e != nil || !bytes.Equal(prior, wire) {
			return Record{}, errors.New("conflicting retry")
		}
		rec, e := s.record(b.Height)
		if e == nil && logical != nil && (rec.Root != logical.Root || !equalJSON(rec.Outcomes, logical.Outcomes)) {
			return Record{}, ErrCorrupt
		}
		return rec, e
	}
	initial := s.head.ID == (common.Hash{})
	if (initial && (b.Height != s.cfg.initial() || b.ParentID != (common.Hash{}) || len(b.Transactions) > 0)) || (!initial && (b.Height != s.head.Height+1 || b.ParentID != s.head.ID || len(b.Allocations) > 0)) {
		return Record{}, errors.New("noncontinuous block")
	}
	previous = s.head.Digest
	view, e := forest.OpenWithCodec(s.head.Root, forest.WithExecutionNodeCache(s.live(ctx), s.nodeCache), s.cfg.codec())
	if e != nil {
		return Record{}, e
	}
	var changes map[common.Address]ds.Change
	var outcomes []da.Outcome
	if logical != nil {
		if initial {
			return Record{}, ErrCorrupt
		}
		changes, e = logical.ChangesFor(view)
		if e != nil {
			return Record{}, e
		}
		outcomes = logical.Outcomes
	} else if evaluated != nil {
		if len(evaluated.Outcomes) != len(b.Transactions) {
			return Record{}, ErrCorrupt
		}
		changes, e = evaluated.ChangesFor(view)
		if e != nil {
			return Record{}, e
		}
		outcomes = evaluated.Outcomes
	} else {
		state := ds.New(view)
		if initial {
			seen := map[common.Address]bool{}
			for _, v := range b.Allocations {
				if e := ctx.Err(); e != nil {
					return Record{}, e
				}
				if seen[v.Address] || v.Balance == nil || v.Balance.Sign() < 0 || v.Balance.BitLen() > 256 {
					return Record{}, errors.New("invalid allocation")
				}
				seen[v.Address] = true
				state.CreateAccount(v.Address)
				state.AddBalance(v.Address, uint256.MustFromBig(v.Balance))
				state.SetNonce(v.Address, v.Nonce)
				state.SetCode(v.Address, v.Code)
				for k, w := range v.Storage {
					state.SetState(v.Address, k, w)
				}
			}
		} else {
			timing.StartTransactions()
			hashes, e := a.hashes.Window(b.Height, s.cfg.initial(), func(h uint64) (common.Hash, error) { r, e := s.stateRecord(h); return r.ID, e })
			if e != nil {
				return Record{}, e
			}

			c := b.Context
			engine, e := rt.New(state, s.cfg.Chain, rt.Block{Number: b.Height, Time: c.Time, GasLimit: c.GasLimit, Coinbase: c.Coinbase, BaseFee: c.BaseFee, BlobBaseFee: c.BlobBaseFee, Random: c.Random, Hash: b.ID, GetHash: func(h uint64) common.Hash { return hashes[h] }})
			if e != nil {
				return Record{}, e
			}
			for _, tx := range b.Transactions {
				if e = ctx.Err(); e != nil {
					return Record{}, e
				}
				out, e := engine.ApplyEncoded(tx)
				if e != nil {
					return Record{}, e
				}
				o := da.Outcome{Receipt: out.Receipt, ReturnData: out.ReturnData}
				if out.VMError != nil {
					o.VMError = out.VMError.Error()
				}
				outcomes = append(outcomes, o)
			}
		}
		timing.StartMaterialize()
		changes, e = state.Changes()
		if e != nil {
			return Record{}, e
		}
	}
	plan, e := view.Materialize(changes, s.cfg.MaxStagingBytes)
	if e != nil {
		return Record{}, e
	}
	if logical != nil && plan.Root != logical.Root {
		return Record{}, fmt.Errorf("%w: logical block root mismatch", ErrCorrupt)
	}
	timing.StartPublish()
	digester := sha256.New()
	digester.Write([]byte(s.cfg.Format + "\x00" + s.cfg.WorkloadID))
	digester.Write(previous[:])
	digester.Write(wire)
	digest := common.BytesToHash(digester.Sum(nil))
	record := Record{Height: b.Height, ID: b.ID, Digest: digest, Root: plan.Root, Outcomes: outcomes, Input: wire}
	if s.cfg.StateOnly && logical != nil {
		_, raw, e := logical.Freeze(b, s.cfg.codec())
		if e != nil {
			return Record{}, e
		}
		id, e := s.artifacts.Ensure("logical-block", raw)
		if e != nil {
			return Record{}, e
		}
		record.Artifacts = &ArtifactRefs{Logical: id}
	}
	committed, e := s.commit(ctx, plan, record)
	if e == nil {
		a.hashes.Remember(committed.Height, committed.ID)
	}
	return committed, e
}

// Query reads the answer and its proof from the same historical trie paths.
func (a *Archive) Query(ctx context.Context, q dq.Request) (dq.Response, error) {
	s := a.s
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := dq.Response{Request: q}
	if e := s.check(); e != nil {
		return out, e
	}
	if q.Kind != dq.Account && q.Kind != dq.Storage && q.Kind != dq.Code || q.Kind != dq.Storage && q.Slot != (common.Hash{}) {
		return out, dq.ErrRequest
	}
	if q.Height < s.cfg.initial() || q.Height > s.head.Height {
		return out, errors.New("height unavailable")
	}
	rec, ok := a.queryHeaders[q.Height]
	var e error
	if !ok {
		rec, e = s.queryHeader(q.Height)
	}
	if e != nil {
		return out, e
	}
	out.Root = rec.Root
	snap, e := s.db.GetSnapshot()
	if e != nil {
		return out, e
	}
	defer snap.Release()
	r := snapshotReader(ctx, snap, s.cfg.Format, q.Height)
	r.mode = s.cfg.codec()
	r.targetID = rec.StateID
	r.targetHeight = q.Height
	r.targetKnown = true
	r.limit = s.cfg.HistoryNodeCheckpointInterval
	r.byteLimit = s.cfg.MaxHistoryDecodeBytes
	r.metrics = &s.recovery
	r.groupEntries = s.cfg.HistoryGroupEntries
	r.indexSpan = s.cfg.HistoryIndexSpan
	if s.cfg.HistoryLayout != nil {
		r.layout = *s.cfg.HistoryLayout
	}
	r.current = (isDifferential(s.cfg.Format) || s.cfg.Format == FullHistoryFormat) && q.Height == s.head.Height
	r.servingProof = true
	out, e = forest.AssembleAtWithProof(ctx, q, rec.Root, r, s.cfg.codec())
	if e != nil {
		return out, e
	}
	if s.cfg.codec() == codec.Ethereum {
		out = out.Compact()
	}
	return out, nil
}

// Breakdown returns uncompressed logical KV bytes, never physical write volume.
func (a *Archive) Breakdown() (map[string]int64, error) {
	s := a.s
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.check(); e != nil {
		return nil, e
	}
	out := map[string]int64{}
	it := s.db.NewIterator(nil)
	defer it.Release()
	for it.Next() {
		k := it.Key()
		kind := "metadata"
		switch k[0] {
		case 'P':
			kind = "current-trie"
		case 'F':
			kind = "current-flat"
		case 'H':
			kind = "reverse-history"
		case 'R':
			kind = "history-directory"
		case 'X', 'Y':
			kind = "history-index"
		case 'G':
			kind = "reverse-history"
		case 'J':
			kind = "history-index"
		case 'D':
			kind = "reverse-history"
		case 'Q':
			kind = "history-checkpoints"
		case 'I':
			kind = "hash-location-index"
		case 'C':
			kind = "code"
		case 'L':
			kind = "ledger"
		case 'B':
			if s.cfg.StateOnly {
				kind = "root-headers"
			} else {
				kind = "roots-outcomes"
			}
		}
		out[kind] += int64(len(k) + len(it.Value()))
	}
	return out, it.Error()
}
func (a *Archive) Audit(ctx context.Context) error {
	s := a.s
	s.mu.RLock()
	defer s.mu.RUnlock()
	if e := s.check(); e != nil {
		return e
	}
	if e := s.auditIndexed(ctx); e != nil {
		return e
	}
	if e := s.auditGroups(ctx); e != nil {
		return e
	}
	if e := s.validateCheckpointCounters(); e != nil {
		return e
	}
	var previous common.Hash
	for h := s.cfg.initial(); h <= s.head.Height; h++ {
		rec, e := s.record(h)
		if e != nil {
			return e
		}
		wire, e := s.input(h)
		if e != nil {
			return e
		}
		if s.cfg.StateOnly && rec.Artifacts.SnapshotManifest != "" {
			if h != s.cfg.initial() || len(rec.Artifacts.SnapshotManifest) != 66 {
				return ErrCorrupt
			}
			raw, e := s.artifacts.Get("snapshot-manifest", common.HexToHash(rec.Artifacts.SnapshotManifest))
			if e != nil {
				return e
			}
			var input da.InputBlock
			if json.Unmarshal(wire, &input) != nil {
				return ErrCorrupt
			}
			if _, e = snap.ParseManifest(raw, s.cfg.WorkloadID, input, rec.Root); e != nil {
				return e
			}
		}
		if s.cfg.StateOnly && rec.Artifacts.Logical != (common.Hash{}) {
			raw, e := s.artifacts.Get("logical-block", rec.Artifacts.Logical)
			if e != nil {
				return e
			}
			var input da.InputBlock
			var trace da.LogicalBlock
			if json.Unmarshal(wire, &input) != nil || json.Unmarshal(raw, &trace) != nil || trace.Validate(input, s.cfg.codec()) != nil || trace.Root != rec.Root || !equalJSON(trace.Outcomes, rec.Outcomes) {
				return ErrCorrupt
			}
		}
		d := sha256.New()
		d.Write([]byte(s.cfg.Format + "\x00" + s.cfg.WorkloadID))
		d.Write(previous[:])
		d.Write(wire)
		if common.BytesToHash(d.Sum(nil)) != rec.Digest {
			return ErrCorrupt
		}
		previous = rec.Digest
		historical := s.live(ctx)
		historical.height = h
		historical.current = false
		historical.targetID = rec.StateID
		historical.targetHeight = h
		historical.targetKnown = true
		v, e := forest.OpenWithCodec(rec.Root, historical, s.cfg.codec())
		if e != nil {
			return e
		}
		var visits uint64
		if e = v.Audit(ctx, &visits, s.cfg.MaxWalkNodes); e != nil {
			return fmt.Errorf("height %d: %w", h, e)
		}
	}
	return nil
}

func (a *Archive) LogicalWrites() logicalio.Stats { return a.s.LogicalWrites() }

// CommittedHead returns the latest published block identity and state root.
func (a *Archive) CommittedHead() (uint64, mpt.Hash, [32]byte, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	if e := a.s.check(); e != nil {
		return 0, mpt.Hash{}, [32]byte{}, e
	}
	h := a.s.head
	return h.Height, h.Root, h.ID, nil
}
