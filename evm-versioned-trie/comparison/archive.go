package comparison

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/holiman/uint256"
	rt "github.com/XiaodongQi1/HybridTrie/evm-runtime"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/blockhashcache"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/params"
	"github.com/XiaodongQi1/HybridTrie/evm-state/artifact"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-state/forest"
	q "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	snap "github.com/XiaodongQi1/HybridTrie/evm-state/snapshot"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/internal/logicalio"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// ArchiveConfig selects a persistent canonical Hash MPT. Execution artifacts
// live in the caller-owned shared artifact store, never in the measured DB.
type ArchiveConfig struct {
	ExecutionNodeCacheBytes        uint64 `json:",omitempty"`
	WorkloadID                     string
	Codec                          codec.Mode
	InitialHeight                  uint64
	Chain                          *params.ChainConfig
	MaxBatchBytes, MaxStagingBytes uint64
	Storage                        kvstore.Options
}

func DefaultArchiveConfig(id string, engine kvstore.Engine) ArchiveConfig {
	return ArchiveConfig{WorkloadID: id, Codec: codec.Ethereum, Chain: rt.DefaultChainConfig(), MaxBatchBytes: 8 << 20, MaxStagingBytes: 8 << 20, Storage: kvstore.DefaultOptions(engine)}
}

type ArchiveHeader struct {
	SnapshotManifest         string `json:",omitempty"`
	Height                   uint64
	ID, ParentID             common.Hash
	Root                     mpt.Hash
	Input, Outcomes, Logical common.Hash
}
type ArchiveResult struct {
	ArchiveHeader
	OutcomesData []block.Outcome
}
type Archive struct {
	nodeCache                   *forest.ExecutionNodeCache
	importPending               bool
	mu                          sync.RWMutex
	db                          *kvstore.DB
	cfg                         ArchiveConfig
	artifacts                   *artifact.Store
	head                        *ArchiveHeader
	hashes                      blockhashcache.Cache
	logical                     logicalio.Stats
	closed, readOnly, uncertain bool
}

func archiveRootKey(h uint64) []byte {
	k := make([]byte, 9)
	k[0] = 'B'
	binary.BigEndian.PutUint64(k[1:], h)
	return k
}
func archiveCodeKey(h common.Hash) []byte { return append([]byte{'C'}, h[:]...) }
func OpenArchive(path string, c ArchiveConfig, create, ro bool, artifacts *artifact.Store) (*Archive, error) {
	if c.WorkloadID == "" || c.Codec != codec.Ethereum || c.Chain == nil || c.MaxBatchBytes < 1024 || c.MaxBatchBytes > 128<<20 || c.MaxStagingBytes < 1024 || c.MaxStagingBytes > 128<<20 || !ro && artifacts == nil {
		return nil, kvstore.ErrInvalid
	}
	if artifacts != nil && !artifacts.Matches(c.WorkloadID, c.Codec) {
		return nil, artifact.ErrCorrupt
	}
	mem, _ := rt.NewMemoryState()
	if _, e := rt.New(mem, c.Chain, rt.Block{Number: c.InitialHeight, GasLimit: 1}); e != nil {
		return nil, e
	}
	if len(c.Storage.Columns) > 0 && (len(c.Storage.Columns) != 2 || !contains(c.Storage.Columns, "default") || !contains(c.Storage.Columns, "hash")) {
		return nil, kvstore.ErrInvalid
	}
	c.Storage.Columns = []string{"default", "hash"}
	var e error
	c.Storage, e = c.Storage.Resolved()
	if e != nil {
		return nil, e
	}
	// Own chain-rule pointers as well as engine options before persisting identity.
	wire, _ := json.Marshal(c)
	if e = json.Unmarshal(wire, &c); e != nil {
		return nil, e
	}
	if c.ExecutionNodeCacheBytes != 0 && c.Storage.ApplicationCacheBytes != 0 {
		return nil, kvstore.ErrInvalid
	}
	nodeCache, err := forest.NewExecutionNodeCache(c.ExecutionNodeCacheBytes, ro)
	if err != nil {
		return nil, err
	}
	db, e := kvstore.Open(path, c.Storage, create, ro)
	if e != nil {
		return nil, e
	}
	a := &Archive{nodeCache: nodeCache, db: db, cfg: c, artifacts: artifacts, readOnly: ro, logical: logicalio.Stats{}, hashes: blockhashcache.Cache{}}
	fail := func(e error) (*Archive, error) { db.Close(); return nil, e }
	manifest, _ := json.Marshal(struct {
		Schema string
		Config ArchiveConfig
	}{"ethereum-hash-archive/v1", c})
	if create {
		b := kvstore.NewBatch()
		b.Put("default", []byte("M/config"), manifest)
		b.Put("default", []byte("M/head"), []byte("null"))
		if e = db.Write(b); e != nil {
			return fail(e)
		}
		a.logical.AddKVBatch(b)
	} else {
		old, e := db.Get("default", []byte("M/config"))
		if e != nil {
			return fail(e)
		}
		if !bytes.Equal(old, manifest) {
			return fail(state.ErrCorrupt)
		}
		raw, e := db.Get("default", []byte("M/head"))
		if e != nil {
			return fail(e)
		}
		if e = json.Unmarshal(raw, &a.head); e != nil {
			return fail(e)
		}
		if a.head != nil {
			if a.head.Height < c.InitialHeight {
				return fail(state.ErrCorrupt)
			}
			h, e := a.header(a.head.Height)
			if e != nil {
				return fail(e)
			}
			if h != *a.head {
				return fail(state.ErrCorrupt)
			}
			view, e := a.db.NewSnapshot()
			if e != nil {
				return fail(e)
			}
			_, e = forest.OpenWithCodec(h.Root, hashReader{view}, c.Codec)
			view.Close()
			if e != nil {
				return fail(e)
			}
		}
	}
	m, me := db.BulkStatus([]byte("M/initial-import"))
	if me == nil {
		if m.Complete != (a.head != nil) {
			return fail(state.ErrCorrupt)
		}
		a.importPending = !m.Complete
	} else if !errors.Is(me, kvstore.ErrNotFound) {
		return fail(me)
	}
	return a, nil
}
func contains(xs []string, s string) bool {
	for _, x := range xs {
		if x == s {
			return true
		}
	}
	return false
}
func (a *Archive) check() error {
	if a.closed {
		return kvstore.ErrClosed
	}
	if a.uncertain {
		return kvstore.ErrUncertain
	}
	if a.importPending {
		return kvstore.ErrImportPending
	}
	return nil
}
func (a *Archive) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	if e := a.db.Close(); e != nil {
		return e
	}
	a.nodeCache = nil
	a.closed = true
	return nil
}
func (a *Archive) KVInfo() kvstore.Info { return a.db.Info() }
func (a *Archive) header(h uint64) (ArchiveHeader, error) {
	var out ArchiveHeader
	raw, e := a.db.Get("default", archiveRootKey(h))
	if e != nil {
		return out, e
	}
	if json.Unmarshal(raw, &out) != nil || out.Height != h || out.ID == (common.Hash{}) || out.Root == (mpt.Hash{}) || out.Input == (common.Hash{}) || out.Outcomes == (common.Hash{}) {
		return out, state.ErrCorrupt
	}
	return out, nil
}
func (a *Archive) Header(h uint64) (ArchiveHeader, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if e := a.check(); e != nil {
		return ArchiveHeader{}, e
	}
	return a.header(h)
}
func (a *Archive) result(h ArchiveHeader) (ArchiveResult, error) {
	out := ArchiveResult{ArchiveHeader: h}
	if a.artifacts == nil {
		return out, errors.New("Archive: external artifact source required")
	}
	raw, e := a.artifacts.Get("block-outcomes", h.Outcomes)
	if e != nil {
		return out, e
	}
	if e = json.Unmarshal(raw, &out.OutcomesData); e != nil {
		return out, state.ErrCorrupt
	}
	return out, nil
}
func (a *Archive) Result(h uint64) (ArchiveResult, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if e := a.check(); e != nil {
		return ArchiveResult{}, e
	}
	r, e := a.header(h)
	if e != nil {
		return ArchiveResult{}, e
	}
	return a.result(r)
}

type hashReader struct{ view *kvstore.Snapshot }

type localProofReader struct{ hashReader }

func (r localProofReader) Get(k []byte) ([]byte, error) {
	return r.readNode(k)
}
func (r hashReader) readNode(k []byte) ([]byte, error) {
	if len(k) != 32 {
		return nil, state.ErrCorrupt
	}
	v, e := r.view.Get("hash", k)
	if errors.Is(e, kvstore.ErrNotFound) {
		return nil, mpt.ErrNotFound
	}
	return v, e
}

func (r hashReader) Get(k []byte) ([]byte, error) {
	v, e := r.readNode(k)
	if e != nil {
		return nil, e
	}
	if !bytes.Equal(crypto.Keccak256(v), k) {
		return nil, state.ErrCorrupt
	}
	return v, nil
}
func (r hashReader) Code(h common.Hash) ([]byte, error) {
	if h == state.EmptyCode {
		return nil, nil
	}
	v, e := r.view.Get("default", archiveCodeKey(h))
	if e == nil && crypto.Keccak256Hash(v) != h {
		return nil, state.ErrCorrupt
	}
	return v, e
}

type archiveDenied struct{}

func (archiveDenied) Put([]byte, []byte) error { return kvstore.ErrReadOnly }
func (archiveDenied) Write() error             { return kvstore.ErrReadOnly }
func (hashReader) NewBatch() mpt.Batch         { return archiveDenied{} }

type ArchiveView struct {
	*forest.View
	snapshot *kvstore.Snapshot
}

func (v *ArchiveView) Close() error { return v.snapshot.Close() }
func (a *Archive) pin(height uint64) (*kvstore.Snapshot, mpt.Hash, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if e := a.check(); e != nil {
		return nil, mpt.Hash{}, e
	}
	header, e := a.header(height)
	if e != nil {
		return nil, mpt.Hash{}, e
	}
	s, e := a.db.NewSnapshot()
	return s, header.Root, e
}
func (a *Archive) ViewAt(h uint64) (*ArchiveView, error) {
	s, root, e := a.pin(h)
	if e != nil {
		return nil, e
	}
	f, e := forest.OpenWithCodec(root, hashReader{s}, a.cfg.Codec)
	if e != nil {
		s.Close()
		return nil, e
	}
	return &ArchiveView{f, s}, nil
}
func (a *Archive) Query(ctx context.Context, request q.Request) (q.Response, error) {
	s, root, e := a.pin(request.Height)
	if e != nil {
		return q.Response{}, e
	}
	defer s.Close()
	out, e := forest.AssembleAtWithProof(ctx, request, root, localProofReader{hashReader{s}}, a.cfg.Codec)
	if e != nil {
		return out, e
	}
	return out.Compact(), nil
}

func (a *Archive) Execute(ctx context.Context, input block.InputBlock) (ArchiveResult, error) {
	r, _, e := a.execute(ctx, input, nil, false, nil)
	return r, e
}
func (a *Archive) ExecuteMeasured(ctx context.Context, input block.InputBlock, timing *block.ExecutionTiming) (ArchiveResult, error) {
	r, _, e := a.execute(ctx, input, nil, false, timing)
	return r, e
}
func (a *Archive) ExecuteWithTrace(ctx context.Context, input block.InputBlock) (ArchiveResult, block.LogicalBlock, error) {
	return a.execute(ctx, input, nil, true, nil)
}
func (a *Archive) ApplyLogicalBlock(ctx context.Context, input block.InputBlock, trace block.LogicalBlock) (ArchiveResult, error) {
	if e := trace.Validate(input, a.cfg.Codec); e != nil {
		return ArchiveResult{}, e
	}
	r, _, e := a.execute(ctx, input, &trace, false, nil)
	return r, e
}
func (a *Archive) execute(ctx context.Context, input block.InputBlock, logical *block.LogicalBlock, capture bool, timing *block.ExecutionTiming) (ArchiveResult, block.LogicalBlock, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out ArchiveResult
	var trace block.LogicalBlock
	if e := a.check(); e != nil {
		return out, trace, e
	}
	if a.readOnly {
		return out, trace, kvstore.ErrReadOnly
	}
	if e := ctx.Err(); e != nil {
		return out, trace, e
	}
	if input.ID == (common.Hash{}) || input.Context.GasLimit == 0 || input.Context.BaseFee == nil || input.Context.BlobBaseFee == nil || input.Context.BaseFee.Sign() < 0 || input.Context.BlobBaseFee.Sign() < 0 {
		return out, trace, state.ErrCorrupt
	}
	wire, e := json.Marshal(input)
	if e != nil {
		return out, trace, e
	}
	if a.head != nil && input.Height == a.head.Height {
		prior, e := a.artifacts.Get("block-input", a.head.Input)
		if e != nil {
			return out, trace, e
		}
		if !bytes.Equal(prior, wire) {
			return out, trace, state.ErrCorrupt
		}
		out, e = a.result(*a.head)
		if e != nil {
			return out, trace, e
		}
		if logical != nil && (logical.Root != out.Root || !sameJSON(logical.Outcomes, out.OutcomesData)) {
			return out, trace, state.ErrCorrupt
		}
		if capture && a.head.Logical != (common.Hash{}) {
			raw, e := a.artifacts.Get("logical-block", a.head.Logical)
			if e != nil {
				return out, trace, e
			}
			e = json.Unmarshal(raw, &trace)
			return out, trace, e
		}
		if capture {
			return out, trace, errors.New("Archive: committed execution has no recorded logical trace")
		}
		return out, trace, nil
	}
	initial := a.head == nil
	if initial && (input.Height != a.cfg.InitialHeight || input.ParentID != (common.Hash{}) || len(input.Transactions) > 0 || logical != nil || capture) || !initial && (a.head.Height == math.MaxUint64 || input.Height != a.head.Height+1 || input.ParentID != a.head.ID || len(input.Allocations) > 0) {
		return out, trace, state.ErrCorrupt
	}
	snap, e := a.db.NewSnapshot()
	if e != nil {
		return out, trace, e
	}
	defer snap.Close()
	root := mpt.EmptyRootHash
	if a.head != nil {
		root = a.head.Root
	}
	base, e := forest.OpenWithCodec(root, forest.WithExecutionNodeCache(hashReader{snap}, a.nodeCache), a.cfg.Codec)
	if e != nil {
		return out, trace, e
	}
	var changes map[common.Address]state.Change
	var outcomes []block.Outcome
	if logical != nil {
		changes, e = logical.ChangesFor(base)
		if e != nil {
			return out, trace, e
		}
		outcomes = logical.Outcomes
	} else {
		journal := state.New(base)
		if initial {
			seen := map[common.Address]bool{}
			for _, v := range input.Allocations {
				if e := ctx.Err(); e != nil {
					return out, trace, e
				}
				if seen[v.Address] || v.Balance == nil || v.Balance.Sign() < 0 || v.Balance.BitLen() > 256 {
					return out, trace, state.ErrCorrupt
				}
				seen[v.Address] = true
				journal.CreateAccount(v.Address)
				journal.AddBalance(v.Address, uint256.MustFromBig(v.Balance))
				journal.SetNonce(v.Address, v.Nonce)
				journal.SetCode(v.Address, v.Code)
				for k, w := range v.Storage {
					journal.SetState(v.Address, k, w)
				}
			}
		} else {
			timing.StartTransactions()
			hashes, e := a.hashes.Window(input.Height, a.cfg.InitialHeight, func(h uint64) (common.Hash, error) { r, e := a.header(h); return r.ID, e })
			if e != nil {
				return out, trace, e
			}
			c := input.Context
			engine, e := rt.New(journal, a.cfg.Chain, rt.Block{Number: input.Height, Time: c.Time, GasLimit: c.GasLimit, Coinbase: c.Coinbase, BaseFee: c.BaseFee, BlobBaseFee: c.BlobBaseFee, Random: c.Random, Hash: input.ID, GetHash: func(h uint64) common.Hash { return hashes[h] }})
			if e != nil {
				return out, trace, e
			}
			for _, tx := range input.Transactions {
				if e := ctx.Err(); e != nil {
					return out, trace, e
				}
				r, e := engine.ApplyEncoded(tx)
				if e != nil {
					return out, trace, e
				}
				o := block.Outcome{Receipt: r.Receipt, ReturnData: r.ReturnData}
				if r.VMError != nil {
					o.VMError = r.VMError.Error()
				}
				outcomes = append(outcomes, o)
			}
		}
		timing.StartMaterialize()
		changes, e = journal.Changes()
		if e != nil {
			return out, trace, e
		}
	}
	plan, e := base.Materialize(changes, a.cfg.MaxStagingBytes)
	if e != nil {
		return out, trace, e
	}
	if logical != nil && plan.Root != logical.Root {
		return out, trace, state.ErrCorrupt
	}
	if capture {
		trace, e = block.NewLogicalBlock(input, a.cfg.Codec, plan.Root, changes, outcomes)
		if e != nil {
			return out, trace, e
		}
	}
	timing.StartPublish()
	header := ArchiveHeader{Height: input.Height, ID: input.ID, ParentID: input.ParentID, Root: plan.Root}
	b := kvstore.NewBatch()
	used := uint64(0)
	put := func(column string, k, v []byte) error {
		size := uint64(len(k) + len(v) + 16)
		if size > a.cfg.MaxBatchBytes-used {
			return kvstore.ErrLimit
		}
		used += size
		b.Put(column, k, v)
		return nil
	}
	keys := make([]mpt.Hash, 0, len(plan.Nodes))
	for h := range plan.Nodes {
		keys = append(keys, h)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	for _, h := range keys {
		if e := ctx.Err(); e != nil {
			return out, trace, e
		}
		raw := plan.Nodes[h]
		if !bytes.Equal(crypto.Keccak256(raw), h[:]) {
			return out, trace, state.ErrCorrupt
		}
		old, e := snap.Get("hash", h[:])
		if e == nil {
			if !bytes.Equal(old, raw) {
				return out, trace, state.ErrCorrupt
			}
			continue
		}
		if !errors.Is(e, kvstore.ErrNotFound) {
			return out, trace, e
		}
		if e = put("hash", h[:], raw); e != nil {
			return out, trace, e
		}
	}
	for h, code := range plan.Codes {
		if crypto.Keccak256Hash(code) != h {
			return out, trace, state.ErrCorrupt
		}
		key := archiveCodeKey(h)
		old, e := snap.Get("default", key)
		if e == nil {
			if !bytes.Equal(old, code) {
				return out, trace, state.ErrCorrupt
			}
			continue
		}
		if !errors.Is(e, kvstore.ErrNotFound) {
			return out, trace, e
		}
		if e = put("default", key, code); e != nil {
			return out, trace, e
		}
	}
	if header.Input, e = a.artifacts.Ensure("block-input", wire); e != nil {
		return out, trace, e
	}
	raw, e := json.Marshal(outcomes)
	if e != nil {
		return out, trace, e
	}
	if header.Outcomes, e = a.artifacts.Ensure("block-outcomes", raw); e != nil {
		return out, trace, e
	}
	if capture || logical != nil {
		chosen := trace
		if logical != nil {
			chosen = *logical
		}
		_, raw, e = chosen.Freeze(input, a.cfg.Codec)
		if e != nil {
			return out, trace, e
		}
		if header.Logical, e = a.artifacts.Ensure("logical-block", raw); e != nil {
			return out, trace, e
		}
	}
	raw, e = json.Marshal(header)
	if e != nil {
		return out, trace, e
	}
	if e = put("default", archiveRootKey(input.Height), raw); e != nil {
		return out, trace, e
	}
	if e = put("default", []byte("M/head"), raw); e != nil {
		return out, trace, e
	}
	if e = ctx.Err(); e != nil {
		return out, trace, e
	}
	if e = a.db.Write(b); e != nil {
		a.uncertain = true
		return out, trace, e
	}
	a.logical.AddKVBatch(b)
	a.head = &header
	a.hashes.Remember(header.Height, header.ID)
	return ArchiveResult{header, outcomes}, trace, nil
}
func sameJSON(a, b any) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return bytes.Equal(x, y)
}
func (a *Archive) LogicalWrites() logicalio.Stats {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.logical.Clone()
}

// Audit checks all canonical historical forests and their external input,
// outcome and trace references. Queries themselves need no artifact store.
func (a *Archive) Audit(ctx context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if e := a.check(); e != nil {
		return e
	}
	if a.head == nil {
		return nil
	}
	if a.artifacts == nil {
		return fmt.Errorf("Archive: external artifact source required")
	}
	s, e := a.db.NewSnapshot()
	if e != nil {
		return e
	}
	defer s.Close()
	var prior common.Hash
	for h := a.cfg.InitialHeight; ; h++ {
		if e = ctx.Err(); e != nil {
			return e
		}
		r, e := a.header(h)
		if e != nil {
			return e
		}
		if r.ParentID != prior {
			return state.ErrCorrupt
		}
		raw, e := a.artifacts.Get("block-input", r.Input)
		if e != nil {
			return e
		}
		var input block.InputBlock
		if json.Unmarshal(raw, &input) != nil || input.Height != h || input.ID != r.ID || input.ParentID != prior {
			return state.ErrCorrupt
		}
		if r.SnapshotManifest != "" {
			if h != a.cfg.InitialHeight || len(r.SnapshotManifest) != 66 {
				return state.ErrCorrupt
			}
			raw, e = a.artifacts.Get("snapshot-manifest", common.HexToHash(r.SnapshotManifest))
			if e != nil {
				return e
			}
			if _, e = snap.ParseManifest(raw, a.cfg.WorkloadID, input, r.Root); e != nil {
				return e
			}
		}
		result, e := a.result(r)
		if e != nil {
			return e
		}
		if len(result.OutcomesData) != len(input.Transactions) {
			return state.ErrCorrupt
		}
		if r.Logical != (common.Hash{}) {
			raw, e = a.artifacts.Get("logical-block", r.Logical)
			if e != nil {
				return e
			}
			var tr block.LogicalBlock
			if json.Unmarshal(raw, &tr) != nil || tr.Validate(input, a.cfg.Codec) != nil || tr.Root != r.Root || !sameJSON(tr.Outcomes, result.OutcomesData) {
				return state.ErrCorrupt
			}
		}
		view, e := forest.OpenWithCodec(r.Root, hashReader{s}, a.cfg.Codec)
		if e != nil {
			return e
		}
		var visits uint64
		if e = view.Audit(ctx, &visits, 1<<24); e != nil {
			return e
		}
		prior = r.ID
		if h == a.head.Height {
			break
		}
	}
	return nil
}

// CommittedHead returns the latest published block identity and state root.
func (a *Archive) CommittedHead() (uint64, mpt.Hash, [32]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if e := a.check(); e != nil {
		return 0, mpt.Hash{}, [32]byte{}, e
	}
	if a.head == nil {
		return 0, mpt.Hash{}, [32]byte{}, kvstore.ErrNotFound
	}
	h := a.head
	return h.Height, h.Root, h.ID, nil
}
