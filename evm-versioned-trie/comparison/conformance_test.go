package comparison

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	pa "github.com/XiaodongQi1/HybridTrie/evm-path-archive"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/artifact"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-state/forest"
	q "github.com/XiaodongQi1/HybridTrie/evm-state/query"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/baseline"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type conformanceTarget struct {
	name        string
	initial     func(block.InputBlock) (mpt.Hash, error)
	importBlock func(block.InputBlock, block.LogicalBlock) (mpt.Hash, error)
	query       func(q.Request) (q.Response, error)
	reopen      func() error
	close       func() error
}

type referenceStore struct{ *mpt.MemoryStore }

func (referenceStore) Code(common.Hash) ([]byte, error) { return nil, state.ErrCorrupt }

type exportedProof struct {
	Root, Key string
	Proof     []string
	Value     string
	Exists    bool
}

func TestNativeComparisonConformance(t *testing.T) {
	runNativeComparisonConformance(t, nil)
}

// The unconfigured fixture also keeps testing the legacy Path layout. The
// configured fixture adds a persistent Archive and selects one engine for all
// three measured representations. Neither fixture is a performance trial.
func runNativeComparisonConformance(t *testing.T, extra func(*testing.T) conformanceTarget) {
	runComparisonConformance(t, extra, nil)
}
func runComparisonConformance(t *testing.T, extra func(*testing.T) conformanceTarget, storage *kvstore.Options) {
	t.Helper()
	raw, e := os.ReadFile("../baseline/testdata/states.json")
	if e != nil {
		t.Fatal(e)
	}
	fixture, e := baseline.Parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	rawRoots, e := os.ReadFile("../baseline/testdata/roots.json")
	if e != nil {
		t.Fatal(e)
	}
	var oracle baseline.Reference
	if e = json.Unmarshal(rawRoots, &oracle); e != nil || oracle.FixtureSHA256 != baseline.Digest(raw) {
		t.Fatal("unbound frozen Geth oracle", e)
	}
	cfg := pa.DefaultConfig("comparison-native-conformance")
	cfg.Codec, cfg.Format, cfg.ZeroGenesis = codec.Ethereum, pa.PackedFormat, true
	layout := pa.DefaultHistoryLayout().ForFormat(cfg.Format)
	cfg.HistoryLayout = &layout
	var source *artifact.Store
	if storage != nil {
		source, e = artifact.Open(filepath.Join(t.TempDir(), "artifacts"), artifact.Config{WorkloadID: cfg.WorkloadID, Codec: codec.Ethereum}, true, false)
		if e != nil {
			t.Fatal(e)
		}
		defer source.Close()
		cfg.Storage, cfg.StateOnly = storage, true
	}
	path := filepath.Join(t.TempDir(), "path")
	openPath := func(create, ro bool) (*pa.Archive, error) {
		if storage != nil {
			return pa.OpenWithArtifacts(path, cfg, create, ro, source)
		}
		return pa.Open(path, cfg, create, ro)
	}
	archive, e := openPath(true, false)
	if e != nil {
		t.Fatal(e)
	}
	pathTarget := conformanceTarget{name: "Path",
		initial: func(b block.InputBlock) (mpt.Hash, error) {
			r, e := archive.ExecuteBlock(context.Background(), b)
			return r.Root, e
		},
		importBlock: func(b block.InputBlock, trace block.LogicalBlock) (mpt.Hash, error) {
			r, e := archive.ApplyLogicalBlock(context.Background(), b, trace)
			return r.Root, e
		},
		query: func(req q.Request) (q.Response, error) { return archive.Query(context.Background(), req) },
		close: func() error { return archive.Close() },
		reopen: func() error {
			if e := archive.Audit(context.Background()); e != nil {
				return e
			}
			if e := archive.Close(); e != nil {
				return e
			}
			var e error
			archive, e = openPath(false, true)
			if e != nil {
				return e
			}
			return archive.PrepareQueryHeaders(context.Background())
		},
	}
	targets := []conformanceTarget{pathTarget}
	if storage != nil {
		info, e := archive.KVInfo()
		if e != nil || info.Engine != storage.Engine {
			t.Fatal("Path did not select shared engine", e)
		}
		ac := DefaultArchiveConfig(cfg.WorkloadID, storage.Engine)
		ac.Storage = *storage
		ap := filepath.Join(t.TempDir(), "archive")
		a, e := OpenArchive(ap, ac, true, false, source)
		if e != nil {
			t.Fatal(e)
		}
		if a.KVInfo().Engine != storage.Engine {
			t.Fatal("Archive did not select shared engine")
		}
		targets = append(targets, conformanceTarget{name: "Archive/" + string(storage.Engine),
			initial: func(b block.InputBlock) (mpt.Hash, error) {
				r, e := a.Execute(context.Background(), b)
				return r.Root, e
			},
			importBlock: func(b block.InputBlock, tr block.LogicalBlock) (mpt.Hash, error) {
				r, e := a.ApplyLogicalBlock(context.Background(), b, tr)
				return r.Root, e
			},
			query: func(r q.Request) (q.Response, error) { return a.Query(context.Background(), r) },
			close: func() error { return a.Close() },
			reopen: func() error {
				if e := a.Audit(context.Background()); e != nil {
					return e
				}
				if e := a.Close(); e != nil {
					return e
				}
				var e error
				a, e = OpenArchive(ap, ac, false, true, nil)
				return e
			},
		})
	}
	if extra != nil {
		targets = append(targets, extra(t))
	}
	for _, target := range targets {
		defer target.close()
	}

	store := referenceStore{mpt.NewMemoryStore()}
	var roots []mpt.Hash
	var inputs []block.InputBlock
	var previous baseline.Snapshot
	for i, snapshot := range fixture.States {
		b := block.InputBlock{Height: uint64(i), ID: common.BigToHash(big.NewInt(int64(i + 1))), Context: block.Context{Time: uint64(i), GasLimit: 1, BaseFee: new(big.Int), BlobBaseFee: big.NewInt(1)}}
		if i > 0 {
			b.ParentID = inputs[i-1].ID
		}
		want := mpt.Hash(common.HexToHash(oracle.Roots[codec.Ethereum][i]))
		if i == 0 {
			for _, a := range snapshot.Accounts {
				balance, _ := new(big.Int).SetString(a.Balance, 10)
				b.Allocations = append(b.Allocations, block.Allocation{Address: a.Address, Nonce: a.Nonce, Balance: balance, Storage: a.Storage})
			}
			root, e := baseline.Build(snapshot, codec.Ethereum, store)
			if e != nil || root != want {
				t.Fatal("Hash initialization disagrees with Geth", e)
			}
			for _, target := range targets {
				got, e := target.initial(b)
				if e != nil || got != want {
					t.Fatal(target.name, "initial root", e)
				}
			}
		} else {
			base, e := forest.OpenWithCodec(roots[i-1], store, codec.Ethereum)
			if e != nil {
				t.Fatal(e)
			}
			changes := fixtureChanges(t, previous, snapshot, base)
			trace, e := block.NewLogicalBlock(b, codec.Ethereum, want, changes, nil)
			if e != nil {
				t.Fatal(e)
			}
			id, wire, e := trace.Freeze(b, codec.Ethereum)
			if e != nil {
				t.Fatal(e)
			}
			trace, e = block.ParseLogicalBlock(wire, id, b, codec.Ethereum)
			if e != nil {
				t.Fatal(e)
			}
			bound, e := trace.ChangesFor(base)
			if e != nil {
				t.Fatal(e)
			}
			plan, e := base.Materialize(bound, 1<<20)
			if e != nil || plan.Root != want {
				t.Fatal("Hash imported root disagrees with Geth", e)
			}
			batch := store.NewBatch()
			for h, raw := range plan.Nodes {
				if e = batch.Put(h[:], raw); e != nil {
					t.Fatal(e)
				}
			}
			if e = batch.Write(); e != nil {
				t.Fatal(e)
			}
			for _, target := range targets {
				wrong := trace
				wrong.Root[0] ^= 1
				if _, e = target.importBlock(b, wrong); e == nil {
					t.Fatal(target.name, "published a wrong root")
				}
				got, e := target.importBlock(b, trace)
				if e != nil || got != want {
					t.Fatal(target.name, "imported root", i, e)
				}
			}
		}
		inputs, roots, previous = append(inputs, b), append(roots, want), snapshot
	}
	for _, target := range targets {
		if e = target.reopen(); e != nil {
			t.Fatal(target.name, "reopen/audit", e)
		}
	}
	// The codec is part of persisted identity; an old-codec opener must not
	// reinterpret this store or silently migrate its authenticated leaves.
	legacy := cfg
	legacy.Codec = ""
	if bad, e := pa.Open(path, legacy, false, true); e == nil {
		bad.Close()
		t.Fatal("wrong codec reopened native Path")
	}

	var proofCases []exportedProof
	queryCount := 0
	for i, root := range roots {
		view, e := forest.OpenWithCodec(root, store, codec.Ethereum)
		if e != nil {
			t.Fatal(e)
		}
		for _, address := range []common.Address{{19: 1}, {19: 2}, common.HexToAddress("0xdead")} {
			requests := []q.Request{{Height: uint64(i), Kind: q.Account, Address: address}, {Height: uint64(i), Kind: q.Code, Address: address}}
			for _, slot := range []common.Hash{{}, {31: 1}, {31: 2}, {31: 99}} {
				requests = append(requests, q.Request{Height: uint64(i), Kind: q.Storage, Address: address, Slot: slot})
			}
			for _, req := range requests {
				want := forestResponse(t, view, req).Compact()
				responses := []q.Response{want}
				for _, target := range targets {
					r, e := target.query(req)
					if e != nil {
						t.Fatal(target.name, "query", i, e)
					}
					responses = append(responses, r)
				}
				wantWire, _ := json.Marshal(want)
				for _, actual := range responses {
					if actual.ProofFormat != q.CompactProof {
						t.Fatal("not using shared compact proof")
					}
					if _, e = q.VerifyWithCodec(context.Background(), root, req, actual, q.DefaultLimits(), codec.Ethereum); e != nil {
						t.Fatal("proof verification", i, e)
					}
					gotWire, _ := json.Marshal(actual)
					if !bytes.Equal(gotWire, wantWire) {
						t.Fatal("proof/value wire differs between methods", i, req)
					}
					accountValue, _ := codec.Ethereum.EncodeAccount(state.EmptyAccount())
					if actual.Account != nil {
						accountValue, _ = codec.Ethereum.EncodeAccount(*actual.Account)
					} else {
						accountValue = nil
					}
					proofCases = append(proofCases, exportProof(root, state.AccountKey(address), actual.AccountProof, accountValue, actual.Account != nil))
					if req.Kind == q.Storage && actual.Account != nil {
						word, _ := codec.Ethereum.EncodeStorage(actual.Value)
						proofCases = append(proofCases, exportProof(actual.Account.StorageRoot, state.SlotKey(req.Slot), actual.StorageProof, word, actual.SlotExists))
					}
					queryCount++
				}
			}
		}
	}
	if report := os.Getenv("COMPARISON_PROOF_REPORT"); report != "" {
		raw, e := json.MarshalIndent(struct {
			Schema string
			Cases  []exportedProof
		}{"versioned-trie-proof-cases/v1", proofCases}, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(report, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
	t.Logf("methods=%d states=%d verified_responses=%d compact_proofs=%d", len(targets)+1, len(roots), queryCount, len(proofCases))
}

func fixtureChanges(t *testing.T, previous, next baseline.Snapshot, base *forest.View) map[common.Address]state.Change {
	t.Helper()
	prior := map[common.Address]baseline.Account{}
	for _, row := range previous.Accounts {
		prior[row.Address] = row
	}
	changes := map[common.Address]state.Change{}
	for _, row := range next.Accounts {
		a, exists, e := base.Account(row.Address)
		if e != nil {
			t.Fatal(e)
		}
		if !exists {
			a = state.EmptyAccount()
		}
		a.Nonce = row.Nonce
		if e = a.Balance.SetFromDecimal(row.Balance); e != nil {
			t.Fatal(e)
		}
		slots := map[common.Hash]common.Hash{}
		for k := range prior[row.Address].Storage {
			slots[k] = row.Storage[k]
		}
		for k, v := range row.Storage {
			slots[k] = v
		}
		changes[row.Address] = state.Change{Account: &a, Slots: slots, ResetStorage: !exists}
		delete(prior, row.Address)
	}
	for address := range prior {
		changes[address] = state.Change{}
	}
	return changes
}

func forestResponse(t *testing.T, view *forest.View, req q.Request) q.Response {
	t.Helper()
	r, e := q.Read(context.Background(), req, view)
	if e != nil {
		t.Fatal(e)
	}
	return r
}

func exportProof(root mpt.Hash, key []byte, p mpt.Proof, value []byte, exists bool) exportedProof {
	c := exportedProof{Root: hex.EncodeToString(root[:]), Key: hex.EncodeToString(key), Value: hex.EncodeToString(value), Exists: exists}
	for _, raw := range p {
		c.Proof = append(c.Proof, hex.EncodeToString(raw))
	}
	return c
}
