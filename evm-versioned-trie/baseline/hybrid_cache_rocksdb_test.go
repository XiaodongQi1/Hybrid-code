//go:build rocksdb && cgo

package baseline

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"strings"
	"testing"
)

// The bounded executor must reproduce the independently frozen Ethereum account
// roots, including the embedded storage-root values. This is not live EVM/forest
// integration: storage tries are built by the existing reference fixture builder.
func TestHybridCachedFrozenEthereumRoots(t *testing.T) {
	input, err := os.ReadFile("testdata/states.json")
	if err != nil {
		t.Fatal(err)
	}
	f, err := Parse(input)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := os.ReadFile("testdata/roots.json")
	if err != nil {
		t.Fatal(err)
	}
	var ref Reference
	if err = json.Unmarshal(encoded, &ref); err != nil {
		t.Fatal(err)
	}
	if ref.FixtureSHA256 != Digest(input) || len(ref.Roots[codec.Ethereum]) != len(f.States) {
		t.Fatal("unbound roots")
	}
	cfg := ht.DefaultConfig("ethereum-mpt", Digest(input))
	path := filepath.Join(t.TempDir(), "db")
	db, err := ht.Open(path, cfg, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { db.Close() }()
	empty, _ := mpt.NewEmpty(mpt.NewMemoryStore())
	if _, _, err = db.Commit(0, [32]byte{}, []ht.TrieUpdate{{Tree: empty}}); err != nil {
		t.Fatal(err)
	}
	cacheCfg := ht.DefaultCacheConfig()
	cacheCfg.NodeBytes, cacheCfg.HashBytes = 16<<10, 8<<10
	c, err := ht.OpenCachedTrie(db, 0, mpt.EmptyRootHash, cacheCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	addresses := map[common.Address]bool{}
	for _, s := range f.States {
		for _, a := range s.Accounts {
			addresses[a.Address] = true
		}
	}
	full := mpt.NewMemoryStore()
	var roots []mpt.Hash
	var expectedValues []map[string][]byte
	for i, s := range f.States {
		root, err := Build(s, codec.Ethereum, full)
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(root[:]) != strings.TrimPrefix(ref.Roots[codec.Ethereum][i], "0x") {
			t.Fatal("frozen root mismatch")
		}
		source, err := mpt.Open(root, full)
		if err != nil {
			t.Fatal(err)
		}
		b, err := c.BeginBlock(uint64(i + 1))
		if err != nil {
			t.Fatal(err)
		}
		values := map[string][]byte{}
		for address := range addresses {
			key := state.AccountKey(address)
			value, err := source.Get(key)
			if err != nil {
				t.Fatal(err)
			}
			if err = b.Update(key, value); err != nil {
				t.Fatal(err)
			}
			values[string(key)] = value
		}
		got, err := b.Root()
		if err != nil || got != root {
			t.Fatal("cached execution root", i, err)
		}
		if _, _, err = b.Commit(); err != nil {
			t.Fatal(err)
		}
		roots = append(roots, root)
		expectedValues = append(expectedValues, values)
	}
	c.Close()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = ht.Open(path, cfg, false, true)
	if err != nil {
		t.Fatal(err)
	}
	var cases []proofCase
	for i, root := range roots {
		view, err := db.ViewAt(uint64(i + 1))
		if err != nil {
			t.Fatal(err)
		}
		for address := range addresses {
			key := state.AccountKey(address)
			proof, err := view.Prove(0, root, key)
			if err != nil {
				t.Fatal(err)
			}
			value, exists, err := mpt.VerifyProof(root, key, proof)
			want := expectedValues[i][string(key)]
			if err != nil || !bytes.Equal(value, want) || exists != (len(want) != 0) {
				t.Fatal("cached historical proof", i, err)
			}
			cases = append(cases, captureProof(root, key, proof, value, exists))
		}
		view.Close()
	}
	if path := os.Getenv("HYBRID_CACHE_PROOF_REPORT"); path != "" {
		raw, err := json.MarshalIndent(struct {
			Schema string
			Cases  []proofCase
		}{"versioned-trie-proof-cases/v1", cases}, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("bounded_cache_frozen_roots=%d cold_proofs=%d", len(roots), len(cases))
}
