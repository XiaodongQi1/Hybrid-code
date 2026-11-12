package baseline

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"os"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"strings"
	"testing"
)

func TestFrozenRootsAndHistoricalProofs(t *testing.T) {
	raw, e := os.ReadFile("testdata/states.json")
	if e != nil {
		t.Fatal(e)
	}
	f, e := Parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	refraw, e := os.ReadFile("testdata/roots.json")
	if e != nil {
		t.Fatal(e)
	}
	var ref Reference
	if e = json.Unmarshal(refraw, &ref); e != nil {
		t.Fatal(e)
	}
	if ref.Schema != "versioned-trie-reference/v1" || ref.FixtureSHA256 != Digest(raw) || !strings.Contains(ref.Generator, "go-ethereum v1.16.9") {
		t.Fatal("unbound reference")
	}
	ids := map[string]bool{}
	for _, mode := range []codec.Mode{codec.Legacy, codec.Ethereum} {
		t.Run(string(mode), func(t *testing.T) {
			id, e := Identity(raw, mode)
			if e != nil || ids[id] {
				t.Fatal("codec identity collision")
			}
			ids[id] = true
			store := mpt.NewMemoryStore()
			roots := make([]mpt.Hash, len(f.States))
			if len(ref.Roots[mode]) != len(roots) {
				t.Fatal("reference count")
			}
			for i, s := range f.States {
				roots[i], e = Build(s, mode, store)
				if e != nil {
					t.Fatal(e)
				}
				want, _ := hex.DecodeString(strings.TrimPrefix(ref.Roots[mode][i], "0x"))
				if !bytes.Equal(roots[i][:], want) {
					t.Fatalf("height %d got %x want %x", i, roots[i], want)
				}
				again, e := Build(s, mode, mpt.NewMemoryStore())
				if e != nil || again != roots[i] {
					t.Fatal("non-deterministic build")
				}
			}
			for i, s := range f.States {
				tree, e := mpt.Open(roots[i], store)
				if e != nil {
					t.Fatal(e)
				}
				for n := int64(1); n <= 3; n++ {
					address := common.BigToAddress(big.NewInt(n))
					var row *Account
					for j := range s.Accounts {
						if s.Accounts[j].Address == address {
							row = &s.Accounts[j]
						}
					}
					key := state.AccountKey(address)
					proof, e := tree.Prove(key)
					if e != nil {
						t.Fatal(e)
					}
					value, exists, e := mpt.VerifyProof(roots[i], key, proof)
					if e != nil || exists != (row != nil) {
						t.Fatal("account proof", i, e)
					}
					if !exists {
						continue
					}
					a, e := mode.DecodeAccount(value)
					if e != nil || a.Nonce != row.Nonce || a.Balance.Dec() != row.Balance {
						t.Fatal("account payload", i, e)
					}
					st, e := mpt.Open(a.StorageRoot, store)
					if e != nil {
						t.Fatal(e)
					}
					for k := int64(0); k < 4; k++ {
						slot := common.BigToHash(big.NewInt(k))
						key := state.SlotKey(slot)
						p, e := st.Prove(key)
						if e != nil {
							t.Fatal(e)
						}
						v, found, e := mpt.VerifyProof(a.StorageRoot, key, p)
						want := row.Storage[slot]
						if e != nil || found != (want != (common.Hash{})) {
							t.Fatal("slot proof", i, k, e)
						}
						word, e := mode.DecodeStorage(v)
						if e != nil || word != want {
							t.Fatal("slot payload", i, k, e)
						}
					}
				}
			}
		})
	}
	bad := bytes.Replace(raw, []byte("1000"), []byte("1001"), 1)
	id, _ := Identity(bad, codec.Legacy)
	old, _ := Identity(raw, codec.Legacy)
	if id == old {
		t.Fatal("input identity not bound")
	}
}
