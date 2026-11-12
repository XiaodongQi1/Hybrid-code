//go:build rocksdb && cgo

package baseline

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"math/big"
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

func TestHybridRocksDBFrozenRootsAndProofs(t *testing.T) {
	input, e := os.ReadFile("testdata/states.json")
	if e != nil {
		t.Fatal(e)
	}
	f, e := Parse(input)
	if e != nil {
		t.Fatal(e)
	}
	reference, e := os.ReadFile("testdata/roots.json")
	if e != nil {
		t.Fatal(e)
	}
	var ref Reference
	if e = json.Unmarshal(reference, &ref); e != nil {
		t.Fatal(e)
	}
	if ref.FixtureSHA256 != Digest(input) || len(ref.Roots[codec.Ethereum]) != len(f.States) {
		t.Fatal("unbound roots")
	}
	cfg := ht.DefaultConfig("ethereum-mpt", Digest(input))
	path := filepath.Join(t.TempDir(), "db")
	db, e := ht.Open(path, cfg, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { db.Close() }()
	full := mpt.NewMemoryStore()
	var head ht.Head
	var roots []mpt.Hash
	active := map[common.Address]bool{}
	var firstStorageID, reinsertedStorageID uint64
	for i, s := range f.States {
		root, e := Build(s, codec.Ethereum, full)
		if e != nil {
			t.Fatal(e)
		}
		if hex.EncodeToString(root[:]) != strings.TrimPrefix(ref.Roots[codec.Ethereum][i], "0x") {
			t.Fatal("Geth root mismatch")
		}
		account, e := mpt.Open(root, full)
		if e != nil {
			t.Fatal(e)
		}
		mask := []uint16{0xffff, 0, 0xaaaa}[i%3]
		plan, e := db.BeginForest(uint64(i), head.CommitID)
		if e != nil {
			t.Fatal(e)
		}
		if e = plan.SetTrie(ht.AccountTrieID, account, mask); e != nil {
			t.Fatal(e)
		}
		present := map[common.Address]bool{}
		for _, row := range s.Accounts {
			present[row.Address] = true
		}
		for address := range active {
			if !present[address] {
				if e = plan.DeleteStorageTrie(address); e != nil {
					t.Fatal(e)
				}
				delete(active, address)
			}
		}
		for _, row := range s.Accounts {
			payload, e := account.Get(state.AccountKey(row.Address))
			if e != nil {
				t.Fatal(e)
			}
			a, e := codec.Ethereum.DecodeAccount(payload)
			if e != nil {
				t.Fatal(e)
			}
			id, exists, e := plan.StorageTrieID(row.Address)
			if e != nil {
				t.Fatal(e)
			}
			// An EOA with no storage needs no mapping. Existing storage instances
			// keep their IDs when their contents become empty.
			if a.StorageRoot == mpt.EmptyRootHash && !exists {
				continue
			}
			if !exists {
				id, e = plan.CreateStorageTrie(row.Address)
				if e != nil {
					t.Fatal(e)
				}
			}
			active[row.Address] = true
			if row.Address == common.BigToAddress(big.NewInt(2)) {
				if i == 0 {
					firstStorageID = id
				} else if i == 4 {
					reinsertedStorageID = id
				}
			}
			storage, e := mpt.Open(a.StorageRoot, full)
			if e != nil {
				t.Fatal(e)
			}
			if e = plan.SetTrie(id, storage, mask); e != nil {
				t.Fatal(e)
			}
		}
		head, _, e = plan.Commit()
		if e != nil {
			t.Fatal(i, e)
		}
		roots = append(roots, root)
	}
	if firstStorageID == 0 || reinsertedStorageID <= firstStorageID {
		t.Fatal("deleted/recreated storage reused namespace", firstStorageID, reinsertedStorageID)
	}
	if e = db.Close(); e != nil {
		t.Fatal(e)
	}
	db, e = ht.Open(path, cfg, false, true)
	if e != nil {
		t.Fatal(e)
	}
	var cases []proofCase
	for i, s := range f.States {
		v, e := db.ViewAt(uint64(i))
		if e != nil {
			t.Fatal(e)
		}
		for n := int64(1); n <= 4; n++ {
			address := common.BigToAddress(big.NewInt(n))
			id, active, e := v.StorageTrieID(address)
			if e != nil {
				t.Fatal(e)
			}
			if n == 1 && active || n == 2 && i == 3 && active {
				t.Fatal("EOA or deleted contract has active storage mapping", i, n, id)
			}
			var row *Account
			for j := range s.Accounts {
				if s.Accounts[j].Address == address {
					row = &s.Accounts[j]
				}
			}
			key := state.AccountKey(address)
			proof, e := v.Prove(0, roots[i], key)
			if e != nil {
				t.Fatal(e)
			}
			payload, exists, e := mpt.VerifyProof(roots[i], key, proof)
			if e != nil || exists != (row != nil) {
				t.Fatal("account proof", e)
			}
			cases = append(cases, captureProof(roots[i], key, proof, payload, exists))
			if !exists {
				continue
			}
			a, e := codec.Ethereum.DecodeAccount(payload)
			if e != nil {
				t.Fatal(e)
			}
			for k := int64(0); k < 4; k++ {
				slot := common.BigToHash(big.NewInt(k))
				slotKey := state.SlotKey(slot)
				var proof [][]byte
				if a.StorageRoot != mpt.EmptyRootHash {
					if !active {
						t.Fatal("nonempty storage has no lifecycle mapping", i, address)
					}
					proof, e = v.Prove(id, a.StorageRoot, slotKey)
					if e != nil {
						t.Fatal(e)
					}
				}
				payload, exists, e := mpt.VerifyProof(a.StorageRoot, slotKey, proof)
				if e != nil {
					t.Fatal(e)
				}
				want, e := codec.Ethereum.EncodeStorage(row.Storage[slot])
				if e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(payload, want) || exists != (len(want) != 0) {
					t.Fatal("storage proof mismatch")
				}
				cases = append(cases, captureProof(a.StorageRoot, slotKey, proof, payload, exists))
			}
		}
		v.Close()
	}
	t.Logf("independent_roots=%d cold_rocksdb_proofs=%d", len(roots), len(cases))
	if path := os.Getenv("HYBRID_PROOF_REPORT"); path != "" {
		raw, e := json.MarshalIndent(struct {
			Schema string
			Cases  []proofCase
		}{"versioned-trie-proof-cases/v1", cases}, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, raw, 0600); e != nil {
			t.Fatal(e)
		}
	}
}
