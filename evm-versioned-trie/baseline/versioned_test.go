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
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
	"strings"
	"testing"
)

type proofCase struct {
	Root, Key string
	Proof     []string
	Value     string
	Exists    bool
}

func captureProof(root mpt.Hash, key []byte, p mpt.Proof, value []byte, exists bool) proofCase {
	c := proofCase{Root: hex.EncodeToString(root[:]), Key: hex.EncodeToString(key), Value: hex.EncodeToString(value), Exists: exists}
	for _, raw := range p {
		c.Proof = append(c.Proof, hex.EncodeToString(raw))
	}
	return c
}
func ownerKey(a common.Address) (out [32]byte) { copy(out[:], state.AccountKey(a)); return }
func TestVersionedFrozenAccountStorageRootsAndProofs(t *testing.T) {
	raw, e := os.ReadFile("testdata/states.json")
	if e != nil {
		t.Fatal(e)
	}
	f, e := Parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	refRaw, e := os.ReadFile("testdata/roots.json")
	if e != nil {
		t.Fatal(e)
	}
	var ref Reference
	if e = json.Unmarshal(refRaw, &ref); e != nil {
		t.Fatal(e)
	}
	if ref.Schema != "versioned-trie-reference/v1" || ref.FixtureSHA256 != Digest(raw) {
		t.Fatal("unbound reference fixture")
	}
	var cases []proofCase
	rootChecks := 0
	rlpChecks := 0
	for _, mode := range []codec.Mode{codec.Legacy, codec.Ethereum} {
		t.Run(string(mode), func(t *testing.T) {
			if len(ref.Roots[mode]) != len(f.States) {
				t.Fatal("reference root count")
			}
			dir := t.TempDir() + "/db"
			cfg := vt.DefaultConfig(string(mode), Digest(raw))
			db, e := vt.Open(dir, cfg, true, false)
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			full := mpt.NewMemoryStore()
			before := mpt.EmptyRootHash
			oldStorage := map[common.Address]mpt.Hash{}
			ids := map[common.Address]uint32{}
			var roots []mpt.Hash
			for i, s := range f.States {
				root, e := Build(s, mode, full)
				if e != nil {
					t.Fatal(e)
				}
				if hex.EncodeToString(root[:]) != strings.TrimPrefix(ref.Roots[mode][i], "0x") {
					t.Fatal("independent root mismatch")
				}
				rootChecks++
				at, e := mpt.Open(root, full)
				if e != nil {
					t.Fatal(e)
				}
				u, e := db.Begin(uint64(i), root, [32]byte{byte(i + 1)})
				if e != nil {
					t.Fatal(e)
				}
				present := map[common.Address]bool{}
				for _, row := range s.Accounts {
					present[row.Address] = true
				}
				for addr := range ids {
					if !present[addr] {
						if e = u.PutLifecycle(ownerKey(addr), vt.Lifecycle{}); e != nil {
							t.Fatal(e)
						}
						delete(ids, addr)
						delete(oldStorage, addr)
					}
				}
				for _, row := range s.Accounts {
					payload, e := at.Get(state.AccountKey(row.Address))
					if e != nil {
						t.Fatal(e)
					}
					account, e := mode.DecodeAccount(payload)
					if e != nil {
						t.Fatal(e)
					}
					q, has := ids[row.Address]
					if !has && account.StorageRoot == mpt.EmptyRootHash {
						continue
					}
					previous := oldStorage[row.Address]
					if !has {
						q, e = u.AllocateTrieID()
						if e != nil {
							t.Fatal(e)
						}
						ids[row.Address] = q
						previous = mpt.EmptyRootHash
						if e = u.PutLifecycle(ownerKey(row.Address), vt.Lifecycle{Active: true, TrieID: q}); e != nil {
							t.Fatal(e)
						}
					}
					tree, e := mpt.Open(account.StorageRoot, full)
					if e != nil {
						t.Fatal(e)
					}
					if _, e = u.ProjectTrie(uint64(q), previous, tree); e != nil {
						t.Fatal(i, e)
					}
					oldStorage[row.Address] = account.StorageRoot
				}
				if _, e = u.ProjectTrie(0, before, at); e != nil {
					t.Fatal(i, e)
				}
				if e = u.Commit(); e != nil {
					t.Fatal(e)
				}
				roots = append(roots, root)
				before = root
			}
			if e = db.Close(); e != nil {
				t.Fatal(e)
			}
			db, e = vt.Open(dir, cfg, false, true)
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			for i, s := range f.States {
				v, e := db.ViewAt(uint64(i))
				if e != nil {
					t.Fatal(e)
				}
				reader, e := v.TrieReader(0)
				if e != nil {
					t.Fatal(e)
				}
				at, e := mpt.Open(roots[i], reader)
				if e != nil {
					t.Fatal(e)
				}
				checkRLP := func(root mpt.Hash, reader *vt.TrieReader) {
					e := mpt.WalkPaths(root, full, func(p []byte, h mpt.Hash, raw []byte) (bool, error) {
						got, e := reader.GetNode(p, h)
						if e != nil || !bytes.Equal(got, raw) {
							t.Fatal("canonical RLP mismatch", i, p, e)
						}
						rlpChecks++
						return true, nil
					}, nil)
					if e != nil {
						t.Fatal(e)
					}
				}
				checkRLP(roots[i], reader)
				for n := int64(1); n <= 3; n++ {
					addr := common.BigToAddress(big.NewInt(n))
					var row *Account
					for j := range s.Accounts {
						if s.Accounts[j].Address == addr {
							row = &s.Accounts[j]
						}
					}
					key := state.AccountKey(addr)
					p, e := at.Prove(key)
					if e != nil {
						t.Fatal(e)
					}
					payload, exists, e := mpt.VerifyProof(roots[i], key, p)
					if e != nil || exists != (row != nil) {
						t.Fatal("account proof", i, e)
					}
					cases = append(cases, captureProof(roots[i], key, p, payload, exists))
					if !exists {
						continue
					}
					account, e := mode.DecodeAccount(payload)
					if e != nil || account.Nonce != row.Nonce || account.Balance.Dec() != row.Balance {
						t.Fatal("payload", i, e)
					}
					var st *mpt.Trie
					if account.StorageRoot == mpt.EmptyRootHash {
						st, _ = mpt.NewEmpty(mpt.NewMemoryStore())
					} else {
						_, life, e := v.Lifecycle(ownerKey(addr))
						if e != nil || !life.Active {
							t.Fatal("lifecycle", i, e)
						}
						storageReader, e := v.TrieReader(uint64(life.TrieID))
						if e != nil {
							t.Fatal(e)
						}
						st, e = mpt.Open(account.StorageRoot, storageReader)
						if e != nil {
							t.Fatal(e)
						}
						checkRLP(account.StorageRoot, storageReader)
					}
					for k := int64(0); k < 4; k++ {
						slot := common.BigToHash(big.NewInt(k))
						slotKey := state.SlotKey(slot)
						p, e := st.Prove(slotKey)
						if e != nil {
							t.Fatal(e)
						}
						payload, exists, e := mpt.VerifyProof(account.StorageRoot, slotKey, p)
						if e != nil || exists != (row.Storage[slot] != (common.Hash{})) {
							t.Fatal("slot proof", i, k, e)
						}
						word, e := mode.DecodeStorage(payload)
						if e != nil || word != row.Storage[slot] {
							t.Fatal("slot value", i, k, e)
						}
						cases = append(cases, captureProof(account.StorageRoot, slotKey, p, payload, exists))
					}
				}
				v.Close()
			}
		})
	}
	if t.Failed() {
		return
	}
	t.Logf("independent_roots=%d canonical_rlp_comparisons=%d proof_cases=%d", rootChecks, rlpChecks, len(cases))
	if path := os.Getenv("VT_PROOF_REPORT"); path != "" {
		wire, e := json.MarshalIndent(struct {
			Schema string
			Cases  []proofCase
		}{"versioned-trie-proof-cases/v1", cases}, "", "  ")
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, append(wire, '\n'), 0600); e != nil {
			t.Fatal(e)
		}
	}
}
