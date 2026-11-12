package baseline

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"strings"
	"testing"
)

// Uses the existing independently frozen upstream Geth account/storage roots.
// This validates the physical codec only; no hybrid database is implemented here.
func TestHybridCodecFrozenEthereumRoots(t *testing.T) {
	input, e := os.ReadFile("testdata/states.json")
	if e != nil {
		t.Fatal(e)
	}
	fixture, e := Parse(input)
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
	if ref.FixtureSHA256 != Digest(input) || !strings.Contains(ref.Generator, "go-ethereum v1.16.9") {
		t.Fatal("unbound oracle")
	}
	if len(ref.Roots[codec.Ethereum]) != len(fixture.States) {
		t.Fatal("oracle count")
	}
	for _, s := range fixture.States {
		t.Run(fmt.Sprint(s.Height), func(t *testing.T) {
			source := mpt.NewMemoryStore()
			root, e := Build(s, codec.Ethereum, source)
			if e != nil {
				t.Fatal(e)
			}
			want, e := hex.DecodeString(strings.TrimPrefix(ref.Roots[codec.Ethereum][s.Height], "0x"))
			if e != nil || !bytes.Equal(root[:], want) {
				t.Fatal("Geth root", root, e)
			}
			// Visit each hashed subtree, including contract storage roots reachable in
			// account values: all native records in this fixture store are roundtripped
			// as they occur along the account and per-contract storage trees below.
			checkHybridTree(t, source, root)
			for _, a := range s.Accounts {
				storage, _ := mpt.NewEmpty(source)
				for key, word := range a.Storage {
					value, e := codec.Ethereum.EncodeStorage(word)
					if e != nil {
						t.Fatal(e)
					}
					// The production codec uses the same secure slot key in Build.
					if e = storage.Update(state.SlotKey(key), value); e != nil {
						t.Fatal(e)
					}
				}
				storageRoot, e := storage.Commit()
				if e != nil {
					t.Fatal(e)
				}
				checkHybridTree(t, source, storageRoot)
			}
		})
	}
}

func checkHybridTree(t *testing.T, source mpt.Store, root mpt.Hash) {
	t.Helper()
	tree, e := mpt.Open(root, source)
	if e != nil {
		t.Fatal(e)
	}
	empty, _ := mpt.NewEmpty(source)
	nodes := map[string]mpt.LogicalChange{}
	_, e = tree.DiffLogicalNodes(empty, 10000, func(c mpt.LogicalChange) error { nodes[string(c.Path)] = c; return nil })
	if e != nil {
		t.Fatal(e)
	}
	for _, mask := range []uint16{0, 0xffff, 0xaaaa} {
		for _, c := range nodes {
			if len(c.Path) > 0 && len(c.RLP) < 32 {
				continue
			}
			desc, e := mpt.InspectCanonicalNode(c.RLP)
			if e != nil {
				t.Fatal(e)
			}
			var eligible uint16
			for i, r := range desc.Children {
				if r.Present && len(r.Inline) == 0 {
					eligible |= 1 << i
				}
			}
			ctx := hybridtrie.NodeKey{TrieID: 1, Path: c.Path, Height: hybridtrie.MaxHeight}
			r, e := hybridtrie.FromCanonical(ctx, c.RLP, mask&eligible)
			if e != nil {
				t.Fatal(e)
			}
			raw, e := hybridtrie.EncodeRecord(r)
			if e != nil {
				t.Fatal(e)
			}
			r, e = hybridtrie.DecodeRecord(raw)
			if e != nil {
				t.Fatal(e)
			}
			rebuilt, h, e := hybridtrie.RebuildCanonical(ctx, r, func(loc hybridtrie.ChildLocator) (mpt.Hash, error) {
				child, ok := nodes[string(loc.Target.Path)]
				if !ok {
					return mpt.Hash{}, fmt.Errorf("missing child")
				}
				return child.Hash, nil
			})
			if e != nil || h != c.Hash || !bytes.Equal(rebuilt, c.RLP) {
				t.Fatal("physical layout changed Geth node", e)
			}
		}
	}
}
