//go:build rocksdb && cgo

package baseline

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

// Export generic MPT short-key cases to upstream Geth as well as the frozen
// Ethereum account/storage fixtures. This exercises minimal inline proofs.
func TestHybridQueryInlineProofs(t *testing.T) {
	var cases []proofCase
	for _, size := range []int{1, 64} {
		cfg := ht.DefaultConfig("raw-bytes/v1", "query-inline")
		db, err := ht.Open(filepath.Join(t.TempDir(), "db"), cfg, true, false)
		if err != nil {
			t.Fatal(err)
		}
		tree, _ := mpt.NewEmpty(mpt.NewMemoryStore())
		for _, key := range [][]byte{{0x10}, {0x11}} {
			if err = tree.Update(key, bytes.Repeat([]byte{key[0]}, size)); err != nil {
				t.Fatal(err)
			}
		}
		root, _ := tree.Hash()
		if _, _, err = db.Commit(0, [32]byte{}, []ht.TrieUpdate{{Tree: tree, PrefixMask: 0xffff}}); err != nil {
			t.Fatal(err)
		}
		view, err := db.ViewAt(0)
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range [][]byte{{0x10}, {0x11}, {0x12}, {0x20}, {}} {
			proof, _, err := view.ProveWithStats(0, root, key)
			if err != nil {
				t.Fatal(err)
			}
			for i, raw := range proof {
				if i != 0 && len(raw) < 32 {
					t.Fatal("inline node emitted independently")
				}
			}
			value, exists, err := mpt.VerifyProof(root, key, proof)
			if err != nil {
				t.Fatal(err)
			}
			cases = append(cases, captureProof(root, key, proof, value, exists))
		}
		view.Close()
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if path := os.Getenv("HYBRID_QUERY_PROOF_REPORT"); path != "" {
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
	t.Logf("query_inline_proof_cases=%d", len(cases))
}
