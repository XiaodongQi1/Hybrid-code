package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/ethereum/go-ethereum/trie"
	"os"
)

// Independent proof verifier: no Statera implementation or decoder imports.
func verifyProofs(path string) error {
	raw, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	var bundle struct {
		Schema string
		Cases  []struct {
			Root, Key string
			Proof     []string
			Value     string
			Exists    bool
		}
	}
	if e = json.Unmarshal(raw, &bundle); e != nil {
		return e
	}
	if bundle.Schema != "versioned-trie-proof-cases/v1" || len(bundle.Cases) == 0 {
		return fmt.Errorf("invalid proof bundle")
	}
	for i, c := range bundle.Cases {
		r, e := hex.DecodeString(c.Root)
		if e != nil || len(r) != 32 {
			return fmt.Errorf("bad root %d", i)
		}
		root := common.BytesToHash(r)
		key, e := hex.DecodeString(c.Key)
		if e != nil {
			return e
		}
		want, e := hex.DecodeString(c.Value)
		if e != nil {
			return e
		}
		if root == types.EmptyRootHash {
			if c.Exists || len(want) != 0 || len(c.Proof) != 0 {
				return fmt.Errorf("bad empty proof %d", i)
			}
			continue
		}
		db := memorydb.New()
		for _, item := range c.Proof {
			b, e := hex.DecodeString(item)
			if e != nil {
				db.Close()
				return e
			}
			if e = db.Put(crypto.Keccak256(b), b); e != nil {
				db.Close()
				return e
			}
		}
		got, e := trie.VerifyProof(root, key, db)
		db.Close()
		if e != nil {
			return fmt.Errorf("proof %d: %w", i, e)
		}
		if (got != nil) != c.Exists || !bytes.Equal(got, want) {
			return fmt.Errorf("proof %d: answer mismatch", i)
		}
	}
	fmt.Printf("upstream Geth v1.16.9 verified %d proof cases\n", len(bundle.Cases))
	return nil
}
