// Independent fixture generator. No imports of Statera's MPT, codec or baseline.
// Run from this directory: go run . > ../baseline/testdata/roots.json
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"math/big"
	"os"
	"sort"
)

type account struct {
	Address common.Address
	Nonce   uint64
	Balance string
	Storage map[common.Hash]common.Hash
}
type fixture struct {
	States []struct {
		Height   uint32
		Accounts []account
	}
}
type kv struct{ k, v []byte }

func root(rows []kv) common.Hash {
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i].k, rows[j].k) < 0 })
	t := trie.NewStackTrie(nil)
	for _, r := range rows {
		if e := t.Update(r.k, r.v); e != nil {
			panic(e)
		}
	}
	return t.Hash()
}
func enc(v interface{}) []byte {
	b, e := rlp.EncodeToBytes(v)
	if e != nil {
		panic(e)
	}
	return b
}
func main() {
	if len(os.Args) > 1 {
		if len(os.Args) != 3 {
			panic("usage: verify-proofs FILE | verify-states FILE")
		}
		var e error
		switch os.Args[1] {
		case "verify-proofs":
			e = verifyProofs(os.Args[2])
		case "verify-states":
			e = verifyStates(os.Args[2])
		default:
			panic("usage: verify-proofs FILE | verify-states FILE")
		}
		if e != nil {
			panic(e)
		}
		return
	}
	raw, e := os.ReadFile("../baseline/testdata/states.json")
	if e != nil {
		panic(e)
	}
	var f fixture
	if e = json.Unmarshal(raw, &f); e != nil {
		panic(e)
	}
	sum := sha256.Sum256(raw)
	out := struct {
		Schema, Generator, FixtureSHA256 string
		Roots                            map[string][]string
	}{"versioned-trie-reference/v1", "go-ethereum v1.16.9 StackTrie + upstream RLP (independent offline generator)", hex.EncodeToString(sum[:]), map[string][]string{}}
	for _, mode := range []string{"legacy-raw105", "ethereum-mpt"} {
		for _, s := range f.States {
			var accounts []kv
			for _, a := range s.Accounts {
				var slots []kv
				for k, w := range a.Storage {
					if w == (common.Hash{}) {
						continue
					}
					v := bytes.Clone(w[:])
					if mode == "ethereum-mpt" {
						v = enc(bytes.TrimLeft(v, "\x00"))
					}
					slots = append(slots, kv{crypto.Keccak256(k[:]), v})
				}
				sr := root(slots)
				balance, ok := new(big.Int).SetString(a.Balance, 10)
				if !ok || balance.Sign() < 0 || balance.BitLen() > 256 {
					panic("bad balance")
				}
				var v []byte
				if mode == "ethereum-mpt" {
					v = enc([]interface{}{a.Nonce, balance, sr, types.EmptyCodeHash})
				} else {
					v = make([]byte, 105)
					v[0] = 1
					binary.BigEndian.PutUint64(v[1:9], a.Nonce)
					balance.FillBytes(v[9:41])
					copy(v[41:73], sr[:])
					copy(v[73:], types.EmptyCodeHash[:])
				}
				accounts = append(accounts, kv{crypto.Keccak256(a.Address[:]), v})
			}
			out.Roots[mode] = append(out.Roots[mode], root(accounts).Hex())
		}
	}
	b, e := json.MarshalIndent(out, "", "  ")
	if e != nil {
		panic(e)
	}
	fmt.Println(string(b))
}
