package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"math/big"
	"os"
)

// Rebuild roots with upstream StackTrie and RLP from the independent map-state
// execution values. Never import the local trie, backend, codec or StateDB.
func verifyStates(path string) error {
	raw, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	var bundle struct {
		Schema string
		Cases  []struct {
			Mode, Root string
			Accounts   []struct {
				Address  common.Address
				Balance  string
				CodeHash common.Hash
				Nonce    uint64
				Storage  map[common.Hash]common.Hash
			}
		}
	}
	if e = json.Unmarshal(raw, &bundle); e != nil {
		return e
	}
	if bundle.Schema != "versioned-evm-state-cases/v1" || len(bundle.Cases) == 0 {
		return fmt.Errorf("invalid state bundle")
	}
	for i, c := range bundle.Cases {
		if c.Mode != "legacy-raw105" && c.Mode != "ethereum-mpt" {
			return fmt.Errorf("unknown codec")
		}
		var accounts []kv
		for _, a := range c.Accounts {
			var slots []kv
			for k, v := range a.Storage {
				if v == (common.Hash{}) {
					return fmt.Errorf("noncanonical zero slot")
				}
				b := v[:]
				if c.Mode == "ethereum-mpt" {
					b = enc(bytes.TrimLeft(b, "\x00"))
				}
				slots = append(slots, kv{crypto.Keccak256(k[:]), b})
			}
			storageRoot := root(slots)
			balance, ok := new(big.Int).SetString(a.Balance, 10)
			if !ok || balance.Sign() < 0 || balance.BitLen() > 256 {
				return fmt.Errorf("invalid balance")
			}
			var value []byte
			if c.Mode == "legacy-raw105" {
				value = make([]byte, 105)
				value[0] = 1
				binary.BigEndian.PutUint64(value[1:9], a.Nonce)
				balance.FillBytes(value[9:41])
				copy(value[41:73], storageRoot[:])
				copy(value[73:], a.CodeHash[:])
			} else {
				value = enc(struct {
					Nonce      uint64
					Balance    *big.Int
					Root, Code common.Hash
				}{a.Nonce, balance, storageRoot, a.CodeHash})
			}
			accounts = append(accounts, kv{crypto.Keccak256(a.Address[:]), value})
		}
		got := root(accounts)
		if fmt.Sprintf("%x", got[:]) != c.Root {
			return fmt.Errorf("state root mismatch in case %d", i)
		}
	}
	fmt.Printf("upstream Geth v1.16.9 rebuilt %d live-EVM state roots\n", len(bundle.Cases))
	return nil
}
