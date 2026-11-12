// Package baseline freezes a small, storage-independent logical state workload.
package baseline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/codec"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

const BaseCommit = "1c7114c58dabcf50ed0c408a8512888f68848524"

type Account struct {
	Address common.Address
	Nonce   uint64
	Balance string
	Storage map[common.Hash]common.Hash
}
type Snapshot struct {
	Height   uint32
	Accounts []Account
}
type Fixture struct {
	Schema string
	Note   string
	States []Snapshot
}
type Reference struct {
	Schema        string
	Generator     string
	FixtureSHA256 string
	Roots         map[codec.Mode][]string
}

func Parse(b []byte) (Fixture, error) {
	var f Fixture
	e := json.Unmarshal(b, &f)
	if e != nil {
		return f, e
	}
	if f.Schema != "versioned-trie-baseline/v1" || len(f.States) == 0 {
		return f, fmt.Errorf("bad fixture")
	}
	for i, s := range f.States {
		if s.Height != uint32(i) {
			return f, fmt.Errorf("noncontinuous fixture")
		}
		seen := map[common.Address]bool{}
		for _, a := range s.Accounts {
			if seen[a.Address] {
				return f, fmt.Errorf("duplicate account")
			}
			seen[a.Address] = true
		}
	}
	return f, nil
}
func Digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func Identity(b []byte, m codec.Mode) (string, error) {
	if e := m.Validate(); e != nil {
		return "", e
	}
	return Digest(append([]byte("versioned-trie-baseline/v1\x00"+m+"\x00"), b...)), nil
}

// Build stores full canonical nodes as the phase-1 reference, not the new backend.
func Build(s Snapshot, m codec.Mode, store mpt.Store) (mpt.Hash, error) {
	if e := m.Validate(); e != nil {
		return mpt.Hash{}, e
	}
	at, e := mpt.NewEmpty(store)
	if e != nil {
		return mpt.Hash{}, e
	}
	for _, row := range s.Accounts {
		st, e := mpt.NewEmpty(store)
		if e != nil {
			return mpt.Hash{}, e
		}
		for key, word := range row.Storage {
			b, e := m.EncodeStorage(word)
			if e != nil {
				return mpt.Hash{}, e
			}
			if e = st.Update(state.SlotKey(key), b); e != nil {
				return mpt.Hash{}, e
			}
		}
		root, e := st.Commit()
		if e != nil {
			return mpt.Hash{}, e
		}
		a := state.EmptyAccount()
		a.Nonce = row.Nonce
		a.StorageRoot = root
		if e = a.Balance.SetFromDecimal(row.Balance); e != nil {
			return mpt.Hash{}, e
		}
		b, e := m.EncodeAccount(a)
		if e != nil {
			return mpt.Hash{}, e
		}
		if e = at.Update(state.AccountKey(row.Address), b); e != nil {
			return mpt.Hash{}, e
		}
	}
	return at.Commit()
}
