// Package snapshot binds a reusable canonical Hash forest to its original
// address-ordered EVM account source. Construction data stays outside state DBs.
package snapshot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
	"github.com/XiaodongQi1/HybridTrie/evm-state/forest"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type Config struct {
	SourceID                         string
	Header                           block.InputBlock
	BatchBytes                       uint64
	ChunkEntries, CheckpointAccounts uint32
	Storage                          *kvstore.Options
}
type Provenance struct {
	Schema string
	Codec  codec.Mode
	Header block.InputBlock
}
type Manifest struct {
	Schema, WorkloadID   string
	Codec                codec.Mode
	Header               block.InputBlock
	Root                 mpt.Hash
	Accounts, NextTrieID uint64
	InputDigest          [32]byte
}

func ValidateHeader(b block.InputBlock) error {
	if b.ID == (common.Hash{}) || b.ParentID != (common.Hash{}) || b.Height > ht.MaxHeight || len(b.Allocations) != 0 || len(b.Transactions) != 0 || b.Context.GasLimit == 0 || b.Context.BaseFee == nil || b.Context.BlobBaseFee == nil || b.Context.BaseFee.Sign() < 0 || b.Context.BlobBaseFee.Sign() < 0 || b.Context.BaseFee.BitLen() > 256 || b.Context.BlobBaseFee.BitLen() > 256 {
		return state.ErrCorrupt
	}
	return nil
}
func BuildConfig(c Config) (ht.SnapshotBuildConfig, error) {
	if e := ValidateHeader(c.Header); e != nil {
		return ht.SnapshotBuildConfig{}, e
	}
	if c.CheckpointAccounts == 0 || c.CheckpointAccounts > 65536 {
		return ht.SnapshotBuildConfig{}, ht.ErrLimit
	}
	raw, e := json.Marshal(Provenance{"evm-snapshot-input/v1", codec.Ethereum, c.Header})
	if e != nil {
		return ht.SnapshotBuildConfig{}, e
	}
	return ht.SnapshotBuildConfig{SourceID: c.SourceID, Height: c.Header.Height, BlockID: [32]byte(c.Header.ID), BatchBytes: c.BatchBytes, ChunkEntries: c.ChunkEntries, Storage: c.Storage, RecordAccounts: true, Provenance: raw}, nil
}

// Build reuses bounded MPT construction. A resumed callback starts at row zero
// and must supply the same immutable source. nil accepts an already ready DB.
// On error the owned scratch remains on disk; no target Head has been published.
func Build(ctx context.Context, path string, c Config, next func() (block.Allocation, error)) (_ *ht.SnapshotBuilder, err error) {
	cfg, e := BuildConfig(c)
	if e != nil {
		return nil, e
	}
	if e = ctx.Err(); e != nil {
		return nil, e
	}
	b, e := ht.OpenSnapshotBuilder(path, cfg)
	if e != nil {
		return nil, e
	}
	defer func() {
		if err != nil {
			b.Close()
		}
	}()
	p := b.Progress()
	if p.Ready && next == nil {
		return b, nil
	}
	if next == nil {
		return nil, fmt.Errorf("snapshot callback required")
	}
	var cursor uint64
	var digest [32]byte
	var last common.Address
	nextID := p.NextTrieID
	for {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		a, e := next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if a.Balance == nil || a.Balance.Sign() < 0 || a.Balance.BitLen() > 256 || cursor > 0 && bytes.Compare(a.Address[:], last[:]) <= 0 {
			return nil, state.ErrCorrupt
		}
		raw, e := json.Marshal(a)
		if e != nil {
			return nil, e
		}
		h := sha256.New()
		h.Write(digest[:])
		h.Write(raw)
		copy(digest[:], h.Sum(nil))
		cursor++
		last = a.Address
		if cursor <= p.Cursor {
			if cursor == p.Cursor && (digest != p.Digest || [20]byte(last) != p.LastAddress) {
				return nil, ht.ErrConflict
			}
			continue
		}
		if p.Ready {
			return nil, ht.ErrConflict
		}
		value := state.EmptyAccount()
		value.Nonce = a.Nonce
		value.Balance.SetFromBig(a.Balance)
		value.CodeHash = crypto.Keccak256Hash(a.Code)
		keys := make([]common.Hash, 0, len(a.Storage))
		for k, v := range a.Storage {
			if v != (common.Hash{}) {
				keys = append(keys, k)
			}
		}
		sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
		if len(a.Code) > 0 || len(keys) > 0 {
			id := nextID
			nextID++
			if id > ht.MaxTrieID {
				return nil, ht.ErrLimit
			}
			if e = b.BindStorage(id, [20]byte(a.Address)); e != nil {
				return nil, e
			}
			for _, k := range keys {
				encoded, e := codec.Ethereum.EncodeStorage(a.Storage[k])
				if e != nil {
					return nil, e
				}
				if e = b.Put(id, state.SlotKey(k), encoded); e != nil {
					return nil, e
				}
			}
			value.StorageRoot, e = b.Root(id)
			if e != nil {
				return nil, e
			}
		}
		if e = b.PutCode(mpt.Hash(value.CodeHash), a.Code); e != nil {
			return nil, e
		}
		encoded, e := codec.Ethereum.EncodeAccount(value)
		if e != nil {
			return nil, e
		}
		if e = b.Put(0, state.AccountKey(a.Address), encoded); e != nil {
			return nil, e
		}
		if e = b.RecordAccount([20]byte(a.Address), raw); e != nil {
			return nil, e
		}
		if cursor%uint64(c.CheckpointAccounts) == 0 {
			if e = b.Save(ht.SnapshotProgress{Cursor: cursor, NextTrieID: nextID, Digest: digest, LastAddress: [20]byte(last)}); e != nil {
				return nil, e
			}
		}
	}
	if cursor < p.Cursor {
		return nil, ht.ErrConflict
	}
	if !p.Ready {
		if e = b.Finish(ht.SnapshotProgress{Cursor: cursor, NextTrieID: nextID, Digest: digest, LastAddress: [20]byte(last)}); e != nil {
			return nil, e
		}
	}
	return b, nil
}

type Reader struct{ Source *ht.SnapshotSource }

func (r Reader) Get(k []byte) ([]byte, error)       { return r.Source.Get(k) }
func (r Reader) Code(h common.Hash) ([]byte, error) { return r.Source.Code(mpt.Hash(h)) }
func (r Reader) NewBatch() mpt.Batch                { return r.Source.NewBatch() }
func Describe(s *ht.SnapshotSource, header block.InputBlock) (Manifest, error) {
	var out Manifest
	if s == nil {
		return out, ht.ErrConflict
	}
	if e := ValidateHeader(header); e != nil {
		return out, e
	}
	info := s.Info()
	var p Provenance
	if !info.RecordAccounts || json.Unmarshal(info.Provenance, &p) != nil || p.Schema != "evm-snapshot-input/v1" || p.Codec != codec.Ethereum || info.Height != header.Height || info.BlockID != [32]byte(header.ID) {
		return out, state.ErrCorrupt
	}
	a, _ := json.Marshal(p.Header)
	b, _ := json.Marshal(header)
	if !bytes.Equal(a, b) {
		return out, state.ErrCorrupt
	}
	return Manifest{"evm-hash-snapshot/v1", info.SourceID, codec.Ethereum, header, info.Root, info.Accounts, info.NextTrieID, info.InputDigest}, nil
}

// Verify binds the account source to the canonical forest, including absence of
// extra storage slots. It retains at most one source account and O(trie depth).
func Verify(ctx context.Context, s *ht.SnapshotSource, header block.InputBlock) (Manifest, error) {
	m, e := Describe(s, header)
	if e != nil {
		return m, e
	}
	view, e := forest.OpenWithCodec(m.Root, Reader{s}, codec.Ethereum)
	if e != nil {
		return m, e
	}
	var count, contracts uint64
	var digest [32]byte
	e = s.WalkAccounts(ctx, func(address [20]byte, raw []byte) error {
		var a block.Allocation
		if json.Unmarshal(raw, &a) != nil || [20]byte(a.Address) != address || a.Balance == nil || a.Balance.Sign() < 0 || a.Balance.BitLen() > 256 {
			return state.ErrCorrupt
		}
		canonical, e := json.Marshal(a)
		if e != nil || !bytes.Equal(canonical, raw) {
			return state.ErrCorrupt
		}
		h := sha256.New()
		h.Write(digest[:])
		h.Write(raw)
		copy(digest[:], h.Sum(nil))
		count++
		actual, exists, e := view.Account(a.Address)
		if e != nil {
			return e
		}
		if !exists || actual.Nonce != a.Nonce || actual.Balance.ToBig().Cmp(a.Balance) != 0 || actual.CodeHash != crypto.Keccak256Hash(a.Code) {
			return state.ErrCorrupt
		}
		code, e := view.Code(actual.CodeHash)
		if e != nil || !bytes.Equal(code, a.Code) {
			return state.ErrCorrupt
		}
		var slots uint64
		wanted := map[common.Hash]common.Hash{}
		for key, value := range a.Storage {
			if value != (common.Hash{}) {
				wanted[common.BytesToHash(state.SlotKey(key))] = value
			}
		}
		e = mpt.WalkPaths(actual.StorageRoot, Reader{s}, nil, func(key, value []byte) error {
			if e := ctx.Err(); e != nil {
				return e
			}
			word, e := codec.Ethereum.DecodeStorage(value)
			if e != nil {
				return e
			}
			// The original slot preimages live only in this one account record.
			want, found := wanted[common.BytesToHash(key)]
			if len(key) != 32 || !found || word != want {
				return state.ErrCorrupt
			}
			slots++
			return nil
		})
		if e != nil {
			return e
		}
		var expected uint64
		for _, v := range a.Storage {
			if v != (common.Hash{}) {
				expected++
			}
		}
		if slots != expected {
			return state.ErrCorrupt
		}
		if len(a.Code) > 0 || expected > 0 {
			contracts++
		}
		return nil
	})
	if e != nil {
		return m, e
	}
	if count != m.Accounts || digest != m.InputDigest || contracts+1 != m.NextTrieID {
		return m, state.ErrCorrupt
	}
	var leaves uint64
	e = mpt.WalkPaths(m.Root, Reader{s}, nil, func(k, v []byte) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		if len(k) != 32 {
			return state.ErrCorrupt
		}
		leaves++
		return nil
	})
	if e != nil {
		return m, e
	}
	if leaves != count {
		return m, state.ErrCorrupt
	}
	e = s.WalkTries(ctx, func(id uint64, address [20]byte, root mpt.Hash) error {
		if id == 0 {
			return nil
		}
		account, exists, e := view.Account(common.Address(address))
		if e != nil {
			return e
		}
		if !exists || account.StorageRoot != root {
			return state.ErrCorrupt
		}
		return nil
	})
	if e != nil {
		return m, e
	}
	return m, nil
}

func ParseManifest(raw []byte, workload string, header block.InputBlock, root mpt.Hash) (Manifest, error) {
	var m Manifest
	if json.Unmarshal(raw, &m) != nil || m.Schema != "evm-hash-snapshot/v1" || m.Codec != codec.Ethereum || m.WorkloadID != workload || m.Root != root || m.NextTrieID == 0 || m.NextTrieID > m.Accounts+1 {
		return m, state.ErrCorrupt
	}
	a, _ := json.Marshal(m.Header)
	b, _ := json.Marshal(header)
	canonical, _ := json.Marshal(m)
	if !bytes.Equal(a, b) || !bytes.Equal(canonical, raw) {
		return m, state.ErrCorrupt
	}
	return m, ValidateHeader(header)
}
func WalkTries(ctx context.Context, s *ht.SnapshotSource, visit func(owner []byte, root mpt.Hash) error) error {
	return s.WalkTries(ctx, func(id uint64, address [20]byte, root mpt.Hash) error {
		owner := []byte{0}
		if id != 0 {
			owner = append([]byte{1}, state.AccountKey(common.Address(address))...)
		}
		return visit(owner, root)
	})
}
