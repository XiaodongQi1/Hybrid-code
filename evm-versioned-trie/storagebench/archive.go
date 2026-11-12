package storagebench

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/holiman/uint256"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math/big"
	"sort"
	rt "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/forest"
	q "github.com/XiaodongQi1/Hybrid-code/evm-state/query"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

type hashStore struct {
	db   *leveldb.DB
	cfg  Config
	root mpt.Hash
}
type hashBatch struct {
	store *hashStore
	batch *leveldb.Batch
}

func (s *hashStore) Get(key []byte) ([]byte, error) {
	v, e := s.db.Get(key, nil)
	if errors.Is(e, leveldb.ErrNotFound) {
		e = mpt.ErrNotFound
	}
	return v, e
}
func (s *hashStore) NewBatch() mpt.Batch   { return &hashBatch{s, new(leveldb.Batch)} }
func (b *hashBatch) Put(k, v []byte) error { b.batch.Put(k, v); return nil }
func (b *hashBatch) Write() error          { return b.store.db.Write(b.batch, &opt.WriteOptions{Sync: true}) }
func (s *hashStore) Code(h common.Hash) ([]byte, error) {
	if h == state.EmptyCode {
		return nil, nil
	}
	v, e := s.db.Get(append([]byte{'C'}, h[:]...), nil)
	if e == nil && crypto.Keccak256Hash(v) != h {
		e = state.ErrCorrupt
	}
	return v, e
}
func dbOptions(c Config, create, ro bool) *opt.Options {
	return &opt.Options{ReadOnly: ro, ErrorIfMissing: !create, ErrorIfExist: create, Compression: opt.SnappyCompression, BlockCacheCapacity: c.Storage.BlockCacheBytes, WriteBuffer: c.Storage.WriteBufferBytes, OpenFilesCacheCapacity: c.Storage.OpenFiles}
}
func rootKey(h uint64) []byte {
	key := make([]byte, 9)
	key[0] = 'z'
	binary.BigEndian.PutUint64(key[1:], h)
	return key
}
func rootValue(r rootRow) []byte { return append(append([]byte(nil), r.Root[:]...), r.ID[:]...) }
func openHash(path string, c Config, create, ro bool) (*hashStore, error) {
	db, e := leveldb.OpenFile(path, dbOptions(c, create, ro))
	if e != nil {
		return nil, e
	}
	s := &hashStore{db: db, cfg: c, root: mpt.EmptyRootHash}
	if create {
		raw, _ := json.Marshal(c)
		if e = db.Put([]byte("M/config"), raw, &opt.WriteOptions{Sync: true}); e != nil {
			db.Close()
			return nil, e
		}
	}
	return s, nil
}
func (s *hashStore) storedRoot(height uint64) (rootRow, error) {
	raw, e := s.db.Get(rootKey(height), nil)
	if e != nil {
		return rootRow{}, e
	}
	if len(raw) != 64 {
		return rootRow{}, state.ErrCorrupt
	}
	r := rootRow{Height: height}
	copy(r.Root[:], raw[:32])
	copy(r.ID[:], raw[32:])
	return r, nil
}
func (s *hashStore) execute(input block.InputBlock) (executionRecord, error) {
	f, e := forest.Open(s.root, s)
	if e != nil {
		return executionRecord{}, e
	}
	view := state.New(f)
	out := executionRecord{rootRow: rootRow{Height: input.Height, ID: input.ID}}
	if input.Height == 0 {
		for _, a := range input.Allocations {
			view.CreateAccount(a.Address)
			view.AddBalance(a.Address, uint256.MustFromBig(a.Balance))
			view.SetNonce(a.Address, a.Nonce)
			view.SetCode(a.Address, a.Code)
			for k, v := range a.Storage {
				view.SetState(a.Address, k, v)
			}
		}
	} else {
		var hashError error
		ctx := input.Context
		engine, e := rt.New(view, nil, rt.Block{Number: input.Height, Time: ctx.Time, GasLimit: ctx.GasLimit, Coinbase: ctx.Coinbase, BaseFee: ctx.BaseFee, BlobBaseFee: ctx.BlobBaseFee, Random: ctx.Random, Hash: input.ID, GetHash: func(h uint64) common.Hash {
			r, e := s.storedRoot(h)
			if e != nil {
				hashError = e
			}
			return r.ID
		}})
		if e != nil {
			return out, e
		}
		for i, wire := range input.Transactions {
			r, e := engine.ApplyEncoded(wire)
			if e != nil {
				return out, fmt.Errorf("transaction %d: %w", i, e)
			}
			if hashError != nil {
				return out, hashError
			}
			o := block.Outcome{Receipt: r.Receipt, ReturnData: r.ReturnData}
			if r.VMError != nil {
				o.VMError = r.VMError.Error()
			}
			out.Evaluated.Outcomes = append(out.Evaluated.Outcomes, o)
		}
	}
	changes, e := view.Changes()
	if e != nil {
		return out, e
	}
	plan, e := f.Materialize(changes, s.cfg.MaxBatchBytes)
	if e != nil {
		return out, e
	}
	batch := new(leveldb.Batch)
	for h, raw := range plan.Nodes {
		batch.Put(h[:], raw)
	}
	for h, code := range plan.Codes {
		batch.Put(append([]byte{'C'}, h[:]...), code)
	}
	out.Root = plan.Root
	batch.Put(rootKey(input.Height), rootValue(out.rootRow))
	if uint64(len(batch.Dump())) > s.cfg.MaxBatchBytes {
		return out, fmt.Errorf("archive batch limit")
	}
	if e = s.db.Write(batch, &opt.WriteOptions{Sync: true}); e != nil {
		return out, e
	}
	s.root = plan.Root
	after, e := forest.Open(plan.Root, s)
	if e != nil {
		return out, e
	}
	// Initialization is rebuilt from the same supplied allocation snapshot by
	// each physical encoder; there are no EVM transactions to repeat at height 0.
	if input.Height > 0 {
		addresses := make([]common.Address, 0, len(changes))
		for a := range changes {
			addresses = append(addresses, a)
		}
		sort.Slice(addresses, func(i, j int) bool { return bytes.Compare(addresses[i][:], addresses[j][:]) < 0 })
		for _, addr := range addresses {
			c := changes[addr]
			row := block.EvaluatedAccount{Address: addr, Deleted: c.Account == nil, Slots: c.Slots, ResetStorage: c.ResetStorage, CodeLoaded: c.CodeLoaded, Code: bytes.Clone(c.Code), ContractCreated: c.ContractCreated}
			if c.Account != nil {
				a, ok, e := after.Account(addr)
				if e != nil || !ok {
					return out, fmt.Errorf("missing changed account: %v", e)
				}
				row.Value = state.Encode(a)
			}
			out.Evaluated.Accounts = append(out.Evaluated.Accounts, row)
		}
	}
	requests := []q.Request{
		{Height: input.Height, Kind: q.Account, Address: common.HexToAddress("0x1000")},
		{Height: input.Height, Kind: q.Account, Address: common.HexToAddress("0xdead")},
		{Height: input.Height, Kind: q.Account, Address: common.BigToAddress(big.NewInt(0x100000 + int64(input.Height*7919)%int64(s.cfg.Workload.BackgroundAccounts)))},
		{Height: input.Height, Kind: q.Account, Address: common.HexToAddress("0x2000")},
		{Height: input.Height, Kind: q.Storage, Address: common.HexToAddress("0x2000"), Slot: common.BigToHash(new(big.Int).SetUint64(input.Height % 4))},
		{Height: input.Height, Kind: q.Storage, Address: common.BigToAddress(big.NewInt(0x2000 + int64(s.cfg.Workload.Contracts) - 1)), Slot: common.BigToHash(new(big.Int).SetUint64((input.Height + 1) % 4))},
		{Height: input.Height, Kind: q.Code, Address: common.HexToAddress("0x2000")},
	}
	if len(out.Evaluated.Accounts) > 0 {
		requests = append(requests, q.Request{Height: input.Height, Kind: q.Account, Address: out.Evaluated.Accounts[len(out.Evaluated.Accounts)-1].Address})
	} else {
		requests = append(requests, q.Request{Height: input.Height, Kind: q.Account, Address: common.HexToAddress("0xcafe")})
	}
	for _, req := range requests {
		response, e := forestQuery(after, req)
		if e != nil {
			return out, e
		}
		verified, e := q.Verify(context.Background(), out.Root, req, response, q.DefaultLimits())
		if e != nil {
			return out, e
		}
		out.Samples = append(out.Samples, sample{Request: req, Expected: verified})
	}
	return out, nil
}

func forestQuery(f *forest.View, req q.Request) (q.Response, error) {
	return f.ReadWithProof(context.Background(), req)
}
