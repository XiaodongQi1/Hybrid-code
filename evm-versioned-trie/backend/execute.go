package backend

import (
	"bytes"
	"context"
	"fmt"
	"github.com/holiman/uint256"
	"io"
	"sort"
	rt "github.com/XiaodongQi1/HybridTrie/evm-runtime"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/common"
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/params"
	"github.com/XiaodongQi1/HybridTrie/evm-state/block"
	"github.com/XiaodongQi1/HybridTrie/evm-state/state"
	"github.com/XiaodongQi1/HybridTrie/mpt"
	vt "github.com/XiaodongQi1/HybridTrie/versionedtrie"
)

type Result struct {
	Height   uint64
	ID       common.Hash
	Root     mpt.Hash
	Outcomes []block.Outcome
}

// Apply executes a complete ordered block and publishes one synchronous atomic
// commit. Each finalized transaction applies its key writes to the live tries.
// Rejected input/backend errors do not publish a partial disk height. Included
// REVERT/OOG transactions retain normal nonce/fee effects and failed receipts.
// chain nil selects the unchanged runtime's Cancun experiment chain.
// IDs are caller-authenticated block/workload IDs, not consensus header hashes.
func (b *Backend) Apply(input block.InputBlock, chain *params.ChainConfig) (*Result, error) {
	return b.apply(input, chain, applyOptions{})
}

func (b *Backend) ApplyMeasured(input block.InputBlock, chain *params.ChainConfig, timing *block.ExecutionTiming) (*Result, error) {
	return b.apply(input, chain, applyOptions{timing: timing})
}

// ApplyWithRecorder calls record with the final result before publishing state.
// A recorder error aborts publication. The callback must durably save the result
// and must not mutate it or call this Backend. Recovery must check the database
// before treating a recorded result as committed. No recorder data enters the MPT.
func (b *Backend) ApplyWithRecorder(input block.InputBlock, chain *params.ChainConfig, record func(Result) error) (*Result, error) {
	return b.apply(input, chain, applyOptions{record: record})
}

// ApplyEvaluated reuses authenticated block-final legacy-codec execution
// records. It executes no transactions. The expected root is checked BEFORE
// publication; this experiment API is not an untrusted transaction interface.
func (b *Backend) ApplyEvaluated(input block.InputBlock, expected mpt.Hash, evaluated block.EvaluatedBlock) (*Result, error) {
	if b.mode != "legacy-raw105" || expected == (mpt.Hash{}) {
		return nil, state.ErrCorrupt
	}
	return b.apply(input, nil, applyOptions{evaluated: &evaluated, expected: &expected})
}

// ApplyWithTrace executes EVM instructions once and captures typed final changes
// for the storage suite. It is not the timed execution path. Initialization is
// supplied separately as a shared allocation snapshot.
func (b *Backend) ApplyWithTrace(input block.InputBlock, chain *params.ChainConfig) (*Result, block.LogicalBlock, error) {
	var trace block.LogicalBlock
	r, e := b.apply(input, chain, applyOptions{trace: &trace})
	if e != nil {
		return nil, block.LogicalBlock{}, e
	}
	return r, trace, nil
}

// ApplyLogicalBlock imports a bound v2 trace without executing transactions.
// Both native and legacy codecs use typed fields, never legacy leaf bytes. The
// expected canonical root is checked before the atomic durable publication.
func (b *Backend) ApplyLogicalBlock(input block.InputBlock, trace block.LogicalBlock) (*Result, error) {
	if e := trace.Validate(input, b.mode); e != nil {
		return nil, e
	}
	return b.apply(input, nil, applyOptions{logical: &trace, expected: &trace.Root})
}

type applyOptions struct {
	timing    *block.ExecutionTiming
	evaluated *block.EvaluatedBlock
	logical   *block.LogicalBlock
	expected  *mpt.Hash
	record    func(Result) error
	trace     *block.LogicalBlock
}

func (b *Backend) apply(input block.InputBlock, chain *params.ChainConfig, options applyOptions) (*Result, error) {
	evaluated, expected, record := options.evaluated, options.expected, options.record
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.readOnly {
		return nil, vt.ErrReadOnly
	}
	if input.Height > b.maxHeight {
		return nil, vt.ErrLimit
	}
	head, e := b.store.Head()
	if e != nil {
		return nil, e
	}
	want := b.initialHeight
	if head.Initialized {
		want = uint64(head.Height) + 1
	}
	if input.Height != want || input.ID == (common.Hash{}) || input.ParentID != common.Hash(head.CommitID) {
		return nil, vt.ErrConflict
	}
	if input.Context.GasLimit == 0 {
		return nil, fmt.Errorf("positive block gas limit required")
	}
	if !head.Initialized && (options.logical != nil || options.trace != nil) {
		return nil, fmt.Errorf("logical traces describe execution blocks, not allocation snapshots")
	}
	if !head.Initialized {
		if hs, ok := b.store.(*hybridStore); ok && hs.snapshot != nil {
			if len(input.Transactions) != 0 {
				return nil, fmt.Errorf("initial snapshot is allocation-only")
			}
			order := make([]int, len(input.Allocations))
			for i := range order {
				order[i] = i
			}
			sort.Slice(order, func(i, j int) bool {
				return bytes.Compare(input.Allocations[order[i]].Address[:], input.Allocations[order[j]].Address[:]) < 0
			})
			header := input
			header.Allocations = nil
			cursor := 0
			out, report, e := b.importSnapshotRecorded(context.Background(), header, func() (block.Allocation, error) {
				if cursor == len(order) {
					return block.Allocation{}, io.EOF
				}
				a := input.Allocations[order[cursor]]
				cursor++
				return a, nil
			}, expected, record)
			b.lastSnapshot = report
			return out, e
		}
	}
	snapshot, abort, e := b.store.ExecutionView(input.Height)
	if e != nil {
		return nil, e
	}
	defer abort()
	var base *View
	if snapshot != nil {
		base, e = openView(snapshot, b.mode)
		if e != nil {
			snapshot.Close()
			return nil, e
		}
	} else {
		base = emptyView(b.mode)
	}
	defer base.Close()
	var live *transactionTrie
	var reader state.Reader = base
	if head.Initialized && evaluated == nil && options.logical == nil {
		live = newTransactionTrie(base, options.trace != nil)
		reader = live
	}
	s := state.New(reader)
	out := &Result{Height: input.Height, ID: input.ID}
	if !head.Initialized {
		if len(input.Transactions) != 0 {
			return nil, fmt.Errorf("initial snapshot is allocation-only")
		}
		seen := map[common.Address]bool{}
		for _, a := range input.Allocations {
			if seen[a.Address] || a.Balance == nil || a.Balance.Sign() < 0 || a.Balance.BitLen() > 256 {
				return nil, fmt.Errorf("invalid/duplicate allocation")
			}
			seen[a.Address] = true
			balance, _ := uint256.FromBig(a.Balance)
			s.CreateAccount(a.Address)
			s.AddBalance(a.Address, balance)
			s.SetNonce(a.Address, a.Nonce)
			s.SetCode(a.Address, a.Code)
			for k, v := range a.Storage {
				s.SetState(a.Address, k, v)
			}
		}
	} else if evaluated == nil && options.logical == nil {
		if len(input.Allocations) != 0 {
			return nil, fmt.Errorf("allocations only in initial snapshot")
		}
		options.timing.StartTransactions()
		var ancestorError error
		getHash := func(height uint64) common.Hash {
			v, err := b.store.ViewAt(height)
			if err != nil {
				// A snapshot may start after earlier canonical block history;
				// never silently invent a BLOCKHASH value if EVM requests it.
				ancestorError = err
				return common.Hash{}
			}
			defer v.Close()
			return common.Hash(v.CommitID())
		}
		ctx := input.Context
		engine, err := rt.New(s, chain, rt.Block{Number: input.Height, Time: ctx.Time, GasLimit: ctx.GasLimit, Coinbase: ctx.Coinbase, BaseFee: ctx.BaseFee, BlobBaseFee: ctx.BlobBaseFee, Random: ctx.Random, Hash: input.ID, GetHash: getHash})
		if err != nil {
			return nil, err
		}
		for i, wire := range input.Transactions {
			r, err := engine.ApplyEncoded(wire)
			if err != nil {
				return nil, fmt.Errorf("transaction %d: %w", i, err)
			}
			if ancestorError != nil {
				return nil, fmt.Errorf("ancestor block: %w", ancestorError)
			}
			o := block.Outcome{Receipt: r.Receipt, ReturnData: bytes.Clone(r.ReturnData)}
			if r.VMError != nil {
				o.VMError = r.VMError.Error()
			}
			out.Outcomes = append(out.Outcomes, o)
			// Finalise has already reverted failed VM effects and retained
			// nonce/fee changes. Apply only this transaction's key writes.
			writes, err := s.Changes()
			if err != nil {
				return nil, err
			}
			if err = live.apply(writes); err != nil {
				return nil, err
			}
			s.ResetTransaction(live)
		}
	}
	options.timing.StartMaterialize()
	changes, e := s.Changes()
	if e != nil {
		return nil, e
	}
	if live != nil {
		changes = live.trace
	}
	if evaluated != nil {
		if len(evaluated.Outcomes) != len(input.Transactions) {
			return nil, state.ErrCorrupt
		}
		if head.Initialized {
			if len(input.Allocations) != 0 {
				return nil, state.ErrCorrupt
			}
			changes, e = evaluated.ChangesFor(base)
			if e != nil {
				return nil, e
			}
		}
		out.Outcomes = evaluated.Outcomes
	}
	if options.logical != nil {
		changes, e = options.logical.ChangesFor(base)
		if e != nil {
			return nil, e
		}
		out.Outcomes = options.logical.Outcomes
	}
	var beforeCommit func(mpt.Hash) error
	if record != nil || options.trace != nil {
		beforeCommit = func(root mpt.Hash) error {
			out.Root = root
			if options.trace != nil {
				trace, e := block.NewLogicalBlock(input, b.mode, root, changes, out.Outcomes)
				if e != nil {
					return e
				}
				*options.trace = trace
			}
			if record != nil {
				return record(*out)
			}
			return nil
		}
	}
	if live != nil {
		out.Root, e = b.publishTries(base, input.Height, [32]byte(input.ID), live.accounts, live.plans(), live.codes, expected, beforeCommit, options.timing)
	} else {
		out.Root, e = b.commitRecorded(base, input.Height, [32]byte(input.ID), changes, expected, beforeCommit, options.timing)
	}
	if e != nil {
		return nil, e
	}
	if !head.Initialized {
		if hs, ok := b.store.(*hybridStore); ok && hs.initialHash {
			h, e := hs.Head()
			if e != nil {
				return nil, e
			}
			b.lastSnapshot = SnapshotReport{Mode: InitializationHashCold, AtomicCommit: hs.lastCommit, ColdCache: true, Accounts: uint64(len(input.Allocations)), NextTrieID: h.NextTrieID, Root: out.Root}
		}
	}
	return out, nil
}
