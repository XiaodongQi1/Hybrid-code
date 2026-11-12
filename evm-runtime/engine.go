// Package evmruntime runs a slim local EVM without a node,
// consensus engine, RPC server, or network. State is supplied through an interface.
package evmruntime

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/vm"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/params"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/internal/transition"
)

// State is the seam for a future Statera backend. EVM-visible values must be
// logical values; provenance envelopes belong below this interface.
type State interface {
	vm.StateDB
	Error() error
}

type Block struct {
	Number      uint64
	Time        uint64
	GasLimit    uint64
	Coinbase    common.Address
	BaseFee     *big.Int
	BlobBaseFee *big.Int
	Random      common.Hash
	Hash        common.Hash
	// GetHash supplies canonical ancestor hashes for BLOCKHASH. A nil function
	// returns zero, suitable only for experiments without ancestor history.
	GetHash func(uint64) common.Hash
}

// Engine executes one block's ordered signed or experiment transactions. It is not concurrent
// safe. Construct a new Engine for the next block, retaining the State.
type Engine struct {
	state    State
	chain    *params.ChainConfig
	block    Block
	gas      transition.GasPool
	used     uint64
	blobUsed uint64
	index    int
	pool     *MachinePool
}

type Result struct {
	Receipt    *types.Receipt
	ReturnData []byte
	// VMError denotes an included but failed transaction (REVERT, OOG, etc.).
	// Apply's error instead denotes a rejected transaction or backend failure.
	VMError error
}

// DefaultChainConfig freezes a post-merge Cancun experiment chain (ID 1337).
// Supply a different complete config to select another supported fork.
func DefaultChainConfig() *params.ChainConfig {
	return params.CancunConfig(big.NewInt(1337))
}

func New(s State, chain *params.ChainConfig, b Block) (*Engine, error) {
	if s == nil || b.GasLimit == 0 {
		return nil, errors.New("state and positive block gas limit required")
	}
	if chain == nil {
		chain = DefaultChainConfig()
	}
	if chain.ChainID == nil {
		return nil, errors.New("chain ID required")
	}
	if err := chain.CheckConfigForkOrder(); err != nil {
		return nil, err
	}
	// This harness implements post-merge, post-Byzantium receipt semantics.
	if !chain.IsByzantium(new(big.Int).SetUint64(b.Number)) {
		return nil, errors.New("pre-Byzantium receipt format is unsupported")
	}
	if chain.IsPrague(new(big.Int).SetUint64(b.Number), b.Time) || chain.IsOsaka(new(big.Int).SetUint64(b.Number), b.Time) {
		return nil, errors.New("this slim runtime supports forks up to Cancun")
	}
	if chain.VerkleTime != nil {
		return nil, errors.New("Verkle execution is unsupported")
	}
	if b.BaseFee == nil {
		b.BaseFee = new(big.Int)
	} else {
		b.BaseFee = new(big.Int).Set(b.BaseFee)
	}
	if b.BlobBaseFee == nil {
		b.BlobBaseFee = big.NewInt(1)
	} else {
		b.BlobBaseFee = new(big.Int).Set(b.BlobBaseFee)
	}
	if b.BaseFee.Sign() < 0 || b.BlobBaseFee.Sign() < 0 {
		return nil, errors.New("negative base fee")
	}
	if b.GetHash == nil {
		b.GetHash = func(uint64) common.Hash { return common.Hash{} }
	}
	e := &Engine{state: s, chain: chain, block: b}
	e.gas.AddGas(b.GasLimit)
	return e, nil
}

func (e *Engine) GasRemaining() uint64 { return e.gas.Gas() }

func (e *Engine) BlobGasUsed() uint64 { return e.blobUsed }

// Apply authenticates the sender and uses geth's transaction state transition,
// including intrinsic gas, fees, nonce, access lists, refunds, and call rollback.
// Invalid transactions restore state and the block gas pool. Included failed
// transactions retain their nonce and gas payment, as on Ethereum.
func (e *Engine) Apply(tx *types.Transaction) (*Result, error) {
	if tx == nil {
		return nil, errors.New("nil transaction")
	}
	number := new(big.Int).SetUint64(e.block.Number)
	msg, err := transition.TransactionToMessage(tx, types.MakeSigner(e.chain, number, e.block.Time), e.block.BaseFee)
	if err != nil {
		return nil, fmt.Errorf("authenticate transaction: %w", err)
	}
	// Execute blob envelopes and BLOBHASH using their versioned hashes. Data
	// availability and sidecar/KZG verification remain the caller's responsibility.
	if tx.Type() == types.BlobTxType {
		if len(tx.BlobHashes()) == 0 {
			return nil, errors.New("blob transaction has no versioned hashes")
		}
		// This runtime supports at most Cancun. Its blob budget comes from
		// the injected execution config; no header/consensus helpers are needed.
		var max uint64
		if schedule := e.chain.BlobScheduleConfig; schedule != nil && schedule.Cancun != nil {
			max = uint64(schedule.Cancun.Max) * params.BlobTxBlobGasPerBlob
		}
		if tx.BlobGas() > max || e.blobUsed > max-tx.BlobGas() {
			return nil, errors.New("block blob gas limit exceeded")
		}
	}
	return e.applyMessage(msg, tx.Type(), tx.Hash(), tx.BlobGas(), false)
}

// applyMessage is shared by authenticated legacy input and explicit-sender
// experiment input. Gas metering, EVM execution and rollback stay identical.
func (e *Engine) applyMessage(msg *transition.Message, txType uint8, hash common.Hash, blobGas uint64, free bool) (*Result, error) {
	number := new(big.Int).SetUint64(e.block.Number)
	if err := e.state.Error(); err != nil {
		return nil, fmt.Errorf("state backend: %w", err)
	}
	ctx := vm.BlockContext{
		CanTransfer: transition.CanTransfer, Transfer: transition.Transfer, GetHash: e.block.GetHash,
		Coinbase: e.block.Coinbase, GasLimit: e.block.GasLimit, BlockNumber: number,
		Time: e.block.Time, Difficulty: new(big.Int), BaseFee: e.block.BaseFee,
		BlobBaseFee: e.block.BlobBaseFee, Random: &e.block.Random,
	}
	snap, oldGas := e.state.Snapshot(), e.gas.Gas()
	evm, release := e.machine(ctx, vm.Config{NoBaseFee: free})
	defer release()
	exec, err := transition.ApplyMessage(evm, msg, &e.gas)
	if err == nil {
		err = e.state.Error()
	}
	if err != nil {
		e.state.RevertToSnapshot(snap)
		e.gas.SetGas(oldGas)
		return nil, err
	}
	e.state.Finalise(e.chain.IsEIP158(number))
	e.used += exec.UsedGas
	e.blobUsed += blobGas
	r := &types.Receipt{
		Type: txType, Status: types.ReceiptStatusSuccessful, TxHash: hash,
		GasUsed: exec.UsedGas, CumulativeGasUsed: e.used, EffectiveGasPrice: new(big.Int).Set(msg.GasPrice),
		BlockHash: e.block.Hash, BlockNumber: number, TransactionIndex: uint(e.index),
	}
	if exec.Failed() {
		r.Status = types.ReceiptStatusFailed
	}
	if txType == types.BlobTxType {
		r.BlobGasUsed = blobGas
		r.BlobGasPrice = new(big.Int).Set(e.block.BlobBaseFee)
	}
	if msg.To == nil {
		r.ContractAddress = crypto.CreateAddress(msg.From, msg.Nonce)
	}
	e.index++
	return &Result{Receipt: r, ReturnData: common.CopyBytes(exec.ReturnData), VMError: exec.Err}, nil
}
