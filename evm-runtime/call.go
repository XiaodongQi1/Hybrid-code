package evmruntime

import (
	"context"
	"errors"
	"github.com/holiman/uint256"
	"math/big"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/vm"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/internal/transition"
)

// CallRequest is an experimental isolated message call, not the complete
// Ethereum JSON-RPC eth_call validation/override API. Gas is execution gas;
// there is no intrinsic transaction gas, signature, nonce increment or fee.
type CallRequest struct {
	From   common.Address
	To     common.Address
	Input  []byte
	Gas    uint64
	Value  *big.Int
	Static bool
}
type CallResult struct {
	ReturnData []byte
	GasUsed    uint64
	VMError    string
}

// Call runs on a caller-owned private State and always reverts its writes.
// Engine.Apply and the canonical signed transaction path are unchanged.
func (e *Engine) Call(ctx context.Context, q CallRequest) (CallResult, error) {
	if err := ctx.Err(); err != nil {
		return CallResult{}, err
	}
	if q.Gas == 0 || q.Gas > e.block.GasLimit {
		return CallResult{}, errors.New("call gas must be positive and within block limit")
	}
	value := new(uint256.Int)
	if q.Value != nil {
		if q.Value.Sign() < 0 || q.Value.BitLen() > 256 {
			return CallResult{}, errors.New("invalid call value")
		}
		value.SetBytes(q.Value.Bytes())
	}
	if q.Static && !value.IsZero() {
		return CallResult{}, errors.New("static call cannot transfer value")
	}
	if err := e.state.Error(); err != nil {
		return CallResult{}, err
	}
	n := new(big.Int).SetUint64(e.block.Number)
	bc := vm.BlockContext{CanTransfer: transition.CanTransfer, Transfer: transition.Transfer, GetHash: e.block.GetHash, Coinbase: e.block.Coinbase, GasLimit: e.block.GasLimit, BlockNumber: n, Time: e.block.Time, Difficulty: new(big.Int), BaseFee: e.block.BaseFee, BlobBaseFee: e.block.BlobBaseFee, Random: &e.block.Random}
	machine, release := e.machine(bc, vm.Config{})
	defer release()
	machine.SetTxContext(vm.TxContext{Origin: q.From, GasPrice: new(big.Int)})
	e.state.Prepare(e.chain.Rules(n, true, e.block.Time), q.From, e.block.Coinbase, &q.To, vm.ActivePrecompiles(e.chain.Rules(n, true, e.block.Time)), nil)
	snap := e.state.Snapshot()
	defer e.state.RevertToSnapshot(snap)
	finished := make(chan struct{})
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			machine.Cancel()
		case <-finished:
		}
	}()
	defer func() { close(finished); <-joined }()
	var data []byte
	var left uint64
	var vmerr error
	if q.Static {
		data, left, vmerr = machine.StaticCall(q.From, q.To, q.Input, q.Gas)
	} else {
		data, left, vmerr = machine.Call(q.From, q.To, q.Input, q.Gas, value)
	}
	if err := ctx.Err(); err != nil {
		return CallResult{}, err
	}
	if err := e.state.Error(); err != nil {
		return CallResult{}, err
	}
	r := CallResult{ReturnData: common.CopyBytes(data), GasUsed: q.Gas - left}
	if vmerr != nil {
		r.VMError = vmerr.Error()
	}
	return r, nil
}
