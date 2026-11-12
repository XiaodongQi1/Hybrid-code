package evmruntime

import (
	"bytes"
	"encoding/json"
	"errors"
	"math/big"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/internal/transition"
)

// ExperimentTransaction is trusted workload input, not an Ethereum transaction.
// No signature, chain ID, supplied nonce or fee fields exist in this format.
// Execution consumes gas but charges no fees; nonce advances from current state.
type ExperimentTransaction struct {
	From  common.Address
	To    *common.Address // nil deploys Data as init code
	Value *big.Int        // nil means zero; nonzero value still requires transfer funds
	Gas   uint64
	Data  []byte
}

// The reserved discriminator cannot be confused with any supported Ethereum
// envelope. The version is bound into both persisted bytes and transaction ID.
const experimentPrefix = "\x7fstatera/experiment/free-v1\n"
const ExperimentReceiptType uint8 = 0x7f

func EncodeExperimentTransaction(tx ExperimentTransaction) ([]byte, error) {
	if tx.Gas == 0 {
		return nil, errors.New("positive execution gas budget required")
	}
	if tx.Value == nil {
		tx.Value = new(big.Int)
	}
	if tx.Value.Sign() < 0 || tx.Value.BitLen() > 256 {
		return nil, errors.New("value is not uint256")
	}
	b, e := json.Marshal(tx)
	if e != nil {
		return nil, e
	}
	return append([]byte(experimentPrefix), b...), nil
}

// ExecutionTransaction owns decoded input and dispatches both historical signed
// records and new experiment records without inferring a sender from calldata.
type ExecutionTransaction struct {
	signed     *types.Transaction
	experiment *ExperimentTransaction
	wire       []byte
}

func DecodeExecutionTransaction(wire []byte) (*ExecutionTransaction, error) {
	t := &ExecutionTransaction{wire: bytes.Clone(wire)}
	if len(wire) > 0 && wire[0] == ExperimentReceiptType {
		if !bytes.HasPrefix(wire, []byte(experimentPrefix)) {
			return nil, errors.New("unknown experiment input version")
		}
		var tx ExperimentTransaction
		if e := json.Unmarshal(wire[len(experimentPrefix):], &tx); e != nil {
			return nil, e
		}
		canonical, e := EncodeExperimentTransaction(tx)
		if e != nil {
			return nil, e
		}
		if !bytes.Equal(wire, canonical) {
			return nil, errors.New("noncanonical experiment input")
		}
		t.experiment = &tx
	} else {
		t.signed = new(types.Transaction)
		if e := t.signed.UnmarshalBinary(wire); e != nil {
			return nil, e
		}
		canonical, e := t.signed.MarshalBinary()
		if e != nil {
			return nil, e
		}
		if !bytes.Equal(wire, canonical) {
			return nil, errors.New("noncanonical signed input")
		}
	}
	return t, nil
}
func (t *ExecutionTransaction) IsExperiment() bool { return t.experiment != nil }
func (t *ExecutionTransaction) Gas() uint64 {
	if t.experiment != nil {
		return t.experiment.Gas
	}
	return t.signed.Gas()
}
func (t *ExecutionTransaction) BlobGas() uint64 {
	if t.experiment != nil {
		return 0
	}
	return t.signed.BlobGas()
}
func (t *ExecutionTransaction) Apply(e *Engine) (*Result, error) {
	if t.experiment == nil {
		return e.Apply(t.signed)
	}
	return e.applyExperiment(*t.experiment, crypto.Keccak256Hash(t.wire))
}
func (e *Engine) ApplyEncoded(wire []byte) (*Result, error) {
	t, err := DecodeExecutionTransaction(wire)
	if err != nil {
		return nil, err
	}
	return t.Apply(e)
}
func (e *Engine) ApplyExperiment(tx ExperimentTransaction) (*Result, error) {
	wire, err := EncodeExperimentTransaction(tx)
	if err != nil {
		return nil, err
	}
	// Decode once to own caller buffers and normalize zero Value.
	return e.ApplyEncoded(wire)
}
func (e *Engine) applyExperiment(tx ExperimentTransaction, hash common.Hash) (*Result, error) {
	msg := &transition.Message{From: tx.From, To: tx.To, Nonce: e.state.GetNonce(tx.From), Value: new(big.Int).Set(tx.Value), GasLimit: tx.Gas, Data: common.CopyBytes(tx.Data), GasPrice: new(big.Int), GasFeeCap: new(big.Int), GasTipCap: new(big.Int), SkipNonceChecks: true, SkipTransactionChecks: true}
	return e.applyMessage(msg, ExperimentReceiptType, hash, 0, true)
}
