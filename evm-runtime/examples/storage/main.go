// A self-contained deployment/call example; no node, RPC, compiler or disk DB.
package main

import (
	"encoding/json"
	"fmt"
	"math/big"
	"os"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"

	"github.com/holiman/uint256"
	runtime "github.com/XiaodongQi1/Hybrid-code/evm-runtime"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/types"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	s, err := runtime.NewMemoryState()
	if err != nil {
		return err
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		return err
	}
	sender := crypto.PubkeyToAddress(key.PublicKey)
	s.AddBalance(sender, uint256.NewInt(1_000_000_000))
	s.Finalise(true)
	engine, err := runtime.New(s, nil, runtime.Block{Number: 1, Time: 1, GasLimit: 10_000_000})
	if err != nil {
		return err
	}
	execute := func(to *common.Address, data []byte) (*runtime.Result, error) {
		tx, err := types.SignTx(types.NewTx(&types.DynamicFeeTx{
			ChainID: big.NewInt(1337), Nonce: s.GetNonce(sender), To: to, Data: data, Gas: 500000,
			GasFeeCap: big.NewInt(2), GasTipCap: big.NewInt(1),
		}), types.LatestSignerForChainID(big.NewInt(1337)), key)
		if err != nil {
			return nil, err
		}
		r, err := engine.Apply(tx)
		if err != nil {
			return nil, err
		}
		if r.VMError != nil {
			return nil, r.VMError
		}
		return r, nil
	}
	// runtime: slot[0] = calldata[0:32]; return slot[0].
	code := common.FromHex("0x60003560005560005460005260206000f3")
	init := append([]byte{0x60, byte(len(code)), 0x60, 0x0c, 0x60, 0, 0x39, 0x60, byte(len(code)), 0x60, 0, 0xf3}, code...)
	deployed, err := execute(nil, init)
	if err != nil {
		return err
	}
	addr := deployed.Receipt.ContractAddress
	input := make([]byte, 32)
	input[31] = 42
	called, err := execute(&addr, input)
	if err != nil {
		return err
	}
	stored := s.GetState(addr, common.Hash{})
	if stored != common.BytesToHash(input) || common.BytesToHash(called.ReturnData) != stored {
		return fmt.Errorf("storage/result mismatch")
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"fork": "Cancun", "backend": "memory", "contract": addr.Hex(), "slot_0": new(big.Int).SetBytes(stored[:]).String(),
		"deploy_gas": deployed.Receipt.GasUsed, "call_gas": called.Receipt.GasUsed, "status": called.Receipt.Status,
	})
}
