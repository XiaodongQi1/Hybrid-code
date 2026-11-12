// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// This file contains only the experiment's execution result, not a
// network/storage receipt codec or a consensus receipt commitment.
package types

import (
	"math/big"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
)

const (
	ReceiptStatusFailed     = uint64(0)
	ReceiptStatusSuccessful = uint64(1)
)

// Receipt records the execution outcome. Events and state/receipt roots are
// intentionally absent from this experiment profile.
type Receipt struct {
	Type              uint8
	Status            uint64
	CumulativeGasUsed uint64
	TxHash            common.Hash
	ContractAddress   common.Address
	GasUsed           uint64
	EffectiveGasPrice *big.Int
	BlobGasUsed       uint64
	BlobGasPrice      *big.Int
	BlockHash         common.Hash
	BlockNumber       *big.Int
	TransactionIndex  uint
}
