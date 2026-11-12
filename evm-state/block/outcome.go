package block

import "github.com/XiaodongQi1/HybridTrie/evm-runtime/geth/core/types"

type Outcome struct {
	Receipt    *types.Receipt
	ReturnData []byte
	VMError    string
}
