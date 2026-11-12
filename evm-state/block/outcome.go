package block

import "github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/core/types"

type Outcome struct {
	Receipt    *types.Receipt
	ReturnData []byte
	VMError    string
}
