package evmruntime

import (
	"github.com/XiaodongQi1/HybridTrie/evm-runtime/state"
)

// NewMemoryState supplies a map-backed state with deep-copy rollback snapshots.
// No on-disk database or node process is opened. The caller owns this state.
func NewMemoryState() (*state.StateDB, error) {
	return state.New()
}
