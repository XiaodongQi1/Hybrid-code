// Package codec preserves the versioned adapter's original import path.
// The leaf formats are shared by all comparison backends in evm-state/codec.
package codec

import shared "github.com/XiaodongQi1/HybridTrie/evm-state/codec"

type Mode = shared.Mode

const (
	Legacy   = shared.Legacy
	Ethereum = shared.Ethereum
)
