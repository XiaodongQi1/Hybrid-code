module github.com/XiaodongQi1/HybridTrie/evm-path-archive

go 1.24.0

require (
	github.com/holiman/uint256 v1.3.2
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
	github.com/XiaodongQi1/HybridTrie v0.0.0
	github.com/XiaodongQi1/HybridTrie/evm-state v0.0.0
	github.com/XiaodongQi1/HybridTrie/evm-runtime v0.0.0
)

require (
	github.com/bits-and-blooms/bitset v1.20.0 // indirect
	github.com/consensys/gnark-crypto v0.18.0 // indirect
	github.com/crate-crypto/go-eth-kzg v1.4.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.0.1 // indirect
	github.com/ethereum/go-bigmodexpfix v0.0.0-20250911101455-f9e208c548ab // indirect
	github.com/golang/snappy v0.0.4 // indirect
	golang.org/x/crypto v0.36.0 // indirect
	golang.org/x/sync v0.12.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
)

replace github.com/XiaodongQi1/HybridTrie => ..

replace github.com/XiaodongQi1/HybridTrie/evm-runtime => ../evm-runtime

replace github.com/XiaodongQi1/HybridTrie/evm-state => ../evm-state
