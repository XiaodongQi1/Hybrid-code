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

package vm

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math"
	"math/big"
	"math/bits"

	patched_big "github.com/ethereum/go-bigmodexpfix/src/math/big"
	"github.com/holiman/uint256"
	"golang.org/x/crypto/ripemd160"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/common/bitutil"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto/blake2b"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto/bn256"
	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto/kzg4844"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/params"
)

// PrecompiledContract is the basic interface for native Go contracts. The implementation
// requires a deterministic gas count based on the input size of the Run method of the
// contract.
type PrecompiledContract interface {
	RequiredGas(input []byte) uint64  // RequiredPrice calculates the contract gas use
	Run(input []byte) ([]byte, error) // Run runs the precompiled contract
	Name() string
}

// PrecompiledContracts contains the precompiled contracts supported at the given fork.
type PrecompiledContracts map[common.Address]PrecompiledContract

// PrecompiledContractsHomestead contains the default set of pre-compiled Ethereum
// contracts used in the Frontier and Homestead releases.
var PrecompiledContractsHomestead = PrecompiledContracts{
	common.BytesToAddress([]byte{0x1}): &ecrecover{},
	common.BytesToAddress([]byte{0x2}): &sha256hash{},
	common.BytesToAddress([]byte{0x3}): &ripemd160hash{},
	common.BytesToAddress([]byte{0x4}): &dataCopy{},
}

// PrecompiledContractsByzantium contains the default set of pre-compiled Ethereum
// contracts used in the Byzantium release.
var PrecompiledContractsByzantium = PrecompiledContracts{
	common.BytesToAddress([]byte{0x1}): &ecrecover{},
	common.BytesToAddress([]byte{0x2}): &sha256hash{},
	common.BytesToAddress([]byte{0x3}): &ripemd160hash{},
	common.BytesToAddress([]byte{0x4}): &dataCopy{},
	common.BytesToAddress([]byte{0x5}): &bigModExp{eip2565: false, eip7823: false, eip7883: false},
	common.BytesToAddress([]byte{0x6}): &bn256AddByzantium{},
	common.BytesToAddress([]byte{0x7}): &bn256ScalarMulByzantium{},
	common.BytesToAddress([]byte{0x8}): &bn256PairingByzantium{},
}

// PrecompiledContractsIstanbul contains the default set of pre-compiled Ethereum
// contracts used in the Istanbul release.
var PrecompiledContractsIstanbul = PrecompiledContracts{
	common.BytesToAddress([]byte{0x1}): &ecrecover{},
	common.BytesToAddress([]byte{0x2}): &sha256hash{},
	common.BytesToAddress([]byte{0x3}): &ripemd160hash{},
	common.BytesToAddress([]byte{0x4}): &dataCopy{},
	common.BytesToAddress([]byte{0x5}): &bigModExp{eip2565: false, eip7823: false, eip7883: false},
	common.BytesToAddress([]byte{0x6}): &bn256AddIstanbul{},
	common.BytesToAddress([]byte{0x7}): &bn256ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x8}): &bn256PairingIstanbul{},
	common.BytesToAddress([]byte{0x9}): &blake2F{},
}

// PrecompiledContractsBerlin contains the default set of pre-compiled Ethereum
// contracts used in the Berlin release.
var PrecompiledContractsBerlin = PrecompiledContracts{
	common.BytesToAddress([]byte{0x1}): &ecrecover{},
	common.BytesToAddress([]byte{0x2}): &sha256hash{},
	common.BytesToAddress([]byte{0x3}): &ripemd160hash{},
	common.BytesToAddress([]byte{0x4}): &dataCopy{},
	common.BytesToAddress([]byte{0x5}): &bigModExp{eip2565: true, eip7823: false, eip7883: false},
	common.BytesToAddress([]byte{0x6}): &bn256AddIstanbul{},
	common.BytesToAddress([]byte{0x7}): &bn256ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x8}): &bn256PairingIstanbul{},
	common.BytesToAddress([]byte{0x9}): &blake2F{},
}

// PrecompiledContractsCancun contains the default set of pre-compiled Ethereum
// contracts used in the Cancun release.
var PrecompiledContractsCancun = PrecompiledContracts{
	common.BytesToAddress([]byte{0x1}): &ecrecover{},
	common.BytesToAddress([]byte{0x2}): &sha256hash{},
	common.BytesToAddress([]byte{0x3}): &ripemd160hash{},
	common.BytesToAddress([]byte{0x4}): &dataCopy{},
	common.BytesToAddress([]byte{0x5}): &bigModExp{eip2565: true, eip7823: false, eip7883: false},
	common.BytesToAddress([]byte{0x6}): &bn256AddIstanbul{},
	common.BytesToAddress([]byte{0x7}): &bn256ScalarMulIstanbul{},
	common.BytesToAddress([]byte{0x8}): &bn256PairingIstanbul{},
	common.BytesToAddress([]byte{0x9}): &blake2F{},
	common.BytesToAddress([]byte{0xa}): &kzgPointEvaluation{},
}

var (
	PrecompiledAddressesCancun    []common.Address
	PrecompiledAddressesBerlin    []common.Address
	PrecompiledAddressesIstanbul  []common.Address
	PrecompiledAddressesByzantium []common.Address
	PrecompiledAddressesHomestead []common.Address
)

func init() {
	for k := range PrecompiledContractsHomestead {
		PrecompiledAddressesHomestead = append(PrecompiledAddressesHomestead, k)
	}
	for k := range PrecompiledContractsByzantium {
		PrecompiledAddressesByzantium = append(PrecompiledAddressesByzantium, k)
	}
	for k := range PrecompiledContractsIstanbul {
		PrecompiledAddressesIstanbul = append(PrecompiledAddressesIstanbul, k)
	}
	for k := range PrecompiledContractsBerlin {
		PrecompiledAddressesBerlin = append(PrecompiledAddressesBerlin, k)
	}
	for k := range PrecompiledContractsCancun {
		PrecompiledAddressesCancun = append(PrecompiledAddressesCancun, k)
	}

}

func activePrecompiledContracts(rules params.Rules) PrecompiledContracts {
	switch {

	case rules.IsCancun:
		return PrecompiledContractsCancun
	case rules.IsBerlin:
		return PrecompiledContractsBerlin
	case rules.IsIstanbul:
		return PrecompiledContractsIstanbul
	case rules.IsByzantium:
		return PrecompiledContractsByzantium
	default:
		return PrecompiledContractsHomestead
	}
}

// ActivePrecompiledContracts returns a copy of precompiled contracts enabled with the current configuration.
func ActivePrecompiledContracts(rules params.Rules) PrecompiledContracts {
	return maps.Clone(activePrecompiledContracts(rules))
}

// ActivePrecompiles returns the precompile addresses enabled with the current configuration.
func ActivePrecompiles(rules params.Rules) []common.Address {
	switch {

	case rules.IsCancun:
		return PrecompiledAddressesCancun
	case rules.IsBerlin:
		return PrecompiledAddressesBerlin
	case rules.IsIstanbul:
		return PrecompiledAddressesIstanbul
	case rules.IsByzantium:
		return PrecompiledAddressesByzantium
	default:
		return PrecompiledAddressesHomestead
	}
}

// RunPrecompiledContract runs and evaluates the output of a precompiled contract.
// It returns
// - the returned bytes,
// - the _remaining_ gas,
// - any error that occurred
func RunPrecompiledContract(p PrecompiledContract, input []byte, suppliedGas uint64) (ret []byte, remainingGas uint64, err error) {
	gasCost := p.RequiredGas(input)
	if suppliedGas < gasCost {
		return nil, 0, ErrOutOfGas
	}

	suppliedGas -= gasCost
	output, err := p.Run(input)
	return output, suppliedGas, err
}

// ecrecover implemented as a native contract.
type ecrecover struct{}

func (c *ecrecover) RequiredGas(input []byte) uint64 {
	return params.EcrecoverGas
}

func (c *ecrecover) Run(input []byte) ([]byte, error) {
	const ecRecoverInputLength = 128

	input = common.RightPadBytes(input, ecRecoverInputLength)
	// "input" is (hash, v, r, s), each 32 bytes
	// but for ecrecover we want (r, s, v)

	r := new(big.Int).SetBytes(input[64:96])
	s := new(big.Int).SetBytes(input[96:128])
	v := input[63] - 27

	// tighter sig s values input homestead only apply to tx sigs
	if bitutil.TestBytes(input[32:63]) || !crypto.ValidateSignatureValues(v, r, s, false) {
		return nil, nil
	}
	// We must make sure not to modify the 'input', so placing the 'v' along with
	// the signature needs to be done on a new allocation
	sig := make([]byte, 65)
	copy(sig, input[64:128])
	sig[64] = v
	// v needs to be at the end for libsecp256k1
	pubKey, err := crypto.Ecrecover(input[:32], sig)
	// make sure the public key is a valid one
	if err != nil {
		return nil, nil
	}

	// the first byte of pubkey is bitcoin heritage
	return common.LeftPadBytes(crypto.Keccak256(pubKey[1:])[12:], 32), nil
}

func (c *ecrecover) Name() string {
	return "ECREC"
}

// SHA256 implemented as a native contract.
type sha256hash struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *sha256hash) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.Sha256PerWordGas + params.Sha256BaseGas
}
func (c *sha256hash) Run(input []byte) ([]byte, error) {
	h := sha256.Sum256(input)
	return h[:], nil
}

func (c *sha256hash) Name() string {
	return "SHA256"
}

// RIPEMD160 implemented as a native contract.
type ripemd160hash struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *ripemd160hash) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.Ripemd160PerWordGas + params.Ripemd160BaseGas
}
func (c *ripemd160hash) Run(input []byte) ([]byte, error) {
	ripemd := ripemd160.New()
	ripemd.Write(input)
	return common.LeftPadBytes(ripemd.Sum(nil), 32), nil
}

func (c *ripemd160hash) Name() string {
	return "RIPEMD160"
}

// data copy implemented as a native contract.
type dataCopy struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
//
// This method does not require any overflow checking as the input size gas costs
// required for anything significant is so high it's impossible to pay for.
func (c *dataCopy) RequiredGas(input []byte) uint64 {
	return uint64(len(input)+31)/32*params.IdentityPerWordGas + params.IdentityBaseGas
}
func (c *dataCopy) Run(in []byte) ([]byte, error) {
	return common.CopyBytes(in), nil
}

func (c *dataCopy) Name() string {
	return "ID"
}

// bigModExp implements a native big integer exponential modular operation.
type bigModExp struct {
	eip2565 bool
	eip7823 bool
	eip7883 bool
}

// byzantiumMultComplexity implements the bigModexp multComplexity formula, as defined in EIP-198.
//
//	def mult_complexity(x):
//		if x <= 64: return x ** 2
//		elif x <= 1024: return x ** 2 // 4 + 96 * x - 3072
//		else: return x ** 2 // 16 + 480 * x - 199680
//
// where is x is max(length_of_MODULUS, length_of_BASE)
// returns MaxUint64 if an overflow occurred.
func byzantiumMultComplexity(x uint64) uint64 {
	switch {
	case x <= 64:
		return x * x
	case x <= 1024:
		// x^2 / 4 + 96*x - 3072
		return x*x/4 + 96*x - 3072

	default:
		// For large x, use uint256 arithmetic to avoid overflow
		// x^2 / 16 + 480*x - 199680

		// xSqr = x^2 / 16
		carry, xSqr := bits.Mul64(x, x)
		if carry != 0 {
			return math.MaxUint64
		}
		xSqr = xSqr >> 4

		// Calculate 480 * x (can't overflow if x^2 didn't overflow)
		x480 := x * 480
		// Calculate 480 * x - 199680 (will not underflow, since x > 1024)
		x480 = x480 - 199680

		// xSqr + x480
		sum, carry := bits.Add64(xSqr, x480, 0)
		if carry != 0 {
			return math.MaxUint64
		}
		return sum
	}
}

// berlinMultComplexity implements the multiplication complexity formula for Berlin.
//
// def mult_complexity(x):
//
//	ceiling(x/8)^2
//
// where is x is max(length_of_MODULUS, length_of_BASE)
func berlinMultComplexity(x uint64) uint64 {
	// x = (x + 7) / 8
	x, carry := bits.Add64(x, 7, 0)
	if carry != 0 {
		return math.MaxUint64
	}
	x /= 8

	// x^2
	carry, x = bits.Mul64(x, x)
	if carry != 0 {
		return math.MaxUint64
	}
	return x
}

// osakaMultComplexity implements the multiplication complexity formula for Osaka.
//
// For x <= 32: returns 16
// For x > 32: returns 2 * ceiling(x/8)^2
func osakaMultComplexity(x uint64) uint64 {
	if x <= 32 {
		return 16
	}
	// For x > 32, return 2 * berlinMultComplexity(x)
	result := berlinMultComplexity(x)
	carry, result := bits.Mul64(result, 2)
	if carry != 0 {
		return math.MaxUint64
	}
	return result
}

// modexpIterationCount calculates the number of iterations for the modexp precompile.
// This is the adjusted exponent length used in gas calculation.
func modexpIterationCount(expLen uint64, expHead uint256.Int, multiplier uint64) uint64 {
	var iterationCount uint64

	// For large exponents (expLen > 32), add (expLen - 32) * multiplier
	if expLen > 32 {
		carry, count := bits.Mul64(expLen-32, multiplier)
		if carry > 0 {
			return math.MaxUint64
		}
		iterationCount = count
	}
	// Add the MSB position - 1 if expHead is non-zero
	if bitLen := expHead.BitLen(); bitLen > 0 {
		count, carry := bits.Add64(iterationCount, uint64(bitLen-1), 0)
		if carry > 0 {
			return math.MaxUint64
		}
		iterationCount = count
	}

	return max(iterationCount, 1)
}

// byzantiumModexpGas calculates the gas cost for the modexp precompile using Byzantium rules.
func byzantiumModexpGas(baseLen, expLen, modLen uint64, expHead uint256.Int) uint64 {
	const (
		multiplier = 8
		divisor    = 20
	)

	maxLen := max(baseLen, modLen)
	multComplexity := byzantiumMultComplexity(maxLen)
	if multComplexity == math.MaxUint64 {
		return math.MaxUint64
	}
	iterationCount := modexpIterationCount(expLen, expHead, multiplier)

	// Calculate gas: (multComplexity * iterationCount) / divisor
	carry, gas := bits.Mul64(iterationCount, multComplexity)
	gas /= divisor
	if carry != 0 {
		return math.MaxUint64
	}
	return gas
}

// berlinModexpGas calculates the gas cost for the modexp precompile using Berlin rules.
func berlinModexpGas(baseLen, expLen, modLen uint64, expHead uint256.Int) uint64 {
	const (
		multiplier = 8
		divisor    = 3
		minGas     = 200
	)

	maxLen := max(baseLen, modLen)
	multComplexity := berlinMultComplexity(maxLen)
	if multComplexity == math.MaxUint64 {
		return math.MaxUint64
	}
	iterationCount := modexpIterationCount(expLen, expHead, multiplier)

	// Calculate gas: (multComplexity * iterationCount) / divisor
	carry, gas := bits.Mul64(iterationCount, multComplexity)
	gas /= divisor
	if carry != 0 {
		return math.MaxUint64
	}
	return max(gas, minGas)
}

// osakaModexpGas calculates the gas cost for the modexp precompile using Osaka rules.
func osakaModexpGas(baseLen, expLen, modLen uint64, expHead uint256.Int) uint64 {
	const (
		multiplier = 16
		minGas     = 500
	)

	maxLen := max(baseLen, modLen)
	multComplexity := osakaMultComplexity(maxLen)
	if multComplexity == math.MaxUint64 {
		return math.MaxUint64
	}
	iterationCount := modexpIterationCount(expLen, expHead, multiplier)

	// Calculate gas: multComplexity * iterationCount
	carry, gas := bits.Mul64(iterationCount, multComplexity)
	if carry != 0 {
		return math.MaxUint64
	}
	return max(gas, minGas)
}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bigModExp) RequiredGas(input []byte) uint64 {
	// Parse input lengths
	baseLenBig := new(uint256.Int).SetBytes(getData(input, 0, 32))
	expLenBig := new(uint256.Int).SetBytes(getData(input, 32, 32))
	modLenBig := new(uint256.Int).SetBytes(getData(input, 64, 32))

	// Convert to uint64, capping at max value
	baseLen := baseLenBig.Uint64()
	if !baseLenBig.IsUint64() {
		baseLen = math.MaxUint64
	}
	expLen := expLenBig.Uint64()
	if !expLenBig.IsUint64() {
		expLen = math.MaxUint64
	}
	modLen := modLenBig.Uint64()
	if !modLenBig.IsUint64() {
		modLen = math.MaxUint64
	}

	// Skip the header
	if len(input) > 96 {
		input = input[96:]
	} else {
		input = input[:0]
	}

	// Retrieve the head 32 bytes of exp for the adjusted exponent length
	var expHead uint256.Int
	if uint64(len(input)) > baseLen {
		if expLen > 32 {
			expHead.SetBytes(getData(input, baseLen, 32))
		} else {
			// TODO: Check that if expLen < baseLen, then getData will return an empty slice
			expHead.SetBytes(getData(input, baseLen, expLen))
		}
	}

	// Choose the appropriate gas calculation based on the EIP flags
	if c.eip7883 {
		return osakaModexpGas(baseLen, expLen, modLen, expHead)
	} else if c.eip2565 {
		return berlinModexpGas(baseLen, expLen, modLen, expHead)
	} else {
		return byzantiumModexpGas(baseLen, expLen, modLen, expHead)
	}
}

func (c *bigModExp) Run(input []byte) ([]byte, error) {
	var (
		baseLenBig       = new(big.Int).SetBytes(getData(input, 0, 32))
		expLenBig        = new(big.Int).SetBytes(getData(input, 32, 32))
		modLenBig        = new(big.Int).SetBytes(getData(input, 64, 32))
		baseLen          = baseLenBig.Uint64()
		expLen           = expLenBig.Uint64()
		modLen           = modLenBig.Uint64()
		inputLenOverflow = max(baseLenBig.BitLen(), expLenBig.BitLen(), modLenBig.BitLen()) > 64
	)
	if len(input) > 96 {
		input = input[96:]
	} else {
		input = input[:0]
	}

	// enforce size cap for inputs
	if c.eip7823 && (inputLenOverflow || max(baseLen, expLen, modLen) > 1024) {
		return nil, errors.New("one or more of base/exponent/modulus length exceeded 1024 bytes")
	}
	// Handle a special case when both the base and mod length is zero
	if baseLen == 0 && modLen == 0 {
		return []byte{}, nil
	}
	// Retrieve the operands and execute the exponentiation
	var (
		base = new(patched_big.Int).SetBytes(getData(input, 0, baseLen))
		exp  = new(patched_big.Int).SetBytes(getData(input, baseLen, expLen))
		mod  = new(patched_big.Int).SetBytes(getData(input, baseLen+expLen, modLen))
		v    []byte
	)
	switch {
	case mod.BitLen() == 0:
		// Modulo 0 is undefined, return zero
		return common.LeftPadBytes([]byte{}, int(modLen)), nil
	case base.BitLen() == 1: // a bit length of 1 means it's 1 (or -1).
		//If base == 1, then we can just return base % mod (if mod >= 1, which it is)
		v = base.Mod(base, mod).Bytes()
	default:
		v = base.Exp(base, exp, mod).Bytes()
	}
	return common.LeftPadBytes(v, int(modLen)), nil
}

func (c *bigModExp) Name() string {
	return "MODEXP"
}

// newCurvePoint unmarshals a binary blob into a bn256 elliptic curve point,
// returning it, or an error if the point is invalid.
func newCurvePoint(blob []byte) (*bn256.G1, error) {
	p := new(bn256.G1)
	if _, err := p.Unmarshal(blob); err != nil {
		return nil, err
	}
	return p, nil
}

// newTwistPoint unmarshals a binary blob into a bn256 elliptic curve point,
// returning it, or an error if the point is invalid.
func newTwistPoint(blob []byte) (*bn256.G2, error) {
	p := new(bn256.G2)
	if _, err := p.Unmarshal(blob); err != nil {
		return nil, err
	}
	return p, nil
}

// runBn256Add implements the Bn256Add precompile, referenced by both
// Byzantium and Istanbul operations.
func runBn256Add(input []byte) ([]byte, error) {
	x, err := newCurvePoint(getData(input, 0, 64))
	if err != nil {
		return nil, err
	}
	y, err := newCurvePoint(getData(input, 64, 64))
	if err != nil {
		return nil, err
	}
	res := new(bn256.G1)
	res.Add(x, y)
	return res.Marshal(), nil
}

// bn256AddIstanbul implements a native elliptic curve point addition conforming to
// Istanbul consensus rules.
type bn256AddIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256AddIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn256AddGasIstanbul
}

func (c *bn256AddIstanbul) Run(input []byte) ([]byte, error) {
	return runBn256Add(input)
}

func (c *bn256AddIstanbul) Name() string {
	return "BN254_ADD"
}

// bn256AddByzantium implements a native elliptic curve point addition
// conforming to Byzantium consensus rules.
type bn256AddByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256AddByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn256AddGasByzantium
}

func (c *bn256AddByzantium) Run(input []byte) ([]byte, error) {
	return runBn256Add(input)
}

func (c *bn256AddByzantium) Name() string {
	return "BN254_ADD"
}

// runBn256ScalarMul implements the Bn256ScalarMul precompile, referenced by
// both Byzantium and Istanbul operations.
func runBn256ScalarMul(input []byte) ([]byte, error) {
	p, err := newCurvePoint(getData(input, 0, 64))
	if err != nil {
		return nil, err
	}
	res := new(bn256.G1)
	res.ScalarMult(p, new(big.Int).SetBytes(getData(input, 64, 32)))
	return res.Marshal(), nil
}

// bn256ScalarMulIstanbul implements a native elliptic curve scalar
// multiplication conforming to Istanbul consensus rules.
type bn256ScalarMulIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256ScalarMulIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn256ScalarMulGasIstanbul
}

func (c *bn256ScalarMulIstanbul) Run(input []byte) ([]byte, error) {
	return runBn256ScalarMul(input)
}

func (c *bn256ScalarMulIstanbul) Name() string {
	return "BN254_MUL"
}

// bn256ScalarMulByzantium implements a native elliptic curve scalar
// multiplication conforming to Byzantium consensus rules.
type bn256ScalarMulByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256ScalarMulByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn256ScalarMulGasByzantium
}

func (c *bn256ScalarMulByzantium) Run(input []byte) ([]byte, error) {
	return runBn256ScalarMul(input)
}

func (c *bn256ScalarMulByzantium) Name() string {
	return "BN254_MUL"
}

var (
	// true32Byte is returned if the bn256 pairing check succeeds.
	true32Byte = []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}

	// false32Byte is returned if the bn256 pairing check fails.
	false32Byte = make([]byte, 32)

	// errBadPairingInput is returned if the bn256 pairing input is invalid.
	errBadPairingInput = errors.New("bad elliptic curve pairing size")
)

// runBn256Pairing implements the Bn256Pairing precompile, referenced by both
// Byzantium and Istanbul operations.
func runBn256Pairing(input []byte) ([]byte, error) {
	// Handle some corner cases cheaply
	if len(input)%192 > 0 {
		return nil, errBadPairingInput
	}
	// Convert the input into a set of coordinates
	var (
		cs []*bn256.G1
		ts []*bn256.G2
	)
	for i := 0; i < len(input); i += 192 {
		c, err := newCurvePoint(input[i : i+64])
		if err != nil {
			return nil, err
		}
		t, err := newTwistPoint(input[i+64 : i+192])
		if err != nil {
			return nil, err
		}
		cs = append(cs, c)
		ts = append(ts, t)
	}
	// Execute the pairing checks and return the results
	if bn256.PairingCheck(cs, ts) {
		return true32Byte, nil
	}
	return false32Byte, nil
}

// bn256PairingIstanbul implements a pairing pre-compile for the bn256 curve
// conforming to Istanbul consensus rules.
type bn256PairingIstanbul struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256PairingIstanbul) RequiredGas(input []byte) uint64 {
	return params.Bn256PairingBaseGasIstanbul + uint64(len(input)/192)*params.Bn256PairingPerPointGasIstanbul
}

func (c *bn256PairingIstanbul) Run(input []byte) ([]byte, error) {
	return runBn256Pairing(input)
}

func (c *bn256PairingIstanbul) Name() string {
	return "BN254_PAIRING"
}

// bn256PairingByzantium implements a pairing pre-compile for the bn256 curve
// conforming to Byzantium consensus rules.
type bn256PairingByzantium struct{}

// RequiredGas returns the gas required to execute the pre-compiled contract.
func (c *bn256PairingByzantium) RequiredGas(input []byte) uint64 {
	return params.Bn256PairingBaseGasByzantium + uint64(len(input)/192)*params.Bn256PairingPerPointGasByzantium
}

func (c *bn256PairingByzantium) Run(input []byte) ([]byte, error) {
	return runBn256Pairing(input)
}

func (c *bn256PairingByzantium) Name() string {
	return "BN254_PAIRING"
}

type blake2F struct{}

func (c *blake2F) RequiredGas(input []byte) uint64 {
	// If the input is malformed, we can't calculate the gas, return 0 and let the
	// actual call choke and fault.
	if len(input) != blake2FInputLength {
		return 0
	}
	return uint64(binary.BigEndian.Uint32(input[0:4]))
}

const (
	blake2FInputLength        = 213
	blake2FFinalBlockBytes    = byte(1)
	blake2FNonFinalBlockBytes = byte(0)
)

var (
	errBlake2FInvalidInputLength = errors.New("invalid input length")
	errBlake2FInvalidFinalFlag   = errors.New("invalid final flag")
)

func (c *blake2F) Run(input []byte) ([]byte, error) {
	// Make sure the input is valid (correct length and final flag)
	if len(input) != blake2FInputLength {
		return nil, errBlake2FInvalidInputLength
	}
	if input[212] != blake2FNonFinalBlockBytes && input[212] != blake2FFinalBlockBytes {
		return nil, errBlake2FInvalidFinalFlag
	}
	// Parse the input into the Blake2b call parameters
	var (
		rounds = binary.BigEndian.Uint32(input[0:4])
		final  = input[212] == blake2FFinalBlockBytes

		h [8]uint64
		m [16]uint64
		t [2]uint64
	)
	for i := 0; i < 8; i++ {
		offset := 4 + i*8
		h[i] = binary.LittleEndian.Uint64(input[offset : offset+8])
	}
	for i := 0; i < 16; i++ {
		offset := 68 + i*8
		m[i] = binary.LittleEndian.Uint64(input[offset : offset+8])
	}
	t[0] = binary.LittleEndian.Uint64(input[196:204])
	t[1] = binary.LittleEndian.Uint64(input[204:212])

	// Execute the compression function, extract and return the result
	blake2b.F(&h, m, t, final, rounds)

	output := make([]byte, 64)
	for i := 0; i < 8; i++ {
		offset := i * 8
		binary.LittleEndian.PutUint64(output[offset:offset+8], h[i])
	}
	return output, nil
}

func (c *blake2F) Name() string {
	return "BLAKE2F"
}

// kzgPointEvaluation implements the EIP-4844 point evaluation precompile.
type kzgPointEvaluation struct{}

// RequiredGas estimates the gas required for running the point evaluation precompile.
func (b *kzgPointEvaluation) RequiredGas(input []byte) uint64 {
	return params.BlobTxPointEvaluationPrecompileGas
}

const (
	blobVerifyInputLength           = 192  // Max input length for the point evaluation precompile.
	blobCommitmentVersionKZG  uint8 = 0x01 // Version byte for the point evaluation precompile.
	blobPrecompileReturnValue       = "000000000000000000000000000000000000000000000000000000000000100073eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001"
)

var (
	errBlobVerifyInvalidInputLength = errors.New("invalid input length")
	errBlobVerifyMismatchedVersion  = errors.New("mismatched versioned hash")
	errBlobVerifyKZGProof           = errors.New("error verifying kzg proof")
)

// Run executes the point evaluation precompile.
func (b *kzgPointEvaluation) Run(input []byte) ([]byte, error) {
	if len(input) != blobVerifyInputLength {
		return nil, errBlobVerifyInvalidInputLength
	}
	// versioned hash: first 32 bytes
	var versionedHash common.Hash
	copy(versionedHash[:], input[:])

	var (
		point kzg4844.Point
		claim kzg4844.Claim
	)
	// Evaluation point: next 32 bytes
	copy(point[:], input[32:])
	// Expected output: next 32 bytes
	copy(claim[:], input[64:])

	// input kzg point: next 48 bytes
	var commitment kzg4844.Commitment
	copy(commitment[:], input[96:])
	if kZGToVersionedHash(commitment) != versionedHash {
		return nil, errBlobVerifyMismatchedVersion
	}

	// Proof: next 48 bytes
	var proof kzg4844.Proof
	copy(proof[:], input[144:])

	if err := kzg4844.VerifyProof(commitment, point, claim, proof); err != nil {
		return nil, fmt.Errorf("%w: %v", errBlobVerifyKZGProof, err)
	}

	return common.Hex2Bytes(blobPrecompileReturnValue), nil
}

func (b *kzgPointEvaluation) Name() string {
	return "KZG_POINT_EVALUATION"
}

// kZGToVersionedHash implements kzg_to_versioned_hash from EIP-4844
func kZGToVersionedHash(kzg kzg4844.Commitment) common.Hash {
	h := sha256.Sum256(kzg[:])
	h[0] = blobCommitmentVersionKZG

	return h
}
