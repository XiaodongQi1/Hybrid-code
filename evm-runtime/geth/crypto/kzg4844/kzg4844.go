// Copyright 2023 The go-ethereum Authors
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

// Package kzg4844 implements the KZG crypto for EIP-4844.
// Only proof verification needed by the Cancun point-evaluation precompile.
package kzg4844

import (
	"embed"
	"encoding/json"
	gokzg4844 "github.com/crate-crypto/go-eth-kzg"
	"sync"
)

//go:embed trusted_setup.json
var content embed.FS

type Commitment [48]byte
type Proof [48]byte
type Point [32]byte
type Claim [32]byte

var context *gokzg4844.Context
var initOnce sync.Once

func initContext() {
	data, err := content.ReadFile("trusted_setup.json")
	if err != nil {
		panic(err)
	}
	setup := new(gokzg4844.JSONTrustedSetup)
	if err = json.Unmarshal(data, setup); err != nil {
		panic(err)
	}
	context, err = gokzg4844.NewContext4096(setup)
	if err != nil {
		panic(err)
	}
}
func VerifyProof(commitment Commitment, point Point, claim Claim, proof Proof) error {
	initOnce.Do(initContext)
	return context.VerifyKZGProof(gokzg4844.KZGCommitment(commitment), gokzg4844.Scalar(point), gokzg4844.Scalar(claim), gokzg4844.KZGProof(proof))
}
func IsValidVersionedHash(h []byte) bool { return len(h) == 32 && h[0] == 0x01 }
