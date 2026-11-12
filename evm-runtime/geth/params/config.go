// Copyright 2016 The go-ethereum Authors
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

// Package params contains only bytecode execution rules and gas constants.
package params

import (
	"fmt"
	"math"
	"math/big"
)

// ChainConfig describes execution fork activation, without network identities,
// consensus engines, genesis hashes, sync rules or database compatibility checks.
type ChainConfig struct {
	ChainID                                               *big.Int
	HomesteadBlock, EIP150Block, EIP155Block, EIP158Block *big.Int
	ByzantiumBlock, ConstantinopleBlock, PetersburgBlock  *big.Int
	IstanbulBlock, BerlinBlock, LondonBlock               *big.Int
	ShanghaiTime, CancunTime                              *uint64
	// Unsupported fork markers are retained only for explicit rejection.
	PragueTime, OsakaTime, VerkleTime *uint64
	BlobScheduleConfig                *BlobScheduleConfig
}
type BlobConfig struct{ Max int }
type BlobScheduleConfig struct{ Cancun *BlobConfig }

// LondonConfig returns independent execution-only settings for legacy gas tests.
func LondonConfig(chainID *big.Int) *ChainConfig {
	var id *big.Int
	if chainID != nil {
		id = new(big.Int).Set(chainID)
	}
	return &ChainConfig{
		ChainID: id, HomesteadBlock: new(big.Int), EIP150Block: new(big.Int),
		EIP155Block: new(big.Int), EIP158Block: new(big.Int), ByzantiumBlock: new(big.Int),
		ConstantinopleBlock: new(big.Int), PetersburgBlock: new(big.Int),
		IstanbulBlock: new(big.Int), BerlinBlock: new(big.Int), LondonBlock: new(big.Int),
	}
}

// CancunConfig returns a fresh post-merge Cancun execution configuration.
func CancunConfig(chainID *big.Int) *ChainConfig {
	c := LondonConfig(chainID)
	shanghai, cancun := uint64(0), uint64(0)
	c.ShanghaiTime = &shanghai
	c.CancunTime = &cancun
	c.BlobScheduleConfig = &BlobScheduleConfig{Cancun: &BlobConfig{Max: 6}}
	return c
}
func activeBlock(fork, number *big.Int) bool {
	return fork != nil && number != nil && fork.Cmp(number) <= 0
}
func activeTime(fork *uint64, time uint64) bool         { return fork != nil && time >= *fork }
func (c *ChainConfig) IsHomestead(n *big.Int) bool      { return activeBlock(c.HomesteadBlock, n) }
func (c *ChainConfig) IsEIP150(n *big.Int) bool         { return activeBlock(c.EIP150Block, n) }
func (c *ChainConfig) IsEIP155(n *big.Int) bool         { return activeBlock(c.EIP155Block, n) }
func (c *ChainConfig) IsEIP158(n *big.Int) bool         { return activeBlock(c.EIP158Block, n) }
func (c *ChainConfig) IsByzantium(n *big.Int) bool      { return activeBlock(c.ByzantiumBlock, n) }
func (c *ChainConfig) IsConstantinople(n *big.Int) bool { return activeBlock(c.ConstantinopleBlock, n) }
func (c *ChainConfig) IsPetersburg(n *big.Int) bool {
	return activeBlock(c.PetersburgBlock, n) || c.PetersburgBlock == nil && c.IsConstantinople(n)
}
func (c *ChainConfig) IsIstanbul(n *big.Int) bool { return activeBlock(c.IstanbulBlock, n) }
func (c *ChainConfig) IsBerlin(n *big.Int) bool   { return activeBlock(c.BerlinBlock, n) }
func (c *ChainConfig) IsLondon(n *big.Int) bool   { return activeBlock(c.LondonBlock, n) }
func (c *ChainConfig) IsShanghai(n *big.Int, t uint64) bool {
	return c.IsLondon(n) && activeTime(c.ShanghaiTime, t)
}
func (c *ChainConfig) IsCancun(n *big.Int, t uint64) bool {
	return c.IsLondon(n) && activeTime(c.CancunTime, t)
}
func (c *ChainConfig) IsPrague(n *big.Int, t uint64) bool {
	return c.IsLondon(n) && activeTime(c.PragueTime, t)
}
func (c *ChainConfig) IsOsaka(n *big.Int, t uint64) bool {
	return c.IsLondon(n) && activeTime(c.OsakaTime, t)
}
func (c *ChainConfig) IsVerkle(n *big.Int, t uint64) bool {
	return c.IsLondon(n) && activeTime(c.VerkleTime, t)
}

// CheckConfigForkOrder validates the supported execution schedule and gas budget.
func (c *ChainConfig) CheckConfigForkOrder() error {
	if c.ChainID == nil || c.ChainID.Sign() <= 0 {
		return fmt.Errorf("positive chain ID required")
	}
	forks := []struct {
		name     string
		block    *big.Int
		optional bool
	}{
		{"homestead", c.HomesteadBlock, false}, {"eip150", c.EIP150Block, false},
		{"eip155", c.EIP155Block, true}, {"eip158", c.EIP158Block, false},
		{"byzantium", c.ByzantiumBlock, false}, {"constantinople", c.ConstantinopleBlock, false},
		{"petersburg", c.PetersburgBlock, true}, {"istanbul", c.IstanbulBlock, false},
		{"berlin", c.BerlinBlock, false}, {"london", c.LondonBlock, false},
	}
	last := forks[0]
	for i, f := range forks {
		if f.block != nil && f.block.Sign() < 0 {
			return fmt.Errorf("negative %s activation", f.name)
		}
		if i > 0 && f.block != nil {
			if last.block == nil {
				return fmt.Errorf("%s requires %s", f.name, last.name)
			}
			if f.block.Cmp(last.block) < 0 {
				return fmt.Errorf("%s activates before %s", f.name, last.name)
			}
		}
		if !f.optional || f.block != nil {
			last = f
		}
	}
	if c.ShanghaiTime != nil && c.LondonBlock == nil {
		return fmt.Errorf("Shanghai requires London")
	}
	if c.CancunTime != nil && (c.ShanghaiTime == nil || *c.CancunTime < *c.ShanghaiTime) {
		return fmt.Errorf("Cancun requires prior Shanghai")
	}
	if s := c.BlobScheduleConfig; s != nil && s.Cancun != nil {
		if s.Cancun.Max <= 0 || uint64(s.Cancun.Max) > math.MaxUint64/BlobTxBlobGasPerBlob {
			return fmt.Errorf("invalid Cancun blob gas budget")
		}
	}
	return nil
}

// Rules is the immutable set of execution switches for one block context.
type Rules struct {
	ChainID                                                      *big.Int
	IsHomestead, IsEIP150, IsEIP155, IsEIP158                    bool
	IsByzantium, IsConstantinople, IsPetersburg, IsIstanbul      bool
	IsBerlin, IsEIP2929, IsLondon, IsMerge, IsShanghai, IsCancun bool
	IsPrague, IsOsaka, IsVerkle                                  bool // fail-fast markers, never execution modes
}

func (c *ChainConfig) Rules(number *big.Int, merge bool, time uint64) Rules {
	id := new(big.Int)
	if c.ChainID != nil {
		id.Set(c.ChainID)
	}
	merge = merge && c.IsLondon(number)
	return Rules{
		ChainID: id, IsHomestead: c.IsHomestead(number), IsEIP150: c.IsEIP150(number),
		IsEIP155: c.IsEIP155(number), IsEIP158: c.IsEIP158(number), IsByzantium: c.IsByzantium(number),
		IsConstantinople: c.IsConstantinople(number), IsPetersburg: c.IsPetersburg(number),
		IsIstanbul: c.IsIstanbul(number), IsBerlin: c.IsBerlin(number), IsEIP2929: c.IsBerlin(number),
		IsLondon: c.IsLondon(number), IsMerge: merge, IsShanghai: merge && c.IsShanghai(number, time),
		IsCancun: merge && c.IsCancun(number, time), IsPrague: merge && c.IsPrague(number, time),
		IsOsaka: merge && c.IsOsaka(number, time), IsVerkle: merge && c.IsVerkle(number, time),
	}
}
