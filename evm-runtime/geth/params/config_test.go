package params

import (
	"math/big"
	"testing"
)

func TestExecutionRuleBoundaries(t *testing.T) {
	cfg := CancunConfig(big.NewInt(1337))
	shanghai, cancun := uint64(10), uint64(20)
	cfg.ShanghaiTime = &shanghai
	cfg.CancunTime = &cancun
	for _, tc := range []struct {
		time                    uint64
		merge, shanghai, cancun bool
	}{
		{9, true, false, false}, {10, true, true, false}, {19, true, true, false}, {20, true, true, true}, {20, false, false, false},
	} {
		r := cfg.Rules(big.NewInt(1), tc.merge, tc.time)
		if r.IsShanghai != tc.shanghai || r.IsCancun != tc.cancun || !r.IsLondon || !r.IsEIP2929 {
			t.Fatalf("rules at time %d merge %v: %+v", tc.time, tc.merge, r)
		}
	}
	cfg.LondonBlock = big.NewInt(2)
	if r := cfg.Rules(big.NewInt(1), true, 20); r.IsMerge || r.IsShanghai || r.IsCancun {
		t.Fatal("post-merge rules before London")
	}
}
func TestConfigCopiesAndOriginalRuleExceptions(t *testing.T) {
	id := big.NewInt(1337)
	a, b := CancunConfig(id), CancunConfig(id)
	id.SetInt64(7)
	a.ChainID.SetInt64(8)
	a.HomesteadBlock.SetInt64(100)
	*a.CancunTime = 42
	a.BlobScheduleConfig.Cancun.Max = 1
	if b.ChainID.Int64() != 1337 || b.HomesteadBlock.Sign() != 0 || *b.CancunTime != 0 || b.BlobScheduleConfig.Cancun.Max != 6 {
		t.Fatal("config alias")
	}
	b.PetersburgBlock = nil
	if !b.IsPetersburg(big.NewInt(0)) {
		t.Fatal("Petersburg fallback")
	}
	r := b.Rules(big.NewInt(0), true, 0)
	r.ChainID.SetInt64(0)
	if b.ChainID.Int64() != 1337 {
		t.Fatal("mutable rules chain ID")
	}
	if b.IsLondon(nil) {
		t.Fatal("nil block number activated fork")
	}
}
func TestRejectInvalidExecutionConfig(t *testing.T) {
	cases := map[string]func(*ChainConfig){
		"chain ID":             func(c *ChainConfig) { c.ChainID = big.NewInt(-1) },
		"negative block":       func(c *ChainConfig) { c.BerlinBlock = big.NewInt(-1) },
		"block order":          func(c *ChainConfig) { c.HomesteadBlock = big.NewInt(2) },
		"missing prerequisite": func(c *ChainConfig) { c.IstanbulBlock = nil },
		"timestamp order":      func(c *ChainConfig) { v := uint64(2); c.ShanghaiTime = &v },
		"negative blob budget": func(c *ChainConfig) { c.BlobScheduleConfig.Cancun.Max = -1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := CancunConfig(big.NewInt(1337))
			mutate(c)
			if err := c.CheckConfigForkOrder(); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
	c := CancunConfig(big.NewInt(1337))
	c.EIP155Block = nil
	c.PetersburgBlock = nil
	if err := c.CheckConfigForkOrder(); err != nil {
		t.Fatal("optional forks", err)
	}
}
