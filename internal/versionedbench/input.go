// Package versionedbench compares physical stores on identical synthetic block
// updates. It does not import or execute the EVM.
package versionedbench

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
)

type Config struct {
	Schema                                          string
	Seed                                            int64
	Keys, Blocks, UpdatesPerBlock, QueriesPerHeight int
	ValueBytes                                      []int
	Patterns                                        []string
	HotFraction, HotProbability                     float64
}

func DefaultConfig() Config {
	return Config{"versioned-trie-storage-bench/v1", 20261101, 10000, 100, 100, 4, []int{32, 105}, []string{"uniform", "hot"}, 0.01, 0.8}
}
func (c Config) Validate() error {
	if c.Schema != "versioned-trie-storage-bench/v1" || c.Keys < 10 || c.Keys > 100000 || c.Blocks < 1 || c.Blocks > 1000 || c.UpdatesPerBlock < 1 || c.UpdatesPerBlock > 10000 || c.Blocks*c.UpdatesPerBlock > 1000000 || c.QueriesPerHeight < 1 || c.QueriesPerHeight > 100 || len(c.ValueBytes) == 0 || len(c.Patterns) == 0 || c.HotFraction <= 0 || c.HotFraction >= 1 || c.HotProbability < 0 || c.HotProbability > 1 || math.IsNaN(c.HotFraction) || math.IsNaN(c.HotProbability) {
		return fmt.Errorf("invalid synthetic config")
	}
	seen := map[int]bool{}
	for _, n := range c.ValueBytes {
		if n < 1 || n > 1024 || seen[n] {
			return fmt.Errorf("invalid/duplicate value width")
		}
		seen[n] = true
	}
	patterns := map[string]bool{}
	for _, p := range c.Patterns {
		if (p != "uniform" && p != "hot") || patterns[p] {
			return fmt.Errorf("invalid/duplicate pattern")
		}
		patterns[p] = true
	}
	return nil
}

type Spec struct {
	Config     Config
	ValueBytes int
	Pattern    string
}
type Mutation struct {
	Index int
	Value []byte
}
type Trace struct {
	Keys              [][32]byte
	Initial           [][]byte
	Blocks            [][]Mutation
	ID                string
	UniqueChangedKeys int
}

func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func Generate(spec Spec) (Trace, error) {
	var tr Trace
	if e := spec.Config.Validate(); e != nil {
		return tr, e
	}
	if spec.ValueBytes < 1 || spec.ValueBytes > 1024 || (spec.Pattern != "uniform" && spec.Pattern != "hot") {
		return tr, fmt.Errorf("invalid case")
	}
	c := spec.Config
	header, _ := json.Marshal(spec)
	hasher := sha256.New()
	hasher.Write([]byte("synthetic-block-writes/v1\x00"))
	hasher.Write(header)
	tr.Keys = make([][32]byte, c.Keys)
	tr.Initial = make([][]byte, c.Keys)
	value := func(height, operation, index int) []byte {
		out := make([]byte, spec.ValueBytes)
		for offset := 0; offset < len(out); offset += 32 {
			var b [40]byte
			for i, n := range []uint64{uint64(c.Seed), uint64(height), uint64(operation), uint64(index), uint64(offset)} {
				binary.BigEndian.PutUint64(b[i*8:], n)
			}
			h := sha256.Sum256(b[:])
			copy(out[offset:], h[:])
		}
		return out
	}
	for i := range tr.Keys {
		var b [16]byte
		binary.BigEndian.PutUint64(b[:8], uint64(c.Seed))
		binary.BigEndian.PutUint64(b[8:], uint64(i))
		tr.Keys[i] = sha256.Sum256(append([]byte("key/"), b[:]...))
		tr.Initial[i] = value(0, i, i)
		hasher.Write(tr.Keys[i][:])
		hasher.Write(tr.Initial[i])
	}
	rng := rand.New(rand.NewSource(c.Seed))
	hot := int(float64(c.Keys) * c.HotFraction)
	if hot < 1 {
		hot = 1
	}
	if hot >= c.Keys {
		hot = c.Keys - 1
	}
	current := make([][]byte, len(tr.Initial))
	copy(current, tr.Initial)
	tr.Blocks = make([][]Mutation, c.Blocks)
	for h := 1; h <= c.Blocks; h++ {
		changed := map[int][]byte{}
		for op := 0; op < c.UpdatesPerBlock; op++ {
			index := rng.Intn(c.Keys)
			if spec.Pattern == "hot" {
				if rng.Float64() < c.HotProbability {
					index = rng.Intn(hot)
				} else {
					index = hot + rng.Intn(c.Keys-hot)
				}
			}
			v := value(h, op, index)
			tr.Blocks[h-1] = append(tr.Blocks[h-1], Mutation{index, v})
			changed[index] = v
			hasher.Write(tr.Keys[index][:])
			hasher.Write(v)
		}
		for index, final := range changed {
			if !bytes.Equal(current[index], final) {
				tr.UniqueChangedKeys++
			}
			current[index] = final
		}
	}
	tr.ID = hex.EncodeToString(hasher.Sum(nil))
	return tr, nil
}
