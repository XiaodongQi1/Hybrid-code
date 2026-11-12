package triecodec

import (
	"errors"
	"golang.org/x/crypto/sha3"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

type Config struct {
	MaxNodeBytes   uint64
	MaxPathNibbles uint32
}

func DefaultConfig() Config { return Config{16 << 20, 64} }

var ErrFormat = errors.New("trie: malformed node")
var ErrLimit = errors.New("trie: resource limit")
var ErrHashMismatch = errors.New("trie: hash mismatch")

func NodeHash(b []byte) (h mpt.Hash) {
	x := sha3.NewLegacyKeccak256()
	x.Write(b)
	x.Sum(h[:0])
	return
}
