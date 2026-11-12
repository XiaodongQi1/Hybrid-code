package patharchive

import (
	"bytes"
	"encoding/binary"
	"math/bits"

	"github.com/XiaodongQi1/Hybrid-code/evm-state/codec"
	ds "github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// Each v2 key is self-delimiting: F has a fixed 32-byte key; P has a
// one-byte nibble count and two nibbles per byte. Owner length is tagged.
func (r reader) key(tag byte, owner, path []byte) []byte {
	if r.format == LegacyFormat || tag != 'P' {
		return latestKey(tag, owner, path)
	}
	b := append(append([]byte{tag}, owner...), byte(len(path)))
	for i := 0; i < len(path); i += 2 {
		v := path[i] << 4
		if i+1 < len(path) {
			v |= path[i+1]
		}
		b = append(b, v)
	}
	return b
}
func (r reader) historyPrefix(k []byte) []byte {
	if r.format == LegacyFormat {
		return historyPrefix(k)
	}
	return append([]byte{'D'}, k...)
}

// Length-prefixed big endian preserves numeric order for LevelDB Seek. Plain
// base-128 varints do not, and would silently select the wrong before-image.
func orderedHeight(h uint64) []byte {
	n := (bits.Len64(h) + 7) / 8
	b := make([]byte, n+1)
	b[0] = byte(n)
	for i := n; i > 0; i-- {
		b[i] = byte(h)
		h >>= 8
	}
	return b
}
func (r reader) heightBytes(h uint64) []byte {
	if r.format == LegacyFormat {
		return num(h)
	}
	return orderedHeight(h)
}
func (r reader) validHeight(b []byte) bool {
	if r.format == LegacyFormat {
		return len(b) == 8
	}
	return len(b) > 0 && b[0] <= 8 && len(b) == int(b[0])+1 && (b[0] == 0 || b[1] != 0)
}

// Flat encoding only. Canonical trie leaves and their hashes stay unchanged.
// Account: one byte (root/code present + six-bit balance length), nonce uvarint, significant
// bytes, optional 32-byte root/code. Slot: significant big-endian bytes.
// An empty history value denotes absence; present zero slots are forbidden.
func encodeValue(k, b []byte) ([]byte, error) {
	return encodeValueWithCodec(k, b, codec.Legacy)
}

func encodeValueWithCodec(k, b []byte, mode codec.Mode) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if k[0] != 'F' {
		return bytes.Clone(b), nil
	}
	if k[1] == 1 {
		word, e := mode.DecodeStorage(b)
		if e != nil {
			return nil, e
		}
		v := bytes.TrimLeft(word[:], "\x00")
		if len(v) == 0 {
			return nil, ErrCorrupt
		}
		return bytes.Clone(v), nil
	}
	if k[1] != 0 {
		return nil, ErrCorrupt
	}
	a, e := mode.DecodeAccount(b)
	if e != nil {
		return nil, e
	}
	flags := byte(0)
	if a.StorageRoot != mpt.EmptyRootHash {
		flags |= 1
	}
	if a.CodeHash != ds.EmptyCode {
		flags |= 2
	}
	bal := a.Balance.Bytes32()
	v := bytes.TrimLeft(bal[:], "\x00")
	flags |= byte(len(v)) << 2
	out := binary.AppendUvarint([]byte{flags}, a.Nonce)
	out = append(out, v...)
	if flags&1 != 0 {
		out = append(out, a.StorageRoot[:]...)
	}
	if flags&2 != 0 {
		out = append(out, a.CodeHash[:]...)
	}
	return out, nil
}
func decodeValue(k, b []byte) ([]byte, error) {
	return decodeValueWithCodec(k, b, codec.Legacy)
}

func decodeValueWithCodec(k, b []byte, mode codec.Mode) ([]byte, error) {
	if len(b) == 0 {
		return nil, nil
	}
	if k[0] != 'F' {
		return bytes.Clone(b), nil
	}
	if k[1] == 1 {
		if len(b) > 32 || b[0] == 0 {
			return nil, ErrCorrupt
		}
		var word [32]byte
		copy(word[32-len(b):], b)
		return mode.EncodeStorage(word)
	}
	if k[1] != 0 || len(b) < 2 || b[0]>>2 > 32 {
		return nil, ErrCorrupt
	}
	original := b
	flags := b[0]
	a := ds.EmptyAccount()
	nonce, n := binary.Uvarint(b[1:])
	if n <= 0 {
		return nil, ErrCorrupt
	}
	a.Nonce = nonce
	b = b[1+n:]
	size := int(flags >> 2)
	if len(b) < size {
		return nil, ErrCorrupt
	}
	a.Balance.SetBytes(b[:size])
	b = b[size:]
	if flags&1 != 0 {
		if len(b) < 32 {
			return nil, ErrCorrupt
		}
		copy(a.StorageRoot[:], b[:32])
		b = b[32:]
	}
	if flags&2 != 0 {
		if len(b) < 32 {
			return nil, ErrCorrupt
		}
		copy(a.CodeHash[:], b[:32])
		b = b[32:]
	}
	if len(b) != 0 {
		return nil, ErrCorrupt
	}
	raw, e := mode.EncodeAccount(a)
	if e != nil {
		return nil, e
	}
	canonical, e := encodeValueWithCodec(k, raw, mode)
	if e != nil || !bytes.Equal(canonical, original) {
		return nil, ErrCorrupt
	}
	return raw, nil
}
