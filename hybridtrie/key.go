package hybridtrie

import (
	"bytes"
)

const (
	TrieIDBytes                    = 6
	TrieIDBits                     = 48
	MaxTrieID               uint64 = 1<<TrieIDBits - 1
	AccountTrieID           uint64 = 0
	nodePositionHeaderBytes        = TrieIDBytes + 1
	HeightBytes                    = 6
	HeightBits                     = 48
	MaxHeight               uint64 = 1<<HeightBits - 1
)

// NodeKey identifies an occurrence, not just content. Path contains unpacked
// nibbles consumed before the node's own fragment. TrieID zero is reserved
// for the account trie; storage allocation/lifecycles belong to the store.
type NodeKey struct {
	TrieID uint64
	Path   []byte
	Height uint64
}

// NewNodeKey bounds external IDs and heights to 48 bits and owns its path.
func NewNodeKey(trieID uint64, path []byte, height uint64) (NodeKey, error) {
	if height > MaxHeight || trieID > MaxTrieID {
		return NodeKey{}, ErrLimit
	}
	if _, err := pack(path); err != nil {
		return NodeKey{}, err
	}
	return NodeKey{trieID, append([]byte(nil), path...), height}, nil
}

func pack(path []byte) ([]byte, error) {
	if len(path) > MaxDepth {
		return nil, ErrLimit
	}
	out := make([]byte, (len(path)+1)/2)
	for i, n := range path {
		if n > 15 {
			return nil, ErrCorrupt
		}
		if i%2 == 0 {
			out[i/2] = n << 4
		} else {
			out[i/2] |= n
		}
	}
	return out, nil
}

func unpack(raw []byte, n int) ([]byte, error) {
	if n < 0 || n > MaxDepth || len(raw) != (n+1)/2 {
		return nil, ErrCorrupt
	}
	if n%2 != 0 && raw[len(raw)-1]&15 != 0 {
		return nil, ErrCorrupt
	}
	out := make([]byte, n)
	for i := range out {
		out[i] = raw[i/2] >> 4
		if i%2 != 0 {
			out[i] = raw[i/2] & 15
		}
	}
	return out, nil
}

// EncodeNodeKey uses bytewise ordering: trieID6 | nibbleLen1 | path | height6.
// All 48 bits of the big-endian height field represent the block height.
// No node-type tag is included: a position may change MPT type across versions.
func EncodeNodeKey(key NodeKey) ([]byte, error) {
	return appendNodeKey(nil, key)
}

// appendNodeKey also serves operation-local reusable key buffers.
func appendNodeKey(dst []byte, key NodeKey) ([]byte, error) {
	if key.Height > MaxHeight || key.TrieID > MaxTrieID {
		return nil, ErrLimit
	}
	if len(key.Path) > MaxDepth {
		return nil, ErrLimit
	}
	start := len(dst)
	dst = append(dst, make([]byte, nodePositionHeaderBytes+(len(key.Path)+1)/2+HeightBytes)...)
	out := dst[start:]
	put48(out[:TrieIDBytes], key.TrieID)
	out[TrieIDBytes] = byte(len(key.Path))
	for i, n := range key.Path {
		if n > 15 {
			return nil, ErrCorrupt
		}
		out[nodePositionHeaderBytes+i/2] |= n << uint(4*(1-i%2))
	}
	put48(out[len(out)-HeightBytes:], key.Height)
	return dst, nil
}

// target must come from EncodeNodeKey/appendNodeKey. Exact encoded identity
// also checks odd-nibble padding, avoiding allocations when only a digest is needed.
func matchEncodedNodeKey(found, target []byte) error {
	n := len(target) - HeightBytes
	if n < nodePositionHeaderBytes || len(found) != len(target) || !bytes.Equal(found[:n], target[:n]) || bytes.Compare(found[n:], target[n:]) > 0 {
		return ErrPositionMismatch
	}
	return nil
}

func DecodeNodeKey(raw []byte) (NodeKey, error) {
	if len(raw) < nodePositionHeaderBytes+HeightBytes {
		return NodeKey{}, ErrCorrupt
	}
	n := int(raw[TrieIDBytes])
	if n > MaxDepth || len(raw) != nodePositionHeaderBytes+HeightBytes+(n+1)/2 {
		return NodeKey{}, ErrCorrupt
	}
	encodedHeight := raw[len(raw)-HeightBytes:]
	path, err := unpack(raw[nodePositionHeaderBytes:len(raw)-HeightBytes], n)
	if err != nil {
		return NodeKey{}, err
	}
	var height uint64
	for _, b := range encodedHeight {
		height = height<<8 | uint64(b)
	}
	return NodeKey{read48(raw[:TrieIDBytes]), path, height}, nil
}

// MatchPredecessor must follow SeekForPrev, even when an iterator has bounds.
// It checks the entire position and height bound, never accepting a neighbor.
func MatchPredecessor(raw []byte, target NodeKey) (NodeKey, error) {
	if target.Height > MaxHeight || target.TrieID > MaxTrieID {
		return NodeKey{}, ErrLimit
	}
	if _, err := pack(target.Path); err != nil {
		return NodeKey{}, err
	}
	found, err := DecodeNodeKey(raw)
	if err != nil {
		return NodeKey{}, err
	}
	if found.TrieID != target.TrieID || !bytes.Equal(found.Path, target.Path) || found.Height > target.Height {
		return NodeKey{}, ErrPositionMismatch
	}
	return found, nil
}

func put48(dst []byte, n uint64) {
	for i := 0; i < 6; i++ {
		dst[i] = byte(n >> uint(8*(5-i)))
	}
}
func read48(src []byte) uint64 {
	var n uint64
	for _, b := range src[:6] {
		n = n<<8 | uint64(b)
	}
	return n
}
