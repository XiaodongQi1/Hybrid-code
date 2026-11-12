package mpt

import (
	"bytes"
	"fmt"
	"sync/atomic"

	"golang.org/x/crypto/sha3"
)

type Hash [32]byte

var EmptyRootHash = Hash{
	0x56, 0xe8, 0x1f, 0x17, 0x1b, 0xcc, 0x55, 0xa6,
	0xff, 0x83, 0x45, 0xe6, 0x92, 0xc0, 0xf8, 0x6e,
	0x5b, 0x48, 0xe0, 0x1b, 0x99, 0x6c, 0xad, 0xc0,
	0x01, 0x62, 0x2f, 0xb5, 0xe3, 0x63, 0xb4, 0x21,
}

type node interface{ isNode() }

type leafNode struct {
	persistRevision uint64
	cache           atomic.Pointer[cachedDigest]
	path            []byte
	value           []byte
}

type extensionNode struct {
	persistRevision uint64
	cache           atomic.Pointer[cachedDigest]
	path            []byte
	child           node
}

type branchNode struct {
	persistRevision uint64
	cache           atomic.Pointer[cachedDigest]
	children        [16]node
	value           []byte
}

type hashNode Hash

func (*leafNode) isNode()      {}
func (*extensionNode) isNode() {}
func (*branchNode) isNode()    {}
func (hashNode) isNode()       {}

func hashBytes(value []byte) Hash {
	hasher := sha3.NewLegacyKeccak256()
	_, _ = hasher.Write(value)
	var hash Hash
	hasher.Sum(hash[:0])
	return hash
}

// encodeNode keeps staging independent of cache validity: collected is filled
// by a separate reachability traversal, even when all hashes are already clean.
func encodeNode(n node, collected map[Hash][]byte) ([]byte, Hash, error) {
	encoded, hash, err := encodeCached(n, nil)
	if err != nil {
		return nil, Hash{}, err
	}
	if collected != nil {
		if err := collectEncodedNodes(n, collected, false); err != nil {
			return nil, Hash{}, err
		}
	}
	return encoded, hash, nil
}

// hashCached stops at every clean subtree without retaining or rebuilding RLP.
func hashCached(n node, work *HashWork) (Hash, uint32, error) {
	if work != nil {
		work.VisitedNodes++
	}
	if n == nil {
		return EmptyRootHash, 1, nil
	}
	if h, ok, err := resolvedReferenceHash(n); ok {
		if err != nil {
			return Hash{}, 0, err
		}
		if work != nil {
			work.CacheHits++
		}
		return Hash(h), 0, nil
	}
	cache := digestCache(n)
	if cache == nil {
		return Hash{}, 0, fmt.Errorf("mpt: unsupported node %T", n)
	}
	if hit := cache.Load(); hit != nil {
		if work != nil {
			work.CacheHits++
		}
		return hit.hash, hit.size, nil
	}
	raw, h, err := encodeCurrent(n, work)
	return h, uint32(len(raw)), err
}

func encodeCached(n node, work *HashWork) ([]byte, Hash, error) {
	if work != nil {
		work.VisitedNodes++
	}
	if n == nil {
		return rlpEncodeBytes(nil), EmptyRootHash, nil
	}
	if h, ok, err := resolvedReferenceHash(n); ok {
		if err != nil {
			return nil, Hash{}, err
		}
		if work != nil {
			work.CacheHits++
		}
		return nil, Hash(h), nil
	}
	return encodeCurrent(n, work)
}

// encodeCurrent creates a temporary encoding of this node. Unchanged hashed
// children contribute their cached digest; only inline children need bytes.
func encodeCurrent(n node, work *HashWork) ([]byte, Hash, error) {
	cache := digestCache(n)
	if cache == nil {
		return nil, Hash{}, fmt.Errorf("mpt: unsupported node %T", n)
	}
	var encoded []byte
	switch n := n.(type) {
	case *leafNode:
		encoded = rlpEncodeList(rlpEncodeBytes(compactEncode(n.path, true)), rlpEncodeBytes(n.value))
	case *extensionNode:
		child, err := encodeCachedReference(n.child, work)
		if err != nil {
			return nil, Hash{}, err
		}
		encoded = rlpEncodeList(rlpEncodeBytes(compactEncode(n.path, false)), child)
	case *branchNode:
		elements := make([][]byte, 17)
		for i, child := range n.children {
			ref, err := encodeCachedReference(child, work)
			if err != nil {
				return nil, Hash{}, err
			}
			elements[i] = ref
		}
		elements[16] = rlpEncodeBytes(n.value)
		encoded = rlpEncodeList(elements...)
	}
	if work != nil {
		work.EncodedNodes++
	}
	if hit := cache.Load(); hit != nil {
		if work != nil {
			work.CacheHits++
		}
		return encoded, hit.hash, nil
	}
	hash := hashBytes(encoded)
	if work != nil {
		work.HashedNodes++
	}
	result := publishDigest(n, hash, uint32(len(encoded)))
	return encoded, result.hash, nil
}

func encodeCachedReference(n node, work *HashWork) ([]byte, error) {
	if n == nil {
		return rlpEncodeBytes(nil), nil
	}
	if h, ok, err := resolvedReferenceHash(n); ok {
		if err != nil {
			return nil, err
		}
		return rlpEncodeBytes(h[:]), nil
	}
	if work != nil {
		work.VisitedNodes++
	}
	cache := digestCache(n)
	if cache == nil {
		return nil, fmt.Errorf("mpt: unsupported node %T", n)
	}
	if hit := cache.Load(); hit != nil && hit.size >= 32 {
		if work != nil {
			work.CacheHits++
		}
		return rlpEncodeBytes(hit.hash[:]), nil
	}
	encoded, hash, err := encodeCurrent(n, work)
	if err != nil {
		return nil, err
	}
	if len(encoded) < 32 {
		return encoded, nil
	}
	return rlpEncodeBytes(hash[:]), nil
}

// collectEncodedNodes is a per-prepare pending write plan, NOT a dirty-hash
// traversal. hashNode references are already backed by the caller's store;
// every reachable object node may still be uncommitted, including clean ones.
func collectEncodedNodes(n node, out map[Hash][]byte, root bool) error {
	if n == nil {
		return nil
	}
	if independentReference(n) {
		return nil
	}
	encoded, hash, err := encodeCached(n, nil)
	if err != nil {
		return err
	}
	if root || len(encoded) >= 32 {
		out[hash] = encoded
	}
	switch n := n.(type) {
	case *extensionNode:
		return collectEncodedNodes(n.child, out, false)
	case *branchNode:
		for _, child := range n.children {
			if err := collectEncodedNodes(child, out, false); err != nil {
				return err
			}
		}
	}
	return nil
}

func decodeNode(encoded []byte) (node, error) {
	item, rest, err := rlpSplit(encoded)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 || item.kind != rlpList {
		return nil, fmt.Errorf("mpt: node is not one complete rlp list")
	}
	items, err := rlpListItems(item)
	if err != nil {
		return nil, err
	}
	switch len(items) {
	case 2:
		if items[0].kind != rlpString {
			return nil, fmt.Errorf("mpt: path is not an rlp string")
		}
		path, leaf, err := compactDecode(items[0].payload)
		if err != nil {
			return nil, err
		}
		if leaf {
			if items[1].kind != rlpString {
				return nil, fmt.Errorf("mpt: leaf value is not an rlp string")
			}
			return &leafNode{path: path, value: cloneBytes(items[1].payload)}, nil
		}
		if len(path) == 0 {
			return nil, fmt.Errorf("mpt: empty extension path")
		}
		child, err := decodeReference(items[1])
		if err != nil {
			return nil, err
		}
		if child == nil {
			return nil, fmt.Errorf("mpt: extension has no child")
		}
		return &extensionNode{path: path, child: child}, nil
	case 17:
		branch := new(branchNode)
		for i := 0; i < 16; i++ {
			child, err := decodeReference(items[i])
			if err != nil {
				return nil, err
			}
			branch.children[i] = child
		}
		if items[16].kind != rlpString {
			return nil, fmt.Errorf("mpt: branch value is not an rlp string")
		}
		branch.value = cloneBytes(items[16].payload)
		return branch, nil
	default:
		return nil, fmt.Errorf("mpt: invalid node element count %d", len(items))
	}
}

func decodeReference(item rlpItem) (node, error) {
	if item.kind == rlpList {
		return decodeNode(item.raw)
	}
	if len(item.payload) == 0 {
		return nil, nil
	}
	if len(item.payload) != len(Hash{}) {
		return nil, fmt.Errorf("mpt: invalid child reference length %d", len(item.payload))
	}
	var hash Hash
	copy(hash[:], item.payload)
	return hashNode(hash), nil
}

func equalPath(a, b []byte) bool { return bytes.Equal(a, b) }
