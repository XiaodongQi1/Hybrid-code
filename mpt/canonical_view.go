package mpt

import (
	"bytes"
	"fmt"
)

const MaxCanonicalNodeBytes = 1 << 20

type CanonicalKind uint8

const (
	CanonicalBranch CanonicalKind = iota
	CanonicalExtension
	CanonicalLeaf
)

// CanonicalReference describes an empty, hashed or inline child. An inline
// reference has Present=true, Inline populated and Hash zero.
type CanonicalReference struct {
	Present bool
	Hash    Hash
	Inline  []byte
}

// CanonicalNode describes encoding fields only, never node cache metadata.
// Path uses compact hex-prefix bytes. Extensions use Children[0] only.
type CanonicalNode struct {
	Kind        CanonicalKind
	Path, Value []byte
	Children    [16]CanonicalReference
}

// NodeIdentity distinguishes logical hosts at one already-scoped trie position.
// Values, child digests and branch occupancy may change without replacing that
// host. A different kind or compressed fragment denotes a different host.
// This local metadata never participates in RLP, hashes or persistent keys.
type NodeIdentity struct {
	Kind     CanonicalKind
	Fragment [64]byte
	Length   uint8
}

// InspectNodeIdentity extracts identity from already authenticated canonical
// RLP. It does not hash, rebuild RLP or authenticate child references; callers
// must keep their existing canonical validation and trusted-root checks.
func InspectNodeIdentity(raw []byte) (NodeIdentity, error) {
	var out NodeIdentity
	if len(raw) > MaxCanonicalNodeBytes {
		return out, ErrInvalidPath
	}
	item, rest, err := rlpSplit(raw)
	if err != nil || len(rest) != 0 || item.kind != rlpList {
		return out, ErrInvalidPath
	}
	items, err := rlpListItems(item)
	if err != nil {
		return out, err
	}
	if len(items) == 17 {
		out.Kind = CanonicalBranch
		return out, nil
	}
	if len(items) != 2 || items[0].kind != rlpString {
		return out, ErrInvalidPath
	}
	path, leaf, err := DecodeCompactPath(items[0].payload)
	if err != nil || !leaf && len(path) == 0 {
		return out, ErrInvalidPath
	}
	out.Kind = CanonicalExtension
	if leaf {
		out.Kind = CanonicalLeaf
	}
	out.Length = uint8(len(path))
	copy(out.Fragment[:], path)
	return out, nil
}

// EncodeCompactPath exposes the existing MPT hex-prefix encoder to physical
// adapters. Path is unpacked nibbles; local positions and heights are not encoded.
func EncodeCompactPath(path []byte, leaf bool) ([]byte, error) {
	if len(path) > 64 || !leaf && len(path) == 0 {
		return nil, ErrInvalidPath
	}
	for _, nibble := range path {
		if nibble > 15 {
			return nil, ErrInvalidPath
		}
	}
	return compactEncode(path, leaf), nil
}

func DecodeCompactPath(raw []byte) ([]byte, bool, error) {
	if len(raw) > 33 {
		return nil, false, ErrInvalidPath
	}
	p, leaf, e := compactDecode(raw)
	if e != nil || len(p) > 64 {
		return nil, false, ErrInvalidPath
	}
	return p, leaf, nil
}
func referenceView(n node) (CanonicalReference, error) {
	if n == nil {
		return CanonicalReference{}, nil
	}
	if h, ok, err := resolvedReferenceHash(n); ok {
		return CanonicalReference{Present: true, Hash: Hash(h)}, err
	}
	raw, _, e := encodeNode(n, nil)
	if e != nil {
		return CanonicalReference{}, e
	}
	if len(raw) >= 32 {
		return CanonicalReference{}, fmt.Errorf("mpt: oversized inline reference")
	}
	return CanonicalReference{Present: true, Inline: cloneBytes(raw)}, nil
}
func InspectCanonicalNode(raw []byte) (CanonicalNode, error) {
	d, _, err := InspectCanonicalNodeWithHash(raw)
	return d, err
}

// InspectCanonicalNodeWithHash authenticates canonical encoding once and
// returns its digest for callers that also bind the node to an expected hash.
func InspectCanonicalNodeWithHash(raw []byte) (CanonicalNode, Hash, error) {
	var out CanonicalNode
	if len(raw) > MaxCanonicalNodeBytes {
		return out, Hash{}, ErrInvalidPath
	}
	n, e := decodeNode(raw)
	if e != nil {
		return out, Hash{}, e
	}
	switch n := n.(type) {
	case *leafNode:
		out.Kind = CanonicalLeaf
		out.Path = compactEncode(n.path, true)
		out.Value = cloneBytes(n.value)
	case *extensionNode:
		out.Kind = CanonicalExtension
		out.Path = compactEncode(n.path, false)
		out.Children[0], e = referenceView(n.child)
	case *branchNode:
		out.Kind = CanonicalBranch
		out.Value = cloneBytes(n.value)
		for i, ch := range n.children {
			out.Children[i], e = referenceView(ch)
			if e != nil {
				return CanonicalNode{}, Hash{}, e
			}
		}
	default:
		return out, Hash{}, ErrInvalidPath
	}
	if e != nil {
		return CanonicalNode{}, Hash{}, e
	}
	canonical, hash, e := RebuildCanonicalNode(out)
	if e != nil || !bytes.Equal(canonical, raw) {
		return CanonicalNode{}, Hash{}, fmt.Errorf("mpt: noncanonical node")
	}
	return out, hash, nil
}
func referenceNode(ref CanonicalReference) (node, error) {
	if !ref.Present {
		if ref.Hash != (Hash{}) || len(ref.Inline) != 0 {
			return nil, ErrInvalidPath
		}
		return nil, nil
	}
	if len(ref.Inline) == 0 {
		return hashNode(ref.Hash), nil
	}
	if len(ref.Inline) >= 32 || ref.Hash != (Hash{}) {
		return nil, ErrInvalidPath
	}
	n, e := decodeNode(ref.Inline)
	if e != nil {
		return nil, e
	}
	raw, hash, e := encodeNode(n, nil)
	if e != nil || !bytes.Equal(raw, ref.Inline) {
		return nil, ErrInvalidPath
	}
	publishDigest(n, hash, uint32(len(raw)))
	return n, nil
}

// RebuildCanonicalNode uses the original private node types and encoder. Its
// result is canonical RLP and Keccak; local bitmap/ID/height are never encoded.
func nodeFromCanonicalFields(desc CanonicalNode, trusted bool) (node, error) {
	return nodeFromFieldsWithReferences(desc, trusted, nil)
}
func nodeFromFieldsWithReferences(desc CanonicalNode, trusted bool, refs *[16]LocalNodeReference) (node, error) {
	refNode := referenceNode
	if trusted {
		refNode = localReferenceNode
	}
	child := func(i int, ref CanonicalReference) (node, error) {
		if refs != nil && refs[i] != nil {
			if !ref.Present || len(ref.Inline) != 0 {
				return nil, ErrInvalidPath
			}
			n := &deferredNode{ref: refs[i]}
			if h, known := refs[i].KnownDigest(); known {
				n.digest.Store(&h)
			}
			return n, nil
		}
		return refNode(ref)
	}
	if len(desc.Value) > MaxCanonicalNodeBytes {
		return nil, ErrInvalidPath
	}
	var n node
	switch desc.Kind {
	case CanonicalBranch:
		if len(desc.Path) != 0 {
			return nil, ErrInvalidPath
		}
		b := &branchNode{value: cloneBytes(desc.Value)}
		for i, ref := range desc.Children {
			ch, e := child(i, ref)
			if e != nil {
				return nil, e
			}
			b.children[i] = ch
		}
		n = b
	case CanonicalExtension, CanonicalLeaf:
		path, leaf, e := DecodeCompactPath(desc.Path)
		if e != nil || leaf != (desc.Kind == CanonicalLeaf) {
			return nil, ErrInvalidPath
		}
		for i, ref := range desc.Children {
			if desc.Kind == CanonicalExtension && i == 0 {
				continue
			}
			if ref.Present || ref.Hash != (Hash{}) || len(ref.Inline) != 0 {
				return nil, ErrInvalidPath
			}
		}
		if leaf {
			n = &leafNode{path: path, value: cloneBytes(desc.Value)}
		} else {
			if len(path) == 0 || len(desc.Value) != 0 || !desc.Children[0].Present {
				return nil, ErrInvalidPath
			}
			ch, e := child(0, desc.Children[0])
			if e != nil {
				return nil, e
			}
			n = &extensionNode{path: path, child: ch}
		}
	default:
		return nil, ErrInvalidPath
	}
	return n, nil
}

func RebuildCanonicalNode(desc CanonicalNode) ([]byte, Hash, error) {
	n, e := nodeFromCanonicalFields(desc, false)
	if e != nil {
		return nil, Hash{}, e
	}
	raw, hash, e := encodeNode(n, nil)
	if e != nil {
		return nil, Hash{}, e
	}
	if len(raw) > MaxCanonicalNodeBytes {
		return nil, Hash{}, ErrInvalidPath
	}
	return cloneBytes(raw), hash, nil
}
