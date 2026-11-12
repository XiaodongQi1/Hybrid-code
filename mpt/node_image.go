package mpt

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// MaterializedNode is an immutable logical node from an authenticated source
// or an explicitly trusted local snapshot. It contains no RLP, and COW edits
// create fresh dirty nodes.
type MaterializedNode struct {
	root node
	hash Hash
}

// MaterializedStore supplies logical nodes directly to execution. Byte stores
// and PathStore remain the proof/persistence compatibility interfaces.
type MaterializedStore interface {
	GetMaterializedNode(path []byte, expectedHash Hash) (MaterializedNode, error)
}

func DecodeMaterializedNode(raw []byte, expected Hash) (MaterializedNode, error) {
	if len(raw) > MaxCanonicalNodeBytes || hashBytes(raw) != expected {
		return MaterializedNode{}, fmt.Errorf("mpt: materialized node hash mismatch")
	}
	n, e := decodeNode(raw)
	if e != nil {
		return MaterializedNode{}, e
	}
	publishDigest(n, expected, uint32(len(raw)))
	canonical, _, e := encodeNode(n, nil)
	if e != nil || !bytes.Equal(canonical, raw) {
		return MaterializedNode{}, ErrInvalidPath
	}
	return MaterializedNode{n, expected}, nil
}

// NodeImage is a parsed field view in a caller-owned fixed arena: node kinds,
// packed path nibbles, raw values, child-presence masks and child hashes. Inline
// children use the same fields recursively. Neither whole nor inline RLP is
// retained. The arena must remain immutable while the image is in use.
// Materialize detaches logical nodes so subsequent arena reuse cannot affect
// an executing trie or its COW copies.
type NodeImage struct {
	data        []byte
	hash        Hash
	encodedSize uint32
}

// ViewNodeImage borrows already parsed, trusted fields. Callers must keep the
// arena immutable for this view's lifetime. Materialize still detaches it.
// This lets compact caches keep the digest once, outside the field arena.
func ViewNodeImage(fields []byte, digest Hash, canonicalSize uint32) NodeImage {
	return NodeImage{data: fields, hash: digest, encodedSize: canonicalSize}
}

// A host has at most sixteen inline references, each shorter than 32 canonical
// bytes. Field-header expansion for those short subtrees plus the host fits
// this fixed margin; large values and hash references do not expand.
func NodeImageMaxBytes(canonicalLimit uint32) uint32 {
	if canonicalLimit > MaxCanonicalNodeBytes {
		return 0
	}
	return canonicalLimit + 272
}
func (v NodeImage) Empty() bool         { return len(v.data) == 0 }
func (v NodeImage) Hash() Hash          { return v.hash }
func (v NodeImage) EncodedSize() uint32 { return v.encodedSize }
func (v NodeImage) DataBytes() int      { return len(v.data) }
func (v NodeImage) CloneTo(dst []byte) (NodeImage, error) {
	if len(v.data) == 0 || len(dst) < len(v.data) {
		return NodeImage{}, ErrInvalidPath
	}
	copy(dst, v.data)
	v.data = dst[:len(v.data):len(v.data)]
	return v, nil
}
func CaptureNodeImage(dst, raw []byte, expected Hash) (NodeImage, error) {
	materialized, e := DecodeMaterializedNode(raw, expected)
	if e != nil {
		return NodeImage{}, e
	}
	return materialized.capture(dst, len(raw))
}

// CaptureTrustedLocalNodeImage stores fields from the local COW tree or its pinned database.
func CaptureTrustedLocalNodeImage(dst, raw []byte, digest Hash) (NodeImage, error) {
	n, err := DecodeTrustedLocalNode(raw, digest)
	if err != nil {
		return NodeImage{}, err
	}
	return n.capture(dst, len(raw))
}

func (materialized MaterializedNode) capture(dst []byte, size int) (NodeImage, error) {
	w := imageWriter{data: dst}
	if e := w.write(materialized.root); e != nil {
		return NodeImage{}, e
	}
	if w.used > int(NodeImageMaxBytes(uint32(size))) {
		return NodeImage{}, ErrInvalidPath
	}
	return NodeImage{dst[:w.used:w.used], materialized.hash, uint32(size)}, nil
}

type imageWriter struct {
	data []byte
	used int
}

func (w *imageWriter) put(p []byte) error {
	if len(p) > len(w.data)-w.used {
		return ErrInvalidPath
	}
	copy(w.data[w.used:], p)
	w.used += len(p)
	return nil
}
func (w *imageWriter) number(n int) error {
	var b [10]byte
	k := binary.PutUvarint(b[:], uint64(n))
	return w.put(b[:k])
}
func (w *imageWriter) path(tag byte, path []byte) error {
	if len(path) > 64 {
		return ErrInvalidPath
	}
	if e := w.put([]byte{tag, byte(len(path))}); e != nil {
		return e
	}
	var packed [32]byte
	for i, n := range path {
		if n > 15 {
			return ErrInvalidPath
		}
		if i%2 == 0 {
			packed[i/2] = n << 4
		} else {
			packed[i/2] |= n
		}
	}
	return w.put(packed[:(len(path)+1)/2])
}
func (w *imageWriter) write(n node) error {
	switch n := n.(type) {
	case *leafNode:
		if e := w.path(byte(CanonicalLeaf), n.path); e != nil {
			return e
		}
		if e := w.number(len(n.value)); e != nil {
			return e
		}
		return w.put(n.value)
	case *extensionNode:
		tag := byte(CanonicalExtension)
		hashed := independentReference(n.child)
		if !hashed {
			tag |= 4
		}
		if e := w.path(tag, n.path); e != nil {
			return e
		}
		if h, ok, err := resolvedReferenceHash(n.child); ok {
			if err != nil {
				return err
			}
			return w.put(h[:])
		}
		return w.write(n.child)
	case *branchNode:
		var mask, inline uint16
		for i, ch := range n.children {
			if ch == nil {
				continue
			}
			mask |= 1 << i
			if !independentReference(ch) {
				inline |= 1 << i
			}
		}
		var head [5]byte
		head[0] = byte(CanonicalBranch)
		binary.LittleEndian.PutUint16(head[1:3], mask)
		binary.LittleEndian.PutUint16(head[3:5], inline)
		if e := w.put(head[:]); e != nil {
			return e
		}
		if e := w.number(len(n.value)); e != nil {
			return e
		}
		if e := w.put(n.value); e != nil {
			return e
		}
		for _, ch := range n.children {
			if ch == nil {
				continue
			}
			if h, ok, err := resolvedReferenceHash(ch); ok {
				if err != nil {
					return err
				}
				if e := w.put(h[:]); e != nil {
					return e
				}
			} else if e := w.write(ch); e != nil {
				return e
			}
		}
		return nil
	default:
		return ErrInvalidPath
	}
}

type imageReader struct {
	data      []byte
	pos       int
	locations *[16]NodeLocation
}

func (r *imageReader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.data)-r.pos {
		return nil, ErrInvalidPath
	}
	p := r.data[r.pos : r.pos+n]
	r.pos += n
	return p, nil
}
func (r *imageReader) value() ([]byte, error) {
	n, k := binary.Uvarint(r.data[r.pos:])
	if k <= 0 || n > MaxCanonicalNodeBytes {
		return nil, ErrInvalidPath
	}
	r.pos += k
	return r.take(int(n))
}
func (r *imageReader) path() ([]byte, uint8, error) {
	raw, e := r.take(1)
	if e != nil || raw[0] > 64 {
		return nil, 0, ErrInvalidPath
	}
	n := raw[0]
	packed, e := r.take((int(n) + 1) / 2)
	if e != nil || n%2 != 0 && packed[len(packed)-1]&15 != 0 {
		return nil, 0, ErrInvalidPath
	}
	return packed, n, nil
}
func unpackImagePath(raw []byte, n uint8) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = raw[i/2] >> 4
		if i%2 != 0 {
			p[i] = raw[i/2] & 15
		}
	}
	return p
}
func (r *imageReader) read(build bool, depth int) (node, error) {
	if depth > 64 {
		return nil, ErrInvalidPath
	}
	tag, e := r.take(1)
	if e != nil {
		return nil, e
	}
	switch tag[0] {
	case byte(CanonicalLeaf):
		p, n, e := r.path()
		if e != nil {
			return nil, e
		}
		value, e := r.value()
		if e != nil {
			return nil, e
		}
		if build {
			return &leafNode{path: unpackImagePath(p, n), value: cloneBytes(value)}, nil
		}
		return nil, nil
	case byte(CanonicalExtension), byte(CanonicalExtension) | 4:
		p, n, e := r.path()
		if e != nil || n == 0 {
			return nil, ErrInvalidPath
		}
		var child node
		if tag[0]&4 != 0 {
			child, e = r.read(build, depth+1)
		} else {
			var raw []byte
			raw, e = r.take(32)
			if e == nil && build {
				child = r.reference(raw, depth, 0)
			}
		}
		if e != nil {
			return nil, e
		}
		if build {
			return &extensionNode{path: unpackImagePath(p, n), child: child}, nil
		}
		return nil, nil
	case byte(CanonicalBranch):
		masks, e := r.take(4)
		if e != nil {
			return nil, e
		}
		mask, inline := binary.LittleEndian.Uint16(masks[:2]), binary.LittleEndian.Uint16(masks[2:])
		if inline&^mask != 0 {
			return nil, ErrInvalidPath
		}
		value, e := r.value()
		if e != nil {
			return nil, e
		}
		var b *branchNode
		if build {
			b = &branchNode{value: cloneBytes(value)}
		}
		for i := 0; i < 16; i++ {
			if mask&(1<<i) == 0 {
				continue
			}
			var child node
			if inline&(1<<i) != 0 {
				child, e = r.read(build, depth+1)
			} else {
				var raw []byte
				raw, e = r.take(32)
				if e == nil && build {
					child = r.reference(raw, depth, i)
				}
			}
			if e != nil {
				return nil, e
			}
			if build {
				b.children[i] = child
			}
		}
		if build {
			return b, nil
		}
		return nil, nil
	default:
		return nil, ErrInvalidPath
	}
}
func (r *imageReader) reference(raw []byte, depth, slot int) node {
	var h Hash
	copy(h[:], raw)
	if depth == 0 && r.locations != nil && r.locations[slot].Kind != 0 {
		return &locatedHashNode{hash: h, location: r.locations[slot]}
	}
	return hashNode(h)
}
func (v NodeImage) Materialize() (MaterializedNode, error) { return v.materialize(nil) }

// MaterializeWithChildLocations constructs one detached parent and its located
// references in one pass. It never modifies the arena or an existing COW node.
func (v NodeImage) MaterializeWithChildLocations(locations [16]NodeLocation) (MaterializedNode, error) {
	return v.materialize(&locations)
}
func (v NodeImage) materialize(locations *[16]NodeLocation) (MaterializedNode, error) {
	r := imageReader{data: v.data, locations: locations}
	n, e := r.read(true, 0)
	if e != nil || r.pos != len(v.data) || n == nil {
		return MaterializedNode{}, ErrInvalidPath
	}
	publishDigest(n, v.hash, v.encodedSize)
	return MaterializedNode{n, v.hash}, nil
}
func (v NodeImage) RLP() ([]byte, error) {
	n, e := v.Materialize()
	if e != nil {
		return nil, e
	}
	raw, _, e := encodeNode(n.root, nil)
	return raw, e
}
func (v NodeImage) Identity() (NodeIdentity, error) {
	var id NodeIdentity
	r := imageReader{data: v.data}
	tag, e := r.take(1)
	if e != nil {
		return id, e
	}
	id.Kind = CanonicalKind(tag[0] & 3)
	if id.Kind == CanonicalBranch {
		return id, nil
	}
	if id.Kind != CanonicalExtension && id.Kind != CanonicalLeaf {
		return id, ErrInvalidPath
	}
	p, n, e := r.path()
	if e != nil {
		return id, e
	}
	id.Length = n
	for i := 0; i < int(n); i++ {
		id.Fragment[i] = p[i/2] >> 4
		if i%2 != 0 {
			id.Fragment[i] = p[i/2] & 15
		}
	}
	return id, nil
}

// IndependentChildren reads the field masks without visiting child bodies.
func (v NodeImage) IndependentChildren() uint16 {
	if len(v.data) == 0 {
		return 0
	}
	switch CanonicalKind(v.data[0] & 3) {
	case CanonicalExtension:
		if v.data[0]&4 == 0 {
			return 1
		}
	case CanonicalBranch:
		if len(v.data) >= 5 {
			return binary.LittleEndian.Uint16(v.data[1:3]) &^ binary.LittleEndian.Uint16(v.data[3:5])
		}
	}
	return 0
}

// ChildHash reads an independently addressed child directly from parsed fields.
func (v NodeImage) ChildHash(slot int) (Hash, bool) {
	var h Hash
	if slot < 0 || slot >= 16 {
		return h, false
	}
	r := imageReader{data: v.data}
	tag, e := r.take(1)
	if e != nil {
		return h, false
	}
	if CanonicalKind(tag[0]&3) == CanonicalExtension {
		if slot != 0 || tag[0]&4 != 0 {
			return h, false
		}
		if _, _, e = r.path(); e != nil {
			return h, false
		}
		raw, e := r.take(32)
		if e != nil {
			return h, false
		}
		copy(h[:], raw)
		return h, true
	}
	if tag[0] != byte(CanonicalBranch) {
		return h, false
	}
	masks, e := r.take(4)
	if e != nil {
		return h, false
	}
	mask, inline := binary.LittleEndian.Uint16(masks[:2]), binary.LittleEndian.Uint16(masks[2:])
	if mask&(1<<slot) == 0 || inline&(1<<slot) != 0 {
		return h, false
	}
	if _, e = r.value(); e != nil {
		return h, false
	}
	for i := 0; i <= slot; i++ {
		if mask&(1<<i) == 0 {
			continue
		}
		if inline&(1<<i) != 0 {
			if _, e = r.read(false, 0); e != nil {
				return h, false
			}
		} else {
			raw, e := r.take(32)
			if e != nil {
				return h, false
			}
			if i == slot {
				copy(h[:], raw)
				return h, true
			}
		}
	}
	return h, false
}

// CaptureLocalFieldsImage shares a trusted local parse with other consumers.
// Only the copied field image survives; canonical RLP is never retained.
func CaptureLocalFieldsImage(dst []byte, fields CanonicalNode, digest Hash, size uint32) (NodeImage, error) {
	n, err := MaterializeLocalFields(fields, digest, size)
	if err != nil {
		return NodeImage{}, err
	}
	return n.capture(dst, int(size))
}
