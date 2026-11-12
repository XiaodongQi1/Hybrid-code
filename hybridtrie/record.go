package hybridtrie

import (
	"encoding/binary"
	"math/bits"
	"github.com/XiaodongQi1/HybridTrie/mpt"
)

// ReferenceKind is physical addressing. Embed is mandatory for short child RLP.
type ReferenceKind uint8

const (
	Empty ReferenceKind = iota
	Prefix
	Hash
	Embed
)

type Reference struct {
	Kind   ReferenceKind
	Hash   mpt.Hash
	Inline []byte
}

// Record is a decoded Prefix Store value. Fragment uses unpacked nibbles,
// unlike mpt.CanonicalNode.Path, which uses compact hex-prefix bytes.
// Extensions use Children[0] only; slot zero does not add a path nibble.
type Record struct {
	Kind     mpt.CanonicalKind
	Hash     mpt.Hash
	Fragment []byte
	Value    []byte
	Children [16]Reference
}

const (
	// tag: type[1:0], extension reference kind[3:2], branch E[4],
	// branch nonempty value[5]. Bits 6-7 are reserved and must be zero.
	recordHeaderBytes = 33
	tagE              = byte(1 << 4)
	tagValue          = byte(1 << 5)
)

func (r Record) Bitmaps() (prefix, hashed, embedded uint16) {
	for i, ref := range r.Children {
		switch ref.Kind {
		case Prefix:
			prefix |= 1 << i
		case Hash:
			hashed |= 1 << i
		case Embed:
			embedded |= 1 << i
		}
	}
	return
}

func (r Record) validate() error {
	if r.Kind > mpt.CanonicalLeaf {
		return ErrCorrupt
	}
	if _, err := pack(r.Fragment); err != nil {
		return err
	}
	if len(r.Value) > MaxRecordBytes {
		return ErrLimit
	}
	switch r.Kind {
	case mpt.CanonicalBranch:
		if len(r.Fragment) != 0 {
			return ErrCorrupt
		}
	case mpt.CanonicalExtension:
		if len(r.Fragment) == 0 || len(r.Value) != 0 || r.Children[0].Kind == Empty {
			return ErrCorrupt
		}
	case mpt.CanonicalLeaf:
		if len(r.Value) == 0 {
			return ErrCorrupt
		}
	}
	for i, ref := range r.Children {
		if ref.Kind > Embed {
			return ErrCorrupt
		}
		if (r.Kind == mpt.CanonicalLeaf || r.Kind == mpt.CanonicalExtension && i != 0) && ref.Kind != Empty {
			return ErrCorrupt
		}
		if ref.Kind != Hash && ref.Hash != (mpt.Hash{}) {
			return ErrCorrupt
		}
		if ref.Kind != Embed && len(ref.Inline) != 0 {
			return ErrCorrupt
		}
		if ref.Kind == Embed {
			if len(ref.Inline) == 0 || len(ref.Inline) >= 32 {
				return ErrCorrupt
			}
			if _, err := mpt.DecodeLocalNodeFields(ref.Inline); err != nil {
				return ErrCorrupt
			}
		}
	}
	return nil
}

// ValidateAt validates structural placement; it does not establish reachability.
func (r Record) ValidateAt(ctx NodeKey) error {
	if err := r.validate(); err != nil {
		return err
	}
	return r.validatePlacement(ctx)
}

// decodeRecord has already checked the fields; validate only their placement.
func (r Record) validatePlacement(ctx NodeKey) error {
	if ctx.Height > MaxHeight || ctx.TrieID > MaxTrieID {
		return ErrLimit
	}
	if _, err := pack(ctx.Path); err != nil {
		return err
	}
	if len(ctx.Path)+len(r.Fragment) > MaxDepth {
		return ErrCorrupt
	}
	if r.Kind == mpt.CanonicalBranch && len(ctx.Path) == MaxDepth {
		bp, bh, be := r.Bitmaps()
		if bp|bh|be != 0 {
			return ErrCorrupt
		}
	}
	return validateInlinePaths(r, len(ctx.Path))
}

// EncodeRecord uses a 33-byte tag/self-hash header. Branches without E have
// exactly 37+32*popcount(BH) bytes. Hash Store values are canonical RLP instead.
func EncodeRecord(r Record) ([]byte, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	tag := byte(r.Kind)
	bp, bh, be := r.Bitmaps()
	if r.Kind == mpt.CanonicalExtension {
		tag |= byte(r.Children[0].Kind) << 2
	}
	if r.Kind == mpt.CanonicalBranch {
		if be != 0 || len(r.Value) != 0 {
			tag |= tagE
		}
		if len(r.Value) != 0 {
			tag |= tagValue
		}
	}
	out := make([]byte, 33)
	out[0] = tag
	copy(out[1:], r.Hash[:])
	if r.Kind == mpt.CanonicalBranch {
		out = appendU16(out, bp)
		out = appendU16(out, bh)
		for _, ref := range r.Children {
			if ref.Kind == Hash {
				out = append(out, ref.Hash[:]...)
			}
		}
		if tag&tagE != 0 {
			out = appendU16(out, be)
			for _, ref := range r.Children {
				if ref.Kind == Embed {
					out = append(out, byte(len(ref.Inline)))
					out = append(out, ref.Inline...)
				}
			}
		}
	} else {
		fragment, _ := pack(r.Fragment)
		out = append(out, byte(len(r.Fragment)))
		out = append(out, fragment...)
		if r.Kind == mpt.CanonicalExtension {
			ref := r.Children[0]
			if ref.Kind == Hash {
				out = append(out, ref.Hash[:]...)
			}
			if ref.Kind == Embed {
				out = append(out, byte(len(ref.Inline)))
				out = append(out, ref.Inline...)
			}
		}
	}
	if r.Kind == mpt.CanonicalLeaf || tag&tagValue != 0 {
		out = binary.BigEndian.AppendUint32(out, uint32(len(r.Value)))
		out = append(out, r.Value...)
	}
	if len(out) > MaxRecordBytes {
		return nil, ErrLimit
	}
	return out, nil
}

func appendU16(out []byte, v uint16) []byte { return append(out, byte(v>>8), byte(v)) }

type decoder struct {
	rest   []byte
	borrow bool
}

func (d *decoder) take(n int) ([]byte, error) {
	if n < 0 || n > len(d.rest) {
		return nil, ErrCorrupt
	}
	v := d.rest[:n]
	d.rest = d.rest[n:]
	return v, nil
}
func (d *decoder) inline() ([]byte, error) {
	b, e := d.take(1)
	if e != nil {
		return nil, e
	}
	if b[0] == 0 || b[0] >= 32 {
		return nil, ErrCorrupt
	}
	v, e := d.take(int(b[0]))
	if e != nil {
		return nil, e
	}
	if d.borrow {
		return v, nil
	}
	return append([]byte(nil), v...), nil
}

func recordKind(tag byte) (mpt.CanonicalKind, error) {
	kind := mpt.CanonicalKind(tag & 3)
	if tag&0xc0 != 0 || kind > mpt.CanonicalLeaf {
		return 0, ErrCorrupt
	}
	if kind != mpt.CanonicalExtension && tag&0x0c != 0 {
		return 0, ErrCorrupt
	}
	if kind != mpt.CanonicalBranch && tag&(tagE|tagValue) != 0 {
		return 0, ErrCorrupt
	}
	if tag&tagValue != 0 && tag&tagE == 0 {
		return 0, ErrCorrupt
	}
	return kind, nil
}

func DecodeRecord(raw []byte) (Record, error) { return decodeRecord(raw, false) }

// Borrowed fields are consumed synchronously by the local materializer.
func decodeRecord(raw []byte, borrow bool) (Record, error) {
	var r Record
	if len(raw) < recordHeaderBytes {
		return r, ErrCorrupt
	}
	if len(raw) > MaxRecordBytes {
		return r, ErrLimit
	}
	tag := raw[0]
	kind, err := recordKind(tag)
	if err != nil {
		return Record{}, err
	}
	r.Kind = kind
	copy(r.Hash[:], raw[1:recordHeaderBytes])
	d := decoder{rest: raw[recordHeaderBytes:], borrow: borrow}
	if r.Kind == mpt.CanonicalBranch {
		b, e := d.take(4)
		if e != nil {
			return Record{}, e
		}
		bp, bh := binary.BigEndian.Uint16(b), binary.BigEndian.Uint16(b[2:])
		if bp&bh != 0 {
			return Record{}, ErrCorrupt
		}
		for i := range r.Children {
			if bp&(1<<i) != 0 {
				r.Children[i].Kind = Prefix
			}
			if bh&(1<<i) != 0 {
				v, e := d.take(32)
				if e != nil {
					return Record{}, e
				}
				r.Children[i].Kind = Hash
				copy(r.Children[i].Hash[:], v)
			}
		}
		if tag&tagE != 0 {
			v, e := d.take(2)
			if e != nil {
				return Record{}, e
			}
			be := binary.BigEndian.Uint16(v)
			if be&(bp|bh) != 0 || be == 0 && tag&tagValue == 0 {
				return Record{}, ErrCorrupt
			}
			for i := range r.Children {
				if be&(1<<i) != 0 {
					inline, e := d.inline()
					if e != nil {
						return Record{}, e
					}
					r.Children[i] = Reference{Kind: Embed, Inline: inline}
				}
			}
		}
	} else {
		v, e := d.take(1)
		if e != nil {
			return Record{}, e
		}
		n := int(v[0])
		if n > MaxDepth {
			return Record{}, ErrCorrupt
		}
		v, e = d.take((n + 1) / 2)
		if e != nil {
			return Record{}, e
		}
		r.Fragment, e = unpack(v, n)
		if e != nil {
			return Record{}, e
		}
		if r.Kind == mpt.CanonicalExtension {
			ref := &r.Children[0]
			ref.Kind = ReferenceKind((tag >> 2) & 3)
			switch ref.Kind {
			case Prefix:
			case Hash:
				v, e := d.take(32)
				if e != nil {
					return Record{}, e
				}
				copy(ref.Hash[:], v)
			case Embed:
				ref.Inline, e = d.inline()
				if e != nil {
					return Record{}, e
				}
			default:
				return Record{}, ErrCorrupt
			}
		}
	}
	if r.Kind == mpt.CanonicalLeaf || tag&tagValue != 0 {
		v, e := d.take(4)
		if e != nil {
			return Record{}, e
		}
		n := binary.BigEndian.Uint32(v)
		if n == 0 || uint64(n) != uint64(len(d.rest)) {
			return Record{}, ErrCorrupt
		}
		if borrow {
			r.Value = d.rest
		} else {
			r.Value = append([]byte(nil), d.rest...)
		}
		d.rest = nil
	}
	if len(d.rest) != 0 {
		return Record{}, ErrCorrupt
	}
	if e := r.validate(); e != nil {
		return Record{}, e
	}
	return r, nil
}

// ExplicitHashOffset returns the raw branch-value offset, excluding any key.
func ExplicitHashOffset(bitmap uint16, slot int) (int, error) {
	if slot < 0 || slot >= 16 || bitmap&(1<<slot) == 0 {
		return 0, ErrCorrupt
	}
	return 37 + 32*bits.OnesCount16(bitmap&uint16((1<<slot)-1)), nil
}
