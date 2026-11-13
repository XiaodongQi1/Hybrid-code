package hybridtrie

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

// Delta versions store a branch as the difference from the previous version of
// the same node. They exist only in the Prefix column; every reader resolves
// them to a complete Record (see normalizePrefix), so no cache or decoder above
// the column read sees one. The Hash column is unchanged canonical RLP.
//
// Layout (tag bits 6 and 7 are otherwise reserved):
//
//	tag(1) | self hash(32) | depth(1) | [base digest(32) if tagHashBase]
//	| changed mask(u16) | per changed slot, ascending: kind(1), then
//	    Hash: digest(32); Embed: len(1) + inline; Empty/Prefix: nothing
//
// The base is the previous version of the node. A Prefix base is the predecessor
// of this record's own key at height-1, so it needs no locator. A Hash base
// (tagHashBase) is named by its digest because Hash Store is content addressed.
// depth is the distance to the nearest full record or Hash version (a Hash base
// gives 1), which bounds how many records a read resolves.
const (
	tagDelta    = byte(1 << 6)
	tagHashBase = byte(1 << 7)

	// MaxDeltaInterval is the longest chain a store may write or read.
	MaxDeltaInterval = 64
)

var errNeedsBase = errors.New("hybridtrie: delta record needs its base")

func isDelta(raw []byte) bool { return len(raw) > 0 && raw[0]&tagDelta != 0 }

// storedDepth is the delta depth of a stored Prefix value (zero for a full one).
func storedDepth(raw []byte) uint8 {
	if isDelta(raw) && len(raw) > recordHeaderBytes {
		return raw[recordHeaderBytes]
	}
	return 0
}

type deltaRecord struct {
	Hash     mpt.Hash
	Depth    uint8
	HashBase bool
	Base     mpt.Hash
	Changed  uint16
	Refs     [16]Reference
}

func sameReference(a, b Reference) bool {
	return a.Kind == b.Kind && a.Hash == b.Hash && bytes.Equal(a.Inline, b.Inline)
}

// encodeDelta encodes cur against base. ok is false when a delta does not
// apply: different kind, a changed branch value, or no changed slot.
func encodeDelta(base, cur Record, hashBase bool, baseDigest mpt.Hash, depth uint8) (out []byte, ok bool, err error) {
	if base.Kind != mpt.CanonicalBranch || cur.Kind != mpt.CanonicalBranch || !bytes.Equal(base.Value, cur.Value) || depth == 0 || depth > MaxDeltaInterval {
		return nil, false, nil
	}
	var changed uint16
	for i := range cur.Children {
		if !sameReference(base.Children[i], cur.Children[i]) {
			changed |= 1 << i
		}
	}
	// An empty mask is valid: the own digest changed because a Prefix child got a
	// new version, while no stored reference did.
	tag := byte(mpt.CanonicalBranch) | tagDelta
	if hashBase {
		tag |= tagHashBase
	}
	out = make([]byte, recordHeaderBytes, recordHeaderBytes+48)
	out[0] = tag
	copy(out[1:], cur.Hash[:])
	out = append(out, depth)
	if hashBase {
		out = append(out, baseDigest[:]...)
	}
	out = appendU16(out, changed)
	for i, ref := range cur.Children {
		if changed&(1<<i) == 0 {
			continue
		}
		out = append(out, byte(ref.Kind))
		switch ref.Kind {
		case Hash:
			out = append(out, ref.Hash[:]...)
		case Embed:
			if len(ref.Inline) == 0 || len(ref.Inline) >= 32 {
				return nil, false, ErrCorrupt
			}
			out = append(out, byte(len(ref.Inline)))
			out = append(out, ref.Inline...)
		}
	}
	return out, true, nil
}

func decodeDelta(raw []byte) (deltaRecord, error) {
	var d deltaRecord
	if len(raw) < recordHeaderBytes+3 || len(raw) > MaxRecordBytes {
		return d, ErrCorrupt
	}
	tag := raw[0]
	if tag&tagDelta == 0 || mpt.CanonicalKind(tag&3) != mpt.CanonicalBranch || tag&^(3|tagDelta|tagHashBase) != 0 {
		return d, ErrCorrupt
	}
	d.HashBase = tag&tagHashBase != 0
	copy(d.Hash[:], raw[1:recordHeaderBytes])
	rest := raw[recordHeaderBytes:]
	take := func(n int) ([]byte, error) {
		if n < 0 || n > len(rest) {
			return nil, ErrCorrupt
		}
		v := rest[:n]
		rest = rest[n:]
		return v, nil
	}
	v, e := take(1)
	if e != nil {
		return d, e
	}
	d.Depth = v[0]
	if d.Depth == 0 || d.Depth > MaxDeltaInterval {
		return d, ErrCorrupt
	}
	if d.HashBase {
		if d.Depth != 1 {
			return d, ErrCorrupt
		}
		if v, e = take(32); e != nil {
			return d, e
		}
		copy(d.Base[:], v)
	}
	if v, e = take(2); e != nil {
		return d, e
	}
	d.Changed = binary.BigEndian.Uint16(v)
	for i := 0; i < 16; i++ {
		if d.Changed&(1<<i) == 0 {
			continue
		}
		if v, e = take(1); e != nil {
			return d, e
		}
		ref := &d.Refs[i]
		ref.Kind = ReferenceKind(v[0])
		switch ref.Kind {
		case Empty, Prefix:
		case Hash:
			if v, e = take(32); e != nil {
				return d, e
			}
			copy(ref.Hash[:], v)
		case Embed:
			if v, e = take(1); e != nil {
				return d, e
			}
			if v[0] == 0 || v[0] >= 32 {
				return d, ErrCorrupt
			}
			if v, e = take(int(v[0])); e != nil {
				return d, e
			}
			ref.Inline = append([]byte(nil), v...)
		default:
			return d, ErrCorrupt
		}
	}
	if len(rest) != 0 {
		return d, ErrCorrupt
	}
	return d, nil
}

func applyDelta(base Record, d deltaRecord) (Record, error) {
	if base.Kind != mpt.CanonicalBranch {
		return Record{}, ErrCorrupt
	}
	r := base
	r.Hash = d.Hash
	for i := range r.Children {
		if d.Changed&(1<<i) != 0 {
			r.Children[i] = d.Refs[i]
		}
	}
	if e := r.validate(); e != nil {
		return Record{}, e
	}
	return r, nil
}

// resolveDelta turns a stored delta found at key into a complete Record. Every
// base read is charged through charge when it is non-nil. A chain is bounded by
// its depth, which must decrease by exactly one per step.
func resolveDelta(snap diskSnapshot, found NodeKey, raw []byte, charge func(int) error) (Record, error) {
	var chain []deltaRecord
	cur, curRaw := found, raw
	var base Record
	for {
		d, e := decodeDelta(curRaw)
		if e != nil {
			return Record{}, e
		}
		if n := len(chain); n > 0 && d.Depth != chain[n-1].Depth-1 {
			return Record{}, ErrCorrupt
		}
		chain = append(chain, d)
		if d.HashBase {
			body, e := snap.get(hashColumn, d.Base[:])
			if e != nil {
				return Record{}, corruptIfMissing(e)
			}
			if charge != nil {
				if e = charge(len(body) + 32); e != nil {
					return Record{}, e
				}
			}
			ctx := NodeKey{cur.TrieID, cur.Path, cur.Height}
			if len(ctx.Path) > 0 && len(body) < 32 {
				return Record{}, ErrCorrupt
			}
			base, e = FromCanonical(ctx, body, 0)
			if e != nil {
				return Record{}, e
			}
			if base.Hash != d.Base {
				return Record{}, ErrHashMismatch
			}
			break
		}
		if cur.Height == 0 {
			return Record{}, ErrCorrupt
		}
		at := NodeKey{cur.TrieID, cur.Path, cur.Height - 1}
		encoded, e := EncodeNodeKey(at)
		if e != nil {
			return Record{}, e
		}
		foundKey, baseRaw, e := snap.prev(prefixColumn, encoded)
		if e != nil {
			return Record{}, corruptIfMissing(e)
		}
		matched, e := MatchPredecessor(foundKey, at)
		if e != nil {
			return Record{}, ErrCorrupt
		}
		if charge != nil {
			if e = charge(len(baseRaw) + len(foundKey)); e != nil {
				return Record{}, e
			}
		}
		if !isDelta(baseRaw) {
			if d.Depth != 1 {
				return Record{}, ErrCorrupt
			}
			if base, e = DecodeRecord(baseRaw); e != nil {
				return Record{}, e
			}
			break
		}
		cur, curRaw = matched, baseRaw
	}
	if chain[len(chain)-1].Depth != 1 {
		return Record{}, ErrCorrupt
	}
	rec := base
	for i := len(chain) - 1; i >= 0; i-- {
		var e error
		if rec, e = applyDelta(rec, chain[i]); e != nil {
			return Record{}, e
		}
	}
	return rec, nil
}

func corruptIfMissing(e error) error {
	if errors.Is(e, ErrNotFound) {
		return ErrCorrupt
	}
	return e
}

// normalizePrefix returns the stored Prefix value as a complete record encoding.
// A full record is returned unchanged. found is the key that value was stored
// under (its own height, not the requested one).
func normalizePrefix(snap diskSnapshot, found NodeKey, raw []byte, charge func(int) error) ([]byte, error) {
	if !isDelta(raw) {
		return raw, nil
	}
	rec, e := resolveDelta(snap, found, raw, charge)
	if e != nil {
		return nil, e
	}
	return EncodeRecord(rec)
}
