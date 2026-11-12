package mpt

// LocalRLPSize sizes trusted, parsed fields without encoding or resolving
// children. Present non-inline references always occupy one 32-byte digest.
func (d CanonicalNode) LocalRLPSize() uint32 { return localEncodedSize(d) }

// DecodeLocalNodeFields parses a trusted local RLP record. Child digests and
// inline encodings are read directly from the record; no digest is computed.
func DecodeLocalNodeFields(raw []byte) (CanonicalNode, error) {
	var d CanonicalNode
	if len(raw) > MaxCanonicalNodeBytes {
		return d, ErrInvalidPath
	}
	item, rest, err := rlpSplit(raw)
	if err != nil || len(rest) != 0 || item.kind != rlpList {
		return d, ErrInvalidPath
	}
	items, err := rlpListItems(item)
	if err != nil {
		return d, err
	}
	// RLP long forms and lengths were checked by the splitter. Reject the
	// remaining non-minimal form without re-encoding or hashing a child.
	for _, field := range items {
		if len(field.raw) == 2 && field.raw[0] == 0x81 && field.raw[1] < 0x80 {
			return d, ErrInvalidPath
		}
	}
	var refs []rlpItem
	switch len(items) {
	case 2:
		if items[0].kind != rlpString {
			return d, ErrInvalidPath
		}
		path, leaf, err := DecodeCompactPath(items[0].payload)
		if err != nil || !leaf && len(path) == 0 {
			return d, ErrInvalidPath
		}
		d.Path = items[0].payload
		if leaf {
			if items[1].kind != rlpString {
				return d, ErrInvalidPath
			}
			d.Kind, d.Value = CanonicalLeaf, items[1].payload
			return d, nil
		}
		d.Kind, refs = CanonicalExtension, items[1:]
	case 17:
		if items[16].kind != rlpString {
			return d, ErrInvalidPath
		}
		d.Kind, d.Value, refs = CanonicalBranch, items[16].payload, items[:16]
	default:
		return d, ErrInvalidPath
	}
	for i, r := range refs {
		switch {
		case r.kind == rlpList && len(r.raw) < 32:
			if _, err := DecodeLocalNodeFields(r.raw); err != nil {
				return d, err
			}
			d.Children[i] = CanonicalReference{Present: true, Inline: r.raw}
		case r.kind == rlpString && len(r.payload) == 32:
			d.Children[i].Present = true
			copy(d.Children[i].Hash[:], r.payload)
		case r.kind == rlpString && len(r.payload) == 0 && d.Kind == CanonicalBranch:
		default:
			return d, ErrInvalidPath
		}
	}
	return d, nil
}

// LocalRLP serializes parsed fields from a trusted physical record for a caller
// requiring canonical MPT bytes. The caller supplies resolved child digests.
func (d CanonicalNode) LocalRLP() ([]byte, error) {
	ref := func(r CanonicalReference) []byte {
		if !r.Present {
			return rlpEncodeBytes(nil)
		}
		if len(r.Inline) > 0 {
			return r.Inline
		}
		return rlpEncodeBytes(r.Hash[:])
	}
	switch d.Kind {
	case CanonicalLeaf:
		return rlpEncodeList(rlpEncodeBytes(d.Path), rlpEncodeBytes(d.Value)), nil
	case CanonicalExtension:
		return rlpEncodeList(rlpEncodeBytes(d.Path), ref(d.Children[0])), nil
	case CanonicalBranch:
		var fields [17][]byte
		for i, child := range d.Children {
			fields[i] = ref(child)
		}
		fields[16] = rlpEncodeBytes(d.Value)
		return rlpEncodeList(fields[:]...), nil
	default:
		return nil, ErrInvalidPath
	}
}

// DecodeTrustedLocalNode installs the digest supplied by the pinned local
// store. Use DecodeMaterializedNode for data requiring authentication.
func DecodeTrustedLocalNode(raw []byte, digest Hash) (MaterializedNode, error) {
	if len(raw) > MaxCanonicalNodeBytes {
		return MaterializedNode{}, ErrInvalidPath
	}
	n, err := decodeNode(raw)
	if err != nil {
		return MaterializedNode{}, err
	}
	publishDigest(n, digest, uint32(len(raw)))
	return MaterializedNode{n, digest}, nil
}

// MaterializeLocalFields detaches already parsed trusted-local fields without
// encoding and reparsing the host. encodedSize is the canonical RLP length;
// neither this function nor the fields authorize an untrusted digest.
func MaterializeLocalFields(d CanonicalNode, digest Hash, encodedSize uint32) (MaterializedNode, error) {
	if encodedSize == 0 || encodedSize > MaxCanonicalNodeBytes {
		return MaterializedNode{}, ErrInvalidPath
	}
	n, err := nodeFromCanonicalFields(d, true)
	if err != nil {
		return MaterializedNode{}, err
	}
	publishDigest(n, digest, encodedSize)
	return MaterializedNode{n, digest}, nil
}
func localReferenceNode(ref CanonicalReference) (node, error) {
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
	return decodeNode(ref.Inline)
}
