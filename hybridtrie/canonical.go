package hybridtrie

import "github.com/XiaodongQi1/HybridTrie/mpt"

// FromCanonical projects one independent node. prefixMask may select only
// native hashed children (bit zero for an extension). Inline children always
// remain embedded regardless of policy. A short root still has its own record.
func FromCanonical(ctx NodeKey, raw []byte, prefixMask uint16) (Record, error) {
	if ctx.Height > MaxHeight || ctx.TrieID > MaxTrieID {
		return Record{}, ErrLimit
	}
	if _, err := pack(ctx.Path); err != nil {
		return Record{}, err
	}
	if len(ctx.Path) != 0 && len(raw) < 32 {
		return Record{}, ErrEmbeddedNode
	}
	desc, hash, err := mpt.InspectCanonicalNodeWithHash(raw)
	if err != nil {
		return Record{}, err
	}
	return recordFromFields(ctx, desc, hash, prefixMask)
}

func recordFromFields(ctx NodeKey, desc mpt.CanonicalNode, hash mpt.Hash, prefixMask uint16) (Record, error) {
	var err error
	r := Record{Kind: desc.Kind, Hash: hash, Value: desc.Value}
	if desc.Kind != mpt.CanonicalBranch {
		r.Fragment, _, err = mpt.DecodeCompactPath(desc.Path)
		if err != nil {
			return Record{}, err
		}
	}
	var eligible uint16
	for i, ref := range desc.Children {
		if !ref.Present {
			continue
		}
		if len(ref.Inline) != 0 {
			r.Children[i] = Reference{Kind: Embed, Inline: ref.Inline}
		} else {
			eligible |= 1 << i
			r.Children[i] = Reference{Kind: Hash, Hash: ref.Hash}
			if prefixMask&(1<<i) != 0 {
				r.Children[i] = Reference{Kind: Prefix}
			}
		}
	}
	if prefixMask & ^eligible != 0 {
		return Record{}, ErrCorrupt
	}
	if err = r.ValidateAt(ctx); err != nil {
		return Record{}, err
	}
	return r, nil
}

// PrefixHashResolver supplies a child's self hash from its declared position
// and target-height context. It must propagate missing referenced data as an
// error. Such hashes are authenticated only after the parent/path reaches a
// trusted root; a decoded physical self hash by itself is not a trust anchor.
type PrefixHashResolver func(ChildLocator) (mpt.Hash, error)

// RebuildCanonical reconstructs the unchanged MPT encoding and checks the
// record's self hash. It never looks up Hash or Embed siblings. The caller must
// additionally bind this self hash to its trusted root or authenticated parent.
func RebuildCanonical(ctx NodeKey, r Record, resolve PrefixHashResolver) ([]byte, mpt.Hash, error) {
	desc, err := canonicalFields(ctx, r, resolve)
	if err != nil {
		return nil, mpt.Hash{}, err
	}
	raw, hash, err := mpt.RebuildCanonicalNode(desc)
	if err != nil {
		return nil, mpt.Hash{}, err
	}
	if hash != r.Hash {
		return nil, mpt.Hash{}, ErrHashMismatch
	}
	return raw, hash, nil
}

func canonicalFields(ctx NodeKey, r Record, resolve PrefixHashResolver) (mpt.CanonicalNode, error) {
	if err := r.ValidateAt(ctx); err != nil {
		return mpt.CanonicalNode{}, err
	}
	desc := mpt.CanonicalNode{Kind: r.Kind, Value: append([]byte(nil), r.Value...)}
	if r.Kind != mpt.CanonicalBranch {
		path, err := mpt.EncodeCompactPath(r.Fragment, r.Kind == mpt.CanonicalLeaf)
		if err != nil {
			return mpt.CanonicalNode{}, err
		}
		desc.Path = path
	}
	for i, ref := range r.Children {
		switch ref.Kind {
		case Empty:
		case Hash:
			desc.Children[i] = mpt.CanonicalReference{Present: true, Hash: ref.Hash}
		case Embed:
			desc.Children[i] = mpt.CanonicalReference{Present: true, Inline: append([]byte(nil), ref.Inline...)}
		case Prefix:
			if resolve == nil {
				return mpt.CanonicalNode{}, ErrUnresolvedPrefix
			}
			loc, err := locateChild(ctx, r, i)
			if err != nil {
				return mpt.CanonicalNode{}, err
			}
			hash, err := resolve(loc)
			if err != nil {
				return mpt.CanonicalNode{}, err
			}
			desc.Children[i] = mpt.CanonicalReference{Present: true, Hash: hash}
		}
	}
	return desc, nil
}

func validateInlinePaths(r Record, depth int) error {
	for _, ref := range r.Children {
		if ref.Kind != Embed {
			continue
		}
		desc, err := mpt.DecodeLocalNodeFields(ref.Inline)
		if err != nil {
			return ErrCorrupt
		}
		childDepth := depth + 1
		if r.Kind == mpt.CanonicalExtension {
			childDepth = depth + len(r.Fragment)
		}
		if err = validateCanonicalPaths(desc, childDepth); err != nil {
			return err
		}
	}
	return nil
}

func validateCanonicalPaths(desc mpt.CanonicalNode, depth int) error {
	if depth > MaxDepth {
		return ErrCorrupt
	}
	childDepth := depth + 1
	if desc.Kind != mpt.CanonicalBranch {
		path, _, err := mpt.DecodeCompactPath(desc.Path)
		if err != nil || depth+len(path) > MaxDepth {
			return ErrCorrupt
		}
		childDepth = depth + len(path)
	}
	for _, ref := range desc.Children {
		if !ref.Present {
			continue
		}
		if childDepth > MaxDepth {
			return ErrCorrupt
		}
		if len(ref.Inline) > 0 {
			child, err := mpt.DecodeLocalNodeFields(ref.Inline)
			if err != nil {
				return ErrCorrupt
			}
			if err = validateCanonicalPaths(child, childDepth); err != nil {
				return err
			}
		}
	}
	return nil
}
