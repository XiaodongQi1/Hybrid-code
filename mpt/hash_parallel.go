package mpt

import (
	"context"
	"errors"
	"sync"
)

// HashParallel encodes immutable root branches concurrently. It neither changes
// the trie nor publishes nodes. Callers must exclude concurrent trie mutation.
func (t *Trie) HashParallel(ctx context.Context, workers int) (Hash, error) {
	roots, err := HashMany(ctx, []*Trie{t}, workers)
	if err != nil {
		return Hash{}, err
	}
	return roots[0], nil
}

// HashMany shares one bounded worker pool across all tries (including each
// root's first branch). No nested pools are created for storage/account tries.
// Existing hash references are reused without loading persisted nodes.
func HashMany(ctx context.Context, tries []*Trie, workers int) ([]Hash, error) {
	if workers < 1 || workers > 256 {
		return nil, errors.New("mpt: hash workers outside 1..256")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	roots := make([]Hash, len(tries))
	if workers == 1 {
		for i, t := range tries {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if t == nil {
				return nil, errors.New("mpt: nil trie")
			}
			root, err := t.Hash()
			if err != nil {
				return nil, err
			}
			roots[i] = root
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return roots, nil
	}
	for _, t := range tries {
		if t == nil {
			return nil, errors.New("mpt: nil trie")
		}
		if t.deferred {
			if err := prepareDeferred(t.root); err != nil {
				return nil, err
			}
		}
	}
	var jobs []*parallelHashPlan
	var prepare func(node, bool, bool) *parallelHashPlan
	prepare = func(n node, split, needsReference bool) *parallelHashPlan {
		p := &parallelHashPlan{node: n}
		if cache := digestCache(n); cache != nil {
			if hit := cache.Load(); hit != nil {
				p.hash, p.size = hit.hash, hit.size
				if hit.size < 32 && needsReference {
					p.encoded, _, p.err = encodeNode(n, nil)
				}
				return p
			}
		}
		switch x := n.(type) {
		case nil:
			p.hash = EmptyRootHash
			return p
		case *deferredNode:
			p.hash, p.err = x.hash()
			return p
		case *locatedHashNode:
			p.hash = x.hash
			return p
		case hashNode:
			p.hash = Hash(x)
			return p
		case *extensionNode:
			if split {
				p.children = []*parallelHashPlan{prepare(x.child, true, true)}
				return p
			}
		case *branchNode:
			if split {
				p.children = make([]*parallelHashPlan, 16)
				for i, ch := range x.children {
					p.children[i] = prepare(ch, false, true)
				}
				return p
			}
		}
		jobs = append(jobs, p)
		return p
	}
	plans := make([]*parallelHashPlan, len(tries))
	for i, t := range tries {
		if t == nil {
			return nil, errors.New("mpt: nil trie")
		}
		plans[i] = prepare(t.root, true, false)
	}
	count := workers
	if len(jobs) < count {
		count = len(jobs)
	}
	var wg sync.WaitGroup
	wg.Add(count)
	for w := 0; w < count; w++ {
		go func(w int) {
			defer wg.Done()
			for i := w; i < len(jobs); i += count {
				if ctx.Err() != nil {
					return
				}
				p := jobs[i]
				p.encoded, p.hash, p.err = encodeNode(p.node, nil)
				p.size = uint32(len(p.encoded))
			}
		}(w)
	}
	wg.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, p := range jobs {
		if p.err != nil {
			return nil, p.err
		}
	}
	for i, p := range plans {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		finishParallelHash(p)
		roots[i] = p.hash
	}
	return roots, nil
}

type parallelHashPlan struct {
	node     node
	children []*parallelHashPlan
	encoded  []byte
	size     uint32
	hash     Hash
	err      error
}

func parallelHashReference(p *parallelHashPlan) []byte {
	if p.node == nil {
		return rlpEncodeBytes(nil)
	}
	if independentReference(p.node) {
		return rlpEncodeBytes(p.hash[:])
	}
	if p.size < 32 {
		return p.encoded
	}
	return rlpEncodeBytes(p.hash[:])
}
func finishParallelHash(p *parallelHashPlan) {
	if len(p.children) == 0 {
		return
	}
	for _, ch := range p.children {
		finishParallelHash(ch)
	}
	switch x := p.node.(type) {
	case *extensionNode:
		p.encoded = rlpEncodeList(rlpEncodeBytes(compactEncode(x.path, false)), parallelHashReference(p.children[0]))
	case *branchNode:
		elements := make([][]byte, 17)
		for i, ch := range p.children {
			elements[i] = parallelHashReference(ch)
		}
		elements[16] = rlpEncodeBytes(x.value)
		p.encoded = rlpEncodeList(elements...)
	}
	p.hash = hashBytes(p.encoded)
	cached := publishDigest(p.node, p.hash, uint32(len(p.encoded)))
	p.hash, p.size = cached.hash, cached.size
}
