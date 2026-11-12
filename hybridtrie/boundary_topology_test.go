package hybridtrie

import (
	"testing"
	"unsafe"

	"github.com/XiaodongQi1/HybridTrie/mpt"
)

func TestBoundaryTopologyCompressedPathsAndRootReuse(t *testing.T) {
	f, err := newBoundaryForest(DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary)
	if err != nil {
		t.Fatal(err)
	}
	bank := &f.current
	leaf, leafRaw := boundaryTestNode(t, []byte{1, 2, 3}, 7)
	branchDesc := mpt.CanonicalNode{Kind: mpt.CanonicalBranch}
	branchDesc.Children[3] = mpt.CanonicalReference{Present: true, Hash: leaf.Hash}
	branchRaw, branchHash, err := mpt.RebuildCanonicalNode(branchDesc)
	if err != nil {
		t.Fatal(err)
	}
	branch := boundaryObservation{Pos: position(0, []byte{1, 2}), boundaryNode: boundaryNode{Hash: branchHash, Version: forestVersion{1, Prefix, true}}}
	root := func(fragment []byte) (boundaryObservation, []byte) {
		path, err := mpt.EncodeCompactPath(fragment, false)
		if err != nil {
			t.Fatal(err)
		}
		d := mpt.CanonicalNode{Kind: mpt.CanonicalExtension, Path: path}
		d.Children[0] = mpt.CanonicalReference{Present: true, Hash: branchHash}
		raw, hash, err := mpt.RebuildCanonicalNode(d)
		if err != nil {
			t.Fatal(err)
		}
		return boundaryObservation{Pos: position(0, nil), boundaryNode: boundaryNode{Hash: hash, Version: forestVersion{1, Prefix, true}}}, raw
	}
	account, accountRaw := root([]byte{1, 2})
	// Parent slots deliberately follow their children in the array.
	for _, item := range []struct {
		slot int
		node boundaryObservation
		raw  []byte
	}{
		{7, account, accountRaw}, {2, branch, branchRaw}, {0, leaf, leafRaw},
	} {
		if err := bank.put(item.slot, item.node, item.raw); err != nil {
			t.Fatal(err)
		}
		if bank.find(item.node.Pos) != item.slot || bank.position(item.slot) != item.node.Pos {
			t.Fatal("position reconstruction")
		}
	}
	storage := account
	storage.Pos.ID = 23
	if err := bank.put(6, storage, accountRaw); err != nil {
		t.Fatal(err)
	}
	oldLeaf := leaf.Pos
	account, accountRaw = root([]byte{4, 5})
	if err := bank.put(7, account, accountRaw); err != nil {
		t.Fatal(err)
	}
	if bank.Nodes[0].Used || bank.Nodes[2].Used || bank.find(oldLeaf) >= 0 {
		t.Fatal("stale descendants after extension rewrite")
	}
	branch.Pos = position(0, []byte{4, 5})
	leaf.Pos = position(0, []byte{4, 5, 3})
	if err := bank.put(2, branch, branchRaw); err != nil {
		t.Fatal(err)
	}
	if err := bank.put(0, leaf, leafRaw); err != nil {
		t.Fatal(err)
	}
	if bank.find(leaf.Pos) != 0 {
		t.Fatal("new compressed path")
	}
	bank.remove(7)
	if bank.find(storage.Pos) != 6 {
		t.Fatal("cross-trie removal")
	}
	bank.remove(6)
	storage = account
	storage.Pos.ID = 24
	if err := bank.put(6, storage, accountRaw); err != nil {
		t.Fatal(err)
	}
	if bank.find(position(23, nil)) >= 0 || bank.find(storage.Pos) != 6 {
		t.Fatal("root descriptor reuse")
	}
	t.Logf("resident=%d link=%d native slot=%d", unsafe.Sizeof(boundaryNode{}), unsafe.Sizeof(boundaryLink{}), f.charge())
}
