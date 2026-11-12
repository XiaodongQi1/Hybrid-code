package mpt

import (
	"bytes"
	"reflect"
	"testing"
)

func TestProofHashesAcrossMemoryAndPersistedNodes(t *testing.T) {
	for _, size := range []int{1, 80} {
		trie, store := newTestTrie(t)
		keys := [][]byte{{0x12, 0x34}, {0x12, 0x35}, {0x99, 0x00}}
		for i, key := range keys {
			if err := trie.Update(key, bytes.Repeat([]byte{byte(i + 1)}, size)); err != nil {
				t.Fatal(err)
			}
		}
		candidates := append(keys, []byte{0x12, 0x36}, []byte{0x88})
		proofs := make([]Proof, len(candidates))
		for i, key := range candidates {
			var err error
			proofs[i], err = trie.Prove(key)
			if err != nil {
				t.Fatal(err)
			}
		}
		root, err := trie.Commit()
		if err != nil {
			t.Fatal(err)
		}
		reopened, err := Open(root, store)
		if err != nil {
			t.Fatal(err)
		}
		for i, key := range candidates {
			persisted, err := reopened.Prove(key)
			if err != nil || !reflect.DeepEqual(proofs[i], persisted) {
				t.Fatalf("size=%d key=%x: memory/persisted proofs differ: %v", size, key, err)
			}
			local, err := AssembleValueProofAt(root, store, key)
			if err != nil || !reflect.DeepEqual(local.Nodes, persisted) {
				t.Fatalf("size=%d key=%x: assembled and checked proof bytes differ: %v", size, key, err)
			}
			// Proof order and duplicate nodes do not affect hash-based lookup.
			shuffled := make(Proof, 0, len(persisted)+1)
			for j := len(persisted) - 1; j >= 0; j-- {
				shuffled = append(shuffled, persisted[j])
			}
			shuffled = append(shuffled, persisted[0])
			value, exists, err := VerifyProof(root, key, shuffled)
			if err != nil || exists != (i < len(keys)) {
				t.Fatalf("size=%d key=%x: exists=%v err=%v", size, key, exists, err)
			}
			if exists && !bytes.Equal(value, bytes.Repeat([]byte{byte(i + 1)}, size)) {
				t.Fatal("wrong authenticated value")
			}
		}
	}
}

func TestProofHashReuseRejectsCorruption(t *testing.T) {
	trie, store := newTestTrie(t)
	key := []byte{0x12, 0x34}
	for i, k := range [][]byte{key, {0x12, 0x35}, {0x99, 0x00}} {
		if err := trie.Update(k, bytes.Repeat([]byte{byte(i + 1)}, 80)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := trie.Commit()
	if err != nil {
		t.Fatal(err)
	}
	proof, err := trie.Prove(key)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := VerifyProof(root, key, proof); err != nil {
		t.Fatal(err)
	}
	for i, encoded := range proof {
		// Long values ensure all path nodes are hash-referenced, not inline.
		if len(encoded) < 32 {
			t.Fatalf("unexpected inline node %d", i)
		}
		encoded[len(encoded)-1] ^= 1
		if _, _, err := VerifyProof(root, key, proof); err == nil {
			t.Fatalf("accepted modified node %d after previous verification", i)
		}
		encoded[len(encoded)-1] ^= 1
		missing := append(append(Proof(nil), proof[:i]...), proof[i+1:]...)
		if _, _, err := VerifyProof(root, key, missing); err == nil {
			t.Fatalf("accepted missing node %d", i)
		}
		// Prove must still authenticate store reads, even after it proved this
		// same root successfully. Cover root and deeper hashed children.
		hash := hashBytes(encoded)
		bad := cloneBytes(encoded)
		bad[len(bad)-1] ^= 1
		batch := store.NewBatch()
		if err := batch.Put(hash[:], bad); err != nil {
			t.Fatal(err)
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}
		if _, err := trie.Prove(key); err == nil {
			t.Fatalf("accepted corrupted stored node %d", i)
		}
		batch = store.NewBatch()
		if err := batch.Put(hash[:], encoded); err != nil {
			t.Fatal(err)
		}
		if err := batch.Write(); err != nil {
			t.Fatal(err)
		}
	}
	wrongRoot := root
	wrongRoot[0] ^= 1
	if _, _, err := VerifyProof(wrongRoot, key, proof); err == nil {
		t.Fatal("accepted wrong trusted root")
	}
	// Even a matching hash does not make malformed node encoding valid.
	malformed := []byte{0xc0}
	if _, _, err := VerifyProof(hashBytes(malformed), key, Proof{malformed}); err == nil {
		t.Fatal("accepted authenticated but malformed RLP node")
	}
}

func TestProvenTerminalValue(t *testing.T) {
	tree, store := newTestTrie(t)
	for _, key := range [][]byte{{}, {0x12}, {0x12, 0x34}, {0x12, 0x35}} {
		if err := tree.Update(key, append([]byte{9}, key...)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := tree.Commit()
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{{}, {0x12}, {0x12, 0x35}, {0x13}, {0xff}, {0x12, 0x34, 0}} {
		got, err := ProveValueAt(root, store, key)
		if err != nil {
			t.Fatal(err)
		}
		local, err := AssembleValueProofAt(root, store, key)
		if err != nil || !reflect.DeepEqual(local, got) {
			t.Fatalf("assembled terminal result differs for %x: %v", key, err)
		}
		value, ok, err := VerifyCompactProof(root, key, CompactProof(got.Nodes))
		if err != nil || ok != got.Exists || !bytes.Equal(value, got.Value) {
			t.Fatalf("key %x: value %x, exists %t, err %v", key, got.Value, got.Exists, err)
		}
	}
}
