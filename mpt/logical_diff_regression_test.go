package mpt

import (
	"bytes"
	"math/rand"
	"reflect"
	"testing"
)

// Inserting/removing siblings can move compressed endpoints. Compare emissions
// with the independent exact-path lookup for both inline and hashed nodes.
func TestDiffCompressedPathsAgainstExactLookup(t *testing.T) {
	rng := rand.New(rand.NewSource(37))
	mem := NewMemoryStore()
	empty, _ := NewEmpty(mem)
	old := empty
	for round := 0; round < 50; round++ {
		next := old.Copy()
		for i := 0; i < 20; i++ {
			key := []byte{0x12, byte(rng.Intn(8)), byte(rng.Intn(4))}
			if rng.Intn(4) == 0 {
				if err := next.Delete(key); err != nil {
					t.Fatal(err)
				}
			} else {
				v := bytes.Repeat([]byte{byte(rng.Intn(8) + 1)}, 1+48*rng.Intn(2))
				if err := next.Update(key, v); err != nil {
					t.Fatal(err)
				}
			}
		}
		var want, got []LogicalChange
		_, err := next.DiffLogicalNodes(empty, 1000, func(c LogicalChange) error {
			h, _, found, e := old.CanonicalNodeAtPath(c.Path)
			if e == nil && (!found || c.Hash != h) {
				want = append(want, c)
			}
			return e
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = next.DiffLogicalNodes(old, 1000, func(c LogicalChange) error { got = append(got, c); return nil })
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("round %d: diff emitted %d, want %d: %v", round, len(got), len(want), err)
		}
		root, err := next.Commit()
		if err != nil {
			t.Fatal(err)
		}
		old, err = Open(root, mem)
		if err != nil {
			t.Fatal(err)
		}
	}
}

type diffReadCounter struct {
	Store
	reads int
}

func (c *diffReadCounter) Get(key []byte) ([]byte, error) {
	c.reads++
	return c.Store.Get(key)
}

func TestDiffDoesNotLoadUnchangedSiblingBodies(t *testing.T) {
	mem := NewMemoryStore()
	before, _ := NewEmpty(mem)
	for i := 0; i < 16; i++ {
		if err := before.Update([]byte{byte(i << 4)}, bytes.Repeat([]byte{byte(i)}, 64)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := before.Commit()
	if err != nil {
		t.Fatal(err)
	}
	counter := &diffReadCounter{Store: mem}
	before, err = Open(root, counter)
	if err != nil {
		t.Fatal(err)
	}
	after, err := Open(root, mem)
	if err != nil {
		t.Fatal(err)
	}
	if err = after.Update([]byte{0x70}, bytes.Repeat([]byte{16}, 64)); err != nil {
		t.Fatal(err)
	}
	counter.reads = 0
	stats, err := after.DiffLogicalNodes(before, 32, func(LogicalChange) error { return nil })
	if err != nil || stats.Pruned != 15 || stats.Emitted != 2 || counter.reads > 2 {
		t.Fatalf("expected reads only along the changed path: %+v, reads=%d, err=%v", stats, counter.reads, err)
	}
}
