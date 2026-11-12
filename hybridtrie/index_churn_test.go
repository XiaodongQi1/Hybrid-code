package hybridtrie

import "testing"

func indexFixture(ids []uint64, buckets int) boundaryBank {
	b := boundaryBank{Index: make([]int32, buckets), Nodes: make([]boundaryNode, len(ids)), Roots: make([]boundaryRoot, len(ids))}
	for i, id := range ids {
		b.Nodes[i] = boundaryNode{Used: true, Link: boundaryLink{Root: uint16(i)}}
		b.Roots[i] = boundaryRoot{ID: id, Used: true}
		b.index(i)
	}
	return b
}

func TestResidentIndexLongChurnKeepsEmptyBuckets(t *testing.T) {
	ids := make([]uint64, 64)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	b := indexFixture(ids, 256)
	for round := 1; round <= 8192; round++ {
		i := (round - 1) % len(ids)
		b.unindex(i)
		b.Roots[i].ID = uint64(len(ids) + round)
		b.index(i)
		if round%64 != 0 {
			continue
		}
		empty := 0
		for _, x := range b.Index {
			if x < 0 {
				t.Fatal("persistent tombstone", round)
			}
			if x == 0 {
				empty++
			}
		}
		if empty != len(b.Index)-len(ids) {
			t.Fatal("lost index capacity", round, empty)
		}
		for j, root := range b.Roots {
			if b.find(position(root.ID, nil)) != j {
				t.Fatal("lost live position", round, j)
			}
		}
		if b.find(position(1_000_000, nil)) != -1 {
			t.Fatal("false hit")
		}
	}
}

func TestResidentIndexDeleteWrappedCollisionChain(t *testing.T) {
	var ids []uint64
	for id := uint64(1); len(ids) < 8; id++ {
		if positionHash(id, nil)%32 == 30 {
			ids = append(ids, id)
		}
	}
	b := indexFixture(ids, 32)
	for _, victim := range []int{3, 0, 7, 1, 5, 2, 6, 4} {
		b.unindex(victim)
		b.Nodes[victim].Used = false
		for j, id := range ids {
			want := -1
			if b.Nodes[j].Used {
				want = j
			}
			if got := b.find(position(id, nil)); got != want {
				t.Fatalf("delete %d, find %d: %d != %d", victim, j, got, want)
			}
		}
	}
	for _, x := range b.Index {
		if x != 0 {
			t.Fatal("empty index retained entries")
		}
	}
}
