package mpt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func TestParallelHashMatchesSerialEncoding(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var tries []*Trie
	empty, _ := NewEmpty(store)
	tries = append(tries, empty)
	for _, size := range []int{1, 2, 31, 32, 128, 4096} {
		trie, _ := NewEmpty(store)
		for i := 0; i < size; i++ {
			key := []byte{0xaa, byte(i >> 8), byte(i)}
			value := bytes.Repeat([]byte{byte(i + 1)}, 1+i%80)
			if err := trie.Update(key, value); err != nil {
				t.Fatal(err)
			}
		}
		tries = append(tries, trie)
		root, err := trie.Commit()
		if err != nil {
			t.Fatal(err)
		}
		saved, err := Open(root, store)
		if err != nil {
			t.Fatal(err)
		}
		tries = append(tries, saved)
		mixed := saved.Copy()
		for i := 0; i < size; i += 7 {
			if err := mixed.Update([]byte{0xaa, byte(i >> 8), byte(i)}, []byte("changed")); err != nil {
				t.Fatal(err)
			}
		}
		if err := mixed.Update([]byte{}, []byte("branch value")); err != nil {
			t.Fatal(err)
		}
		tries = append(tries, mixed)
	}
	// Small inline nodes, root branch values, and extensions must use exactly
	// the existing <32 byte embedding rule (not unconditional child hashes).
	inline, _ := NewEmpty(store)
	for _, k := range [][]byte{{0x10}, {0x11}, {0x20}, {}} {
		if err := inline.Update(k, []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	tries = append(tries, inline)
	want := make([]Hash, len(tries))
	for i, trie := range tries {
		var e error
		want[i], e = trie.Hash()
		if e != nil {
			t.Fatal(e)
		}
	}
	for _, workers := range []int{1, 2, 4, 8, 32} {
		got, e := HashMany(ctx, tries, workers)
		if e != nil {
			t.Fatal(e)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatal("root changed", workers, i)
			}
		}
		for i, trie := range tries {
			h, e := trie.HashParallel(ctx, workers)
			if e != nil || h != want[i] {
				t.Fatal("single hash", workers, i, e)
			}
		}
	}
	// Hashing shared immutable snapshots concurrently must not mutate them.
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i, trie := range tries {
				h, e := trie.HashParallel(ctx, 4)
				if e != nil || h != want[i] {
					t.Errorf("concurrent hash %d: %v", i, e)
				}
			}
		}()
	}
	wg.Wait()
	for i, trie := range tries {
		h, e := trie.Hash()
		if e != nil || h != want[i] {
			t.Fatal("mutated", i, e)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := HashMany(cancelled, tries, 4); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	for _, w := range []int{0, -1, 257} {
		if _, e := HashMany(ctx, tries, w); e == nil {
			t.Fatal(fmt.Sprint("invalid workers ", w))
		}
	}
}
