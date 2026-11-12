package mpt

import (
	"bytes"
	"errors"
	"testing"
)

type noLoadStore struct{ calls int }

func (s *noLoadStore) Get([]byte) ([]byte, error) { s.calls++; return nil, ErrNotFound }
func (s *noLoadStore) NewBatch() Batch            { panic("unexpected write") }

func TestWalkMaterializedDoesNotLoadHashSubtrees(t *testing.T) {
	db := NewMemoryStore()
	tree, _ := NewEmpty(db)
	for i := 0; i < 256; i++ {
		if err := tree.Update([]byte{byte(i), 0}, bytes.Repeat([]byte{byte(i)}, 40)); err != nil {
			t.Fatal(err)
		}
	}
	root, err := tree.Commit()
	if err != nil {
		t.Fatal(err)
	}
	cold, err := Open(root, db)
	if err != nil {
		t.Fatal(err)
	}
	guard := &noLoadStore{}
	cold.store = guard
	count := 0
	if err := cold.WalkMaterialized(1, func(LogicalChange) error { count++; return nil }); err != nil || count != 1 || guard.calls != 0 {
		t.Fatal(err, count, guard.calls)
	}
	// Open retains the already decoded root, but no cold descendants.
	count = 0
	cold.store = db
	if err := cold.Update([]byte{7, 0}, []byte{99}); err != nil {
		t.Fatal(err)
	}
	cold.store = guard
	if err := cold.WalkMaterialized(64, func(c LogicalChange) error {
		count++
		if len(c.Path) != 0 && len(c.RLP) < 32 {
			t.Fatal("inline node emitted")
		}
		return nil
	}); err != nil || count == 0 || count > 4 || guard.calls != 0 {
		t.Fatal(err, count, guard.calls)
	}
	sentinel := errors.New("callback")
	if err := cold.WalkMaterialized(64, func(LogicalChange) error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatal(err)
	}
	if err := cold.WalkMaterialized(1, func(LogicalChange) error { return nil }); !errors.Is(err, ErrInvalidPath) {
		t.Fatal("walk limit", err)
	}
}

func TestWalkMaterializedGroupsInlineChildren(t *testing.T) {
	tree, _ := NewEmpty(NewMemoryStore())
	tree.Update([]byte{0x10}, []byte{1})
	tree.Update([]byte{0x11}, []byte{2})
	count := 0
	if err := tree.WalkMaterialized(10, func(c LogicalChange) error {
		count++
		if len(c.Path) != 0 {
			t.Fatal("inline got independent entry")
		}
		return nil
	}); err != nil || count != 1 {
		t.Fatal(err, count)
	}
}
