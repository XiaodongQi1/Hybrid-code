package mpt

import "testing"

func TestEqualDirtyAndPersisted(t *testing.T) {
	db := NewMemoryStore()
	a, _ := NewEmpty(db)
	for _, k := range []string{"a", "ab", "b", "long"} {
		if e := a.Update([]byte(k), []byte(k)); e != nil {
			t.Fatal(e)
		}
	}
	root, e := a.Commit()
	if e != nil {
		t.Fatal(e)
	}
	b, e := Open(root, db)
	if e != nil {
		t.Fatal(e)
	}
	b.Update([]byte("a"), []byte("other"))
	eq, e := a.Equal(b)
	if e != nil || eq {
		t.Fatal(eq, e)
	}
	b.Update([]byte("a"), []byte("a"))
	eq, e = a.Equal(b)
	if e != nil || !eq {
		t.Fatal(eq, e)
	}
	for _, k := range []string{"a", "ab", "b", "long"} {
		b.Delete([]byte(k))
	}
	if !b.IsEmpty() {
		t.Fatal("not normalized empty")
	}
	eq, e = a.Equal(b)
	if e != nil || eq {
		t.Fatal(eq, e)
	}
}
