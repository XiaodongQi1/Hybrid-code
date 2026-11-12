package artifact

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/HybridTrie/evm-state/codec"
)

func TestSharedObjectsReadOnlyReopenAndCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	c := Config{WorkloadID: "fixture", Codec: codec.Ethereum}
	s, e := Open(path, c, true, false)
	if e != nil {
		t.Fatal(e)
	}
	raw := []byte("frozen input")
	id, e := s.Ensure("block-input", raw)
	if e != nil {
		t.Fatal(e)
	}
	other, e := s.Ensure("logical-block", raw)
	if e != nil || id == other {
		t.Fatal("kind not bound to identity", e)
	}
	st, e := os.Stat(filepath.Join(path, "block-input", blobName(id)))
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Get("block-input", id); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	s, e = Open(path, c, false, true)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	again, e := s.Ensure("block-input", raw)
	if e != nil || again != id {
		t.Fatal("read-only reuse failed", e)
	}
	after, e := os.Stat(filepath.Join(path, "block-input", blobName(id)))
	if e != nil || !st.ModTime().Equal(after.ModTime()) {
		t.Fatal("reuse rewrote object", e)
	}
	got, e := s.Get("block-input", id)
	if e != nil || !bytes.Equal(got, raw) {
		t.Fatal(e)
	}
	got[0] ^= 1
	got, e = s.Get("block-input", id)
	if e != nil || !bytes.Equal(got, raw) {
		t.Fatal("unowned Get", e)
	}
	if _, e = s.Ensure("block-input", []byte("absent")); !errors.Is(e, ErrReadOnly) {
		t.Fatal(e)
	}
	if _, e = s.Ensure("../escape", raw); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	wrong := c
	wrong.WorkloadID = "different"
	if bad, e := Open(path, wrong, false, true); e == nil {
		bad.Close()
		t.Fatal("identity mismatch opened")
	}
	// Corruption must be reported on reuse as well as on retrieval.
	if e = os.WriteFile(filepath.Join(path, "block-input", blobName(id)), []byte("corrupt"), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Get("block-input", id); !errors.Is(e, ErrCorrupt) {
		t.Fatal(e)
	}
	if _, e = s.Ensure("block-input", raw); !errors.Is(e, ErrCorrupt) {
		t.Fatal("corrupt object replaced", e)
	}
}

func TestSeparateHandlesReuseOneImmutableObject(t *testing.T) {
	path := filepath.Join(t.TempDir(), "source")
	c := Config{WorkloadID: "shared", Codec: codec.Ethereum}
	a, e := Open(path, c, true, false)
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := Open(path, c, false, false)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	type result struct{ err error }
	done := make(chan result, 2)
	for _, store := range []*Store{a, b} {
		go func(s *Store) { _, e := s.Ensure("block-input", []byte("same")); done <- result{e} }(store)
	}
	for i := 0; i < 2; i++ {
		if r := <-done; r.err != nil {
			t.Fatal(r.err)
		}
	}
	files, e := os.ReadDir(filepath.Join(path, "block-input"))
	if e != nil || len(files) != 1 {
		t.Fatal("duplicated shared artifact", len(files), e)
	}
}
