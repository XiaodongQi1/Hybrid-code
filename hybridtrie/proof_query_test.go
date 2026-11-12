package hybridtrie

import (
	"errors"
	"testing"
)

type cursorFallbackSnapshot struct {
	calls  int
	closed bool
}

func (s *cursorFallbackSnapshot) get(column, []byte) ([]byte, error) { return nil, ErrNotFound }
func (s *cursorFallbackSnapshot) prev(column, []byte) ([]byte, []byte, error) {
	s.calls++
	return []byte{1}, []byte{2}, nil
}
func (s *cursorFallbackSnapshot) close() { s.closed = true }
func TestProofCursorFallbackPreservesSnapshotHooks(t *testing.T) {
	snap := &cursorFallbackSnapshot{}
	cursor, reusable, err := openPredecessor(snap, prefixColumn)
	if err != nil || reusable {
		t.Fatal(err, reusable)
	}
	for i := 0; i < 3; i++ {
		if _, _, err = cursor.prev(nil); err != nil {
			t.Fatal(err)
		}
	}
	cursor.close()
	cursor.close()
	if _, _, err = cursor.prev(nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if snap.calls != 3 || snap.closed {
		t.Fatal("adapter bypassed hooks or closed shared snapshot")
	}
}
