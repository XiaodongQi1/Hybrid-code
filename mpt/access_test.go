package mpt

import (
	"reflect"
	"testing"
)

type accessLog struct{ paths []string }

func (l *accessLog) ObserveTrieAccess(p []byte) { l.paths = append(l.paths, string(p)) }
func TestAccessObserverIncludesMaterializedNodesAndCopies(t *testing.T) {
	tr, _ := NewEmpty(NewMemoryStore())
	tr.Update([]byte{0x12}, []byte("one"))
	tr.Update([]byte{0x13}, []byte("two"))
	log := &accessLog{}
	tr.SetAccessObserver(log)
	for k := 0; k < 2; k++ {
		if _, e := tr.Get([]byte{0x12}); e != nil {
			t.Fatal(e)
		}
	}
	n := len(log.paths)
	if n == 0 || n%2 != 0 || !reflect.DeepEqual(log.paths[:n/2], log.paths[n/2:]) {
		t.Fatal("repeated memory accesses not counted", log.paths)
	}
	tr.Hash()
	tr.Prove([]byte{0x12})
	tr.CanonicalNodeAtPath(nil)
	if len(log.paths) != n {
		t.Fatal("hash/proof/inspection changed heat")
	}
	cp := tr.Copy()
	cp.Update([]byte{0x12}, []byte("changed"))
	if len(log.paths) == n {
		t.Fatal("COW lost observer")
	}
}
