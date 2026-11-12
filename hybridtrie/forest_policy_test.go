package hybridtrie

import (
	"github.com/XiaodongQi1/HybridTrie/mpt"
	"testing"
	"unsafe"
)

func replacementFixture(t *testing.T, score float64) (*forestShared, int) {
	t.Helper()
	f, err := newForestShared(DefaultReservedSharedConfig())
	if err != nil {
		t.Fatal(err)
	}
	f.heat.height = 20
	f.remaining = 1 << 20
	f.cfg.TreesPerAttempt = 32
	f.cfg.PathsPerTrie = 4
	f.cfg.HashBudget = ClassBudget{}
	add := func(id uint64, path []byte, parent int32, size int, a float64, state uint8) int {
		i := f.count
		f.count++
		e := forestPlanNode{Pos: position(id, path), Hash: mpt.Hash{byte(i + 1)}, Parent: parent, Body: make([]byte, size), State: state, A: initialFrequency(a, 20)}
		for j := range e.Children {
			e.Children[j] = -1
		}
		if parent < 0 {
			e.RootSlot = int32(f.rootN)
			f.roots[f.rootN] = int32(i)
			f.rootN++
		} else {
			e.RootSlot = f.graph[parent].RootSlot
			f.graph[parent].Children[path[len(path)-1]] = int32(i)
		}
		f.graph[i] = e
		return i
	}
	add(0, nil, -1, 8, 10, nodeState)
	add(0, []byte{1}, 0, 512, 6, nodeState)
	add(0, []byte{2}, 0, 512, 6, nodeState)
	add(1, nil, -1, 8, 10, nodeState)
	x := add(1, []byte{1}, 3, 1024, score, diskState)
	f.initializeUsage()
	total := f.nUsed[0] + f.nUsed[1]
	rootCost := uint64(unsafe.Sizeof(forestN{})) + 8
	f.cfg.NodeBudget = ClassBudget{total, rootCost, rootCost}
	return f, x
}
func TestForestReplacementComparesTotalLossAndRollback(t *testing.T) {
	f, x := replacementFixture(t, 10)
	beforeN, beforeH, _, _, _, _ := f.usage()
	if f.try(x, nodeState) {
		t.Fatal("10 displaced two entries worth 6 each")
	}
	n, h, _, _, _, _ := f.usage()
	if n != beforeN || h != beforeH || f.graph[1].State != nodeState || f.graph[2].State != nodeState || f.graph[x].State != diskState {
		t.Fatal("partial rollback")
	}
	if f.stats.ScoreRejects == 0 {
		t.Fatal("not rejected by total utility")
	}
	f, x = replacementFixture(t, 20)
	if !f.try(x, nodeState) || f.graph[1].State != diskState || f.graph[2].State != diskState {
		t.Fatal("profitable replacement not accepted")
	}
	n, h, _, _, _, _ = f.usage()
	if !planUsage(f.cfg, beforeN, beforeH, n, h, 1) {
		t.Fatal("floor crossed")
	}
}
func TestForestReplacementIncludesHashCascadeAndProtectsQuota(t *testing.T) {
	f, x := replacementFixture(t, 10)
	f.graph[2].State = diskState
	f.graph[x].Body = make([]byte, 512)
	i := f.count
	f.count++
	f.graph[i] = forestPlanNode{Pos: position(0, []byte{1, 1}), Parent: 1, RootSlot: 0, State: hashState}
	for j := range f.graph[i].Children {
		f.graph[i].Children[j] = -1
	}
	f.graph[1].Children[1] = int32(i)
	f.graph[1].A = initialFrequency(1, 20)
	f.graph[1].U = initialFrequency(20, 20)
	f.cfg.HashBudget = ClassBudget{TotalBytes: 1024}
	f.initializeUsage()
	f.cfg.NodeBudget.TotalBytes = f.nUsed[0] + f.nUsed[1]
	if f.try(x, nodeState) {
		t.Fatal("ignored forced H loss")
	}
	if f.graph[i].State != hashState || f.graph[1].State != nodeState {
		t.Fatal("cascade rollback")
	}
	f.graph[x].A = initialFrequency(100, 20)
	f.cfg.HashBudget.AccountReserveBytes = uint64(unsafe.Sizeof(forestH{}))
	if f.try(x, nodeState) {
		t.Fatal("N cascade stole account H reservation")
	}
}
func TestForestFenwickWeightAndUndo(t *testing.T) {
	f, x := replacementFixture(t, 100)
	before := f.weightTotal(0, 0)
	a := f.graph[1]
	weight := uint64(unsafe.Sizeof(forestN{})) + uint64(len(a.Body))
	start := f.undoN
	if !f.change(1, diskState) {
		t.Fatal("change")
	}
	if f.weightTotal(0, 0) != before-weight {
		t.Fatal("weight update")
	}
	f.rollback(start)
	if f.weightTotal(0, 0) != before {
		t.Fatal("weight rollback")
	}
	if !f.try(x, nodeState) {
		t.Fatal("replacement")
	}
	if f.weightTotal(0, 0) != uint64(unsafe.Sizeof(forestN{}))+8 {
		t.Fatal("index did not follow multiple victims")
	}
}

func TestForestAdmitsCompleteMissingAncestorChain(t *testing.T) {
	f, x := replacementFixture(t, 20)
	f.graph[3].State = diskState
	f.graph[x].Body = make([]byte, 900)
	f.initializeUsage()
	if !f.try(x, nodeState) || f.graph[3].State != nodeState || f.graph[x].State != nodeState {
		t.Fatal("missing root not included in admission")
	}
	n, h, _, _, _, tries := f.usage()
	if !f.cfg.NodeBudget.fits(n) || !f.cfg.HashBudget.fits(h) || tries != 2 {
		t.Fatal("ancestor cost not charged")
	}
}
