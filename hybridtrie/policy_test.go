package hybridtrie

import (
	"reflect"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	"testing"
)

func policyFixture(t *testing.T) *cachePolicy {
	t.Helper()
	cfg := DefaultPolicyConfig()
	cfg.PathsPerAttempt = 0
	p, _, _, err := newCachePolicy(cfg, 64, 64, 64, 1<<16, 1<<16, 1<<16, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	p.begin(256)
	p.nLimit = 704
	p.hLimit = 384
	return p
}
func addPolicyNode(t *testing.T, p *cachePolicy, path []byte, parent int32, state uint8, size uint64) int32 {
	t.Helper()
	i, err := p.newNode(path, mpt.Hash{byte(p.count + 1)}, parent, []byte{1})
	if err != nil {
		t.Fatal(err)
	}
	p.graph[i].NSize = size
	p.graph[i].HSize = 128
	if parent >= 0 {
		slot := int(path[len(path)-1])
		p.graph[parent].Children[slot] = i
	}
	p.apply(i, state)
	return i
}
func paperFixture(t *testing.T, score float64) (*cachePolicy, int32) {
	p := policyFixture(t)
	r := addPolicyNode(t, p, nil, -1, nodeState, 0)
	pp := addPolicyNode(t, p, []byte{1}, r, nodeState, 256)
	q := addPolicyNode(t, p, []byte{2}, r, nodeState, 256)
	a := addPolicyNode(t, p, []byte{1, 1}, pp, nodeState, 192)
	addPolicyNode(t, p, []byte{1, 2}, pp, hashState, 128)
	addPolicyNode(t, p, []byte{2, 1}, q, hashState, 128)
	x := addPolicyNode(t, p, []byte{2, 2}, q, diskState, 384)
	p.graph[a].A = initialFrequency(8, 128)
	p.graph[pp].A = initialFrequency(16, 192)
	p.graph[q].A = initialFrequency(50, 256)
	p.graph[r].U = initialFrequency(10, 256)
	p.graph[q].U = initialFrequency(5, 256)
	p.graph[x].A = initialFrequency(score, 256)
	p.nodes.offer(p.handle(a))
	p.nodes.offer(p.handle(q))
	return p, x
}
func states(p *cachePolicy) []uint8 {
	s := make([]uint8, p.count)
	for i := range s {
		s[i] = p.graph[i].State
	}
	return s
}
func assertPolicyTree(t *testing.T, p *cachePolicy) {
	t.Helper()
	var n, h, b uint64
	nc, hc := 0, 0
	for i := 0; i < p.count; i++ {
		e := p.graph[i]
		switch e.State {
		case nodeState:
			n += e.NSize
			b += uint64(len(e.Body))
			nc++
		case hashState:
			h += e.HSize
			hc++
		}
		if e.State != diskState && i != 0 {
			if e.Parent < 0 || p.graph[e.Parent].State != nodeState {
				t.Fatal("disconnected resident", i)
			}
		}
		if e.State == hashState {
			for _, child := range e.Children {
				if child >= 0 && p.graph[child].State != diskState {
					t.Fatal("H owns resident child")
				}
			}
		}
	}
	if n != p.nUsed || h != p.hUsed || b != p.bodyUsed || nc != p.nCount || hc != p.hCount || n > p.nLimit || h > p.hLimit {
		t.Fatal("accounting", n, h, p.nUsed, p.hUsed)
	}
}
func TestPolicyPaperCascadeAndRollback(t *testing.T) {
	p, x := paperFixture(t, 13)
	if !p.tryNode(x) {
		t.Fatal("paper successful case rejected", p.metrics)
	}
	if got := states(p); !reflect.DeepEqual(got, []uint8{nodeState, hashState, nodeState, diskState, diskState, hashState, nodeState}) {
		t.Fatal(got)
	}
	if p.nUsed != 640 || p.hUsed != 256 || p.metrics.MaxCascade != 2 {
		t.Fatal(p.nUsed, p.hUsed, p.metrics)
	}
	assertPolicyTree(t, p)
	p, x = paperFixture(t, 4.33)
	before := states(p)
	n, h := p.nUsed, p.hUsed
	if p.tryNode(x) {
		t.Fatal("paper failure accepted")
	}
	if !reflect.DeepEqual(before, states(p)) || p.nUsed != n || p.hUsed != h || p.undoN != 0 {
		t.Fatal("partial eviction survived rollback")
	}
	if p.metrics.NodeRollbacks != 1 || !p.tryHash(x, false) {
		t.Fatal("rejected N did not admit H", p.metrics)
	}
	assertPolicyTree(t, p)
}
func TestPolicyNestedHashFailureDoesNotUndoNode(t *testing.T) {
	p, x := paperFixture(t, 13)
	p.hLimit = 256
	p.graph[x].NSize = 192
	if !p.tryNode(x) {
		t.Fatal("node admission failed")
	}
	if p.graph[3].State != diskState || p.graph[4].State != hashState || p.graph[1].State != nodeState || p.graph[x].State != nodeState {
		t.Fatal("failed nested H undid outer plan", states(p))
	}
	if p.metrics.HashRejects == 0 {
		t.Fatal("nested H path not tested")
	}
	assertPolicyTree(t, p)
}
func TestPolicyHashContinuousRollbackAndTie(t *testing.T) {
	p := policyFixture(t)
	p.nLimit = 4096
	r := addPolicyNode(t, p, nil, -1, nodeState, 0)
	low := addPolicyNode(t, p, []byte{1}, r, nodeState, 100)
	high := addPolicyNode(t, p, []byte{2}, r, nodeState, 100)
	mid := addPolicyNode(t, p, []byte{3}, r, nodeState, 100)
	a := addPolicyNode(t, p, []byte{1, 1}, low, hashState, 0)
	b := addPolicyNode(t, p, []byte{2, 1}, high, hashState, 0)
	addPolicyNode(t, p, []byte{2, 2}, high, hashState, 0)
	x := addPolicyNode(t, p, []byte{3, 1}, mid, diskState, 0)
	p.graph[x].HSize = 256
	p.graph[low].U = initialFrequency(1, 256)
	p.graph[high].U = initialFrequency(8, 256)
	p.graph[mid].U = initialFrequency(5, 256)
	p.hashes.offer(p.handle(a))
	p.hashes.offer(p.handle(b))
	before := states(p)
	if p.tryHash(x, false) || !reflect.DeepEqual(before, states(p)) || p.metrics.HashRollbacks != 1 {
		t.Fatal("partial H eviction not undone", states(p), p.metrics)
	}
	p.graph[mid].U = p.graph[low].U
	if p.tryHash(x, false) || !reflect.DeepEqual(before, states(p)) {
		t.Fatal("equal score replaced an H")
	}
	assertPolicyTree(t, p)
}
func TestPolicySamplerBudgetAndStaleHandles(t *testing.T) {
	p, x := paperFixture(t, 13)
	p.cfg.PathsPerAttempt = 4
	before := p.random
	p.sample()
	if p.random == before {
		t.Fatal("PRNG reset/not advanced")
	}
	for _, h := range p.nodes.slots {
		if h.Allocation == 0 {
			continue
		}
		if !p.frontier(h.Index) {
			t.Fatal("stopped at H edge instead of N frontier")
		}
	}
	p.cfg.PathsPerAttempt = 0
	for i := range p.nodes.slots {
		p.nodes.slots[i] = sampleHandle{9999, 99}
	}
	if p.victim(true, x) != -1 {
		t.Fatal("stale handle selected")
	}
	for units := uint64(0); units < 80; units++ {
		q, candidate := paperFixture(t, 13)
		q.remaining = units
		original := states(q)
		accepted := q.tryNode(candidate)
		if !accepted && !reflect.DeepEqual(original, states(q)) {
			t.Fatal("budget failure did not restore structure", units)
		}
		if q.metrics.WorkUnits > units || q.undoN != 0 {
			t.Fatal("unbounded work/undo", units, q.metrics)
		}
		assertPolicyTree(t, q)
	}
}

func TestPolicyRetryDoesNotRefillBudgetOrResetPRNG(t *testing.T) {
	p := policyFixture(t)
	initial := p.remaining
	if !p.spend(100) {
		t.Fatal("budget")
	}
	before := p.random
	p.rng(7)
	advanced := p.random
	p.begin(256)
	if p.remaining != initial-100 || p.random != advanced || before == advanced {
		t.Fatal("retry reset budget/PRNG")
	}
	p.begin(257)
	if p.remaining != initial {
		t.Fatal("new block did not receive budget")
	}
}
