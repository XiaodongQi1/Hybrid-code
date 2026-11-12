package hybridtrie

import (
	"context"
	"encoding/json"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func TestParallelProofBranchComparison(t *testing.T) {
	out := os.Getenv("HYBRID_PARALLEL_BRANCH_REPORT")
	if out == "" {
		t.Skip("opt-in branch proof measurement")
	}
	engine := kvstore.Engine(os.Getenv("HYBRID_PARALLEL_PROOF_ENGINE"))
	if engine == "" {
		engine = kvstore.LevelDB
	}
	type row struct {
		Children, PrefixChildren, Trial                        int
		Mode                                                   string
		Verified                                               bool
		LatencyNS                                              []int64
		ReadNodes, HeaderReads, WorkerStarts, PeakContextBytes uint64
	}
	var rows []row
	for _, children := range []int{2, 4, 8, 16} {
		prefixCounts := []int{0, children}
		if children == 16 {
			prefixCounts = []int{0, 1, 2, 4, 8, 16}
		}
		for _, prefixes := range prefixCounts {
			s := planStore(t, engine)
			var keys [][]byte
			for i := 0; i < children; i++ {
				keys = append(keys, []byte{byte(i << 4)})
			}
			tr := planTree(t, keys, 64)
			mask := uint16((uint32(1) << prefixes) - 1)
			planCommit(t, s, tr, mask, false)
			root, _ := tr.Hash()
			want := make([]mpt.ProofResult, children)
			for i, key := range keys {
				v, e := s.ProofViewAt(0)
				if e != nil {
					t.Fatal(e)
				}
				want[i], _, e = v.AssembleValueProofWithStats(0, root, key)
				v.Close()
				if e != nil {
					t.Fatal(e)
				}
			}
			for trial := 0; trial < 3; trial++ {
				for order := 0; order < 5; order++ {
					mode := (order + trial) % 5
					workers := []uint32{0, 0, 1, 2, 4}[mode]
					names := []string{"legacy", "planned-sync", "parallel-1", "parallel-2", "parallel-4"}
					c := DefaultParallelProofConfig()
					c.Workers = workers
					read := func(key []byte) (mpt.ProofResult, ParallelProofStats, error) {
						v, e := s.ProofViewAt(0)
						if e != nil {
							return mpt.ProofResult{}, ParallelProofStats{}, e
						}
						defer v.Close()
						if mode == 0 {
							r, st, e := v.AssembleValueProofWithStats(0, root, key)
							return r, ParallelProofStats{ReadNodes: st.ReadNodes, SiblingHeaderReads: st.SiblingHeaderReads}, e
						}
						ps, e := v.BeginProofSession(context.Background(), c)
						if e != nil {
							return mpt.ProofResult{}, ParallelProofStats{}, e
						}
						defer ps.Close()
						if _, e = ps.Walk(0, root, key); e != nil {
							return mpt.ProofResult{}, ParallelProofStats{}, e
						}
						r, st, e := ps.Finish()
						if e != nil {
							return mpt.ProofResult{}, st, e
						}
						return r[0], st, nil
					}
					for i := 0; i < 64; i++ {
						if _, _, e := read(keys[i%children]); e != nil {
							t.Fatal(e)
						}
					}
					r := row{Children: children, PrefixChildren: bits.OnesCount16(mask), Trial: trial, Mode: names[mode], LatencyNS: make([]int64, 256)}
					results := make([]mpt.ProofResult, len(r.LatencyNS))
					for i := range results {
						start := time.Now()
						got, st, e := read(keys[i%children])
						r.LatencyNS[i] = time.Since(start).Nanoseconds()
						if e != nil {
							t.Fatal(e)
						}
						results[i] = got
						r.ReadNodes += st.ReadNodes
						r.HeaderReads += st.SiblingHeaderReads
						r.WorkerStarts += st.WorkerStarts
						if st.PeakContextBytes > r.PeakContextBytes {
							r.PeakContextBytes = st.PeakContextBytes
						}
					}
					for i, got := range results {
						key := keys[i%children]
						if !reflect.DeepEqual(got, want[i%children]) {
							t.Fatal("changed bytes")
						}
						if _, exists, e := mpt.VerifyCompactProof(root, key, got.Nodes); e != nil || !exists {
							t.Fatal(e)
						}
					}
					r.Verified = true
					rows = append(rows, r)
				}
			}
			if e := s.Close(); e != nil {
				t.Fatal(e)
			}
			data, e := json.MarshalIndent(struct {
				Engine kvstore.Engine
				Rows   []row
			}{engine, rows}, "", "  ")
			if e != nil {
				t.Fatal(e)
			}
			if e = os.MkdirAll(filepath.Dir(out), 0700); e != nil {
				t.Fatal(e)
			}
			if e = os.WriteFile(out+".tmp", append(data, '\n'), 0600); e != nil {
				t.Fatal(e)
			}
			if e = os.Rename(out+".tmp", out); e != nil {
				t.Fatal(e)
			}
			t.Logf("children=%d prefix=%d complete", children, prefixes)
		}
	}
}
