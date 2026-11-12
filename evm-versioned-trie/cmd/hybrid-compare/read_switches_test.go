package main

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/comparison"
	"github.com/XiaodongQi1/Hybrid-code/evm-versioned-trie/workload"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

func TestHybridIndependentReadSwitches(t *testing.T) {
	for _, execution := range []bool{false, true} {
		for _, proof := range []bool{false, true} {
			t.Run(fmt.Sprintf("execution=%v/query=%v", execution, proof), func(t *testing.T) {
				layout := ht.DefaultEthereumPrefixBoundaryForestCacheConfig().Boundary
				layout.ExecutionReads = ht.DefaultExecutionReadConfig()
				layout.ExecutionReads.Enabled = execution
				boundary := boundaryTemplate(layout)
				c := config{
					Methods: []string{"hybrid"}, Suites: []string{"execution", "historical-query"},
					Engine: kvstore.LevelDB, PrefixSnapshot: true, HybridBoundary: &boundary,
					Resources: comparison.DefaultResourceBudget(), ParallelProof: ht.DefaultParallelProofConfig(),
					Synthetic: &workload.Options{BackgroundAccounts: 64, Blocks: 2, TransactionsPerBlock: 8, Contracts: 2, Seed: 31},
					Trials:    1, Queries: 8, WarmupBlocks: 0,
				}
				c.Resources.ApplicationCacheBytes = 8 << 20
				c.Resources.BlockCacheBytes = 1 << 20
				c.Resources.GlobalWriteBufferBytes = 1 << 20
				c.ParallelProof.Enabled = proof
				dir := t.TempDir()
				path, out := filepath.Join(dir, "config.json"), filepath.Join(dir, "run")
				if err := save(path, c); err != nil {
					t.Fatal(err)
				}
				if err := run(context.Background(), path, out); err != nil {
					t.Fatal(err)
				}
				var result reportData
				if err := strictFile(filepath.Join(out, "results.json"), &result); err != nil {
					t.Fatal(err)
				}
				if len(result.Trials) != 1 {
					t.Fatal("missing trial")
				}
				trial := result.Trials[0]
				if !trial.Verified || !trial.ReadOnlyUnchanged || !trial.SamplesComplete || len(trial.Blocks) != 2 || len(trial.Queries) != c.Queries {
					t.Fatal("incomplete switched trial")
				}
				var resources comparison.ResourcePlan
				if err := strictFile(filepath.Join(out, "resources.json"), &resources); err != nil {
					t.Fatal(err)
				}
				if (resources.Hybrid.ExecutionReadBudgetBytes > 0) != execution || (resources.Hybrid.ProofContextBudgetBytes > 0) != proof {
					t.Fatal("independent switches did not control budgets", resources.Hybrid)
				}
			})
		}
	}
}
