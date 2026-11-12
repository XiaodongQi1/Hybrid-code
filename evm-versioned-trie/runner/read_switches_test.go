package runner

import (
	"path/filepath"
	"testing"

	ht "github.com/XiaodongQi1/HybridTrie/hybridtrie"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
)

func TestHybridReadSwitchesLevelDB(t *testing.T) { testHybridReadSwitches(t, kvstore.LevelDB) }

func testHybridReadSwitches(t *testing.T, engine kvstore.Engine) {
	var reference Report
	for _, execution := range []string{"disabled", "sync", "async"} {
		for _, proof := range []string{"disabled", "sync", "async"} {
			t.Run(execution+"-"+proof, func(t *testing.T) {
				cfg := hybridLevelConfig()
				cfg.HybridStorage.Engine = engine
				cfg.HybridInitialization = "prefix"
				cfg.Synthetic.BackgroundAccounts = 256
				cfg.Synthetic.TransactionsPerBlock = 24
				cfg.Synthetic.StorageSlots = 16
				allocation, err := ht.BoundaryCacheAllocation(cfg.HybridCache.Boundary)
				if err != nil {
					t.Fatal(err)
				}
				cfg.HybridCache.Boundary.NodeBudget = ht.ClassBudget{TotalBytes: 8 * allocation.Boundary.SlotBytes}
				reads := ht.DefaultExecutionReadConfig()
				reads.Enabled = execution != "disabled"
				if execution == "sync" {
					reads.Workers = 0
				}
				cfg.HybridCache.Boundary.ExecutionReads = reads
				cfg.ParallelProof = ht.DefaultParallelProofConfig()
				cfg.ParallelProof.Enabled = proof != "disabled"
				if proof == "sync" {
					cfg.ParallelProof.Workers = 0
				}
				dir := t.TempDir()
				path, out := filepath.Join(dir, "config.json"), filepath.Join(dir, "run")
				if err = jsonFile(path, cfg); err != nil {
					t.Fatal(err)
				}
				r, err := Run(path, out)
				if err != nil {
					t.Fatal(err)
				}
				if reference.Schema == "" {
					reference = r
				}
				if r.FinalRoot != reference.FinalRoot || r.Transactions != reference.Transactions || r.FailedTransactions != reference.FailedTransactions || r.AccountProofs != reference.AccountProofs || r.StorageProofs != reference.StorageProofs || !r.ReadOnlyReopen {
					t.Fatal("switch changed results", r)
				}
				if execution == "disabled" {
					if r.Cache.ExecutionReadBudgetBytes != 0 || r.Cache.ExecutionReads != (ht.ExecutionReadStats{}) {
						t.Fatal("disabled execution reader ran", r.Cache.ExecutionReads)
					}
				} else {
					if r.Cache.ExecutionReadBudgetBytes == 0 || r.Cache.ExecutionReads.RecordReads == 0 {
						t.Fatal("configured execution reader inactive")
					}
					if execution == "sync" && (r.Cache.ExecutionReads.Prefetches != 0 || r.Cache.ExecutionReads.BackgroundReads != 0 || r.Cache.ExecutionReads.QueueBytes != 0) {
						t.Fatal("sync execution started background work")
					}
					if execution == "async" && r.Cache.ExecutionReads.Prefetches == 0 {
						t.Fatal("async execution never dispatched work")
					}
				}
				if proof == "disabled" {
					if r.QueryRequests != 0 || r.QueryWorkersStarted != 0 || r.QueryBackgroundReads != 0 {
						t.Fatal("disabled proof path ran")
					}
				} else {
					if r.QueryRequests != uint64(r.AccountProofs+r.StorageProofs) {
						t.Fatal("runner did not exercise serving queries")
					}
					if proof == "sync" && (r.QueryWorkersStarted != 0 || r.QueryBackgroundReads != 0) {
						t.Fatal("sync proof started background work")
					}
					if proof == "async" && (r.QueryWorkersStarted == 0 || r.QueryBackgroundReads == 0) {
						t.Fatal("async proof configuration was ignored")
					}
				}
				if execution == "sync" && proof == "sync" {
					again, err := RunWithOptions(path, out, Options{Resume: true})
					if err != nil || again != r {
						t.Fatal("completed result changed on resume", err)
					}
					cfg.ParallelProof.Workers = 2
					if err = jsonFile(path, cfg); err != nil {
						t.Fatal(err)
					}
					if _, err = RunWithOptions(path, out, Options{Resume: true}); err == nil {
						t.Fatal("changed switch accepted for sealed run")
					}
				}
			})
		}
	}
}
