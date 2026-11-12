package hybridtrie

import "testing"

func TestExecutionReadSwitchSuppressesBackgroundWork(t *testing.T) {
	for _, mode := range []string{"disabled", "sync", "async"} {
		t.Run(mode, func(t *testing.T) {
			cfg := DefaultExecutionReadConfig()
			if mode == "disabled" {
				cfg.Enabled = false // Nonzero Workers must not override the switch.
			} else if mode == "sync" {
				cfg.Workers = 0
			}
			snapshot, key, loc := asyncFixture(t, 64)
			reads := newExecutionReads(snapshot, 0, cfg)
			defer reads.close()
			reads.prefetch(key, loc)
			reads.sealAndDrain()
			stats := reads.snapshotStats()
			if mode == "async" {
				if !reads.started || stats.BackgroundReads != 1 || stats.Prefetches != 1 {
					t.Fatal("enabled read did not run", stats)
				}
			} else if reads.started || reads.queue != nil || len(reads.entries) != 0 || stats.BackgroundReads != 0 || stats.Prefetches != 0 {
				t.Fatal("disabled background work allocated or ran", stats)
			}
		})
	}
}
