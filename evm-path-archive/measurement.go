package patharchive

import (
	"context"
	"fmt"

	"github.com/XiaodongQi1/Hybrid-code/kvstore"
)

var measuredStateClasses = map[byte]string{'P': "current-trie", 'F': "current-flat", 'C': "code", 'B': "root-headers", 'Q': "history-checkpoints", 'I': "history-index", 'J': "history-index", 'X': "history-index", 'Y': "history-index", 'R': "history-directory", 'D': "history-data", 'G': "history-data", 'H': "history-data"}

func (a *Archive) EngineStats() (kvstore.Statistics, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	if e := a.s.check(); e != nil {
		return kvstore.Statistics{}, e
	}
	if d, ok := a.s.db.(*sharedEngine); ok {
		stats, err := d.db.Statistics()
		a.s.nodeCache.Report(&stats)
		return stats, err
	}
	return kvstore.Statistics{}, kvstore.ErrUnsupported
}

// Only the separate state-only layout participates in the common measurement.
// Legacy ledgers remain available through the existing diagnostic APIs.
func (a *Archive) MeasureStorage(ctx context.Context, stabilize bool) (kvstore.Measurement, error) {
	a.s.mu.RLock()
	defer a.s.mu.RUnlock()
	if e := a.s.check(); e != nil {
		return kvstore.Measurement{}, e
	}
	d, ok := a.s.db.(*sharedEngine)
	if !ok || !a.s.cfg.StateOnly {
		return kvstore.Measurement{}, kvstore.ErrUnsupported
	}
	result, err := d.db.Measure(ctx, func(column string, k, v []byte) (kvstore.Classification, error) {
		if column != "default" || len(k) == 0 {
			return kvstore.Classification{}, ErrCorrupt
		}
		if string(k) == "M/config" {
			return kvstore.Classification{Name: "configuration"}, nil
		}
		if string(k) == "M/head" {
			return kvstore.Classification{Name: "coordination", State: true}, nil
		}
		if string(k) == "M/initial-import" {
			return kvstore.Classification{Name: "initialization-marker", State: true}, nil
		}
		if class := measuredStateClasses[k[0]]; class != "" {
			return kvstore.Classification{Name: class, State: true}, nil
		}
		return kvstore.Classification{}, fmt.Errorf("%w: unclassified state-only Path record", kvstore.ErrCorrupt)
	}, stabilize)
	a.s.nodeCache.Report(&result.BeforeMaintenance)
	a.s.nodeCache.Report(&result.AfterMaintenance)
	return result, err
}
