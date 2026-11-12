package comparison

import (
	"context"
	"fmt"

	"github.com/XiaodongQi1/Hybrid-code/evm-runtime/geth/crypto"
	"github.com/XiaodongQi1/Hybrid-code/kvstore"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
)

func (a *Archive) EngineStats() (kvstore.Statistics, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if e := a.check(); e != nil {
		return kvstore.Statistics{}, e
	}
	stats, err := a.db.Statistics()
	a.nodeCache.Report(&stats)
	return stats, err
}
func (a *Archive) MeasureStorage(ctx context.Context, stabilize bool) (kvstore.Measurement, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if e := a.check(); e != nil {
		return kvstore.Measurement{}, e
	}
	result, err := a.db.Measure(ctx, func(column string, k, v []byte) (kvstore.Classification, error) {
		state := func(name string) (kvstore.Classification, error) {
			return kvstore.Classification{Name: name, State: true}, nil
		}
		if column == "hash" {
			if len(k) != 32 || !equalHash(k, v) {
				return kvstore.Classification{}, stateCorrupt()
			}
			n, e := mpt.InspectCanonicalNode(v)
			if e != nil {
				return kvstore.Classification{}, e
			}
			switch n.Kind {
			case mpt.CanonicalBranch:
				return state("hash-branch")
			case mpt.CanonicalExtension:
				return state("hash-extension")
			case mpt.CanonicalLeaf:
				return state("hash-leaf")
			}
		}
		if column == "default" {
			if string(k) == "M/initial-import" {
				return state("initialization-marker")
			}
			if string(k) == "M/config" {
				return kvstore.Classification{Name: "configuration"}, nil
			}
			if string(k) == "M/head" {
				return state("coordination")
			}
			if len(k) == 9 && k[0] == 'B' {
				return state("root-headers")
			}
			if len(k) == 33 && k[0] == 'C' && equalHash(k[1:], v) {
				return state("code")
			}
		}
		return kvstore.Classification{}, stateCorrupt()
	}, stabilize)
	a.nodeCache.Report(&result.BeforeMaintenance)
	a.nodeCache.Report(&result.AfterMaintenance)
	return result, err
}
func equalHash(key, raw []byte) bool {
	h := crypto.Keccak256Hash(raw)
	return string(h[:]) == string(key)
}
func stateCorrupt() error {
	return fmt.Errorf("%w: unknown or invalid Archive state record", kvstore.ErrCorrupt)
}
