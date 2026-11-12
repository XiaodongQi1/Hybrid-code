package backend

import (
	"context"
	"github.com/XiaodongQi1/HybridTrie/kvstore"
)

func (b *Backend) EngineStats() (kvstore.Statistics, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.store.(*hybridStore); ok {
		return s.store.EngineStats()
	}
	return kvstore.Statistics{}, kvstore.ErrUnsupported
}
func (b *Backend) MeasureStorage(ctx context.Context, stabilize bool) (kvstore.Measurement, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if s, ok := b.store.(*hybridStore); ok {
		return s.store.MeasureStorage(ctx, stabilize)
	}
	return kvstore.Measurement{}, kvstore.ErrUnsupported
}
