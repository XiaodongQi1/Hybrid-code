package backend

import (
	"context"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/block"
	snap "github.com/XiaodongQi1/Hybrid-code/evm-state/snapshot"
	"github.com/XiaodongQi1/Hybrid-code/evm-state/state"
	ht "github.com/XiaodongQi1/Hybrid-code/hybridtrie"
	"github.com/XiaodongQi1/Hybrid-code/mpt"
	vt "github.com/XiaodongQi1/Hybrid-code/versionedtrie"
)

// ImportPreparedSnapshot reuses a caller-owned common source. It never removes
// or rebuilds that source, so Archive and Path can import it independently.
func (b *Backend) ImportPreparedSnapshot(ctx context.Context, header block.InputBlock, builder *ht.SnapshotBuilder, expected *mpt.Hash) (*Result, SnapshotReport, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	var report SnapshotReport
	if b.readOnly {
		return nil, report, vt.ErrReadOnly
	}
	s, ok := b.store.(*hybridStore)
	if !ok || s.snapshot == nil || builder == nil {
		return nil, report, vt.ErrConflict
	}
	source, e := builder.OpenSource()
	if e != nil {
		return nil, report, e
	}
	defer source.Close()
	m, e := snap.Verify(ctx, source, header)
	if e != nil {
		return nil, report, e
	}
	if m.WorkloadID != s.sourceID || header.Height != b.initialHeight || expected != nil && m.Root != *expected {
		return nil, report, state.ErrCorrupt
	}
	head, e := s.Head()
	if e != nil {
		return nil, report, e
	}
	if head.Initialized && (head.Height != header.Height || head.Root != m.Root || head.CommitID != [32]byte(header.ID)) {
		return nil, report, vt.ErrConflict
	}
	mode, install := "streaming-prefix", s.store.ImportSnapshotContext
	if s.initialHash {
		mode, install = InitializationHashColdBulk, s.store.ImportHashSnapshotContext
	}
	_, stats, e := install(ctx, builder)
	if e != nil {
		return nil, report, hybridError(e)
	}
	report = SnapshotReport{Mode: mode, PreparedSource: true, BuildSessionOnly: true, ColdCache: true, Accounts: m.Accounts, NextTrieID: m.NextTrieID, Root: m.Root, InputDigest: m.InputDigest, AlreadyComplete: head.Initialized, Build: builder.Stats(), Import: stats}
	b.lastSnapshot = report
	return &Result{Height: header.Height, ID: header.ID, Root: m.Root}, report, nil
}
