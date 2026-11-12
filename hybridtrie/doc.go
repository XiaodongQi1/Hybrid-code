// Package hybridtrie implements the physical encoding layer for an
// Ethereum-compatible hybrid versioned MPT. Canonical RLP and Keccak remain
// owned by package mpt; local positions, heights and reference modes never
// participate in authentication.
//
// The shared KV layer selects RocksDB (build tag rocksdb) or GoLevelDB for
// Prefix versions and closed Hash representations, with atomic block commits
// and snapshot-scoped reads.
// The prefix-boundary execution cache retains parsed node fields and local
// source/version hints; heat sketches and replacement policy are local metadata.
// Initial Hash-only forests can publish in one cold atomic commit; source-aware
// roots preserve later mixed versions. Account namespace zero is implicit;
// storage lifecycles are versioned. Dirty-node emissions drive publication;
// transient child locations and ancestor cursors avoid repeated root walks.
// Trusted local reads decode fields without rehashing the record; strict audit
// and client-proof verification authenticate against an independent root.
// Decoding a record alone does not authenticate its self hash or prove that it
// is reachable from a trusted root.
package hybridtrie

import "errors"

const (
	LogicalScheme  = "ethereum-mpt-keccak-rlp/v1"
	StoreFormat    = "hybrid-mpt-dualref/v6"
	MaxDepth       = 64
	MaxRecordBytes = 1 << 20
)

var (
	ErrCorrupt          = errors.New("hybridtrie: invalid encoding")
	ErrLimit            = errors.New("hybridtrie: size limit exceeded")
	ErrHashMismatch     = errors.New("hybridtrie: canonical hash mismatch")
	ErrUnresolvedPrefix = errors.New("hybridtrie: prefix hash resolver required")
	ErrEmbeddedNode     = errors.New("hybridtrie: embedded child belongs to parent record")
	ErrPositionMismatch = errors.New("hybridtrie: predecessor outside requested position or height")
)
