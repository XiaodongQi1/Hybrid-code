// Package mpt implements a small, synchronous Merkle Patricia Trie.
//
// It intentionally contains no background workers, asynchronous commits,
// blockchain consensus, or replay engine. IntervalState provides single-node,
// fixed-height snapshot persistence; callers replay post-snapshot blocks after
// restart. Committed roots are immutable snapshots and proofs can authenticate
// both membership and non-membership.
//
// Logical COW nodes memoize their hash and encoded size independently of their
// persistence revision. MaterializedStore and trusted field construction avoid
// redundant RLP decoding. Store-defined NodeLocation hints remain transient;
// CanonicalCursor retains an ancestor chain for fixed-root repeated lookups.
// Neither hints, revision markers nor cache state participate in Ethereum RLP.
// Public proof verification remains distinct from trusted local assembly.
package mpt
