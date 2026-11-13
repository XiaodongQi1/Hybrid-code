# HybridTrie

HybridTrie is a research implementation of versioned Ethereum state storage.
It combines position-addressed node versions with hash-addressed MPT nodes to
reduce historical storage while preserving Ethereum's logical trie, canonical
RLP, Keccak state roots, and account/storage proofs. Cache policy influences
physical node placement; it does not change the authenticated state.

This independent repository contains the storage engine, EVM adapter, and local
comparison framework for HybridTrie, Hash Archive, and Path Archive. All local
Go modules are included under `github.com/XiaodongQi1/Hybrid-code`. No checkout of
the original Statera project is required.

The supplied workloads are synthetic and execute through the included EVM.
Downloaded mainnet data, experiment results, databases, binaries, and private
configurations are not part of the repository. This is a local storage research
framework; it does not include a network node, consensus service, or SSH launcher.

- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Storage and execution design](#storage-and-execution-design)
- [Cache policy](#cache-policy)
- [Asynchronous reads and historical proofs](#asynchronous-reads-and-historical-proofs)
- [Comparing methods](#comparing-methods)
- [Initialization and recovery](#initialization-and-recovery)
- [Testing](#testing)
- [Repository layout](#repository-layout)
- [Scope and third-party code](#scope-and-third-party-code)

## Quick start

Use **Go 1.24 or newer** for the complete EVM framework. Keep the directory
layout intact: the nested modules use relative `replace` directives. Run the
following commands from the repository root. Go may fetch pinned dependencies
on the first build; no blockchain dataset is required.

```sh
git clone https://github.com/XiaodongQi1/Hybrid-code.git
cd Hybrid-code
```

### LevelDB: no native library required

```sh
CGO_ENABLED=0 sh artifact/scripts/run-versioned-evm.sh \
  -config artifact/configs/hybrid-evm-leveldb.example.json \
  -out .local/hybrid-leveldb-run
```

The wrapper's historical filename does not select the method. This template
selects HybridTrie on GoLevelDB, `prefix-boundary` caching, and `hash-cold`
initialization. It supplies 1,000 background accounts, 16 contracts, and a
sender, then executes **20 blocks of 40 transactions**. Background accounts
belong to the initial snapshot, not those execution blocks.

The runner commits each block, checks historical account/storage proofs, and
reopens the database read-only. Inspect `report.json` for the final root,
execution/proof counts, cache statistics, and `ReadOnlyReopen` result. Every new
run requires a fresh output directory; use `-resume` to continue an existing run.

### RocksDB: native build

RocksDB requires CGO, a C/C++ toolchain, `make`, and the pinned **9.10.0** library.
The build script downloads and checksums that release under `.local/native/`.

```sh
sh artifact/scripts/build-hybrid-rocksdb.sh
sh artifact/scripts/run-hybrid-evm.sh \
  -config artifact/configs/hybrid-evm-boundary.example.json \
  -out .local/hybrid-rocksdb-run
```

An existing build can be selected with `ROCKSDB_PREFIX`, pointing to the built
9.10.0 source directory containing `include/` and the shared library. That path
must not contain whitespace. The reproducible build disables optional
compression libraries, so its configurations must use `Compression: "none"`.
Snappy requires a separately built library with Snappy support.

### Continue the same run

```sh
CGO_ENABLED=0 sh artifact/scripts/run-versioned-evm.sh \
  -config artifact/configs/hybrid-evm-leveldb.example.json \
  -out .local/hybrid-leveldb-run -resume
```

For RocksDB, use its original wrapper, configuration, and output directory.
Changing the workload, engine, cache policy, or read settings requires a new
run. See [Initialization and recovery](#initialization-and-recovery) for the
different checkpoint boundaries used by execution, import, and comparison.

## Configuration

Copy a template when changing parameters. Local copies under `.local/` and
non-example JSON files under `artifact/configs/` are ignored by Git. Unknown
configuration fields are rejected.

### Main workflows

| Workflow | Configuration | Entry point |
| --- | --- | --- |
| HybridTrie EVM on LevelDB | [hybrid-evm-leveldb.example.json](artifact/configs/hybrid-evm-leveldb.example.json) | `run-versioned-evm.sh` |
| HybridTrie EVM on RocksDB | [hybrid-evm-boundary.example.json](artifact/configs/hybrid-evm-boundary.example.json) | `run-hybrid-evm.sh` |
| Archive / Path / Hybrid comparison | [hybrid-compare.example.json](artifact/configs/hybrid-compare.example.json) | `hybrid-compare` CLI |
| Comparison with execution and proof read workers | [hybrid-compare-async.example.json](artifact/configs/hybrid-compare-async.example.json) | `hybrid-compare` CLI |
| Comparison with planned proof reads | [hybrid-compare-parallel-proof.example.json](artifact/configs/hybrid-compare-parallel-proof.example.json) | `hybrid-compare` CLI |
| Typed cache and adaptive replacement | [hybrid-compare-adaptive.example.json](artifact/configs/hybrid-compare-adaptive.example.json) | `hybrid-compare` CLI |
| EVM with streaming Prefix initialization | [hybrid-evm-streaming.example.json](artifact/configs/hybrid-evm-streaming.example.json) | `run-hybrid-evm.sh` |
| Standalone Prefix / Hash bulk snapshot import | [Prefix template](artifact/configs/hybrid-snapshot.example.json), [Hash template](artifact/configs/hybrid-snapshot-hash-bulk.example.json) | `hybrid-snapshot` CLI |

Scripts in this table live in [artifact/scripts](artifact/scripts/). The
comparison build command is in [Comparing methods](#comparing-methods).
The storage-only and older cache/baseline tools are listed there separately.

### EVM runner fields

| Field | Meaning |
| --- | --- |
| `Schema` / `Codec` | Use `versioned-evm-run/v1` and `ethereum-mpt` for HybridTrie. |
| `Backend` | `hybrid`; `hybrid-rocksdb` is the RocksDB-only compatibility alias. |
| `Synthetic` or `WorkloadFile` | Exactly one workload source. |
| `HybridStorage` | KV engine, compression, engine caches, write buffers, and open-file settings. |
| `HybridCache` | Cache mode and its capacity/policy parameters. |
| `HybridInitialization` / `HybridSnapshot` | Initial physical representation and optional streaming-import settings. |
| `MaxBatchBytes` | Commit staging limit; the quick-start templates use 8 MiB. |
| `ParallelProof` | Independent historical-query read settings. |

CLI paths resolve from the caller's directory. Input paths inside configuration
files, such as `WorkloadFile` and `SourceFile`, resolve from the configuration
file's directory. Moving a configuration may therefore require updating them.
For HybridTrie, use `HybridStorage` rather than the older runner's top-level
`CacheBytes`, `BlockCacheBytes`, `WriteBufferBytes`, or `OpenFiles` fields.

Explicit templates matter: `backend.DefaultHybridConfig` retains the older
`partitioned` cache and Prefix initialization. The recommended EVM templates
explicitly select `prefix-boundary` and `hash-cold`; the common comparison
template explicitly selects Prefix initialization with `PrefixSnapshot: true`.
These are different experiment settings, not interchangeable defaults.

## Storage and execution design

### Ethereum state and trie identities

The shared [mpt](mpt/) package implements branches, extensions, leaves, inline
children, canonical RLP, and Keccak hashing. Physical trie IDs, heights,
bitmaps, source flags, and cache metadata do not enter that representation.
The logical scheme is `ethereum-mpt-keccak-rlp/v1`; the Hybrid node codec is
`hybrid-mpt-dualref/v6`.

The account trie has implicit ID **0** and needs no allocation mapping.
Each storage-trie lifecycle receives a monotonically allocated **48-bit ID**.
A height-indexed contract lifecycle mapping resolves the active storage trie
at a requested height. Deletion retires the lifecycle; recreation receives a
new ID. IDs are not reused. Ethereum's account value authenticates the storage
root; local metadata supplies its physical source and lifecycle identity.

### Node records

HybridTrie uses three logical columns: `default` for metadata, `prefix` for
position-addressed versions, and `hash` for canonical node bodies.

```text
Prefix key = trieID (6 bytes) | nibble length (1 byte)
             | packed prefix | height (6 bytes)

Hash key   = Keccak digest (32 bytes)
```

IDs and heights use unsigned big-endian encoding. All 48 height bits are height;
none are reserved for a format version. The prefix is the path consumed before
the node's own extension/leaf fragment. Two nibbles share a byte, with zero
low-nibble padding for odd lengths. Prefix keys occupy
`13 + ceil(depth / 2)` bytes, at most 45 bytes for a 64-nibble secure key.

Including prefix length makes each position's versions contiguous. A
predecessor lookup must match the entire trie ID and prefix and must return a
height no greater than the requested height. A record from another position
is never accepted as a substitute.

| Record | Stored value |
| --- | --- |
| Prefix | Local node tag, self digest, node fields, and child-source information. The version height is in the key, not repeated in the value. |
| Hash | Canonical MPT RLP. No local height or bitmap. |
| Metadata | Head, historical roots, block identities, lifecycle/owner mappings, allocator, and immutable code. |

A Prefix branch without inline children or a branch value occupies
`37 + 32 * popcount(HashChildren)` value bytes. Prefix-child digests are omitted
from that persistent parent. Leaves, extensions, inline children, and branch
values have their own payloads; this is not a universal node-size formula.
See [key.go](hybridtrie/key.go) and [record.go](hybridtrie/record.go).

Ethereum's inline rule remains intact: a non-root child with canonical RLP
shorter than 32 bytes stays inside its parent. A nonempty root is independently
addressable. Readers follow explicit Prefix/Hash sources; a missing referenced
record is an error, not key absence or a reason to try a different store.

### Cache boundary and physical placement

A resident parent keeps newly written children Prefix-backed, including the
first level outside the resident tree. In memory it retains those children's
digests and known Prefix heights. A known height permits an exact Get; an
unknown height requires predecessor lookup.

Eviction does not rewrite or delete persisted nodes. Once a parent is outside
the protected cache boundary, a later logical update makes its modified
children candidates for Hash Store. Unchanged siblings retain their previous
sources, so a Prefix parent may contain both Prefix and Hash references.
Positions removed during the current commit preserve the pre-commit boundary
rule.

A canonical Hash body carries no per-child source, so every non-inline child of
a Hash node must itself be reachable through Hash Store. A candidate is decided
after its own children (post-order) and becomes Hash only if each child is a
modified node stored as Hash in the same commit or an existing Hash body.
Otherwise it is written as one Prefix record that keeps mixed references: its
modified Hash children are Hash references and its unchanged children keep their
existing Prefix or Hash source. Unchanged descendants are never copied, so cold
placement adds no storage. A node's physical form changes only when a later
logical update modifies it; no commit ever copies an unchanged node into Hash
Store, so `ClosureBackfillRecords` / `ClosureBackfillBytes` are an invariant that
stays zero. In practice the conversion starts at modified leaves and rises while
all siblings are already Hash. A `PrefixMask` or layout selector (including the
legacy `partitioned` and `reserved-shared` policies) only marks children cold;
it never forces an unchanged child to Hash. Old Prefix versions remain available
to historical parents; a converted node is not also written as Prefix.

### Prefix delta versions

A modified Prefix branch normally rewrites every child reference, including the
32-byte digests of its unchanged Hash siblings. With `Config.DeltaInterval` set
(`HybridDeltaInterval` in a `hybrid-compare` config; zero, the default, writes no
deltas) a branch that is not resident in the cache is stored as the difference
from its previous version when that is smaller: a changed-slot mask and only the
changed references. Reading always supports deltas, so a store written with them
stays readable with the interval set back to zero.

- The base is the node's previous version. When that version is Prefix it is the
  predecessor of the record's own key, so no locator is stored. When it is a Hash
  version the delta carries that version's digest, because Hash Store is
  content addressed.
- Each delta records its depth, the distance to the nearest full record or Hash
  version. A modified branch is written in full once its chain would exceed
  `DeltaInterval` (at most 64), so a read resolves at most that many records. A
  depth that does not follow the chain is corrupt.
- Hash Store values stay canonical RLP. Only branches use deltas; leaves,
  extensions, branches whose own value changed and nodes whose previous version
  is missing or of another kind are written in full.
- Every read path returns complete records: the column read layer resolves a
  delta before any cache, parser or proof code sees it, so caches hold complete
  records and no step merges deltas in memory.
- A delta needs the node's previous version, so the commit opens each modified
  branch's own previous record (`CommitStats.DeltaBaseReads`), resident or not;
  no unchanged node is read or rewritten for this purpose. With
  `DeltaInterval` zero the commit reads no old node record, as before.
  `CommitStats.DeltaRecords` and `DeltaBytes` count the deltas, and the storage
  inventory reports them as `account-prefix-delta` and `storage-prefix-delta`.

After a 200-block warmup, growth over the next 200 blocks (64 transactions per
block, one run each, GoLevelDB; computed as a 400-block run minus the shared first
200 blocks) was 21.2 MB for Archive, 7.6 MB for Path, 14.1 MB for Hybrid with
Hash initialization and 6.9 MB with interval 8 (67% below Archive, 9% below
Path), and 5.2 MB with Prefix initialization and 5.1 MB with interval 8. Deltas
help Hash initialization and barely change Prefix initialization.

On the first 200 blocks (same setup, interval 8) deltas
cut Hybrid's growth from 16.1 MB to 7.0 MB with Hash initialization (24% to 67%
below Archive) and from 5.10 MB to 5.03 MB with Prefix initialization, where
records already omit most digests. Median historical-query service time rose from
128 to 139 microseconds at interval 8 and to 203 at interval 32 (single run, so
noisy).

### Transaction execution and commits

Transactions execute serially. StateDB buffers logical key writes and supports
VM rollback in its transaction journal. After transaction finalization, the
remaining writes are applied directly to the live tries. The next transaction
sees those updates. Block completion computes the final root and persists the
final node versions in a synchronous atomic batch.

Clean MPT hashes are memoized, so unchanged subtrees do not need rehashing.
Hash validity and pending persistence are separate: computing a digest does not
mark a node durable. Dirty-path traversal emits changed nodes without comparing
whole old/new trees. The Prefix-boundary cache is maintained in place; there is
no full-cache copy or block-level cache merge.

Publication binds nodes, roots, lifecycle records, block identity, and Head.
An uncertain native write returns `ErrUncertain`; reopen before determining
the durable outcome. Historical queries pin persisted heights and never use
the latest-state execution cache as historical state.

### Ordered KV access

[kvstore](kvstore/) provides exact Gets, synchronous atomic batches, consistent
snapshots, reusable bounded iterators, measurement, and engine statistics.
RocksDB uses native column families; GoLevelDB stores namespaced logical columns
in one database so cross-column writes remain atomic.

`SeekForPrev` finds the last key less than or equal to the target. RocksDB uses
its native operation. LevelDB uses `Seek` and at most one `Prev`, or `Last`
when the target is beyond the range. There is no application loop over every
historical version. `ReadForPrev` combines positioning and owned key/value-prefix
retrieval; unbounded RocksDB reads fuse these steps into one C call.

Close iterators before snapshots, and snapshots before the database. Engine
selection is recorded in `KVSTORE.json`; reopening with another engine is
rejected. The library does not convert one engine's files into the other's.

## Cache policy

### Resident data and budgets

The current `prefix-boundary` mode uses one forest-wide pool of parsed node
fields and **no separate hash cache**. Account nodes and storage-trie nodes
have separate reserved capacity plus a shared remainder. The storage
reservation is shared by all storage tries, not repeated per contract.

| State | Retained data and lifetime |
| --- | --- |
| Resident node | Parsed fields, own digest/source/version, child digest/height hints, topology links, scalar heat, and allocation metadata; retained until removal. |
| Logical execution node | Copy-on-write fields, cached hash/size, persistence revision, and transient child locations; retained by the active trie. |
| Candidate buffer | Bounded block-local observations and available bodies; cleared after publication or abort. |
| Heat sketches | Four shared matrices with aligned bucket metadata; no per-key history table. |
| Open storage handle | Snapshot-bound contract/root handle; at most 256 per EVM View. |

Resident nodes do not each retain a complete trie ID/path or a long-lived RLP
copy. Trie IDs belong to root descriptors; links and fragments identify the
position. Ordinary traversal carries child source/version hints downward,
without restarting from the root. RLP is produced when hashing, persistence,
or proof output requires it.

The explicit native EVM preset uses a 256 KiB node-pool budget, including
64 KiB account and 64 KiB storage reservations. Candidate, metadata, and plan
limits are separate: 2 MiB, 4 MiB, and 2 MiB respectively. Actual allocations
and occupancy are reported separately. `MaxNodeBytes: 532` assumes Ethereum
secure keys and bounded account/storage payloads; it is not a generic limit
for arbitrary opaque values.

Node-pool capacity, complete application-cache allocation, KV block cache,
write buffers, and total process RSS are different measurements. Use
`resources.json` in comparisons and cache allocation counters in EVM reports.

### Heat and replacement

A resident stores a scalar score and its last heat-update height. Each real
Get/Update/Delete visit applies:

```text
score = score * 2^(-(height - lastHeight) / HalfLife) + 1
lastHeight = height
```

The default half-life is 64 blocks. Admission starts from a sketch estimate
capped at the parent's score. Path visits and the common decay rate preserve
parent heat at least as high as child heat. Hashing, proof work, and cache
preparation do not count as logical accesses.

Nonresident estimates use four Count-Min Sketch buckets aligned to absolute
64-block intervals. Every real visit updates the shared sketches, including
resident visits. Bucket weights support the same decay; expiration discards
only the expired matrix. There is no per-node heat cleanup pass or retained
64-block candidate list. The current partial bucket plus three preceding
buckets cover 193–256 block heights during continuous observation. Resident
scores decay independently and can include older contributions.

Default replacement runs every **16 blocks**. Free slots can fill between
replacement blocks. In the default `updated` mode, both replacement candidates
and victims must have been updated in the current policy block. Up to five
eligible resident frontiers are sampled; `SampleSize` supports 1–64. Lower
heat loses, with deeper nodes preferred on ties. Admission must beat its
victim's heat. The account root is protected, and eviction preserves ancestor
closure. Structural repair is separate from policy eviction.

### Optional cache variants

| Setting | Default | Alternative |
| --- | --- | --- |
| `ResidentLayout` | `fixed` | `typed`: size-class bodies in a fixed arena. |
| `VictimEligibility` | `updated` | `frontier`: sample resident frontiers beyond the current update set. |
| `ReservationPolicy` | `strict` | `borrow`: lend unused reservations with bounded reclaim priority. |

These options are independent and do not change database or proof formats.
The [adaptive template](artifact/configs/hybrid-compare-adaptive.example.json)
selects them explicitly. Admission reads, policy work, and capacity repair are
bounded; inability to admit a node does not change the state result.
Older `partitioned` and `reserved-shared` modes remain for controlled comparisons.
See [boundary_cache.go](hybridtrie/boundary_cache.go),
[boundary_policy.go](hybridtrie/boundary_policy.go), and
[boundary_adaptive.go](hybridtrie/boundary_adaptive.go).

## Asynchronous reads and historical proofs

Execution prefetch and proof construction are independent opt-in features.
Omitted settings and the two quick-start templates disable both. Explicit
`DefaultExecutionReadConfig()` and `DefaultParallelProofConfig()` helpers
return enabled presets for callers that choose to use them.

| Configuration location | Execution reads | Historical proofs |
| --- | --- | --- |
| EVM runner | `HybridCache.Boundary.ExecutionReads` | `ParallelProof` |
| Comparison runner | `HybridBoundary.ExecutionReads` | `ParallelProof` |
| Backend Go API | `Cache.Boundary.ExecutionReads` | `ParallelProof` |

| Switch | Effect |
| --- | --- |
| `Enabled: false` | Original synchronous path, without the optional read table or worker queue. |
| `Enabled: true, Workers: 0` | Lazy/planned synchronous path, without background workers. |
| `Enabled: true, Workers: 1` | Same path with one background reader. |

Comparing zero workers with one isolates background scheduling from the other
read-path changes. These options do not enable concurrent EVM transactions or
parallel hashing. They are fixed when opening a backend, not hot-reloaded.

### Execution prefetch

Add `ExecutionReads` under `HybridCache.Boundary` in the EVM runner or under
`HybridBoundary` in the comparison runner:

```json
{
  "ExecutionReads": {
    "Enabled": true,
    "Workers": 1,
    "Queue": 256,
    "Entries": 8192,
    "BodyBytes": 8388608
  }
}
```

The foreground reads required nodes and owns mutations, decoding, heat, and
replacement. After an edit, background readers prefetch unresolved Prefix
siblings needed for hashing from the block's immutable snapshot. A Prefix
digest can be read from its 33-byte header without decoding the node body.
Known digests do not trigger prefetch.

A block-local table reuses results by trie ID and packed path. Required queued
work can be claimed by the foreground; already running work is awaited.
Queue saturation skips optional submissions instead of blocking execution.
Retained-body limits can require a later exact body Get; table exhaustion uses
counted foreground fallback. Execution accepts 0–8 workers; the cross-engine
conformance cases cover 0, 1, and 2.

`ForestBlock.FinishReads()` drains submitted reads before final block hashing
and publication. Commit, abort, and close join readers before releasing their
snapshot. The read table is operation-local, not an additional permanent node
cache. See [execution_reads.go](hybridtrie/execution_reads.go).

### Planned historical proofs

Merge this top-level object into either runner's configuration:

```json
{
  "ParallelProof": {
    "Enabled": true,
    "Workers": 1,
    "MinBackgroundReads": 4,
    "MaxContextBytes": 262144
  }
}
```

The foreground descends each account/storage path once and carries the
selected child into the next step. Off-path Prefix digests are read in groups
of up to four siblings in slot order. Each reader reuses its own iterator on
the same snapshot. Hash and inline sibling references require no extra read.
Account and storage paths share one session and join before final assembly.

Proof workers support 0, 1, 2, or 4. The queue holds **16 groups**, a code
constant rather than an execution `Queue` setting. When it fills, unscheduled
siblings remain in path frames. After descent, the foreground completes those
reads and claims remaining queued groups, then waits for work already running.
Each group has one owner. Queue saturation does not block descent or duplicate
reads. All-Hash queries need no workers.

Local proof service assembles canonical bytes from trusted stored fields and
digests; the client authenticates them against an independently trusted root.
Strict audit/proof APIs retain their checks. A slot query obtains the account
and its storage proof from their respective single walks, rather than issuing
separate value lookups. See [proof_plan.go](hybridtrie/proof_plan.go),
[proof_workers.go](hybridtrie/proof_workers.go), and
[proof_query.go](hybridtrie/proof_query.go).

### Reading the counters

Execution counters are under `Cache.ExecutionReads`; planned-query counters
are available through `Backend.QueryWithProofStats`. They separate foreground
and background work, waits, header/body reads, decoding, and retained memory.
`ForegroundReadNanos` includes foreground completion after proof descent;
`WaitNanos` describes waiting for outstanding work.

Record counts are database API operations, not physical device I/O. Copying a
33-byte header does not imply a 33-byte device read. Background durations can
overlap foreground work, so use end-to-end latency to assess benefit rather
than adding phase times. Fast cache-resident reads may not offset scheduling
overhead.

Read-table and proof-context budgets are additional to resident-cache backing.
They exclude some temporary buffers, goroutine/allocator overhead, and native
engine memory; reports expose these scopes separately. Equal resident-cache
budgets alone do not imply equal total memory when optional readers are enabled.

## Comparing methods

### Unified EVM comparison

The main comparison uses the same Ethereum codec, verified initial source,
workload, engine, and resource plan for all methods, with independent state
databases:

| Method | Representation |
| --- | --- |
| `archive` | Immutable canonical MPT nodes keyed by hash, with historical roots. |
| `path` | Current trie/flat state plus reverse history and indexes for historical reconstruction. This is the repository's Path implementation, not a running upstream Geth node. |
| `hybrid` | Mixed Prefix/Hash versions with the configured boundary cache. |

Build and run the portable comparison:

```sh
mkdir -p .local
CGO_ENABLED=0 go -C evm-versioned-trie build -mod=readonly \
  -o ../.local/hybrid-compare ./cmd/hybrid-compare
.local/hybrid-compare \
  -config artifact/configs/hybrid-compare.example.json \
  -out .local/compare-run
```

The template uses 256 background accounts, 8 contracts, 2,000 blocks of 16
transactions, three trials, and 30 queries. The first 1,000 blocks warm each
execution trial; the remaining 1,000 provide its timing samples. Storage still
covers the full chain. For a short functional run, copy the configuration and
explicitly reduce `Blocks`, `WarmupBlocks`, and `Trials`, leaving at least one
measured block. Warmup and measurement consume successive blocks, not repeated
copies of the same transaction sequence.

For RocksDB, first build the native library and set `Engine: "rocksdb"` in a
local copy of the comparison configuration. Then build a native binary:

```sh
rocks_prefix="${ROCKSDB_PREFIX:-$PWD/.local/native/rocksdb-9.10.0}"
CGO_ENABLED=1 CGO_CPPFLAGS="-I$rocks_prefix/include" \
  CGO_LDFLAGS="-L$rocks_prefix -Wl,-rpath,$rocks_prefix" \
  go -C evm-versioned-trie build -mod=readonly -tags rocksdb \
  -o ../.local/hybrid-compare-native ./cmd/hybrid-compare
```

Use the native binary with that configuration and a fresh output directory.

### Suites and measurement boundaries

Choose `Methods` and `Suites` in the configuration. Reference execution runs
once to freeze roots, receipts, and authenticated block-final changes.

| Suite | What is measured |
| --- | --- |
| `storage` | Import identical block-final changes. Inventory after initialization and after the last block, including compaction outside execution timing. Report logical state keys plus values, physical SSTs, component sizes, and write counters. This is not live EVM execution timing. |
| `execution` | Execute frozen transactions through each adapter from a fresh initialized database per trial. Time through final hashing, synchronous commit, and view release; exclude leading warmup blocks, external verification, and result-file writes. |
| `historical-query` | Reopen final state read-only. Record local value/proof construction, separate client verification, and their sum. Preserve proof size, request order, and read-only integrity checks. |

Initialization is stored and reported, but **execution storage growth subtracts
each method's own initial inventory**. Path's live current nodes remain in the
final inventory; overwritten versions are not counted as simultaneously live.
Logical state bytes, encoded KV bytes, SST bytes, directory bytes, and cumulative
write bytes remain distinct. A final-minus-initial SST result is net physical
growth, not the size of isolated history files. LevelDB's column namespace bytes
are included in encoded/physical accounting.

Archive and Path use parsed-node execution LRUs. The resolver matches their
allocated backing to Hybrid's fitted node pool and auxiliary cache structures.
All methods also receive the same engine block-cache and global write-buffer
targets. `resources.json` records actual allocation and slack. These are
component budgets, not process RSS limits. Latest-state execution caches are
disabled on historical read-only reopens; the engine caches remain. The OS page
cache is not flushed between query trials.

Use `Synthetic` or `WorkloadFile`, and optionally an ordered `QueryFile`.
Generated queries mix account/storage requests, uniform/recent-biased heights,
and absent entries. Thirty queries are suitable for a functional check, not a
stable tail-latency claim. Method order rotates between trials.

### Results and checkpoints

| Output | Contents |
| --- | --- |
| `workload.json`, `snapshot.json`, `blocks.json` | Common input and reference identities. |
| `resources.json` | Resolved per-method resources and extra optional-reader budgets. |
| `queries.json`, `corpus.json` | Query inputs and their identity, when selected. |
| Per-group JSON and digest sidecars | Sealed storage results and timing samples. |
| `results.json` | Final aggregate, per-block/per-query nanoseconds, means, and nearest-rank P50/P70/P90/P95. |

Shared source/artifact files stay outside measured state databases. Completed
execution-trial databases are removed after their results are sealed. Results,
reference data, and separately built storage/query databases remain. Failed or
interrupted groups retain their artifacts.

```sh
.local/hybrid-compare \
  -config artifact/configs/hybrid-compare.example.json \
  -out .local/compare-run -resume
```

`-snapshot-only` prepares the shared initial source and stops.
`-through-storage` stops after sealing selected storage groups. Continue either
with the same configuration and `-resume`.

### Additional and older tools

| Purpose | Entry point / template | Scope |
| --- | --- | --- |
| Synthetic Hybrid storage representations | [run-hybrid-storage.sh](artifact/scripts/run-hybrid-storage.sh), [configuration](artifact/configs/hybrid-storage.example.json) | MPT updates without EVM timing; separates initialization and subsequent growth. |
| Cache-policy comparison | [run-hybrid-cache-bench.sh](artifact/scripts/run-hybrid-cache-bench.sh) | Partitioned, reserved-only, reserved-shared, and optional boundary modes. |
| Normalized Snappy projection | [project-hybrid-storage-snappy.sh](artifact/scripts/project-hybrid-storage-snappy.sh) | Re-encodes inventories into common LevelDB tables; not a measurement of native RocksDB Snappy performance. |
| Older versioned EVM baseline | [versioned-evm.example.json](artifact/configs/versioned-evm.example.json) via `run-versioned-evm.sh` | Distinct compact versioned format with 4-byte IDs/heights. |
| Older synthetic storage baseline | [storage.synthetic.example.json](artifact/configs/storage.synthetic.example.json) via `go run ./cmd/versioned-storage-bench` | Opaque values, not Ethereum account payloads. |
| Older EVM storage comparison | [run-versioned-evm-storage.sh](artifact/scripts/run-versioned-evm-storage.sh), [configuration](artifact/configs/versioned-evm-storage.example.json) | Legacy codec/artifacts; do not mix with native Hybrid results without matching semantics. |

The older [partitioned](artifact/configs/hybrid-evm.example.json) and
[reserved-shared](artifact/configs/hybrid-evm-shared.example.json) EVM templates
remain available for policy studies. They are not the quick-start presets.

## Initialization and recovery

### Initial state modes

| Mode | EVM selection | Behavior |
| --- | --- | --- |
| Atomic Prefix | `HybridInitialization: "prefix"` or omitted, without `HybridSnapshot` | Publish the initial forest as Prefix versions in one state batch. |
| Atomic Hash | `HybridInitialization: "hash-cold"` | Write canonical Hash nodes in one state batch, then start with an empty execution cache. |
| Streaming Prefix | Prefix mode with `HybridSnapshot` | Build/export through bounded scratch storage and publish the complete Head after validation. |
| Bulk Hash | `HybridInitialization: "hash-cold-bulk"` | Build a reusable canonical source, copy reachable Hash nodes in bounded batches, validate, then publish Head. |

Atomic initialization must fit batch/projection limits and does not silently
switch to streaming. `hash-cold` rejects `HybridSnapshot`. An empty execution
cache does not imply a flushed OS or engine cache. The comparison's
`PrefixSnapshot` selects its prepared-source target representation separately.

### Initial representation and write savings

The initial representation sets how much a Prefix record can save, because a
Prefix record omits the digest of each Prefix child but must keep the 32-byte
digest of every Hash child. Unchanged siblings keep their initial source (they
are never copied to change it), so every new version of a modified ancestor
re-stores the digests of its Hash siblings, as an archive node's RLP does.

- **Prefix initialization**: siblings are Prefix, their digests are omitted, and
  the new ancestor versions are much smaller than the archive nodes.
- **Hash initialization** (`hash-cold`): siblings are Hash, so most of the
  bytes of a new ancestor Prefix record are Hash-child digests and the saving
  is much smaller.

Measured with `hybrid-compare` on the chain-shaped workload from
`hybrid-genstate` (100,000 accounts, 3,000 contracts, 335,430 genesis storage
slots (largest contract 186,400, no cap), 200 blocks of 64 transactions, GoLevelDB, no compression, one trial,
configs `hybrid-compare-chainlike-{hash,prefix}.example.json`). Growth is final
minus initial logical state bytes (keys plus values):

| Method | Growth | Versus archive |
| --- | --- | --- |
| Archive | 21.2 MB | |
| Path | 7.5 MB | 65% less |
| Hybrid, Hash initialization | 16.1 MB | 24% less |
| Hybrid, Prefix initialization | 5.1 MB | 76% less |

Prefix initialization also starts smaller (64.5 MB against 76.0 MB for Hash)
because its initial records omit Prefix-child digests. These are one run on
synthetic data, not real chain state, so treat
them as an illustration of the mechanism, not a prediction. The saving on real
state depends on how many unchanged siblings are already Prefix. Identical
subtrees at different positions are stored once per position in the Prefix
column but once in the Hash column.

### Standalone snapshot import

The importer consumes address-sorted JSONL allocation rows with no duplicates.
Balance is a nonnegative integer JSON number, code is base64, and storage uses
32-byte hex keys and values. A two-account input looks like this:

```jsonl
{"Address":"0x0000000000000000000000000000000000000001","Balance":1000,"Nonce":0,"Code":"","Storage":{}}
{"Address":"0x0000000000000000000000000000000000000002","Balance":2000,"Nonce":0,"Code":"","Storage":{}}
```

Save the input at `.local/accounts.jsonl` to match the shipped source path.
The Prefix and Hash templates supply the block header and import limits.
For RocksDB:

```sh
sh artifact/scripts/import-hybrid-snapshot.sh \
  -config artifact/configs/hybrid-snapshot-hash-bulk.example.json \
  -out .local/snapshot-import
```

For LevelDB, copy the template to `.local/snapshot-leveldb.json`, set
`HybridStorage.Engine` to `leveldb`, and adjust `SourceFile` to `accounts.jsonl`
relative to that copy. Run:

```sh
CGO_ENABLED=0 go -C evm-versioned-trie run -mod=readonly ./cmd/hybrid-snapshot \
  -base-dir "$PWD" -config .local/snapshot-leveldb.json \
  -out .local/snapshot-leveldb-import
```

The standalone field is `Initialization`, while the EVM runner uses
`HybridInitialization`. The target and owned scratch database use the selected
engine. Source-file identity, header, and import plan are bound to the run.
Incomplete imports do not expose a completed Head. Each individual record must
fit the batch ceiling. `KeepScratch` controls retention after successful import.

### What resumes, and at which boundary

| Workflow | Recovery behavior |
| --- | --- |
| EVM run | Resume after the last durable block. A pending result repairs the database-commit/result-log gap. An uncommitted block executes again. |
| Snapshot import | Validate the same immutable source and plan, resume durable construction/export progress, and publish only after final validation. |
| Comparison reference/storage | Resume verified reference traces or durable storage blocks; preserve cumulative application write counters and initialization inventory. |
| Comparison timing group | Reuse completed sealed groups. Restart an interrupted method/trial group from its beginning, including warmup. |

Use `-resume` with the original configuration and output directory. Resume does
not adopt unrelated databases, accept changed parameters, or reconstruct
in-memory cache state. Exclusive run locks prevent two controllers advancing
the same output concurrently. Cache/heat statistics after restart are marked
with their session scope; native engine statistics also retain open scopes.

For an EVM run, `run-manifest.json` binds inputs, `blocks.jsonl` records committed
results, `pending-block.json` bridges publication and log synchronization,
`initialization.json` retains initialization statistics, and `report.json`
records completion. `ReusedBlocks` includes initialization;
`ExecutedBlocksThisRun` counts newly executed non-initialization blocks.
Completed runs return their sealed report without repeating execution.

## Testing

Run the portable suite across the root and nested EVM modules:

```sh
CGO_ENABLED=0 sh artifact/scripts/test-versioned-baseline.sh
```

The native wrapper covers RocksDB storage, initialization, cache, EVM, and
comparison integration. It requires the pinned library described above:

```sh
sh artifact/scripts/test-hybrid-rocksdb.sh
```

Running `go test ./...` only at the root does not cover nested Go modules.
The wrappers handle those boundaries. Correctness coverage includes Ethereum
roots/receipts, membership and absence proofs, lifecycle recreation, Prefix/Hash
closure, cache topology and heat, both engines, interrupted writes/imports,
read-only reopen, and optional-reader ownership/cancellation.

For a focused race check and bundled-runtime provenance check:

```sh
go test -race ./mpt ./hybridtrie ./kvstore
python3 evm-runtime/tools/verify_sources.py
```

### Independent upstream oracle

The separate [oracle module](evm-versioned-trie/oracle/) uses pinned upstream
Geth v1.16.9, without importing the local MPT or storage adapters. Export local
fixtures and verify their proofs and reconstructed state roots:

```sh
mkdir -p .local/oracle
HYBRID_EVM_PROOF_REPORT="$PWD/.local/oracle/proofs.json" \
HYBRID_EVM_STATE_REPORT="$PWD/.local/oracle/states.json" \
  sh artifact/scripts/test-hybrid-rocksdb.sh
go -C evm-versioned-trie/oracle run -mod=mod . \
  verify-proofs ../../.local/oracle/proofs.json
go -C evm-versioned-trie/oracle run -mod=mod . \
  verify-states ../../.local/oracle/states.json
```

The optional oracle may fetch its pinned dependencies. It independently checks
encoding, roots, and proofs; it is not a second independent transaction engine.
Functional tests and small examples do not establish a performance claim.

## Repository layout

| Path | Responsibility |
| --- | --- |
| [hybridtrie](hybridtrie/) | Hybrid records, versioned forest, lifecycle, cache policy, execution reads, proofs, and snapshot import. |
| [kvstore](kvstore/) | Common LevelDB/RocksDB API, snapshots, ordered reads, batches, and measurements. |
| [mpt](mpt/) | Logical Ethereum-compatible trie, hash memoization, copy-on-write updates, dirty emission, and canonical proofs. |
| [versionedtrie](versionedtrie/) | Older compact versioned baseline. |
| [evm-runtime](evm-runtime/) | Adapted EVM and pinned third-party dependencies. |
| [evm-state](evm-state/) | Account/storage forest, codecs, StateDB, queries, common snapshots, and external artifacts. |
| [evm-path-archive](evm-path-archive/) | Path Archive current state, history, indexes, and historical reconstruction. |
| [evm-versioned-trie/backend](evm-versioned-trie/backend/) | EVM backend adapters. |
| [evm-versioned-trie/comparison](evm-versioned-trie/comparison/) | Common-engine Archive, matched resource plans, and conformance tests. |
| [evm-versioned-trie/cmd](evm-versioned-trie/cmd/) | Execution, comparison, snapshot, cache, and storage command-line tools. |
| [evm-versioned-trie/workload](evm-versioned-trie/workload/), [runner](evm-versioned-trie/runner/) | Workload generation, execution control, reports, and recovery. |
| [internal](internal/) | Shared cache, logical-I/O accounting, and baseline helpers. |
| [artifact/configs](artifact/configs/), [artifact/scripts](artifact/scripts/) | Public templates and build/run/test wrappers. |

Tests live beside their implementations as `*_test.go`. Project documentation
is maintained in this root README; generated outputs belong under `.local/`.

## Scope and third-party code

The implemented history model is a linear canonical chain. Fork switching,
historical pruning/garbage collection, and a production network service are
outside the current implementation. Missing features should not be inferred
from the presence of older baseline or policy templates.

The adapted go-ethereum components and vendored dependencies retain their
copyright headers and license files, including
[COPYING](evm-runtime/COPYING) and
[COPYING.LESSER](evm-runtime/COPYING.LESSER).
[UPSTREAM.json](evm-runtime/UPSTREAM.json) records source provenance.
Small test vectors and required cryptographic constants are included;
experimental datasets and private deployment settings are excluded by
[.gitignore](.gitignore).
