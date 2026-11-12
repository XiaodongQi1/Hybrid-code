# HybridTrie

HybridTrie is an independent research implementation of versioned Ethereum state storage.
The current **HybridTrie** backend changes the physical representation of trie
nodes while retaining Ethereum's logical Merkle Patricia Trie, canonical RLP,
Keccak roots, and historical proofs. It supports an account trie, separately
identified contract-storage tries, real EVM execution, and bounded execution
caches. The shared KV layer selects RocksDB or GoLevelDB for HybridTrie,
including its EVM adapter and initialization scratch store.

The repository includes the HybridTrie implementation, its EVM adapter,
Archive/Path/Versioned comparison backends, and reproducible local test tools.
All local Go module dependencies are contained in this repository. The module
namespace is `github.com/XiaodongQi1/HybridTrie`; no Statera checkout is required.

Generated databases, datasets, binaries, results, and private deployment
configuration are excluded from Git. The public templates use synthetic data;
they do not require downloaded mainnet transactions.

## Current implementation

This standalone snapshot includes the asynchronous execution reader, planned
historical proofs, foreground assistance, fused RocksDB predecessor reads, and
packed execution-key lookups. Async execution and proof construction remain
configurable; library defaults and CLI presets are documented below.

| Component | Available now | Remaining work |
| --- | --- | --- |
| HybridTrie | Ethereum-compatible forest, explicit Prefix/Hash sources, execution cache, historical proofs, and resumable EVM execution. | Fork switching, pruning, and integration with the multi-node launcher. |
| KV engines | RocksDB/GoLevelDB selection, atomic batches, snapshots, reusable cursors, global memtable targets, and scoped write/engine statistics. | A common checkpoint API. |
| Comparison adapters | Persistent Archive, Path, and HybridTrie on one engine; shared codec/traces/proofs, external artifacts, matched cache backing, and common storage measurements. | Broader contract/storage workloads and longer repeated measurements. |

Conformance tests exercise persistent Archive, Path, and HybridTrie under
GoLevelDB and RocksDB, alongside an in-memory Hash reference and frozen upstream
Geth roots. These are correctness checks; they do not measure performance.

## Entry points

| Task | Entry point |
| --- | --- |
| Execute EVM blocks with the current HybridTrie cache | [HybridTrie quick start](#hybridtrie-quick-start) |
| Find a complete configuration for each supported workflow | [Configuration templates](#configuration-templates) |
| Select RocksDB or LevelDB through the shared KV layer | [KV storage engines](#kv-storage-engines) |
| Continue an interrupted EVM run | [Resume](#resume-an-interrupted-run) |
| Inspect storage, initialization, and authentication rules | [Storage model](#storage-model) |
| Configure cache reservations, heat, and replacement | [Execution cache](#execution-cache) |
| Import a large initial state with restart support | [Streaming initialization](#streaming-initialization) |
| Compare storage representations or cache policies | [Comparison tools](#comparison-tools) |
| Run correctness checks and an independent Geth oracle | [Validation](#validation) |
| Use the LevelDB versioned backend | [Comparison backends](#comparison-backends) |

## HybridTrie quick start

Use Go 1.24 or newer and keep the repository layout intact because the Go
modules use local `replace` directives. Run the commands from the repository
root. Generated inputs and outputs belong under the ignored `.local/` directory.

### LevelDB

This path needs neither CGO nor a native database library:

```sh
CGO_ENABLED=0 sh artifact/scripts/run-versioned-evm.sh \
  -config artifact/configs/hybrid-evm-leveldb.example.json \
  -out .local/hybrid-leveldb-run
```

Despite its historical filename, `run-versioned-evm.sh` runs the common EVM CLI;
the configuration selects `Backend: "hybrid"` and `HybridStorage.Engine: "leveldb"`.

### RocksDB

Also install a C/C++ toolchain and `make`, and enable CGO on Linux or macOS.
The native driver pins RocksDB **9.10.0** and checks its runtime version. The
builder downloads and verifies that release into `.local/native/`:

```sh
sh artifact/scripts/build-hybrid-rocksdb.sh
sh artifact/scripts/run-hybrid-evm.sh \
  -config artifact/configs/hybrid-evm-boundary.example.json \
  -out .local/hybrid-evm-run
```

For a fresh run, the output directory must not already exist. Both templates
select native `ethereum-mpt` values, `hash-cold` initialization, and the current
`prefix-boundary` cache. The RocksDB template retains the `hybrid-rocksdb` alias.
They supply 1,000 background accounts, 16 contracts, and a sender, then execute
20 blocks of 40 synthetic transactions.
Background accounts are supplied as an initial snapshot; they are not created
by those execution blocks.

The runner freezes the workload, executes transactions through the local EVM,
persists each block atomically, verifies sampled historical account/storage
proofs, and checks a read-only reopen. A completed run produces:

| File or directory | Contents |
| --- | --- |
| `configuration.json`, `database-config.json`, `chain-config.json` | Effective run, backend, and EVM configuration. |
| `workload.json` | Frozen workload bound to its identity. |
| `db/` | Private state database; `KVSTORE.json` identifies its engine and logical columns. |
| `blocks.jsonl` | A synchronized result record after each committed input block, including initialization. |
| `run-manifest.json` | Immutable configuration, workload, backend, and chain-rule bindings for resume. |
| `pending-block.json` | The current block's result, synced before state publication; removed after its result log is synced. |
| `initialization.json`, `run.lock` | Saved initialization statistics and the process lock protecting the run. |
| `report.json` | Final roots, execution/proof counts, initialization report, cache counters, reopen status, and `KVEngine`. |
| `failure.json` | Failure diagnostics when the runner can write them. |

### Resume an interrupted run

Use the same configuration and output directory with `-resume`:

```sh
sh artifact/scripts/run-hybrid-evm.sh \
  -config artifact/configs/hybrid-evm-boundary.example.json \
  -out .local/hybrid-evm-run -resume
```

The portable `run-versioned-evm.sh` wrapper accepts the same flag for LevelDB.
Keep the original engine and configuration when resuming. Go callers use
`runner.RunWithOptions(config, output, runner.Options{Resume: true})`.

Resume checks the immutable inputs and the database's committed height, block
IDs, and roots. A block already committed to the database is not executed again.
If interruption happened between database commit and result-log Sync, the saved
pending result repairs the log, including a partially written final line. A
prepared block that was not committed executes again in full. Invalid complete
log records, changed inputs, or unexplained gaps cause an error without replacing
result artifacts. A process lock prevents concurrent controllers from using the
same run. Completed runs return their saved report without repeating execution
or final proof verification.

`ReusedBlocks` includes the supplied initialization block;
`ExecutedBlocksThisRun` counts newly executed non-initialization blocks.
`Resumed` identifies a report completed through resume. Cache contents and heat
are rebuilt from subsequent accesses; `CacheSessionOnly` marks cache counters
that cover only the completing process. Initialization statistics are retained
separately; if an interruption lost those statistics immediately after the initial
commit, `InitializationStatsMissing` reports that loss rather than inventing
measurements. Final historical proof checks still cover the full workload.

Resume requires the bindings and pending-result protocol created by this runner
version. It does not adopt unrelated databases or runs lacking `run-manifest.json`.
Use a new directory when changing experiment parameters.

## KV storage engines

[kvstore](kvstore/) provides one API for RocksDB 9.10.0 and the pinned GoLevelDB
implementation. HybridTrie, its EVM adapter, and streaming initialization now use
this layer. Engine selection changes physical KV storage, not the trie codec,
logical hashes, lifecycle rules, execution cache, or historical proof format.

Use `Backend: "hybrid"` and select the engine in `HybridStorage`:

```json
{
  "Backend": "hybrid",
  "HybridStorage": {
    "Engine": "leveldb",
    "Compression": "none",
    "BlockCacheBytes": 8388608,
    "WriteBufferBytes": 8388608,
    "OpenFiles": 128
  }
}
```

Set `Engine` to `rocksdb` to use the same configuration through the native
`run-hybrid-evm.sh` wrapper. `Backend: "hybrid-rocksdb"` remains a compatible
RocksDB-only alias. Omitted `HybridStorage` preserves the previous RocksDB
settings; engines never fall back automatically. The snapshot importer accepts
the same `HybridStorage` object, and its scratch store uses the selected engine.

The Go API is `kvstore.Open(path, options, create, readOnly)`. HybridTrie API
callers set `hybridtrie.Config.Storage` to a `*kvstore.Options`; standalone
snapshot builders use `hybridtrie.SnapshotBuildConfig.Storage`.

| Operation | Shared semantics |
| --- | --- |
| `DB.Get(column, key)` | Exact lookup; absence is `ErrNotFound`. |
| `DB.Write(batch)` | One synchronous atomic Put/Delete batch, including updates across columns. |
| `DB.NewSnapshot()` | One consistent read view across all columns. |
| `Snapshot.NewIterator(column, bounds)` | Reusable cursor over a half-open `[Start, Limit)` key range. |
| `Iterator.Seek` / `SeekForPrev` | Locate the first key >= target / last key <= target. |
| `Iterator.ReadForPrev(key, limit)` | Locate the predecessor and return owned key/value-prefix bytes plus the full value length in one call. |
| `Iterator.First`, `Last`, `Next`, `Prev` | Explicit positioning and sequential movement. |
| `DB.Compact()` | Explicit flush/compaction outside normal query/block timing. |
| `DB.Info()` / `DB.IntProperty()` | Engine identity/namespace overhead and engine-specific integer diagnostics. Unsupported properties return `ErrUnsupported`. |
| `DB.Statistics()` | Configured/observed memory, per-column logical writes, serialized batch bytes, engine counters and an explicit open scope. |
| `DB.Measure(ctx, classifier, stabilize)` | Offline full-key/value and file inventory; optional flush/full compaction, with maintenance counters separated. |

RocksDB implements `SeekForPrev` natively. GoLevelDB uses `Seek` followed by at
most one `Prev`, or `Last` when the target is beyond the range and no error
occurred. Neither implementation scans historical versions in an application
loop. One cursor can be repositioned repeatedly for ordered sibling prefixes.
Hybrid's predecessor reads use `ReadForPrev` under one cursor lock. Unbounded
RocksDB reads combine seek, validity and key/value retrieval in one C call;
bounded cursors and GoLevelDB retain the same range semantics. Only the requested
value prefix is copied, and a missing predecessor leaves the cursor reusable.
Get and iterator Key/Value results are copied and remain valid after the next
seek. Close iterators before their snapshot, and snapshots before the database;
busy close attempts return `ErrBusy`. A native batch-write error fails stop with
`ErrUncertain`, requiring reopen to determine its durable outcome.

RocksDB uses native column families. GoLevelDB stores all logical columns in
one database, encoding `nameLength1 | columnName | applicationKey`; thus a batch
across columns stays atomic. `DB.Info().ColumnKeyOverhead` reports these added
key bytes. Its namespace registry is separate from trie metadata. Account for
this physical overhead when measuring LevelDB storage.

New databases contain an immutable `KVSTORE.json` engine/column marker. A
mismatched engine or column set is rejected before opening the native database.
Existing HybridTrie RocksDB databases without that marker remain readable with
their unchanged native CF layout and application manifest. This layer does not
convert RocksDB files to LevelDB files; create a separate output directory for
the other engine. Run manifests also reject changing engines on resume.

`BlockCacheBytes = 0` retains the engine's default block-cache policy. Explicit
positive values set one RocksDB cache shared across CFs, or one LevelDB cache.
With `GlobalWriteBufferBytes = 0`, `WriteBufferBytes` retains its legacy
per-memtable meaning: per CF on RocksDB and per database on GoLevelDB.
Positive `GlobalWriteBufferBytes` attaches one RocksDB WriteBufferManager to
all CFs, with write stalling enabled and no charge against the block cache.
GoLevelDB has one database-wide active/frozen memtable pair; its per-memtable
target is clamped to at most half the global target. This is an active/frozen
target, not a native global allocator limit. Retired snapshot memory, pool
retention and a large in-flight write can exceed these targets. The comparison
resolver selects half the global target per memtable on both engines.
These component budgets are not a process RSS limit.

`ApplicationCacheBytes` optionally allocates a fixed-slot point-read LRU,
including its full key/value arenas, directory and bookkeeping. Values larger
than 4096 bytes or keys larger than 128 bytes bypass it. Writes invalidate
changed keys; snapshots share it only while their generation is current.
`ExecutionCacheOnly` disables this cache on a read-only reopen. Zero bytes
preserve the uncached default. `EnableStatistics` enables native RocksDB
tickers without histogram/timer collection. `BackgroundJobs` configures RocksDB;
GoLevelDB retains its fixed workers and rejects an explicit nonzero setting.

The comparison runner applies the same cache backing targets and engine budgets across methods.
The comparison Archive and explicit Path adapter use this layer. The older
versioned storage driver and legacy comparison runners keep their LevelDB
compatibility paths.

### Configuration templates

| Workflow | Template | Launcher |
| --- | --- | --- |
| Compare Archive, Path, and HybridTrie | [hybrid-compare.example.json](artifact/configs/hybrid-compare.example.json) | [Unified comparison](#unified-comparison) |
| HybridTrie on GoLevelDB, current boundary cache and Hash-cold initialization | [hybrid-evm-leveldb.example.json](artifact/configs/hybrid-evm-leveldb.example.json) | `run-versioned-evm.sh` |
| HybridTrie on RocksDB, same cache and initialization | [hybrid-evm-boundary.example.json](artifact/configs/hybrid-evm-boundary.example.json) | `run-hybrid-evm.sh` |
| Resumable streaming Prefix initialization during EVM execution | [hybrid-evm-streaming.example.json](artifact/configs/hybrid-evm-streaming.example.json) | `run-hybrid-evm.sh`; configure `HybridStorage` and use the portable wrapper for LevelDB. |
| Standalone streaming allocation import | [hybrid-snapshot.example.json](artifact/configs/hybrid-snapshot.example.json) | `import-hybrid-snapshot.sh` for RocksDB; see the direct LevelDB command below. |
| Older compact versioned backend | [versioned-evm.example.json](artifact/configs/versioned-evm.example.json) | `run-versioned-evm.sh` |

The shipped templates run local EVM, storage, and historical-query experiments.

## Storage model

### Ethereum compatibility and forest identity

The shared [mpt](mpt/) implementation owns branch/extension/leaf structure,
path compression, inline children, canonical RLP, and Keccak hashing. Local
trie IDs, version heights, bitmaps, reference-source flags, and cache statistics
do not enter that authenticated representation. The logical scheme is
`ethereum-mpt-keccak-rlp/v1`; the node codec is `hybrid-mpt-dualref/v6`.

The account trie has implicit ID **0**, with no account-trie allocation mapping.
Each storage-trie lifecycle receives a monotonically allocated 48-bit ID.
A contract's height-indexed lifecycle records identify its active storage trie;
a predecessor lookup at the requested height resolves the appropriate ID.
Deletion retires the lifecycle, and recreation allocates a new ID. IDs are not
reused. Storage roots remain authenticated through the account value.

Current lifecycle handling is for a linear canonical history. Fork switching,
historical garbage collection, and pruning are not implemented.

### Keys and column families

There are three logical columns: `default` for metadata, `prefix` for positional
node versions, and `hash` for canonical node bodies. These use native column
families on RocksDB and namespaced keys in a single GoLevelDB database. Columns
are not partitioned by trie depth.

A Prefix node key is:

```text
trieID (6 bytes) | nibble length (1 byte) | packed prefix | height (6 bytes)
```

IDs and heights use unsigned big-endian encoding. All 48 height bits represent
the height; no version bits are reserved. The prefix is the path consumed
**before** this node's own extension/leaf fragment. Two nibbles share a byte;
an odd final nibble uses zero low-nibble padding. Key length is
`13 + ceil(depth / 2)` bytes, up to 45 bytes at depth 64. A node-type tag is not
part of the key because the same position can change MPT type.

Including the nibble length keeps one exact position's versions contiguous.
After `SeekForPrev`, a lookup checks the complete trie ID and prefix and requires
the returned height to be no greater than the requested height. A predecessor
from a different position is not a valid answer.

| Representation | Key | Value |
| --- | --- | --- |
| Prefix node | Position and version height | Local node tag, self hash, fields, and child-source information. |
| Hash node | 32-byte Keccak digest | Canonical MPT RLP, without a separate height or local bitmap. |
| Metadata | Typed metadata keys | Head, roots, block identities, lifecycle/owner records, allocator, and immutable code. |

A simple Prefix branch without inline children or a branch value occupies
`37 + 32 * popcount(BH)` Value bytes: tag, self hash, two 16-bit bitmaps, and
the explicit Hash-child digests. Prefix children do not duplicate their hashes
in the persistent parent. Optional inline children and branch values add their
own payload. Leaf records contain their path fragment and value; extension
records contain the fragment and the selected child representation. See
[record.go](hybridtrie/record.go) for the actual codec rather than treating
34 bytes as a universal node size.

Ethereum's inline rule still applies: a non-root child with RLP shorter than
32 bytes stays inside its parent. A nonempty root is independently addressable
even if its RLP is short. An empty trie has no node body.

### Root sources and authentication

The root directory records whether a nonempty root is Prefix-backed or
Hash-backed. Existing Prefix entries store the 32-byte hash. Hash roots use
`0x02 | hash32`. The new reader accepts the old Prefix root encoding; databases
containing new Hash-root entries require a reader supporting this extension.
Preserving the v6 node codec does not imply that old binaries understand all
new root metadata.

Readers follow the declared source. A missing Prefix record does not trigger a
speculative Hash fallback, or vice versa. A missing referenced record is an
error, not a proof of key absence. Authentication and local decoding have
separate responsibilities:

| Operation | Checks and work |
| --- | --- |
| Trusted local execution read | Decode fields, validate shape/limits and Prefix identity/version, and preserve expected digest/source hints. Do not recompute the logical hash merely to read a local node. |
| Local proof assembly | Reconstruct canonical bytes using the pinned records and child digests; return them for client verification. |
| Client verification or strict audit/import | Authenticate canonical node bytes against the independent root/parent. A record's self hash or local bitmap alone is not a trust anchor. |

The local fast paths do not turn untrusted peer data into trusted state. Strict
interfaces remain available separately from execution and response assembly.

Historical proof generation follows one retained path context. The default
synchronous path reuses one Prefix cursor; the optional parallel path below
uses one cursor per active reader. Each can seek successive sibling
prefixes without an application loop over their historical versions. For each
off-path Prefix child, Hybrid copies the record header containing its digest.
It decodes the selected child once and carries it into the next step. Read
limits charge the full logical record size. The historical query server assembles
the path from its pinned local data and stored child digests. The client verifies
the proof against an independently trusted block root. Standalone checked proof
and audit APIs also authenticate node contents locally.
Hybrid, Hash Archive, and Path return the terminal value and proof from the
same walk. A storage request first decodes the account, then walks its
authenticated storage root once. Hybrid resolves the storage lifecycle on the
pinned height. An absent account or empty storage root needs no storage walk.
Use `Backend.Query(ctx, request)` to pin a Hybrid height for a single response,
or `query.Read` to reuse an existing view. The client independently verifies
the compact response. Path uses its historical index and reverse differences
to resolve the trie nodes needed by that walk.

### Initialization and durable commits

| Initialization mode | Selection | Behavior |
| --- | --- | --- |
| Atomic Hash, cold execution cache | `HybridInitialization: "hash-cold"` | Build the supplied state and write independent nodes directly to Hash Store in one atomic batch. |
| Atomic Prefix | `"prefix"` or omitted | Build and publish the supplied state with Prefix node versions. |
| Streaming Prefix | Configure `HybridSnapshot`; do not select `hash-cold` | Construct/export through bounded scratch storage, then publish the complete Head. |
| Bulk Hash, cold execution cache | `HybridInitialization: "hash-cold-bulk"` | Build a reusable canonical source and original account records, copy reachable Hash nodes in bounded batches, verify, then publish Head. |

`hash-cold` accepts only an uninitialized Head. Nodes, code, roots, lifecycle
records, allocator, block identity, and Head are written together using WAL and
Sync. Hash bodies deduplicate globally and no Prefix node copies are created
for initialization. Database manifest creation is separate from this state
commit. The initial cache publication bypasses admission and heat recording;
the execution cache and its statistics start empty. This does not flush OS or
RocksDB caches.

The atomic path obeys batch and projection limits. It does not silently fall
back to streaming. Initialization space and time are reported separately from
subsequent execution/growth; the supplied snapshot is still physically stored.

The boundary cache defers physical conversion until a later logical update.
A resident parent keeps newly written children in Prefix Store, including the
first level outside the resident tree. The single resident pool is updated in
place; a block-local set of removed positions preserves the pre-commit boundary
rule. Thus eviction during this commit does not itself convert children. When a
parent is outside the resident boundary and is not protected by that retained
position set, only children pending from logical updates switch to Hash.
Unchanged siblings retain
their prior Prefix or Hash source. A Prefix parent can therefore persist a mix
of unchanged Prefix references and hashes of newly modified children.

Hash Store retains its canonical MPT format. Converting a modified internal
node can require Hash copies of its unchanged descendants to close that Hash
subtree; `ClosureBackfillRecords/Bytes` report these copies. This happens during
logical modification, never solely because of eviction. Existing Prefix versions
remain available for historical references. Newly converted nodes do not also
receive redundant Prefix versions in the same batch.

The legacy `HashUpdatedVictims` option remains parseable but does not control
placement. Existing Hash-backed snapshots remain readable. Reopening an old
Prefix node may require reading its Prefix children to recover their omitted
digests. The resident parent retains its boundary children's digests and actual
Prefix heights for exact point reads. A reverted or read-only update does not
trigger conversion.

Logical updates record persistence dirtiness independently of hash dirtiness.
A missing digest cache means `hashDirty`; in-memory node revision markers and a
per-trie acknowledgment watermark implement `persistDirty`. These markers are
not block heights and never enter RLP, physical records, proofs, or hashes. A
watermark belongs to one COW trie handle: committing one handle cannot clear
another copy's pending writes. Computing a hash only updates the digest cache.

The current Prefix-boundary commit walks dirty paths without reopening or
comparing the old logical tree.
The boundary cache and storage projection reuse the same temporary emissions
and parsed canonical fields. Preparation validates their shape once and captures
the cache image from those fields. Projection finalizes Prefix parents when the
preorder walk leaves their subtree, retaining only a depth-bounded parent stack.
Each published Prefix record is encoded once (`PrefixEncodes == PrefixRecords`);
child digests and statistics do not require decoding freshly encoded records.
`MaxBatchBytes` still limits encoded key/value bytes. The temporary parent stack
has a separate conservative bound of twice that limit plus depth-bounded metadata;
`ProjectionScratchPeakBytes` reports its peak charge. Prepared emissions count
field slices and backing capacity against their own batch-sized ceiling.
If this buffer reaches the batch-sized memory ceiling (for example during a
large import), projection walks dirty in-memory nodes again; it still does not
compare trees. Acknowledgment occurs only after the durable batch succeeds.
Failed staging and uncertain writes do not acknowledge pending state. Native
hash-store staging remains independent of this versioned-store acknowledgment.

Deletion and path compression are represented by the updated parents and roots;
cache repair removes disconnected descendants. Obsolete disk positions remain
historical orphans, not tombstones, because queries follow authenticated parent
links. A replacement tree with a different acknowledged base is exported in
full, rather than trusting dirty markers from an unrelated base. Changes reverted
within a block can conservatively remain dirty; an unchanged final root skips
all node writes, while unchanged subtrees of a changed root may be rewritten.

The store checks parent identity and height, prepares references and budgets,
then writes a single durable batch. The latest cache generation is published
only after successful Sync. Errors before writing do not publish a new Head.
A native write with uncertain outcome returns `ErrUncertain`; stop using that
handle and reopen to determine the durable state.

## Execution cache

### Current Prefix boundary policy

The policy below describes the default `fixed` / `strict` / `updated` modes.
The optional typed layout and adaptive policy are described under
[Typed cache and adaptive policy](#typed-cache-and-adaptive-policy).

`prefix-boundary` uses one forest-wide **N pool of parsed node fields** and no
independent H cache. Account nodes and all storage-trie nodes have separate
reserved capacity plus a common remainder. The storage reservation is shared
by the storage class, not multiplied by the number of contracts. Reservations
protect capacity; entries are not permanently labelled as shared or reserved.

Each cached node retains its own digest and known source/version. Resident
nodes have no stored trie ID or complete prefix. A 16-byte link contains the
parent, first-child and next-sibling slot references, a root-descriptor index,
and the incoming child slot. Trie IDs live in root descriptors. The position
index stores slot numbers; equality checks compare the supplied path directly
against parent links and extension fragments, without rebuilding a full temporary
position or copying ancestor nodes. Block-local observations carry full
positions for staging and admission. Topology changes detach obsolete descendants
before replacing the path fragments used to locate them. At a cached
boundary, parent fields retain child hashes; Prefix edge hints additionally
retain the child's actual matched height. A known Prefix height permits an
exact lookup rather than a predecessor search. Cold nodes without a hint still
pay version discovery. Hash references need no height.

Materialized execution nodes carry a small local `NodeLocation` on each
independent child reference: its Prefix/Hash source and, when known, the exact
Prefix height. COW edits retain these locators with unchanged references.
Reading a resident cache image constructs the detached parent and located
child references together. `LocalLookupReader` returns this node from the same
lookup that checks for a cold point-read path, avoiding a second cache probe and
parent copy. The returned node owns its data independently of cache-slot reuse.
Reading a disk parent attaches the child locations discovered during materialization. The
next child load consumes this information directly, even when its parent is
outside the resident pool. No node stores an additional complete path or trie
ID. These transient locators do not enter hashes, canonical RLP, cache images,
proofs, or persistent records, and cannot outlive their pinned execution reader.

A known Prefix height uses exact Get; an unknown height seeks directly within
the child's prefix. Materializing one parent reuses one Prefix iterator for
its necessary child-digest reads. Ordinary descent does not restart at the
root. Direct/legacy calls without a locator can still resolve physical links
from the pinned root, without reconstructing unrelated canonical ancestors.
The strict canonical byte/proof interface retains its authentication checks.

Execution reuses canonical or compact bodies already in the bounded block-local
candidate buffer, including calls that supply child locators. Prefix physical
fields materialize directly into COW nodes; canonical bytes are still generated
when needed by the candidate buffer, but are not decoded again to construct the
same host. Generic byte-store MPT opens likewise retain the root they decoded.

Hash-closure publication uses an operation-local canonical cursor shared by a
trie's closure requests. It retains an ancestor chain, descends from the nearest
loaded ancestor, and prunes subtrees already present in the write plan or Hash
store. Older shared and per-trie policies use dirty-node emission and the same
cursor for old-host identity checks. Their shadow-bank policy remains distinct
from the in-place Prefix boundary policy.

An EVM View retains up to 256 opened storage-trie root handles, scoped to its
immutable snapshot and contract address/root. Reads reuse the owner/lifecycle
binding; modification and speculative probes receive constant-size COW copies.
Closing the View discards these handles. This is not an answer cache and does
not add a separate retained node-body pool.

A local account or slot lookup trusts the pinned database and decodes only the
nodes on the selected path. The predecessor key must match the trie, path and
height bound. The execution cache can record a locator and load the canonical
fields later if the node is admitted. Forest commits reuse unchanged child
routes from the pinned parent. Before a changed resident is refreshed, a bounded
operation-local table retains its children's source, digest and Prefix height.
Publication reuses those locators; missing hints follow physical parent links
in the pinned snapshot without reconstructing old canonical nodes or fetching
siblings solely for their hashes.
Materializing a node for modification or proof
can require sibling digests; historical proof assembly follows the pinned local view,
and the client verifies the result against its trusted root.
A historical EVM query keeps one RocksDB Prefix iterator through the account
proof and, for a slot, the storage proof. Each walk enters its own root once
and carries the selected child record into the next step. Root-directory and
contract-lifecycle lookups share a separate iterator in the metadata column.
Closing the query view releases both iterators.

Resident N nodes connect to their trie's resident root. The account root is
protected; a storage root can leave as an eligible frontier. Replacement does
not evict a parent while retaining its resident descendants. Latest-state cache
entries are not reused by historical Views, which pin their own database
snapshot and requested height. Close all Views before closing the store.

Ordinary N hits provide detached copy-on-write node fields through
`mpt.MaterializedStore`, avoiding RLP generation, decoding, and hashing on that
hit. Full canonical RLP is generated only for an operation that needs it, such
as hashing, persistence, or proofs. Block-local candidate entries are bounded
and invalidated after publication/abort; the billed backing arena remains
allocated for reuse by later blocks.

The MPT separately memoizes a clean logical hash and encoded size. Copy-on-write
updates invalidate changed paths and reuse clean subtrees. A valid cached hash
does not mean that the node has been persisted; pending writes and durable
source information are tracked separately.

### Node state and lifetime

| Object | Retained information | Lifetime |
| --- | --- | --- |
| Resident cache slot | Parsed `NodeImage` fields, own hash, physical source/version, child source/height hints, parent/child links, scalar heat and allocation flags/lengths. Complete trie ID/prefix is not stored on each slot. | Until eviction, retirement, invalidation or close. |
| Logical MPT node | Leaf value or extension fragment/child or branch children/value; cached hash and encoded size; a persistence revision marker. Hash references may carry transient `NodeLocation`. | Active COW tree and its copies. |
| Block-local candidate | Position/hash, compact physical payload or canonical body when already available, version and admission metadata. | Current block only, bounded by `CandidateBytes`. |
| Shared heat sketches | Four fixed matrices and aligned bucket metadata, no per-key identity table. | Expire by the 64-block bucket clock. |
| Open storage handle | Address/root binding to a COW root and its pinned reader. | One View; at most 256 handles. |

`NodeImage` is a parsed arena representation, not a retained canonical RLP
buffer. `Size` records encoded length; `Used` marks a resident slot, while
`PreparationOnly` belongs to block-local observations and prevents bookkeeping
reads from becoming admissions. These have different lifetimes and roles.
The 64-block heat buckets do **not** retain candidates for 64 blocks.

A Prefix node's actual version is recovered from its key and retained as a
local hint. It is not redundantly serialized inside the Prefix value. A Hash
record needs no height. The hash-valid flag, persistence revision and heat
height are different from the physical version height and never enter the
Ethereum hash.

During execution, a parent supplies the next child's digest/source/version.
A resident child supplies detached fields; otherwise the reader reuses a
matching block-local body or loads the declared physical version. Prefix
siblings may need digest reads when their hashes are not resident. Parsed
fields then construct the logical node directly. This does not retain an
unbounded read cache or restart ordinary descent from the root.

### Resident heat and nonresident estimates

Heat comparisons use the resident node's own score when its body is cached.
Nonresident candidates obtain an approximate score from four shared Count-Min
Sketch buckets. The default half-life for both is 64 blocks.

| State | Retained heat information | Score used for comparison |
| --- | --- | --- |
| Resident node | `residentHeat.Value` (`float64`) and `residentHeat.Height` (`uint64`), 16 bytes per node | Stored value decayed from its last heat-update height |
| Nonresident node | Observations in the shared sketch matrices | Sum of the decayed estimates from the retained buckets |

A real Get/Update/Delete visit at height `h` updates a resident's two fields
using the following rule. `T` is the configured `HalfLife`.

```text
Value  = Value * 2^(-(h - Height) / T) + 1
Height = h
```

Every visit counts, including repeated visits within one block. A comparison
evaluates the decay formula without the `+1` and leaves the stored fields
untouched. `residentHeat.Height` advances on reads as well as writes. The
separate `Version.Height` identifies the node's persisted Prefix version.
With `T = 64`, heat 8 at height 100 decays to 4 at height 164.
One access then records heat 5 at height 164.

At admission, the sketch estimate initializes the scalar at the current height
and is capped at the parent's score at that same height. Admission itself adds
zero visits. Subsequent path visits and the common decay rate preserve parent
heat at least child heat. The invariant check permits up to eight floating-point
ULPs at the parent's score to accommodate rounding from different decay anchors.
Initial sketch estimates remain subject to collisions.

The shared sketches record every real logical visit, including resident visits,
so eviction leaves an approximate recent history available for later admission.
Sketch estimates are consulted for nonresident nodes. Proof construction, hashing
and cache preparation leave access heat untouched.

Each sketch bucket is aligned to **64 absolute block heights**, with
`epoch = floor(height / 64)` and `slot = epoch % 4`. For bucket anchor `a`,
access height `t`, and evaluation height `h`, the sketch uses these weights.

```text
increment(t)   = 2^((t - a) / T)
bucket score  = min(row counters) * 2^(-(h - a) / T)
sketch score  = sum of the retained bucket scores
```

The anchor is the first observed height in the bucket. Under contiguous
observation, the current partial bucket plus three older buckets cover
193–256 blocks. Height 255 retains 0–255, height 256 retains 64–256, and
height 300 retains 64–300. Partial buckets use their observed accesses.

Bucket expiration clears only the expired sketch matrix. Resident scores keep
decaying from their own timestamps, including contributions older than the
sketch window. Expiration therefore requires no resident scan. Eviction discards
the resident scalar, and subsequent estimates use the retained sketch history.
Empty blocks advance the clock, and height jumps expire old sketch slots.
Attempts and aborted execution count as observations. Restart or a Head mismatch
starts heat tracking cold. Retired-trie sketch residue expires with its buckets.

### Replacement interval and limits

`PolicyIntervalBlocks = K` gates policy eviction at heights satisfying
`h > origin && (h - origin) % K == 0`. Other blocks can warm free slots.
When full, both candidates and victims must be marked updated in that policy
block. This uses persistence-dirty emissions; edits reverted within a changed
root can remain conservatively dirty. A completely unchanged final root emits
no logical changes. Candidates are block-local; old candidate bodies are not retained.
Victims must be resident frontiers, respect the other class's reservation,
and cannot be the requesting candidate's ancestor. The account root is protected.

Each replacement draws up to `SampleSize` distinct eligible resident nodes uniformly
across classes that may donate a slot. A per-block index tracks free slots,
class usage and eligible nodes without scanning the forest for each admission.
The sampler uses `SampleSeed` and the current height. A zero or omitted sample
size selects five; positive sizes from 1 to 64 are supported.
Within the sample, selection prefers lower heat, then deeper nodes on ties, with stable
identity/path tie breaks. A candidate must have **strictly greater** heat to
replace its victim. Scalar parent clamping also applies at installation.
Bounded `WorkUnits` can skip optional admissions without changing state results.
Topology repair and trie retirement may remove invalid residents outside K;
these are counted separately from policy eviction.

For a fixed-capacity sampling study, run `hybrid-cache-bench` with
`-boundary-study-slots 48 -boundary-interval 4 -boundary-samples 5 -boundary-seed 11`.
Repeat with sizes 1, 10 and 64 in separate output directories. Size 64 examines
every eligible candidate in a 48-slot study. The partitioned run supplies the
block outcome reference. The boundary checkpoint includes per-block decisions,
timings, reads, measured logical writes and a final storage inventory.

### Configuration and defaults

Use [hybrid-evm-boundary.example.json](artifact/configs/hybrid-evm-boundary.example.json)
as the current explicit preset. The generic `DefaultBoundaryConfig` and the
native `DefaultEthereumPrefixBoundaryForestCacheConfig` are distinct:

| Setting | Native Ethereum preset | Generic boundary default |
| --- | ---: | ---: |
| `MaxNodeBytes` | 532 | 1 MiB |
| `NodeBudget.TotalBytes` | 256 KiB | 32 MiB |
| Account / storage reserve | 64 KiB / 64 KiB | 8 MiB / 8 MiB |
| Shared remainder | 128 KiB | 16 MiB |
| `CandidateBytes` | 2 MiB | 2 MiB |
| `MetadataBytes` | 4 MiB | 4 MiB |
| `PlanBytes` | 2 MiB | 40 MiB |
| `SampleSize` / `SampleSeed` | 5 / 0 | 5 / 0 |
| `PolicyIntervalBlocks` / `PolicyOriginHeight` | 16 / 0 | 16 / 0 |
| `HalfLife` / `MaxResidentTries` / `WorkUnits` | 64 / 256 / 262144 | 64 / 256 / 262144 |
| Buckets / blocks per bucket / rows / total width | 4 / 64 / 4 / 16384 | 4 / 64 / 4 / 16384 |

The 532-byte native preset validates 32-byte secure keys, empty branch values,
and bounded Ethereum account/storage RLP. It is not suitable for arbitrary
large opaque values. The sketch width splits between account and storage
classes; four float64 matrices occupy **2 MiB for the forest**, not per trie.
Metadata also includes indexes and descriptors. N uses reserved field capacity;
entry counts depend on the build's structure sizes, not average RLP length.
The single resident pool, candidates and metadata are billed separately. These
are owned-cache bounds, not a process RSS limit. The Prefix-boundary cache has no block write
bank, cache merge, or per-block full-pool copy. Existing slot/link metadata is
updated in place; the list of changed slot numbers only bounds policy work and
checks. It does not retain old nodes or a second parent/child graph. Free-slot
and account/storage occupancy indexes are maintained incrementally. Physical
Prefix/Hash rules and the on-disk format remain unchanged.

Ordered EVM execution uses the existing StateDB key write set and VM journal:

1. A transaction reads the live account/storage tries and buffers its account,
   code and slot changes in StateDB. Nested VM rollback stays within that journal.
2. After transaction finalization, apply its remaining key writes/deletions
   directly to the live tries, then clear the transaction write set. Included
   REVERT/OOG transactions retain their normal nonce/fee effects only.
3. The next transaction reads the updated tries. At block end, compute the final
   root and persist the final node versions once. Key writes are not applied a
   second time and intermediate transaction roots are not stored as block versions.

The first write to an address reuses its current account as the block-start
account, since every previously written address already has block-local lifecycle
metadata. This avoids a second account-trie traversal. An ordinary account with
no storage/code change, reset, active lifetime, or contract creation skips empty
storage-trie allocation and hashing. Lifecycle lookup remains once per changed
address per block; an empty storage root alone never proves a missing lifetime.
Inert storage plans are not passed to the publisher.

The transaction write set contains no trie nodes or parent/child relationships.
The normal trie update and cache-replacement code still maintains its own
necessary structure. There is no speculative duplicate of the resident cache
for block rollback. Exceptional I/O/preparation failures invalidate the cache
(or require reopen for an uncertain disk write) instead of maintaining a cache
undo log on every successful block. Historical queries remain bound to persisted
heights. Allocation snapshots and trusted block-final trace imports retain their
bulk application paths. Only trace-export runs accumulate an optional logical
key summary; that summary is never replayed into the live tries.

`ChangedNodes` and `IncrementalChecks` report resident-cache maintenance work.
Previously saved experiment allocations/timings retain their original semantics:
refit budgets for new runs. In live execution, the transaction timing phase now
includes applying each transaction's key writes; the final materialization phase
covers the block-final root work. Do not compare those subphases directly with
older block-overlay results without accounting for this change.

`DefaultHybridConfig` selects `partitioned` and Prefix initialization unless
overridden. The boundary example explicitly selects `prefix-boundary` and `hash-cold`.
Supported cache factory modes are `partitioned` (also the empty mode),
`reserved-shared`, and `prefix-boundary`. `reserved-only` is a comparison-bench
label implemented with reservations, not a factory mode.

The EVM runner accepts exactly one of `Synthetic` and `WorkloadFile`. Relative
workload paths resolve from the config directory; CLI `-config`/`-out` paths
resolve from the working directory through the shell wrapper. Set
`Schema: "versioned-evm-run/v1"`, `Backend: "hybrid"` (or the compatible
`hybrid-rocksdb` alias),
`Codec: "ethereum-mpt"`, `HybridCache`, and a positive `MaxBatchBytes`
(8 MiB in the example). Unknown JSON fields are rejected.

The runner does not expose read/projection limits directly. Store defaults are
32 MiB per read, 4,096 read nodes, and 1,048,576 projection nodes; API callers
can configure these in `hybridtrie.Config`. The original RocksDB defaults use
no compression, 8 MiB write buffers per CF, at most two buffers, and 128 open
files. Explicit KV engine options belong in `HybridStorage`.
The `CacheBytes`, `BlockCacheBytes`, `WriteBufferBytes`, and `OpenFiles`
runner fields must be omitted for both HybridTrie backend names.

## Streaming initialization

Streaming import supports Prefix and explicit `hash-cold-bulk` initialization.
The existing atomic `hash-cold` mode remains separate and does not accept
`HybridSnapshot`. See
[hybrid-evm-streaming.example.json](artifact/configs/hybrid-evm-streaming.example.json)
for runner integration and
[hybrid-snapshot.example.json](artifact/configs/hybrid-snapshot.example.json)
for the Prefix importer, or
[hybrid-snapshot-hash-bulk.example.json](artifact/configs/hybrid-snapshot-hash-bulk.example.json)
for the Hash importer with its reusable source retained.

The importer builds through an owned scratch database, exports the selected
representation in bounded batches, verifies it, then atomically publishes Head.
An unpublished/partial snapshot is not queryable as complete state. Default
construction chunks and checkpoint intervals are 256 entries/accounts, with
1 MiB logical batch payloads. One node or code record must fit a batch; code
records are not split. Working buffers, a callback's current account/storage
map, and database/runtime buffers are distinct memory costs.

Supply allocation objects in strictly ascending address order, without
duplicates. Balance is a nonnegative integer JSON number, Code is base64,
and Storage uses 32-byte hex keys/values. For example:

```json
{"Address":"0x0000000000000000000000000000000000000001","Balance":1000,"Nonce":0,"Code":"","Storage":{}}
{"Address":"0x0000000000000000000000000000000000000002","Balance":2000,"Nonce":0,"Code":"","Storage":{}}
```

Set `SourceFile` and header in the snapshot template. Source paths are relative
to the configuration file. Keep generated inputs and outputs under `.local/`.

```sh
sh artifact/scripts/import-hybrid-snapshot.sh \
  -config artifact/configs/hybrid-snapshot.example.json \
  -out .local/snapshot-import

# Resume the same immutable input, configuration, and output directory.
sh artifact/scripts/import-hybrid-snapshot.sh \
  -config artifact/configs/hybrid-snapshot.example.json \
  -out .local/snapshot-import -resume
```

The importer wrapper loads the native RocksDB library. For a portable import,
copy the snapshot template to a local configuration, set `HybridStorage.Engine`
to `leveldb`, and update `SourceFile` relative to that copied configuration.
Run the CLI directly, adding `-resume` only when continuing the same output:

```sh
CGO_ENABLED=0 go -C evm-versioned-trie run -mod=readonly ./cmd/hybrid-snapshot \
  -base-dir "$PWD" -config .local/snapshot-leveldb.json \
  -out .local/snapshot-leveldb-import
```

The target and its owned scratch database use the same engine. Changing that
engine, the source, or the import plan rejects resume.

For Hash bulk import, select `Initialization: "hash-cold-bulk"` in the dedicated
snapshot CLI, or `HybridInitialization: "hash-cold-bulk"` in the EVM runner.
`HybridSnapshot` supplies chunk/checkpoint/batch settings; the Go backend supplies
defaults with retained scratch when that object is omitted. The Hash template
sets `KeepScratch: true` explicitly. BaseFee and BlobBaseFee must be nonnegative
and present in its header. Every account source record, code record and canonical
node must fit a batch; a single huge contract row is not split automatically.

The source-file SHA-256 and header bind `WorkloadID`; `import.json` records the
effective plan. Resume rejects changed input/configuration, re-reads the source
prefix to check its digest, and skips durably exported target records. Final
verification still walks the snapshot. Failures preserve construction/target
files; successful import removes owned scratch unless `KeepScratch` is true.
`report.json` separates session-only construction counters from persisted,
cumulative target-import counters. `BuildSessionOnly`, `ResumedBuild` and
`AlreadyComplete` identify their scope. Snapshot construction/export checkpoints are separate from the block
runner's `-resume` protocol; the runner can also continue an interrupted streaming
initialization with the same frozen input and configuration.

### One shared source for all comparison methods

[evm-state/snapshot](evm-state/snapshot/) adds a prepared native Ethereum source.
`snapshot.Build` consumes sorted allocation rows, reuses the bounded MPT builder,
and preserves original account rows, rolling digest, source ID and full initial
header. The ready source contains canonical construction nodes plus its root
directory. Consumers traverse only final reachable nodes; intermediate build
nodes and original rows stay outside measured state databases.

```go
builder, err := snapshot.Build(ctx, sourcePath, snapshot.Config{
    SourceID: workloadID, Header: initialHeader,
    BatchBytes: 1 << 20, ChunkEntries: 256, CheckpointAccounts: 256,
    Storage: &options,
}, nextAllocation) // callback begins at row zero on resume
if err != nil { return err }
defer builder.Close()
source, err := builder.OpenSource()
if err != nil { return err }
defer source.Close() // release before closing builder

_, _, err = archive.ImportSnapshot(ctx, initialHeader, source, expectedRoot)
if err != nil { return err }
_, _, err = path.ImportSnapshot(ctx, initialHeader, source, expectedRoot)
if err != nil { return err }
_, _, err = hybrid.ImportPreparedSnapshot(ctx, initialHeader, builder, expectedRoot)
if err != nil { return err }
```

All methods use the same workload ID, header and expected root. Header allocation
and transaction arrays are empty: account rows are supplied separately. Archive
uses its configured initial height; indexed/packed state-only Path currently
uses height 0 or 1 selected by `ZeroGenesis`. The Hybrid target can import
the prepared source into either Prefix or Hash storage. Targets and source
must have independent, non-nested paths.
Prepared sources can be reused across either supported target engine.

Before target publication, validation binds the original account rows and digest
to the canonical account/storage forest, including code and absence of extra
slots. Archive copies Hash nodes/code; Path projects native current trie/flat
records with no pre-initial history. Hybrid projects the reachable forest into
the selected layout and imports roots, code and lifecycle metadata. Both layouts
yield the same Ethereum roots and start with empty resident and heat caches.

Archive and Path retain a small snapshot manifest reference in their initial
headers; the full descriptor is in the shared artifact store. The prepared-source
and JSONL paths avoid a large allocation array in `workload.json` or a 64 MiB
artifact blob. The older EVM runner's frozen-workload format still retains its
allocation array. The target batch
ceiling is the smaller of the source batch setting and target commit ceiling.
Durable progress blocks ordinary operations while import is incomplete. Resume
checks the previously copied prefix as well as new rows; root/Head and completion
publish together only after verification. Unknown write outcomes require reopen.
An existing completed import is idempotent. Queries can reopen state databases
without the source; audits of Archive/Path still need their artifact store.

The common-engine smoke test uses 305 accounts and 4 KiB batches, interrupts and
reopens imports, then checks three execution blocks against a one-shot Archive.
Each engine verifies 144 identical historical query responses across all three
methods. Additional tests inject failures before/after data and final writes,
reject changed/truncated sources and verify that partial roots stay hidden.

## Implementation discipline

Future changes to execution and storage paths must preserve these constraints:

- Carry physical child locators, parsed fields and computed digests forward to
  the next consumer. Explain any reread or recomputation that remains. Ordinary
  traversal continues along its path; root walks are reserved for explicit,
  measured cold/legacy fallbacks.
- Keep canonical hash dirtiness distinct from persistence dirtiness. Visit dirty
  paths, rather than copying the entire cache or comparing entire old/new trees.
  Local execution decodes trusted data; imports and client verification retain
  authentication. Strict canonical inspection can return its computed digest
  through `InspectCanonicalNodeWithHash` instead of hashing twice.
- Bound temporary state as well as resident storage. Do not retain RLP as a
  second long-lived node representation. Preserve pinned-view/COW isolation and
  account lifecycle identity when reusing a read result.
- Resident-index deletion closes the affected probe chain, including wraparound.
  It must not accumulate tombstones or require an unbounded whole-cache sweep.
  Test long position churn, not just a short stable working set.
- Write-plan-owned bytes may use `Batch.PutBorrowed` until synchronous Write
  returns. Public `Batch.Put` keeps its copying semantics. Drivers must never
  retain borrowed slices. Block atomicity, WAL and synchronous durability remain
  unchanged; batch preparation time is not automatically fsync time.
- Add regression checks for repeated traversals, encoding counts, allocation
  ownership and bounded work when changing a hot path. Keep state roots,
  historical membership/absence proofs, deletion/redeployment and error recovery
  in the validation set. Apply shared low-level improvements to comparison
  baselines too, with matched resource budgets.

The implementation fixes above preserve the default cache policy, physical
key/value formats and proof formats. The following modes explicitly opt into
the separately designed cache changes.

### Typed cache and adaptive policy

The native Ethereum cache supports three independent configuration choices.
Omitted fields preserve the original policy:

| `Boundary` field | Default | Optional mode |
|---|---|---|
| `ResidentLayout` | `fixed` | `typed`: 256/512/1024-byte leaf/extension/branch bodies |
| `VictimEligibility` | `updated` | `frontier`: sample all resident frontiers |
| `ReservationPolicy` | `strict` | `borrow`: lend unused class reservations |
| `AdmissionReadNodes` | 64 for adaptive admission | Per-block optional admission read limit |
| `AdmissionReadBytes` | 256 KiB for adaptive admission | Per-block optional admission byte limit |

Use `DefaultAdaptiveEthereumForestCacheConfig()` for the complete opt-in mode.
The [adaptive comparison template](artifact/configs/hybrid-compare-adaptive.example.json)
selects it through `HybridBoundary`. That comparison field merges onto the native
preset before fitting the common total cache budget; unknown fields are rejected.
For `hybrid-evm`, put these settings inside `HybridCache.Boundary`, alongside the
ordinary cache settings. This changes no database or proof format.

Typed residents keep one self digest in an **80-byte common header on the tested
64-bit platforms**, with stable NodeIDs, links, heat, source/version and a body
handle. There is no long-lived NodeImage/RLP copy or per-node trie ID/full prefix.
Leaf bodies have no child locators. Other bodies use two masks and six bytes per
known Prefix child; Hash children carry no height. Height zero remains valid.
Field views borrow the arena, while materialized execution nodes detach it.

One fixed arena contains 1 KiB frames, each split into four small, two medium, or
one large unit. Non-full and empty frame lists support local allocation and
release. Empty frames can change class; live frames are never compacted or moved
wholesale. The pool reserves four headers per frame and accounts for unused
headers, body slack, frame metadata, indexes and candidate/selection buffers.
`SlotBytes` is zero for the variable layout; use `HeaderBytes`, `FrameCount`,
`AllocationQuantumBytes`, `NodeAllocatedBytes`, `UnusedHeaderBytes` and
`UnusedBodyBytes`. `NodeLiveBytes` includes used locator bytes as well as fields
and headers. These describe cache backing, not total process RSS or EVM working
state; normal staging/read budgets still bound operation-local data.

Legal growth within a node type fits its reserved body. A smaller replacement
may keep a larger existing unit, billed at its actual capacity. The account root
reserves a large unit from admission. Type growth may move only that node's body;
if no suitable unit exists after structural repair, discard that changed
resident and count `StructuralResizeDrops`, without evicting unrelated nodes.
A parent keeps the child's digest and physical locator. Structural subtree
invalidation is separate from policy eviction and can visit its affected subtree.

Full-cache replacement still requires an updated candidate in a K block. The
`frontier` mode maintains eligible boundaries incrementally on attach/detach;
it does not scan the resident pool to rebuild them each block. Candidate parent
identity and child slot flow into installation without repeating parent lookup.
Different body classes may require freeing one frame's group of at most four
frontiers. Each owner must satisfy protection and quota rules. Ordinary admission
requires the candidate's heat to exceed the **sum** of group heat. Bounded random
sampling can reject an admission because of fragmentation. In adaptive mode,
`EligibleSum` describes the sampled frontier population before group feasibility
checks; it is not a guarantee that every sampled node can donate a frame.

With `borrow`, an entire candidate fitting within its class reservation can
reclaim the other class's excess without winning the heat comparison. The donor
must keep its own reservation. A candidate crossing its reservation boundary
uses ordinary competition. Floors are scaled to the actual frame-rounded node
pool, preserving the protected account-root minimum. There are no per-node loan
tags or cross-block candidate bodies. Return is a bounded periodic priority,
not an immediate capacity guarantee.

Optional admission reads reserve a maximum legal record before issuing I/O and
charge actual logical record/key bytes afterward. Defaults are 64 reads and
256 KiB per policy call; a zero setting selects these defaults. Native reads use
a conservative 4096-byte per-record bound. Preparation, shape/identity checks and
capacity planning finish before policy victims are removed. Budget exhaustion
skips admission; corruption retains normal error/invalidated-cache semantics.
Eviction itself neither backfills Hash records nor deletes Prefix records.

Portable tests cover frame churn and fragmentation, memory accounting, Prefix
height hints, K gating, reservation return, grouped heat, failed admission,
structural growth and historical proofs. The EVM conformance tests run the new
mode on LevelDB and RocksDB, including receipts, restart, CREATE2 redeployment,
Cancun semantics and rejected publication. Fixed layout, typed layout, borrowing
and frontier eligibility remain separately selectable for controlled comparisons.

### Execution and query read switches

Execution prefetch and historical proof reads are independently configurable.
They are disabled when their configuration is omitted. No command-line flag,
database migration or recompilation is needed to change them.

| Entry point | Execution read settings | Query read settings |
| --- | --- | --- |
| `backend.HybridConfig` | `Cache.Boundary.ExecutionReads` | `ParallelProof` |
| EVM runner JSON | `HybridCache.Boundary.ExecutionReads` | `ParallelProof` |
| Comparison runner JSON | `HybridBoundary.ExecutionReads` | `ParallelProof` |

Both objects use the same switch convention:

| Settings | Behavior |
| --- | --- |
| `Enabled: false` | Original synchronous reader; no new read context, queue or background readers, even if `Workers` is nonzero. |
| `Enabled: true, Workers: 0` | New optimized synchronous path; no background queue or workers. |
| `Enabled: true, Workers: 1/2` | New path with background reads. Proof queries also support four workers. |

To turn off **only asynchronous work** while keeping the new synchronous
optimizations, set `Workers` to zero. To return to the original implementation,
set `Enabled` to false. The execution switch does not enable query workers and
the query switch does not enable execution prefetch. These switches control
reads, independently of transaction concurrency and parallel hashing.

The [combined comparison example](artifact/configs/hybrid-compare-async.example.json)
contains both switches enabled. The
[LevelDB EVM example](artifact/configs/hybrid-evm-leveldb.example.json) and
[RocksDB boundary-cache example](artifact/configs/hybrid-evm-boundary.example.json)
expose both switches with `Enabled: false`; changing either boolean enables that
path with the supplied worker count and resource limits.

Settings are read when opening the backend; there is no in-flight hot reload.
For a new experiment setting, use a new output directory. Resume requires the
original configuration and must not mix switch settings in a sealed run.
The EVM runner exercises configured proof queries during its final read-only
verification and records `QueryRequests`, `QueryWorkersStarted` and
`QueryBackgroundReads`. Each response is verified against the recorded block
root. With `ParallelProof.Enabled: false`, its original checked-view audit
remains in use. Execution counters are under `Cache.ExecutionReads`.

### Optional asynchronous Prefix reads

`ForestCacheConfig.Boundary.ExecutionReads` enables an experimental read path
for the latest-state `prefix-boundary` execution cache. It is disabled by
default. See [the runnable comparison template](artifact/configs/hybrid-compare-async.example.json).

```json
{ "ExecutionReads": {
  "Enabled": true,
  "Workers": 1,
  "Queue": 256,
  "Entries": 8192,
  "BodyBytes": 8388608
} }
```

`Workers: 0` enables the same lazy node representation with synchronous I/O;
`Workers: 1` or `2` adds background raw-byte prefetch. `Enabled: false` uses the
original synchronous reader. Worker counts up to eight are accepted, but only
0/1/2 are covered by the current cross-engine conformance suite. This switch is
independent of parallel hashing.

The execution thread reads necessary nodes and owns all trie edits, decoding,
heat updates and cache replacement. After a real edit, it queues unresolved
Prefix siblings needed for hashing. Workers read immutable records from the
block's pinned snapshot using one cursor each. A Prefix digest comes from the
33-byte header; acquiring it does not decode or hash the complete node. A child
with an already known digest generates no prefetch. Hash Store records keep
their canonical RLP format and use the digest already present in the reference.

One block-scoped table identifies original physical positions by trie ID,
packed nibble path and path length. A queued read can be claimed synchronously;
an in-flight read is awaited individually. Raw bytes and parsed nodes are
reused, and raw retention ends after successful decoding. Immutable source
locators survive path splits/merges; delayed results cannot overwrite a modified
live node. No node-cache snapshot, whole-tree comparison or root-to-child retrace
is added. Public proofs still authenticate canonical bytes.

Queue saturation skips optional prefetch. Body-budget pressure retains the
digest and actual Prefix height; a later body request performs a counted exact
Get at that height. Table saturation uses serialized foreground fallback, so
bounded operation can cause rereads. Optional admission reads retain their own
existing limits. Tables allocate on the first miss; subsequent blocks reuse
backing arrays by clearing occupied entries. Fully resident workloads start no
read workers. No raw encoding is added to the persistent resident cache.

`ForestBlock.FinishReads()` seals submissions and drains background I/O without
decoding pending results. The EVM adapter invokes it before final hashing and
publication; transaction-local storage-root hashes wait only for their required
references. Commit, abort and close join reads before releasing the snapshot.
As with the existing execution API, one foreground thread owns trie mutation;
this does not enable concurrent transactions on the same trie.

`Backend.CacheStats().ExecutionReads` reports record-level Get/Seek counts,
foreground/background ownership, claims, waits, raw/parsed reuse, decoding,
header-only reads, body upgrades, budget fallbacks and retained-byte peaks.
`RecordReads` counts database API operations, **not physical disk I/O**.
`ReadBytes` counts returned/copied value bytes; `LogicalRecordBytes` also includes
the omitted body of a limited header read. `IONanos` sums raw-read-stage durations, including positioning and header checks;
these can overlap. Use end-to-end block time to measure speedup.
`MaxReadBufferBytes` and `PeakDecodeScratchBytes` report temporary buffers
separately; the latter conservatively includes the source bytes and detached
node fields during conversion, rather than silently dropping that overlap.
Physical execution lookups encode their already packed path directly. They
compare the returned encoded position and height bound without unpacking the
path again; the node key format and version checks remain unchanged.

`ExecutionReadBudgetBytes` is a separate upper bound for table backing, queue
slots and retained raw/parsed results. `BodyBytes` charges detached node fields
and reference descriptors conservatively. It excludes Go allocator overhead,
the live COW execution tree, one temporary read buffer per active reader,
transient decoding/encoding and native database buffers. These remain separate
from the retained cache allocation. The comparison runner records this extra
budget in `resources.json`; enabling it does not establish equal total process
memory merely because resident cache budgets match.

Correctness tests cover both KV engines, 0/1/2 workers, roots, receipts, historical
account/storage proofs, lifecycle changes, deletion compression, synchronous
claiming, bounded fallback and close with pending I/O. Channel-controlled tests
prove execution continues while a sibling read is blocked, without timing
sleeps. An opt-in end-to-end experiment compares the old reader, lazy synchronous
reader and one/two workers with cold Prefix data, a small cache after 1,000 warmup
blocks, and a resident-cache control:

```sh
HYBRID_EXECUTION_READ_REPORT="$PWD/.local/execution-reads.json" \
  go -C evm-versioned-trie test ./backend \
  -run '^TestHybridExecutionReadComparison$' -count=1 -timeout=10m -v
```

For RocksDB, set `HYBRID_EXECUTION_READ_ENGINE=rocksdb`, the native CGO flags
shown above, and add `-tags rocksdb`. The experiment writes aggregate counters
and block timings outside Git. Short local runs do not establish a general
asynchronous speedup; scheduling overhead can dominate when database reads are
already served quickly.

### Optional parallel historical proofs

`HybridConfig.ParallelProof` enables a separate historical-query path. It is
disabled by default and does not change stored records, execution caches or
logical hashes. The same top-level `ParallelProof` object is accepted by the
EVM runner and comparison runner; it applies only to Hybrid in a comparison.
See [the runnable template](artifact/configs/hybrid-compare-parallel-proof.example.json).

```json
{ "ParallelProof": {
  "Enabled": true,
  "Workers": 2,
  "MinBackgroundReads": 4,
  "MaxContextBytes": 262144
} }
```

`Workers: 0` uses the new planned walk with synchronous sibling reads. Values
1, 2 and 4 enable that many background readers at most. `Enabled: false`
retains the original path. Comparing zero workers with background workers
isolates scheduling from changes to decoding and proof assembly.

The foreground walks each required trie once, using decoded physical fields
to continue immediately. It reads each selected Prefix child body once and
uses the digest in that body for its parent. Missing off-path Prefix digests
are grouped by parent and ascending slot, with at most four siblings per task.
Each worker reuses its own predecessor cursor on the same immutable snapshot;
it copies only a 33-byte header and does not decode or hash sibling bodies.
Hash and embedded sibling references require no database read. Header copying
does not imply a 33-byte device read.

One query session spans both account and storage paths. Once the account leaf
is available, the foreground resolves its historical storage trie and continues
without waiting for account siblings. Root-directory results and decoded
account fields are reused. The session joins once after both paths and any code
read, encodes each Prefix proof node once, and transfers existing Hash RLP and
already compact proof bytes. It does not revisit the root, create a per-child
future or read-result map, or perform proof verification to recover the value.
Public checked-proof APIs and client verification retain their checks.

The 16-group queue and workers start only after `MinBackgroundReads` unresolved
siblings accumulate (allowed range 1–16). Unsubmitted slots stay in their path
frames when the queue fills. After descent, the foreground completes those
slots and claims remaining queued groups with its existing cursor, then waits
only for work already in flight. Each group has one owner; no read is repeated.
`ForegroundReadNanos` reports this final foreground reading time separately
from `WaitNanos`. Scheduling remains bounded and never blocks descent.
All-Hash queries start no workers. A snapshot wrapper without independent
cursor support uses counted synchronous fallback. Errors or cancellation join
in-flight readers before closing cursors and releasing the snapshot; native
reads already in progress are allowed to return. No partial proof is returned.

The context budget covers the session, path frames, queue payload, retained
values/code/RLP and output buffers. It excludes Go allocator metadata, goroutine
stacks, native cursors/engine caches and temporary decoding/read buffers.
Per-trie node and logical-byte limits remain in force, including the full size
of a record whose header alone was copied. A limit error does not retry the
query using another mode. `resources.json` records the extra per-query
`ProofContextBudgetBytes`; equal resident-cache settings do not mean equal
total process memory when this option is enabled.

`Backend.QueryWithProofStats` returns per-query counts for metadata, Prefix/Hash
reads, header copies, body decodes, encoded nodes, workers/cursors, foreground
fallback and retained-context peaks. It also reports walk, final-wait and
assembly times. These phases exclude some setup/cleanup, and background work
overlaps the walk: use end-to-end query time as the latency measure. Temporary
conversion peaks and the largest per-reader buffer are separate from retained
context bytes. Logical read counts are not device I/O counts. Legacy mode
returns empty planned-path statistics.

Both GoLevelDB and native RocksDB tests compare proof bytes and values with
the original path and verify the result independently. They cover historical
updates, deletion/redeployment, identical prefixes in different tries,
Hash/Prefix/embedded references, absence, queue saturation, read errors,
cancellation and concurrent view closure. Controlled channels demonstrate
account/storage overlap and no duplicate record reads; race tests cover both
engines. Opt-in experiments record warm-read latency, allocation/CPU costs,
read counts and a separate throughput pass:

```sh
HYBRID_PARALLEL_PROOF_REPORT="$PWD/.local/parallel-proof.json" \
  go -C evm-versioned-trie test ./backend \
  -run '^TestParallelProofComparison$' -count=1 -timeout=5m -v

HYBRID_PARALLEL_BRANCH_REPORT="$PWD/.local/parallel-proof-branches.json" \
  go test ./hybridtrie -run '^TestParallelProofBranchComparison$' -count=1 -v
```

The end-to-end measurement uses process CPU counters on macOS/Linux. For
RocksDB, use the native CGO flags above, add `-tags rocksdb` and set
`HYBRID_PARALLEL_PROOF_ENGINE=rocksdb`. Reports are saved outside Git after each
group (each branch scenario for the microbenchmark). Warm cache measurements
may show scheduling overhead rather than a speedup; the feature stays opt-in.

## Validation

Portable codec, MPT, LevelDB backend, and EVM tests do not require RocksDB:

```sh
CGO_ENABLED=0 sh artifact/scripts/test-versioned-baseline.sh
```

The script checks the root module, local EVM runtime, state interfaces, Path
baseline, and HybridTrie EVM/comparison modules. All relative module replacements
resolve inside this repository. The independent upstream oracle is optional.

Native storage, cache, initialization, EVM, and snapshot tests require the pinned
library and the `rocksdb` build tag, configured by the test wrapper:

```sh
sh artifact/scripts/test-hybrid-rocksdb.sh
```

Coverage includes key ordering and bounds, inline/short-root handling,
Prefix/Hash source closure, contract lifecycle reuse prevention, historical
membership/absence proofs, cache topology/parent heat, bucket rotation,
commit budgets, CAS conflicts, injected write failures, and read-only reopen.
Hash validity and durable publication are tested separately. Small EVM fixtures
also compare roots, receipts, and outcomes with the same runtime on an
independent map-backed state.

The recent implementation regressions are exercised directly by:

```sh
go test ./mpt ./hybridtrie -run 'Test(CanonicalCursor|MaterializeLocalFields|ExecutionLocations|ExecutionReuses|HashClosure)' -count=1
go -C evm-versioned-trie test ./backend -run '^TestStorageViewReuse' -count=1
go test ./mpt ./hybridtrie ./kvstore -run 'Test(ResidentIndex|FusedMaterialization|LocalLookup|InspectWithHash|ProjectionEncodes|BorrowedBatch)' -count=1
go -C evm-versioned-trie test ./backend -run '^TestPlainTransaction' -count=1
go test -race ./mpt ./hybridtrie ./kvstore
```

These check deletion-chain repair, single Prefix encoding, fused materialization,
borrowed-write ownership, plain-account updates, ancestor reuse, byte-identical
Hash closure, direct field
materialization, block-local body reuse, closed-reader/version guards, and
storage-handle COW isolation. The full native suite also checks legacy heat
accounting after retaining the decoded root, historical proof equivalence,
lifecycle recreation, snapshots and recovery. On 2026-11-11 the relevant full
RocksDB/LevelDB suite, EVM backend/comparison tests and MPT/Hybrid race checks
passed. The performance results below come from the earlier six-group run;
this implementation update has not repeated that end-to-end benchmark.

The separate [oracle module](evm-versioned-trie/oracle/) uses upstream Geth
v1.16.9 and imports neither the local MPT nor storage adapters. Export native
EVM proof/state fixtures and verify them with:

```sh
HYBRID_EVM_PROOF_REPORT="$PWD/.local/hybrid-evm-proofs.json" \
HYBRID_EVM_STATE_REPORT="$PWD/.local/hybrid-evm-states.json" \
  sh artifact/scripts/test-hybrid-rocksdb.sh
(cd evm-versioned-trie/oracle && go run -mod=mod . verify-proofs ../../.local/hybrid-evm-proofs.json)
(cd evm-versioned-trie/oracle && go run -mod=mod . verify-states ../../.local/hybrid-evm-states.json)
```

The oracle is an independent Go module; `-mod=mod` allows Go to resolve its
pinned dependency graph on the first run. LevelDB fixtures can be exported
without CGO using `VT_EVM_PROOF_REPORT` and `VT_EVM_STATE_REPORT` with the targeted
`TestHybridLevelDBLiveEVMRootsReceiptsHistoryAndReopen` backend test, then verified
by the same oracle.

The KV integration checks cover both drivers' atomic batches, snapshot isolation,
bounded/reusable predecessor cursors, read-only reopen, engine mismatch rejection,
and write-error recovery. A matched smoke run produced byte-identical block
results for 6 blocks and 72 transactions on both engines, with 224 account and
896 storage proofs checked per engine. Upstream Geth also verified 700 exported
LevelDB historical proofs and rebuilt 14 state roots.

This oracle independently validates Ethereum state encoding, roots, and proofs;
it is not a second independent full transaction-execution engine. Small smoke
tests establish correctness and operability, not a general throughput result.
The project does not yet claim a large-scale performance evaluation of the
current boundary cache and Hash-cold combination.

## Comparison tools

### Hybrid storage representations

[hybrid-storage.example.json](artifact/configs/hybrid-storage.example.json)
drives the synthetic MPT storage experiment. Inspect its output path before
running; the shell launcher resolves relative output paths from the repository.

```sh
sh artifact/scripts/run-hybrid-storage.sh artifact/configs/hybrid-storage.example.json
```

The experiment compares configured representations on identical deterministic
updates and verifies roots, historical proofs, and unchanged database fingerprints
during read-only checks. Matching sealed checkpoints can be reused only when
configuration, trace, executable identity, and database fingerprint agree.
Unfinished groups remain on disk and cause an error rather than being overwritten.

The report separates live Prefix/Hash/metadata Key+Value bytes, actual RocksDB
SST bytes, and whole-directory space. Each method's own measured initialization
is subtracted for execution growth. Final-minus-initial SST is net physical
growth after compaction, not the size of isolated history-only files.

An optional projection and summarizer accept the actual result directory:

```sh
sh artifact/scripts/project-hybrid-storage-snappy.sh RESULTS_DIRECTORY
python3 artifact/scripts/summarize-hybrid-storage.py RESULTS_DIRECTORY
```

The projection re-encodes the complete live inventory into common GoLevelDB
Snappy tables and verifies ordered Key/Value digests. It is a normalized
compression measurement, not actual compressed RocksDB storage or a prediction
of compressed RocksDB performance. The existing native storage-experiment
presets disable compression; the shared KV layer separately supports explicit
`none` or `snappy` selection. RocksDB Snappy requires a library built with Snappy
support; the bundled reproducible native build disables optional compression
libraries and must use `none`.
Synthetic storage commits exclude EVM execution; their timings are diagnostics.

### Forest cache comparison

```sh
sh artifact/scripts/run-hybrid-cache-bench.sh \
  -out .local/hybrid-cache-comparison -include-boundary \
  -accounts 256 -blocks 2000 -warmup-blocks 1000 -tx 32 -contracts 16 -repetitions 3
```

The benchmark compares partitioned, reserved-only, reserved-shared, and optionally
Prefix-boundary policies on account-hot, storage-hot, rotate, migrate, and
revisit workloads. It matches total backing allocation and verifies identical
results. Block counts include an unmeasured warmup; output records the effective
configuration, allocation, measured block times, and correctness status.
This comparison uses atomic Prefix initialization.

For an account-cache pressure workload, the live-EVM generator also accepts
`Pattern: "account-sweep"`. Each pass visits every background account in a
seeded random permutation, using real balance-changing experiment transfers.
The next pass reshuffles recipients and increases the transferred value, so
measurement does not replay warmup transaction bytes. The pattern performs no
contract calls; it is a synthetic account-update stress case, not a mainnet mix.
With 100,000 background accounts and 100 transfers per block, 1,000 sweep blocks
cover the entire background once. A pressure experiment can then measure ten
blocks from the next permutation and append a successor for historical queries.
Provision candidate buffering and Hash-closure write batches equally across
cache sizes. A cold all-Prefix forest can overflow these auxiliary limits before
the resident cache fills; an unmeasured bootstrap block and explicitly reported
common auxiliary budgets separate that startup effect from capacity pressure.
Cached capacity alone does not establish storage pressure:
check occupancy, eviction counts, and actual Prefix/Hash placement. In particular,
a nonresident child may remain Prefix when its parent is resident.

## Comparison backends

### Unified comparison

Execution trials start cold and apply 1,000 warmup blocks by default. The
example uses 2,000 execution blocks after initialization. Blocks 1–1,000 warm
the live adapter and blocks 1,001–2,000 supply timing samples, with the same
cache throughout. All blocks are checked against the reference. For short
correctness runs, set `WarmupBlocks` explicitly. Storage measurements still
cover the full chain and historical queries use their separate read-only view.

When execution needs a Prefix sibling's digest, the boundary cache can stage
the physical body already fetched, along with its actual version. Admission
uses the existing parent, capacity, reservation and replacement rules. Such
reads add no logical access heat. Point reads still follow only the requested path.

The example sets `"PrefixSnapshot": true`. Hybrid imports the shared source
as Prefix records. Subsequent updates follow the deferred boundary placement
rule described above.
An unchanged version retains its original address. Omitting this option or
setting it to false reproduces the earlier Hash-initialized comparison.

From the repository root, run the portable example in a fresh output directory.

```sh
mkdir -p .local
go -C evm-versioned-trie build -mod=readonly -o ../.local/hybrid-compare ./cmd/hybrid-compare
.local/hybrid-compare -config artifact/configs/hybrid-compare.example.json -out .local/compare-run
```

For RocksDB, build against the project's native library and set `"Engine": "rocksdb"` in a copy of the example. Use a different output directory for each run.

```sh
r="${ROCKSDB_PREFIX:-$PWD/.local/native/rocksdb-9.10.0}"
CGO_ENABLED=1 CGO_CPPFLAGS="-I$r/include" CGO_LDFLAGS="-L$r -Wl,-rpath,$r" \
  go -C evm-versioned-trie build -mod=readonly -tags rocksdb -o ../.local/hybrid-compare-native ./cmd/hybrid-compare
```

Select methods (`archive`, `path`, `hybrid`) and suites (`storage`, `execution`, `historical-query`) in the configuration. Each method uses the same verified bulk snapshot, Ethereum leaf codec, engine and resolved resource plan. The reference executes the workload once to freeze per-height roots, receipts and logical changes. All methods then use independent state directories.

| Suite | Input and measurement |
| --- | --- |
| Storage | Import identical authenticated block-final changes. Measure after initialization and after the last block with full compaction. Report signed differences in state key plus value bytes and physical SST bytes. Record per-component inventories, cumulative KV writes and engine statistics separately. |
| Execution | Reopen a freshly initialized database for each trial and execute the frozen transactions serially. Exclude the configured leading warmup blocks. Time the complete adapter call through synchronous commit and view release. Record preparation, EVM setup/execution, materialization through root hashing, and publication (including physical encoding, batching and cleanup). Archive and Path validate their prebuilt external artifacts with read-only lookups during publication. Reference execution, comparison and result-file writes run outside the timed call. |
| Historical query | Reopen the final database read-only. Time local service through value and compact proof construction, then client verification against the independent height/root index. Save both intervals, their sum, proof size, request order, engine statistics and read-only inventory integrity. Queries execute in order from a fresh database handle each trial; the OS page cache is retained. |

Use either `Synthetic` or `WorkloadFile` for block input. File paths are relative to the configuration. Generated queries alternate accounts and storage slots, mixing uniform heights with a recent bias (maximum of two uniform draws). They include absent-key probes. Supply a `QueryFile` containing an ordered JSON array of requests to replay a specific query set. A query trial reuses the final state database. An execution-only Path trial also prepares its external change artifacts first.

Completed execution timing databases are disposable: after sealing their
results, the runner removes `trial-*-db`. It retains the result samples, seals,
reference data and any separately constructed storage/query databases. Preserve
additional execution inventories before cleanup when measuring an interval
inside a trial; the default execution suite does not collect per-block storage.

The runner rotates method order between trials. `workload.json`, `snapshot.json`, `blocks.json`, `resources.json`, and (for query runs) `queries.json` and `corpus.json` identify the common inputs. Each completed storage group and trial writes its own JSON. `results.json` appears after all selected groups pass. It contains per-block and per-query samples in nanoseconds and per-trial means and nearest-rank P50/P70/P90/P95. Each result has a digest sidecar binding it to the run. A failed run retains its files for diagnosis. The 30-query example is a correctness smoke test.

Add `-snapshot-only` to prepare and verify the shared initial trie. This stops after writing `snapshot.json`. Continue with the same configuration and output directory using `-resume` when ready to execute blocks.

Use `-through-storage` to stop after the selected storage results are sealed. This uses the original run configuration and lets a later `-resume` reuse the same databases for timing trials.

#### Resume a comparison

Use the original configuration and output directory with the same binary and the additional `-resume` flag.

```sh
.local/hybrid-compare -config artifact/configs/hybrid-compare.example.json \
  -out .local/compare-run -resume
```

* The run manifest binds the effective settings, chain rules, workload and explicit query input. An exclusive directory lock prevents two controllers from advancing one run. Resume requires a directory created by the resumable runner.
* Reference execution reuses committed traces and executes the remaining blocks. After the block index and query corpus are sealed, resume loads their verified digests directly.
* Storage resumes from the verified database head. Before each measured block's synchronous batch, the controller durably records the block ID, height, root, and cumulative KV counters on both sides of that write. On reopening, the published head selects the correct total, including the engine's encoded batch bytes. The original compacted initialization inventory stays fixed. Checkpoint files are external to measured state. Native engine counters retain their individual open scopes; `EngineCountersSingleScope` indicates whether the endpoints share one scope.
* Each timing group comprises one method, one trial and its configured suites. Resume checks the result seal and sample counts. An interrupted group restarts with a fresh execution database and the same warmup and query order. Previous partial trial databases are retained in `trial-*-interrupted-*` for inspection. Storage and trial checkpoint I/O runs outside timed intervals.

### Comparison adapters and correctness

- [evm-state/codec](evm-state/codec/) defines the explicit `ethereum-mpt` and
  `legacy-raw105` leaf formats. The original versioned codec import path is a
  type-alias compatibility facade. `forest.Open` and `query.Verify` retain their
  legacy defaults; use `forest.OpenWithCodec` and `query.VerifyWithCodec` for
  explicit formats.
- Path's `Config.Codec` accepts `ethereum-mpt`, including its historical flat
  account/slot encoding and restoration. Omission preserves existing legacy
  configurations. The codec is part of database identity; reopening with a
  different codec fails instead of migrating old data.
- [comparison.OpenArchive](evm-versioned-trie/comparison/archive.go) provides a
  persistent canonical Hash Archive. It deduplicates immutable MPT nodes by
  Keccak hash across account/storage tries, retains historical roots and code,
  and atomically publishes nodes and Head in a synchronous KV batch. Its pinned
  `ViewAt` and `Query` APIs use the same forest and compact-proof code as the
  other methods. It supports live EVM execution and logical v2 imports.
- Path's explicit `Config.Storage` selects GoLevelDB or RocksDB through
  `kvstore`; its existing differential history, flat values, packed indexes,
  and restoration algorithms are reused. Leave `Storage` nil for the original
  raw-key LevelDB layout and `OpenWithOptions` cache controls. The two physical
  layouts are distinct and are not automatically converted.
- `Backend.ApplyWithTrace` executes an execution block and returns a typed
  `evm-logical-block/v2` record. `Backend.ApplyLogicalBlock` and Path's
  `ApplyLogicalBlock` import that record without running transactions. They bind
  complete input bytes, codec, block/parent IDs, lifecycle flags, slot changes,
  code and outcomes, and check the expected root before publication. Initial
  allocation snapshots are imported separately. Use `Freeze`/`ParseLogicalBlock`
  with a manifest-bound artifact digest; these are trusted experiment APIs.
- `query.Read` assembles a common compact response from a pinned view, without
  retaining that view or verifying its own response. Clients authenticate it
  using `VerifyWithCodec` and an independently trusted root. Compact proofs
  contain the root and independently hashed path nodes; inline descendants stay
  in their parent. Duplicate, reordered, missing and surplus items are rejected.
  Existing expanded-proof responses remain supported.

#### State databases and shared experiment artifacts

[evm-state/artifact](evm-state/artifact/) stores immutable, content-addressed
block inputs, outcomes, logical v2 traces, and Path encoding metrics outside
measured state databases. Its manifest binds workload ID and codec; object IDs
also bind kind and content. Share one directory across methods and engine runs
of the same workload. Objects are synced before state commits reference them;
retries reuse validated objects. A rejected commit may leave an unreferenced
object but cannot publish a partial state. Each object is limited to 64 MiB;
large snapshots instead use the bounded prepared-source database described above.

Archive always uses an external artifact store for writing. For Path, explicitly
set `StateOnly = true` and open with `OpenWithArtifacts`. In that mode, the
state database retains only compact headers/references rather than full inputs,
receipts, changes statistics, or traces. Trie nodes, flat values, reverse
history, indexes, code, historical root headers and Head remain part of state
storage. Path's `StorageInventory` includes root headers and Head in its state
components. `KVInfo` reports the actual selected engine; application-key
inventory excludes LevelDB's reported column namespace overhead.

```go
// Imports under github.com/XiaodongQi1/HybridTrie:
// kvstore, evm-state/{artifact,codec}, evm-versioned-trie/comparison,
// and pa "github.com/XiaodongQi1/HybridTrie/evm-path-archive".
options := kvstore.DefaultOptions(kvstore.LevelDB) // or kvstore.RocksDB
source, err := artifact.Open("run/artifacts", artifact.Config{
    WorkloadID: "workload-1", Codec: codec.Ethereum,
}, true, false)
if err != nil { return err }
defer source.Close() // caller owns this shared source; close databases first

archiveConfig := comparison.DefaultArchiveConfig("workload-1", options.Engine)
archiveConfig.Storage = options
archive, err := comparison.OpenArchive("run/archive", archiveConfig, true, false, source)
if err != nil { return err }
defer archive.Close()

pathConfig := pa.DefaultConfig("workload-1")
pathConfig.Codec, pathConfig.Format = codec.Ethereum, pa.PackedFormat
pathConfig.ZeroGenesis, pathConfig.StateOnly = true, true
pathConfig.Storage = &options
path, err := pa.OpenWithArtifacts("run/path", pathConfig, true, false, source)
if err != nil { return err }
defer path.Close()
```

Align `ArchiveConfig.InitialHeight` with Path's `ZeroGenesis` and the workload;
the example starts with an allocation-only snapshot at height 0. Use distinct
state directories for every method/engine. Read-only historical queries can
reopen Archive or state-only Path with a nil artifact source, even if the
artifact directory is offline. `Result`, `Audit`, write retries, and imports
need the original source. An existing read-only source can be shared by writers
only when all referenced objects have already been generated.

The common-engine fixtures compare persistent Archive, Path, and HybridTrie
against an in-memory reference and frozen Geth roots, including absence, slot
deletion and same-address recreation. Each engine verifies 432 byte-identical
compact responses and 608 constituent proofs across six states. Live EVM tests
run 72 transactions across six execution blocks plus initialization, compare
independently executed Path with Archive, import the same changes into
Path/HybridTrie, and verify 840 historical responses after reopening with
external artifacts offline. Both engines run these tests. Existing Path
formats and its legacy compatibility adapter also retain full regression tests.

```sh
(cd evm-versioned-trie && go test -mod=readonly ./comparison)

# Export common-engine proofs for independent upstream verification.
mkdir -p .local/comparison-check
COMPARISON_PROOF_REPORT="$PWD/.local/comparison-check/proofs.json" \
  sh artifact/scripts/test-hybrid-rocksdb.sh
(cd evm-versioned-trie/oracle && \
  go run -mod=mod . verify-proofs ../../.local/comparison-check/proofs.json)
```

#### Resource planning and measurement

[comparison.ResourceBudget](evm-versioned-trie/comparison/resources.go) resolves
one budget into Archive, Path and HybridTrie configurations. It counts Hybrid's
single resident pool, node fields, indexes, roots, four sketches, candidate
arena and cache-instance metadata using the same layout calculation as actual
allocation. It fits node slots without allocating a probe cache. Archive and
Path each use an execution-only LRU keyed by the expected node hash. A hit
supplies parsed MPT fields for account or storage execution. On a miss Archive
reads by hash and Path reads the current owner/path. The LRU retains a node
after reading it. Hybrid uses its boundary cache. Each method has one
application cache and the same separate engine block-cache budget.

| Default resource | Setting |
| --- | --- |
| Application cache backing | 64 MiB target, including the resident pool and auxiliary buffers |
| Engine block cache | 64 MiB per database, shared across RocksDB CFs |
| Global memtable target | 32 MiB per database |
| Synchronous commit / Hash-forest staging ceiling | 8 MiB each |
| Open files / RocksDB background jobs | 128 / 2 |
| Compression | None |

```go
budget := comparison.DefaultResourceBudget()
plan, err := budget.Resolve(kvstore.RocksDB,
    hybridtrie.DefaultEthereumPrefixBoundaryForestCacheConfig())
if err != nil { return err }
archiveConfig := plan.ArchiveConfig("workload-1")
pathConfig := plan.PathConfig("workload-1")
hybridConfig := plan.HybridConfig("workload-1")
// Open separate state directories using these configurations and one shared
// artifact source. Save plan, including resolved budgets and allocation slack.
```

The common application target is Hybrid's fitted allocation. Both baselines allocate
fixed node-image slots and spend the sub-slot remainder on usable hash buckets.
The execution trial checks the actual allocation against the common target
and records the allocation, entries, hits, misses and evictions after warmup
and after measurement. The exact fitted size depends on the build's structure
sizes and is recorded in `resources.json`. `NodeBudget` describes the resident
node pool. The common application budget also includes policy indexes and
auxiliary structures. Replacement work limits remain those of the supplied cache template; record its policy/work
exhaustion counters when evaluating larger caches.

On read-only reopen, these baseline configurations disable their execution
LRUs; Hybrid likewise constructs no latest-state execution cache for historical
queries. All three then use the same configured engine block-cache target.
Reported `ReadCache` bytes are actual allocations, while `Info.Memory` retains
the persisted configuration. The budgets count requested backing arrays and
struct sizes, not Go allocator rounding, EVM journals, active COW trees, the
bounded per-View storage-root handles, temporary encoding, or all native/runtime
allocations. `CaptureProcessMemory` separately
reports Go memory and process-lifetime peak RSS on macOS/Linux. Use isolated
processes for per-method RSS; the smoke test's shared-process peak is diagnostic.

Archive, state-only Path, and the Hybrid backend all expose `EngineStats` and
`MeasureStorage(ctx, stabilize)`. The common measurement separates:

- Required state components, including complete application keys, values,
  historical roots, Head, code, lifecycle and indexes. Configuration records are
  explicitly excluded; unknown records fail classification. Components close to
  the state total, and state plus excluded records closes to all application KV.
- GoLevelDB's injected column-key bytes and namespace registry. These close with
  application KV to `EncodedKVBytes`; they are not omitted from physical cost.
- Native SST, WAL, engine-log and other file bytes, whose sum closes to the
  directory total. Native SSTs still contain configuration/engine overhead;
  they are not presented as a configuration-free projection of logical state.
  `Engine.LiveSSTBytes` is the current-version SST size; directory SST bytes can
  also include obsolete files awaiting asynchronous deletion. Entries that
  disappear during directory scanning are skipped; other IO errors fail it.
- Per-open Put/Delete counts, repeated overwrite bytes, per-column namespace
  bytes, and actual serialized write-batch payloads, including batch headers.
  Native WAL/flush/compaction tickers and GoLevelDB file-write counters are
  separate engine metrics. None is labelled physical device IO.

Hybrid's `Backend.CacheStats()` separately returns its actual forest-cache
allocation and policy counters; use `AllocatedBytes()` to sum all its backing
components. Archive/Path's execution node LRU allocation and counters are in
`EngineStats().ReadCache`. Smoke reports save both initial and final Hybrid
cache statistics alongside the common measurements.

`MeasureStorage(ctx, true)` flushes and fully compacts outside timing and rejects
live snapshots or read-only handles. It captures counters before and after
maintenance. For execution-write deltas use the initial measurement's
`AfterMaintenance` and the final measurement's `BeforeMaintenance`, so initial
sealing is excluded. Measure initialization before the first execution block;
`kvstore.Delta` subtracts each method's own initial inventory and preserves
negative increments. It rejects incomplete measurements and different database
paths/engines/column sets. Scanning can warm engine caches; reopen before a cold
execution trial. File inventory is a separate point-in-time measurement.

Statistics carry a fresh scope ID on every open and explicitly state whether
coverage begins at creation or the current reopen. `kvstore.WriteDelta` rejects
mixing scopes. The comparison controller persists cumulative application write
counts with each block. Native counters retain their original scope across restarts. Unsupported
or disabled engine metrics serialize as null, including GoLevelDB's separate
WAL/flush counters and current memtable allocation. A measurement is marked
`Complete` only after every scan and diagnostic succeeds.

Both engines have three-method resource smoke tests covering actual allocation,
initial/final inventories, write counts, negative deltas, read-only stability and
proof equality. To save their diagnostic reports in an ignored directory:

```sh
COMPARISON_RESOURCE_REPORT_DIR="$PWD/.local/resource-check" \
  sh artifact/scripts/test-hybrid-rocksdb.sh
```

The [unified comparison](#unified-comparison) uses these resource plans and bulk initialization. The older runners below retain their existing configurations.

### Existing comparison runners

[versionedtrie](versionedtrie/) provides the LevelDB compact baseline.
Its 4-byte IDs/heights and physical format differ from HybridTrie's 6-byte
fields. Its EVM backend accepts `legacy-raw105` and `ethereum-mpt`; HybridTrie
accepts only `ethereum-mpt`.

```sh
sh artifact/scripts/run-versioned-evm.sh \
  -config artifact/configs/versioned-evm.example.json \
  -out .local/versioned-evm-run

go run ./cmd/versioned-storage-bench \
  -config artifact/configs/storage.synthetic.example.json \
  -out .local/versioned-storage-run

sh artifact/scripts/run-versioned-evm-storage.sh \
  -config artifact/configs/versioned-evm-storage.example.json \
  -out .local/versioned-evm-storage-run
```

The single-trie synthetic comparison uses opaque byte values, not Ethereum
account payloads. The three-method EVM storage comparison runs
HashArchive, then constructs Path v7 and HashVersioned from the authenticated
block-final changes. That existing runner remains pinned to `legacy-raw105`
and the v1 execution artifact, even though the Path library now also supports
the native codec through its explicit API. It is a local Path
implementation, not an upstream Geth deployment. Do not combine its storage
results with native HybridTrie results without matching codec, workload,
initialization policy, and measurement scope. These runners require fresh
output directories and preserve failed/interrupted artifacts.

## Code map

| Directory or file | Responsibility |
| --- | --- |
| [mpt](mpt/) | Shared logical MPT, canonical encoding, hash memoization, COW nodes, materialized field views, and proofs. |
| [mpt/canonical_cursor.go](mpt/canonical_cursor.go), [persistence.go](mpt/persistence.go), [local_fields.go](mpt/local_fields.go), [location.go](mpt/location.go) | Ancestor reuse, dirty-node emission, direct local materialization and transient child locations. |
| [kvstore](kvstore/) | Shared ordered KV API, atomic batches, snapshots, reusable cursors, and RocksDB/GoLevelDB drivers. |
| [evm-state/artifact](evm-state/artifact/) | Shared immutable execution artifacts outside measured state storage. |
| [evm-state/snapshot](evm-state/snapshot/) | Reusable bounded Hash snapshot construction, account provenance and validation. |
| [evm-versioned-trie/comparison](evm-versioned-trie/comparison/) | Persistent common-engine Archive and multi-method correctness fixtures. |
| [hybridtrie/engine_kv.go](hybridtrie/engine_kv.go) | Connects HybridTrie columns and error classes to the selected KV engine. |
| [hybridtrie/key.go](hybridtrie/key.go), [record.go](hybridtrie/record.go), [locator.go](hybridtrie/locator.go) | Physical node keys, records, and explicit sources. |
| [hybridtrie/store.go](hybridtrie/store.go), [forest.go](hybridtrie/forest.go) | Atomic versions, Head/CAS, roots, lifecycle, code, and Hash initialization. |
| [hybridtrie/view.go](hybridtrie/view.go), [proof_query.go](hybridtrie/proof_query.go) | Snapshot-scoped authenticated reads and historical proof contexts. |
| [hybridtrie/boundary_cache.go](hybridtrie/boundary_cache.go), [boundary_reader.go](hybridtrie/boundary_reader.go) | Parsed-field N pool and source/version hints. |
| [hybridtrie/boundary_heat.go](hybridtrie/boundary_heat.go), [boundary_policy.go](hybridtrie/boundary_policy.go) | Scalar resident heat, four aligned sketches for nonresident estimates, and K-block replacement. |
| [hybridtrie/snapshot_builder.go](hybridtrie/snapshot_builder.go), [snapshot_import.go](hybridtrie/snapshot_import.go) | Bounded resumable Prefix/Hash initialization and atomic final publication. |
| [evm-versioned-trie/backend](evm-versioned-trie/backend/), [runner](evm-versioned-trie/runner/) | EVM state adapter, frozen workloads, configuration, and local execution reports. |
| [evm-versioned-trie/oracle](evm-versioned-trie/oracle/) | Independent upstream Geth encoding/root/proof validation. |
| [versionedtrie](versionedtrie/) | LevelDB compact version store. |


## Repository scope and third-party code

The original Statera networking, consensus, SSH deployment, and Ethereum download
programs are outside this repository. Test fixtures under `testdata/` are small
correctness inputs; experiment databases, mainnet datasets, binaries, profiles,
and private configurations remain ignored.

The local EVM runtime contains adapted go-ethereum components and vendored
dependencies. Their existing copyright headers and license files are preserved
in `evm-runtime/`, including `COPYING` and `COPYING.LESSER`. This extraction
does not replace or relicense third-party code.
