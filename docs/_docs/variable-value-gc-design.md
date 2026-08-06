---
docid: variable-value-gc-design
title: Cost-aware GC for variable-size values
layout: docs
permalink: /docs/variable-value-gc-design.html
---

## Research scope

The target workload is an update-intensive profile store. A stable user-ID key
range is repeatedly overwritten, while serialized profile values vary from
1 KiB to 16 KiB and values larger than 512 B are separated into immutable
value SSTs (vSSTs). Foreground update throughput and physical space
amplification are the primary metrics.

Entry-count garbage ratios are inaccurate in this setting: invalidating one
16 KiB value and invalidating one 1 KiB value have the same entry weight but
different reclamation value. Correcting the ratio alone can trigger more useful
GC work, but it also exposes the cost of choosing a batch, validating each
candidate, and migrating survivors. The design therefore treats GC as three
connected decisions rather than three independent features.

## Design overview

| Stage | Question | Mechanism | Primary effect |
| --- | --- | --- | --- |
| Byte accounting | How much space can GC reclaim? | Compact logical-size field in each immutable value reference | Accurate victim ranking |
| Cost-aware batching | Which vSSTs should run together? | Greedy marginal utility over reclaimed bytes, live bytes, and shared key-SST scan bytes | Fewer low-yield GC jobs |
| Streaming validation | How should the batch execute? | Bounded-memory merge join over sorted vSST records and relevant key SSTs | Remove point lookups and enable certified purge |

### Lightweight byte accounting

A new separated-value reference uses the high bit of the encoded file number to
mark a following varint logical value size. The remaining low 62 bits retain
the file number. Key-SST construction aggregates reference sizes into each
`SST -> vSST` dependence without fetching the full value.

References written by older versions remain readable. If any reference in a
dependence lacks a logical size, the dependence is marked incomplete and GC
falls back to an entry-count estimate. Byte accounting is enabled with
`precise_gc`.

### Dependence-aware batch selection

For each eligible vSST, the picker estimates:

- reclaimable physical bytes;
- live bytes that GC must migrate;
- key-SST bytes needed to validate its references.

The picker greedily maximizes marginal utility:

```text
reclaimable bytes
-----------------------------------------------
live migration bytes + new reference scan bytes
```

Key-SST scan bytes already charged to a selected vSST are not charged again.
This makes vSSTs with overlapping dependence sets natural batch partners.
Immutable Versions build the compact reachability certificate lazily. Candidate
validation files are then found by key-range overlap, so normal Version
installation does not materialize a large blob-to-key-SST edge index.
Marked files retain priority, existing eligibility thresholds remain in force,
and a batch is bounded to six files and half the target vSST size in estimated
live bytes. Additional candidates must share validation files and cannot reduce
the aggregate reclaim-to-cost utility. Enable this stage with
`gc_cost_aware_selection`.

### Streaming validation and tiered reclamation

The original GC validates each vSST record through an LSM point lookup.
Streaming validation instead merges the current key-SST iterators that overlap
the selected vSST key ranges and advances them in the same key order as the
vSST scan. L0 files use separate iterators, while non-overlapping files in
higher levels use one lazy concatenating iterator per level. It keeps only the
current user key and source file in memory.

The fast path accepts only ordinary value records. Snapshots, write-conflict
snapshots, range deletions, MapSSTs, merge operands, malformed references, and
conflicting duplicate internal keys conservatively fall back to the original
point lookup. The iterator status is checked before a streaming result is used.
Streaming is also disabled for a candidate when the overlapping key-SST entry
count exceeds eight times the candidate entry count. This prevents a broad
random-key vSST range from repeatedly scanning the complete key LSM. Point
lookup timing uses sampling rather than two clock reads per candidate.
Enable this stage with `gc_streaming_validation`.

Key SSTs also persist a compact completeness certificate containing the total
record count, separated-record count, range-deletion count, and sorted set of
referenced vSST file numbers. Its space is proportional to the number of
dependence targets, not the number of keys.

When complete dependence metadata proves that a selected vSST is unreachable,
`gc_purge_only` deletes it through the normal VersionEdit installation path
without scanning or rewriting values. Purgeable files can schedule GC without
waiting for the global garbage ratio. A certificate is revalidated against the
latest Version before installation, so an unrelated flush does not invalidate
it. A mixed batch is split: certified files are purged first, while the
remaining files return to the regular GC queue.

## Correctness and compatibility

- Missing, malformed, or incomplete metadata never proves absence.
- Rewrite GC output is discarded when its input Version changes; purge-only GC
  revalidates its reachability certificate against the latest Version.
- Purge certificates reject live snapshots and unknown direct or MapSST
  dependencies.
- vSST deletion and replacement use the existing atomic manifest installation
  path.
- Legacy value references continue to use entry-based accounting and point
  lookup validation.

## Evaluation plan

The authoritative paper scale is a 100 GiB initial database. The development
machine is used only for preliminary validation with:

- a 10 GiB initial database;
- 1--16 KiB uniformly distributed values and a 512 B separation threshold;
- two flush threads, two compaction threads, and explicit GC workers;
- ten update operations per initial record over the same key range;
- update throughput as the foreground result;
- peak and post-drain space amplification as the space result.

The main ablation sequence is:

1. original entry-based TerarkDB GC;
2. byte accounting;
3. byte accounting plus cost-aware batching;
4. all three stages.

Fixed-size values are the negative control. Additional variable-size workloads
should vary the correlation between value size and update hotness. External
comparisons use RocksDB, BlobDB, and Titan after every engine passes the same
load, update, drain, and post-drain verification gates.

Required secondary measurements include GC input/output bytes, point-lookup
time, streaming validations and fallbacks, selected batch composition, metadata
bytes, write amplification, and tail latency. One-off development runs are not
paper results.

## Paper narrative

The paper should present one space--foreground-performance problem:

1. variable-size values make entry-count GC choose the wrong work;
2. accurate byte accounting reveals that the best victim is not always the best
   batch once migration and validation costs are included;
3. better batches make point lookup and survivor migration the remaining
   execution bottleneck;
4. TerarkDB's immutable sorted vSSTs and dependence graph allow a bounded-memory
   streaming join and certified zero-rewrite purge.

The three contributions therefore form one causal chain: accurate benefit
measurement, cost-aware decision making, and efficient execution.
