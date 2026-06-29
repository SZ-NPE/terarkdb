# TerarkDB Motivation Figure Notes

This file records the current M1-M5 motivation design supported by `test-sh/new-ycsb/motivation.sh`. It does not preserve old matrices as active defaults.

If it conflicts with current scripts or logs, trust the scripts/logs and update this file.

---

## 1. Current alignment

- Entry: `test-sh/new-ycsb/motivation.sh`.
- Matrix: 5 cases, one case per motivation figure.
- Mixed-value path: `uniform_fixed`, not Pareto.
- M5 uses key-correlated mixed value size plus `byte_precise_gc=true` and `use_separated_value_meta_block=true`.
- Final/interface ablations live in `test-sh/new-ycsb/interface.sh`; motivation figures explain why the optimizations are needed.

Relevant code: `test-sh/new-ycsb/motivation.sh:111`, `test-sh/new-ycsb/motivation.sh:97`, `test-sh/new-ycsb/motivation.sh:314`.

---

## 2. Figure map

| Figure | Case | Workload/settings | Primary data | Message |
| --- | --- | --- | --- | --- |
| M1 | `m1_garbage_cdf` | fixed values, `overwrite-zipf1.2`, unoptimized baseline (no hot/cold routing) | `BLOB_GC_RECLAIM_STATS`, fallback `BLOB_GC_FILE_GARBAGE_STATS` | Stock TerarkDB under write-hotspot skewed overwrites produces many mixed-garbage vSSTs, so reclaiming them still requires full scan and lookup work. |
| M2 | `m2_gc_io` | fixed values, `overwrite-zipf1.2` | `BLOB_GC_RECLAIM_STATS`, `BLOB_GC_BYTES` | Blob GC cost includes vSST reads, invalid/dead reads, kSST reverse lookups, and live relocation writes. |
| M3 | `m3_gc_timeline` | fixed values, `workloada`, default 600s, timechart on | timechart CSV + `BLOB_GC_RECLAIM_STATS` + `BLOB_GC_LATENCY` | GC scan/reverse lookup traffic can disturb foreground throughput/latency. |
| M4 | `m4_cache_residency` | fixed values, `workloada`, default 900s, obsolete tracking on, standard LRU cache (unoptimized residency) | `BLOCK_CACHE_OBSOLETE_SAMPLE` + timechart CSV | Obsolete blocks can remain resident long enough to motivate GC-aware eviction. |
| M5 | `m5_byte_accounting` | mixed `uniform_fixed`, key-correlated value size | `BLOB_GC_FILE_GARBAGE_STATS.entry_ratio/byte_ratio` | Dead-entry ratio is not a reliable proxy for reclaimable bytes under mixed values. |

Case overrides are defined in `test-sh/new-ycsb/motivation.sh:297`.

---

## 3. Diagnostic families

The current suite expects these diagnostics when enabled:

```text
BLOB_GC_FILE_GARBAGE_STATS
BLOB_GC_RECLAIM_STATS
BLOB_GC_BYTES
BLOB_GC_LATENCY
BLOCK_CACHE_OBSOLETE_SAMPLE
*_kvbench_timechart_*.csv
```

`motivation_log_inventory.tsv` counts these families; see `test-sh/new-ycsb/motivation.sh:394`.

---

## 4. Mapping to paper optimizations

| Optimization theme | Motivation figures | Interface validation |
| --- | --- | --- |
| Hot/cold routing + drop-key reverse-lookup acceleration | M1, M2, M3 | `hotness_base` vs `hotness_opt` |
| GC-aware block cache | M4 | `gc_cache_base` vs `gc_cache_opt` |
| Byte-precise GC over separated-value metadata | M5 | `precise_base` vs `precise_opt` |

---

## 5. SVG sketches

Use these only as structural guides:

- `third-party/terarkdb/paper/motivation-expected-figures-m1-m2-m4-m5-m6.svg`
- `third-party/terarkdb/paper/m3-expected-gc-foreground-timeline.svg`

If an SVG disagrees with current code/log semantics, update the SVG or this note; do not let the sketch override reality.

---

## 6. Maintenance rule

Update this file only when the case matrix, mixed-value default, figure meaning, diagnostic source, or plotting path changes in a stable way.
