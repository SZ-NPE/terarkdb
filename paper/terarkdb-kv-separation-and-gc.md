# TerarkDB KV Separation and Blob GC Notes

This file aligns current TerarkDB implementation facts with the paper-facing concepts used in `KV_Bench_Env`.

Rule: current code is the highest-credibility source. If this file disagrees with code, update this file.

---

## 1. Current paper baseline

- Basic KV separation is implemented and used as the paper baseline.
- Large values are stored in ordered value SSTables (`vSST` / Blob SST); key SSTables (`kSST`) store key plus value index.
- Hot/cold routing and drop-key-cache reverse-lookup acceleration are implemented.
- GC-aware block-cache diagnostics and obsolete-residency sampling are implemented.
- Byte-precise GC is implemented in the picker and metadata pipeline.
- Current script matrix: 5 motivation cases and 14 interface cases.

Script anchors: `test-sh/new-ycsb/motivation.sh:114`, `test-sh/new-ycsb/interface.sh:140`, `test-sh/new-ycsb/final_config.sh:83`.

---

## 2. Paper-facing configuration boundaries

| Topic | Current paper setting | Evidence |
| --- | --- | --- |
| Basic value separation | enabled through `blob_size=512` | `third-party/terarkdb/include/rocksdb/options.h:305`, `test-sh/new-ycsb/motivation.sh:250` |
| Middle-value delta-separate | supported by engine but disabled in paper path | `test-sh/new-ycsb/interface.sh:283`, `test-sh/new-ycsb/final_config.sh:79` |
| Read separated value by handle | disabled | `test-sh/new-ycsb/interface.sh:285`, `test-sh/new-ycsb/final_config.sh:44` |
| Third paper optimization | byte-precise GC with `use_separated_value_meta_block` explicitly enabled | `test-sh/new-ycsb/interface.sh:339`, `test-sh/new-ycsb/final_config.sh:78`, `test-sh/new-ycsb/final_config.sh:83` |
| Mixed values | `uniform_fixed` with 50% uniform 550B-800B values and 50% fixed 16KB values; M5 motivation enables key correlation by default, and interface precise cases can opt into the same correlation through `MIX_VALUE_SIZE_KEY_CORRELATION` / `MIX_VALUE_SIZE_KEY_CORRELATION_POWER` | `test-sh/new-ycsb/motivation.sh:105`, `test-sh/new-ycsb/motivation.sh:318`, `test-sh/new-ycsb/interface.sh:307` |
| Hot/warm vSST GC policy | engine option `hot_warm_blob_gc_max_files=0` disables the policy; TerarkDB experiment wrappers default it to `ceil(dataset * 4 / target_blob_file_size)` and optimized hotness/full paths inherit that value | `third-party/terarkdb/include/rocksdb/options.h:370`, `test-sh/new-ycsb/terarkdb_config.sh:134`, `test-sh/new-ycsb/final_config.sh:48`, `benchmark/kv_bench_tool.cc:1833`, `db/terarkdb/terarkdb.cc:244` |

Do not describe the current third optimization as standalone delta-separate or read-by-handle.

---

## 3. KV separation model

Conceptually:

```text
kSST: internal key -> value_index(blob_file_number + optional metadata)
vSST: same internal key -> real separated value
```

Separated values remain in ordered SST-like files, so scans still traverse ordered kSST state and lazily fetch separated values when needed.

Key paths:

- separation decision and value-index creation: `third-party/terarkdb/db/compaction_iterator.cc:864`, `third-party/terarkdb/db/dbformat.cc:203`
- flush/build path: `third-party/terarkdb/db/flush_job.cc:386`, `third-party/terarkdb/db/builder.cc:325`
- compaction output path: `third-party/terarkdb/db/compaction_job.cc:1864`, `third-party/terarkdb/db/compaction_job.cc:3317`
- read/scan fetch path: `third-party/terarkdb/table/iterator.cc:114`, `third-party/terarkdb/table/get_context.cc:195`, `third-party/terarkdb/db/version_set.cc:1298`

Important wording: use **ordered value SSTables**, not unordered blob logs.

---

## 4. Separation conditions

- `blob_size` is the base threshold: smaller values stay inline; values at or above the threshold may be separated; `size_t(-1)` disables separation.
- `blob_large_key_ratio` can suppress separation when the key is too large relative to the value.
- Middle-value separation options exist (`enable_delta_separate`, `middle_blob_size`, `middle_combine_level`) but are not current paper defaults. When enabled, the engine tags normal, large-blob, hot-middle-blob, and cold-middle-blob SSTs with `sst_type`, routes flush-separated middle values separately from large values, and keeps Blob GC within the same `sst_type` class.
- The `sst_type` enum has six values: `kNormal` (0), `kLargeBlob` (1), `kHotMidBlob` (2), `kColdMidBlob` (3), `kWarmLargeBlob` (4), `kColdLargeBlob` (5). The two `*MidBlob` variants belong to the middle-value path above; the two `*LargeBlob` variants are the access-hotness tags used by hot/cold routing (§7) and only appear when `enable_hotness_tracker=true`.

Implementation anchors: `third-party/terarkdb/include/rocksdb/options.h:305`, `third-party/terarkdb/include/rocksdb/options.h:356`, `third-party/terarkdb/db/compaction_iterator.cc:882`, `third-party/terarkdb/db/builder.cc:352`, `third-party/terarkdb/db/compaction_picker.cc:956`.

---

## 5. Blob/vSST GC model

Blob GC is implemented as a compaction type (`kGarbageCollection`) over hidden-level value files, not as a separate blob-job abstraction.

Trigger and picker:

- GC eligibility is checked in `ColumnFamilyData::NeedsGarbageCollection()`.
- Picker entry is `CompactionPicker::PickGarbageCollection()`.
- Candidates come from hidden level `-1`; files must be GC-permitted, not already compacting, and meet the threshold unless explicitly marked.
- The picker may expand selected files and caps input/output size.
- The picker also has a dynamic-threshold hook and a small-vSST defragmentation fallback; when a version has too many fragmented blob/vSST files, GC can rewrite them even if no single file meets the garbage-ratio threshold.

Key paths: `third-party/terarkdb/db/column_family.cc:1007`, `third-party/terarkdb/db/compaction_picker.cc:862`, `third-party/terarkdb/db/compaction_picker.cc:1104`, `third-party/terarkdb/db/version_set.cc:1903`.

Execution:

1. scan selected old vSST files;
2. reverse-lookup each record through the current version;
3. discard dead records;
4. rewrite live values into new vSST outputs;
5. install new outputs and retire old files.

Correctness boundary: a Blob GC job writes one replacement vSST for its selected
input/inherited file set. The version install path redirects each old blob file
number through the inheritance map to that replacement, so GC must not split live
records for the same input set across multiple hot/cold routed outputs. Hot/cold
routing still applies to flush-generated vSSTs; GC keeps a single output owner to
avoid stale value-index references after retiring old files.

Key paths: `third-party/terarkdb/db/db_impl_compaction_flush.cc:2037`, `third-party/terarkdb/db/compaction_job.cc:2315`, `third-party/terarkdb/db/compaction_job.cc:2525`, `third-party/terarkdb/db/compaction_job.cc:2574`.

---

## 6. Entry-based vs byte-precise GC

| Mode | Score basis | Best suited for | Key paths |
| --- | --- | --- | --- |
| `byte_precise_gc=false` / `precise_gc=false` | obsolete-entry ratio | fixed or near-uniform value sizes | `third-party/terarkdb/db/compaction_picker.cc:91` |
| `byte_precise_gc=true` / `precise_gc=true` | obsolete bytes / accounting bytes from separated-value metadata | mixed-size values | `third-party/terarkdb/include/rocksdb/options.h:88`, `third-party/terarkdb/include/rocksdb/table.h:193`, `third-party/terarkdb/db/compaction_picker.cc:86`, `third-party/terarkdb/db/version_set.cc:1891`, `third-party/terarkdb/db/version_builder.cc:496` |

This distinction is the basis for M5 and the `precise_base` vs `precise_opt` interface ablation.

Current boundary: `byte_precise_gc` is a simple boolean switch for byte-based Blob/vSST GC scoring. It does not introduce a value-fetch repair mode; it only consumes byte metadata that is already available from separated-value metadata blocks or dependence properties. In db_bench, use `--byte_precise_gc=true` together with `--use_separated_value_meta_block=true` for the mixed-value optimization path. The dependence metadata pipeline stores only file number, entry count, and separated-size bytes, and writes those bytes through `rocksdb.sst.dependence.separated-size`.

---

## 7. Three current optimizations

| Optimization | GC-pipeline position | Main effect |
| --- | --- | --- |
| Hot/cold routing + drop-key cache | before and during GC | concentrate garbage and short-circuit some reverse lookups for overwritten keys |
| GC-aware block cache | after GC/compaction changes file usefulness | keep normal LRU behavior and move obsolete vSST data blocks to the LRU tail for earlier eviction |
| Byte-precise GC + separated-value metadata block | GC candidate selection | choose candidates by reclaimable bytes rather than entry counts |

Hot/cold routing uses one `HotnessTracker` per column family, but foreground write-window learning has been removed. `RecordWrite()` is a compatibility no-op, and the removed knobs `hotness_window_capacity`, `hotness_enable_write_window`, and `hotness_admit_threshold` must not be described as current controls. Current hotness comes from background compaction feedback batches: each sub-compaction aggregates dropped separated-value versions by user key, the compaction install path publishes one batch after successful commit, and that single feedback stream updates two internal index surfaces with one lifecycle/epoch policy. Routing heat is approximate fixed-size hash-bucket state with a strong-hot score threshold; drop-key lookup is exact per `(user_key, sequence)` state and may only produce misses after bounded-cache eviction, never false hits. Cold routing is coarse negative evidence from two consecutive flush-miss epochs over routing buckets. See `third-party/terarkdb/util/hotness_tracker.h:23`, `third-party/terarkdb/util/hotness_tracker.h:137`, `third-party/terarkdb/util/hotness_tracker.h:172`, `third-party/terarkdb/util/hotness_tracker.h:218`, `third-party/terarkdb/util/hotness_tracker.h:262`, and `third-party/terarkdb/util/hotness_tracker.h:400`.

Hot/cold cluster preservation is split between flush and the GC picker, not done by switching outputs inside a GC job:

- Flush tags each large-blob vSST with an access-hotness `sst_type`: when `enable_hotness_tracker=true`, the cold route emits `kColdLargeBlob` (5) and the warm/hot routes emit `kWarmLargeBlob` (4); when it is off, large blobs keep the legacy `kLargeBlob` (1). Flush does not update the global tracker per key on a hot miss; it collects missed routing buckets locally and publishes them with `ApplyColdObservationBatch()` only after the flush finishes successfully. See `third-party/terarkdb/include/rocksdb/types.h:28`, `third-party/terarkdb/db/builder.cc:283`, `third-party/terarkdb/db/builder.cc:356`, `third-party/terarkdb/db/builder.cc:367`, and `third-party/terarkdb/db/builder.cc:690`.
- The GC picker keeps each job's inputs homogeneous in `sst_type` (`sst_type_group_enabled = middle_delta_enabled || enable_hotness_tracker`), so warm and cold large blobs are GC'd in separate jobs and the single replacement file inherits exactly one variant. See `third-party/terarkdb/db/compaction_picker.cc:893`, `third-party/terarkdb/db/compaction_picker.cc:1006`.
- A GC job keeps a single output owner (`ensure_gc_blob_output` opens one blob builder and does not switch routes), and the GC output inherits the input `sst_type` (warm-stays-warm, cold-stays-cold). This respects the one-to-one inheritance constraint in §5. See `third-party/terarkdb/db/compaction_job.cc:2447`, `third-party/terarkdb/db/compaction_job.cc:2384`, `third-party/terarkdb/db/compaction_job.cc:3644`.
- Optimized hotness runs may enable the tracker during load (`load_enable_hotness_tracker=true`), but the tracker does not learn from foreground load writes. Any useful load-phase routing signal must come from background compaction feedback that actually occurs during that phase; otherwise the default route remains warm until feedback/cold batches arrive. See `test-sh/new-ycsb/interface.sh:327`, `test-sh/new-ycsb/final_config.sh:27`, `test-sh/new-ycsb/terarkdb.sh:148`, and `third-party/terarkdb/util/hotness_tracker.h:137`.
- Hot/warm vSSTs use a stricter GC trigger when `hot_warm_blob_gc_max_files > 0`: they are normally selected only when entry- or byte-garbage reaches 100%, and they are excluded from small-file defragmentation picks. If hot/warm file count exceeds `hot_warm_blob_gc_max_files`, the picker falls back to the normal `blob_gc_ratio`; fallback remains active until the count drops below half the threshold. See `third-party/terarkdb/include/rocksdb/options.h:370`, `third-party/terarkdb/db/version_set.cc:1891`, `third-party/terarkdb/db/compaction_picker.cc:939`, `third-party/terarkdb/db/compaction_picker.cc:1184`.
- The policy is mutable and plumbed through both `db_bench` and KV_Bench_Env's TerarkDB adapter. The raw engine flag defaults to `0` (disabled), but `test-sh/new-ycsb/terarkdb_config.sh` computes a paper-run default from dataset size and target vSST size, and `final_config.sh` preserves that default unless overridden. See `third-party/terarkdb/tools/db_bench_tool.cc:1061`, `benchmark/kv_bench_tool.cc:1833`, `db/terarkdb/terarkdb.cc:244`, `test-sh/new-ycsb/terarkdb_config.sh:134`, `test-sh/new-ycsb/final_config.sh:48`.

`enable_hotness_tracker` is plumbed through `MutableCFOptions` so the picker can read it (`third-party/terarkdb/options/cf_options.h:251`, `third-party/terarkdb/options/cf_options.cc:331`). This routing is decoupled from `middle_delta_enabled`: it activates from `enable_hotness_tracker` alone, even when middle-value delta-separate is disabled (the current paper default).

Representative paths: sub-compaction drop aggregation at `third-party/terarkdb/db/compaction_iterator.cc:264`, compaction batch publication at `third-party/terarkdb/db/compaction_job.cc:1545`, flush cold-batch publication at `third-party/terarkdb/db/builder.cc:690`, and GC drop-key lookup shortcut at `third-party/terarkdb/db/compaction_job.cc:2520`.

GC-aware block cache boundary (`cache/garbage_aware_cache.*`):

- The optimization (mark a GC input file's vSST data blocks obsolete via `garbage_ratio=1.0` and `MoveToLRUTail`) runs after every Blob GC and is **independent of any diagnostic switch**. `db/compaction_job.cc` calls `MarkBlockCacheFilesObsolete` unconditionally for `kGarbageCollection`.
- The diagnostic residency sample (`LogBlockCacheObsoleteSample`, emits `[BLOCK_CACHE_OBSOLETE_SAMPLE]`) is the motivation-experiment instrumentation and is the only part gated by `block_cache_obsolete_tracking`. It full-scans the cache, so it must stay off the GC hot path when the option is off. Do not re-couple them: toggling the diagnostic must not change cache hit-rate behavior in ablation runs.
- Same-key overwrite correctness: `TableInsert` unlinks the replaced node before the new node is bookkept, so the overwrite path uses `RemoveReplacedFromCache` (no `TableRemove`) to avoid deleting the freshly inserted entry by key+hash. Strict-capacity rejection of a same-key insert restores the displaced entry toward the LRU head and keeps it queryable.

---

## 8. Paper wording guardrails

- Say `vSST` / ordered value SSTable, not generic unordered blob log.
- Say kSST stores a compact value index; vSST stores separated values.
- Say Blob GC is a special compaction over hidden-level value files.
- Say byte-precise GC uses reclaimable bytes, crucial for mixed-size values.
- Do not revive middle-value delta-separate or read-by-handle as current paper optimizations.
