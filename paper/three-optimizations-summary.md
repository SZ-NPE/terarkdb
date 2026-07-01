# TerarkDB Three Current Paper Optimizations

This document keeps only the stable paper-facing narrative. Mechanism details live in `terarkdb-kv-separation-and-gc.md`; experiment mappings live in `motivation-test-figures.md` and `experiment-result-analysis-handoff.md`.

The current optimizations are:

1. **Hot/cold routing + drop-key-cache reverse-lookup acceleration**
2. **GC-aware block cache**
3. **Byte-precise GC over separated-value metadata**, namely `byte_precise_gc=true` with `use_separated_value_meta_block=true`

All three target the vSST/Blob-GC path introduced by TerarkDB KV separation, but they act at different points in that path.

---

## 1. Background problem

The current paper baseline uses TerarkDB KV separation: kSST stores keys plus value indexes, while vSST/Blob SST stores the real separated values. A vSST is an ordered SST-like file, not an unordered blob log.

The Blob/vSST GC path is: scan old vSSTs, reverse-lookup each record, discard dead values, rewrite live values, install new files, and retire old files.

The resulting bottlenecks are:

| Problem | Consequence | Optimization |
| --- | --- | --- |
| Hot and cold values are mixed, dispersing garbage | GC scans more vSSTs and performs more kSST reverse lookups | Hot/cold routing + drop-key cache |
| Obsolete blocks may remain in cache after GC/compaction | Old blocks pollute foreground read cache | GC-aware block cache |
| Entry ratio does not represent reclaimable bytes under mixed value sizes | The GC picker can choose low-benefit files | Byte-precise GC + separated-value metadata |

---

## 2. Optimization 1: Hot/cold routing + drop-key cache

### Target problem

Under Zipfian overwrite workloads, hot keys are overwritten frequently while cold keys remain live for a long time. If hot and cold values are written into the same vSSTs, many files become partially dirty: they contain useful garbage, but still retain enough live values to force expensive GC work.

This leads to:

- fewer clearly high-garbage files for the picker;
- high scan and reverse-lookup cost;
- contention with foreground CPU, I/O, cache, and background workers.

### Method

- Background compaction feedback identifies hot overwrite/drop patterns; foreground write-window learning is no longer part of the current mechanism.
- Routing heat is approximate hash-bucket state with a strong-hot threshold, while cold routing is coarse two-epoch miss evidence collected after successful flushes.
- Flush and compaction output route values by temperature so garbage becomes more concentrated.
- The drop-key cache records exact `(user_key, sequence)` pairs for compaction-confirmed dropped separated values so GC can skip part of the expensive reverse-lookup path without false positives.
- Cold vSSTs keep the normal `blob_gc_ratio` trigger. Hot/warm vSSTs are
  normally reclaimed only when their entry or byte garbage ratio reaches 100%,
  favoring whole-file deletion instead of relocating still-hot live values.
- The hot/warm policy has a single max-file threshold: if the number of
  hot/warm vSSTs exceeds `hot_warm_blob_gc_max_files`, they fall back to the
  normal `blob_gc_ratio` trigger; otherwise the 100%-garbage policy is used.
- Hot/warm vSSTs must not be selected for GC only because their file size is
  below the small-file defragmentation threshold.

### Boundary

- The benefit depends on clear hot/cold skew; uniform updates provide limited signal.
- It reduces inefficient GC work but does not remove GC.
- The routing heat surface is approximate and may collide by hash bucket; it is intentionally not a per-key hot LRU.
- The drop-key cache is not a complete version index; it accelerates only safely identifiable records and cache eviction may turn a potential hit into a safe miss.
- Tracking and cache maintenance are moved to background batch publication where possible, but the hotness interface ablation is still required to validate net benefit.
- The hot/warm 100%-garbage policy needs count-based fallback to avoid
  unbounded file-count or space-amplification growth when a small amount of live
  data remains in otherwise obsolete hot/warm vSSTs.

### Evidence form

- M1/M2/M3 show garbage distribution, GC I/O, and foreground disturbance.
- `hotness_base` vs `hotness_opt` must show the core expected benefits:
  more colored dropped-key records, shorter GC task duration, fewer GC read
  bytes, lower space amplification, and fewer reverse lookups.

---

## 3. Optimization 2: GC-aware block cache

### Target problem

After Blob GC or compaction, old files or blocks may no longer be useful to the current version. A normal block cache manages entries mostly by access history and can keep obsolete blocks long enough to evict useful foreground-read blocks.

### Method

The cache policy uses GC and file-obsolescence information to demote or evict obsolete or low-value blocks earlier, reducing post-GC cache pollution.

### Boundary

- It optimizes cache residency and read performance; it does not directly change GC reclaim bytes or reverse-lookup count.
- The benefit is easiest to observe under enough cache pressure and read-sensitive workloads.
- It depends on accurate obsolete-block identification. Conservative tracking reduces benefit; overly aggressive eviction can increase misses.

### Evidence form

- M4 shows obsolete block residency.
- `gc_cache_base` vs `gc_cache_opt` must show the core expected benefits:
  lower read latency, higher block-cache hit rate, and higher reverse-lookup
  hit rate.

---

## 4. Optimization 3: Byte-precise GC + separated-value metadata

### Target problem

Entry-based GC estimates benefit with:

```text
dead_entries / total_entries
```

This approximation works for fixed-size values, but not for mixed-size values. Many dead small entries may reclaim few bytes, while a small number of dead large entries may reclaim many bytes.

### Method

- `byte_precise_gc=true`: the GC picker uses obsolete bytes divided by accounting bytes.
- `use_separated_value_meta_block=true`: SST-side separated-value metadata supports byte-level accounting.

This is not the old `enable_delta_separate`, `middle_blob_size`, `middle_combine_level`, or `read_separated_value_by_handle` path.

### Boundary

- It mainly matters for mixed or variable-size values.
- It improves GC candidate selection; it does not remove scan, lookup, or rewrite cost once a file is selected.
- It depends on accurate byte-accounting metadata.

### Evidence form

- M5 shows divergence between entry ratio and byte ratio (entry ratio is a poor proxy for reclaimable bytes under mixed values).
- `precise_base` vs `precise_opt` must show the core expected benefits:
  fewer GC read bytes, higher throughput, and lower space amplification.

---

## 5. Pipeline placement

```text
Flush/compaction creates vSSTs
  -> Hot/cold routing concentrates future garbage

GC picker chooses vSSTs
  -> Byte-precise GC chooses by reclaimable bytes

GC scans vSSTs and performs reverse lookups
  -> Drop-key cache skips part of the lookup work

GC/compaction replaces old files
  -> GC-aware cache reduces obsolete block residency
```

---

## 6. Recommended paper wording

### Hot/cold routing + drop-key cache

> We observe that update skew mixes hot and cold values in the same value SSTables, dispersing garbage and forcing Blob GC to scan many files and perform expensive reverse lookups. We introduce hot/cold value routing with compaction feedback to concentrate garbage, and a drop-key cache to short-circuit reverse lookups for overwritten keys.

### GC-aware block cache

> Blob GC and compaction may leave obsolete blocks resident in the block cache. These blocks no longer contribute to foreground reads but still compete with useful blocks. We introduce a GC-aware cache policy that uses GC/file-obsolescence information to demote or evict obsolete blocks earlier.

### Byte-precise GC over separated-value metadata

> Entry-based garbage estimation is inaccurate for variable-size values because obsolete entry ratios do not reflect reclaimable bytes. We introduce byte-precise GC based on separated-value size metadata, enabling the picker to estimate reclaimable bytes and reduce GC I/O per reclaimed byte.

---

## 7. One-sentence summary

This work goes beyond basic KV separation by optimizing Blob GC on top of TerarkDB's ordered vSST design: hot/cold routing and drop-key cache reduce GC generation and lookup cost, GC-aware cache reduces foreground cache side effects, and byte-precise GC improves candidate selection under mixed-size values.
