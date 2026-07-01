# Experiment Result Analysis Handoff

This is the concise handoff for analyzing a finished or partial full-paper batch. The script-local file `test-sh/new-ycsb/experiment_result_analysis_handoff.md` is only a compatibility pointer.

If this file conflicts with current code or an actual batch, trust the current code/batch and update this file.

---

## 1. Main command

Run from the `KV_Bench_Env` root:

```bash
nohup bash test-sh/new-ycsb/run_paper_experiments.sh run > paper_full_run.log 2>&1 &
tail -f paper_full_run.log
```

The wrapper runs:

1. `test-sh/new-ycsb/motivation.sh run all`
2. `test-sh/new-ycsb/interface.sh run all`

Relevant code: `test-sh/new-ycsb/run_paper_experiments.sh:133`, `test-sh/new-ycsb/run_paper_experiments.sh:150`.

---

## 2. First files to inspect

For a full result root `result/paper_full_<RUN_ID>`:

```text
paper_full_run.log
motivation/motivation_suite.env
motivation/case_plan.txt
motivation/motivation_log_inventory.tsv
motivation/motivation_metrics.tsv
motivation/**/case.env
motivation/**/out/updatex*.txt
interface/run_manifest.txt
interface/case_plan.txt
interface/summary.tsv        # may be absent in partial runs
interface/**/case.env
interface/**/out/updatex*.txt
```

Pull `info_log/` only after the high-level completeness check, or for figures that need detailed GC/cache diagnostics.

---

## 3. Completeness checks

Expected full-run status:

- wrapper log ends with `All requested paper experiments finished`;
- motivation has 5 cases plus `motivation_log_inventory.tsv` and `motivation_metrics.tsv`;
- interface has 14 cases and `summary.tsv`.

Current motivation cases (5):

```text
m1_garbage_cdf, m2_gc_io, m3_gc_timeline,
m4_cache_residency, m5_byte_accounting
```

Current interface cases (14):

```text
rocksdb_baseline, blobdb_baseline, terarkdb_baseline,
hotness_base, hotness_opt,
gc_cache_base, gc_cache_opt,
precise_base, precise_opt,
terarkdb_full,
rocksdb_baseline_rw, blobdb_baseline_rw, terarkdb_baseline_rw,
terarkdb_full_rw
```

Relevant code: `test-sh/new-ycsb/motivation.sh:114`, `test-sh/new-ycsb/interface.sh:140`.

---

## 4. Figure data map

| Figure family | Cases | Primary data source |
| --- | --- | --- |
| M1 garbage distribution | `m1_garbage_cdf` | `BLOB_GC_FILE_GARBAGE_STATS` (unoptimized baseline) |
| M2 GC I/O breakdown | `m2_gc_io` | `BLOB_GC_RECLAIM_STATS`, `BLOB_GC_BYTES` |
| M3 foreground timeline | `m3_gc_timeline` | timechart CSV + GC reclaim/latency logs |
| M4 obsolete cache residency | `m4_cache_residency` | `BLOCK_CACHE_OBSOLETE_SAMPLE` + timechart CSV |
| M5 entry vs byte accounting | `m5_byte_accounting` | `BLOB_GC_FILE_GARBAGE_STATS.entry_ratio/byte_ratio` |
| End-to-end (write-hotspot random write) | `terarkdb_baseline`, `terarkdb_full` (fixed value, `overwrite-zipf1.2`) | `summary.tsv`, GC/write-amp tickers |
| End-to-end (read/write-mixed) | `terarkdb_baseline_rw`, `terarkdb_full_rw` (fixed value, `workloada`) | `summary.tsv`, GC/write-amp tickers |
| Cross-engine (two workloads) | `rocksdb/blobdb/terarkdb_baseline(_rw)`, `terarkdb_full(_rw)` | `summary.tsv` ops_sec / chart P99 |
| Hotness ablation | `hotness_base`, `hotness_opt` | `summary.tsv`, update outputs, GC byte counters |
| GC-aware cache ablation | `gc_cache_base`, `gc_cache_opt` (fixed value, `workloada` read/write-mixed) | `summary.tsv`, cache hit/miss tickers |
| Precise-GC ablation | `precise_base`, `precise_opt` | `summary.tsv`, optional GC logs |

---

## 5. Plotting entry

The experiment runner does not generate figures. Use:

```bash
python3 plot_tools/plot_paper_figures.py \
  --result-root result/paper_full_<RUN_ID> \
  --out-dir result/paper_full_<RUN_ID>/paper_figures
```

The wrapper prefers `python3.13` when available. It calls the motivation plotter and the interface plotter, and writes all figures directly under the requested `--out-dir` (default: `result/paper_full_<RUN_ID>/paper_figures`). The individual plotters only use `paper_figures/motivation` or `paper_figures/interface` subdirectories when run directly without `--out-dir`. `plot_interface.py` renders Base-vs-Opt benefit figures for all three optimizations plus overall and cross-engine views for both write-hotspot and read/write-mixed workloads; figures whose cases are missing are skipped (printed as `[skipped]`) so partial interface results still produce whatever is available.

Interface benefit figures (all driven by statistics tickers in `out/updatex*.txt` + run-phase timechart CSV, no extra instrumentation, no foreground overhead):

| Figure | Cases | Key data |
| --- | --- | --- |
| Hotness key signals / GC I/O breakdown / timeline | `hotness_base`, `hotness_opt` | `ops_sec`, chart P99, `rocksdb.bytes.gc.*` |
| GC-aware cache signals / timeline | `gc_cache_base`, `gc_cache_opt` | `ops_sec`, chart P99, `block.cache.hit/miss` |
| Byte-precise GC signals / GC I/O breakdown | `precise_base`, `precise_opt` | `ops_sec`, chart P99, `bytes.gc.*`, write-amp from `compact/flush.write.bytes` |
| Overall gain | `terarkdb_baseline`, `terarkdb_full` | `ops_sec`, chart P99, GC total I/O, write-amp |
| Cross-engine | `rocksdb/blobdb/terarkdb_baseline`, `terarkdb_full` | `ops_sec`, chart P99 |

`plot_interface.py` accepts `--only <group...>` (`hotness gc-cache precise full full-rw cross cross-rw`) for single-group debugging. Run-phase timechart is auto-discovered by excluding the `load_` warm-up file, so it is workload-prefix agnostic.

Relevant code: `plot_tools/plot_paper_figures.py:13`, `plot_tools/plot_paper_figures.py:31`, `plot_tools/plot_interface.py:453`, `plot_tools/plot_interface.py:468`.

---

## 6. Current progress snapshot to remember

The current workspace contains these useful but non-authoritative devbox batches:

- `result/paper_full_smoke_1gb_20260630_221207`: complete 1GB/2-thread full-wrapper smoke run. It finished both stages, has 5 motivation cases, 14 interface cases, `motivation_metrics.tsv`, and `interface/summary.tsv`; use it to verify the current wrapper, compact case matrix, artifact layout, and plotting plumbing, not to judge optimization benefit.
- `result/interface_10g_t1_20260630`: complete 10GB/1-thread interface matrix with all 14 current cases and `summary.tsv`; useful for checking result layout, summaries, plotting, and qualitative direction, not for final paper numbers.
- `result/interface_10g_t1_rerun_20260630_105151`: 10GB/1-thread focused hotness/gc-cache/precise rerun with `summary.tsv`.
- `result/interface_10g_t1_precise_fixed_20260630_181215`: targeted precise-GC rerun after the mixed-value widening to 550B-800B plus 16KB.
- `result/interface_10g_t1_gccache_gc002_20260630_192500`: targeted gc-cache rerun with lower GC ratio; `result/interface_10g_t1_gccache_stress300_20260630_194700` is partial and was used for stress/debug only.

Do not copy numeric conclusions from these batches into the paper without rechecking the current logs and regenerated figures. The 1GB smoke run is a plumbing check only. Current 10GB/1-thread snapshots show the scripts and summaries working, but several optimization groups still need physical-machine validation: hotness/precise reduce space or write volume in some runs but may lose throughput on devbox scale, and gc-cache benefit is small unless the cache/working-set and GC pressure expose eviction.

---

## 7. End-to-end analysis and plotting workflow

After `run_paper_experiments.sh run` finishes, follow this exact sequence to go
from raw logs to the full paper figure set.

### 7.1 Locate and verify the batch
1. `RESULT_BASE = result/paper_full_<RUN_ID>` (from wrapper log / dir name).
2. Confirm completeness (§3): wrapper log tail, 5 motivation cases, 14 interface
   cases, `interface/summary.tsv` present.
3. Quick sanity counts (no figures yet):
   - `interface/summary.tsv` has 14 data rows; `ops_sec` columns are non-zero.
   - each case has `out/updatex3.txt` and one run-phase `*_kvbench_timechart_*.csv`.
   - motivation: `motivation_log_inventory.tsv` shows non-zero counts for the
     diagnostic family each case needs (M1/M5 → FILE_GARBAGE_STATS; M2 → RECLAIM
     + BYTES; M3 → RECLAIM + LATENCY + timechart; M4 → OBSOLETE_SAMPLE + timechart).

### 7.2 Generate all figures (one command)
```bash
python3 plot_tools/plot_paper_figures.py \
  --result-root result/paper_full_<RUN_ID> \
  --out-dir result/paper_full_<RUN_ID>/paper_figures
```
This calls both the motivation plotter (M1-M5) and the interface plotter
(ablation + end-to-end + cross-engine, both workloads). Missing cases are
skipped with `[skipped]`; check stdout for the rendered/skipped list.

### 7.3 Per-figure: data source, how it is read, expected shape

Motivation (problem evidence; one-command figures live directly under `paper_figures`):

| Fig | Read from | Parser | Expected reading |
| --- | --- | --- | --- |
| M1 garbage CDF | info_log `BLOB_GC_FILE_GARBAGE_STATS` | `parse_garbage_ratios` | wide spread of per-vSST garbage ratio (mixed-garbage files), not all near 1.0 |
| M2 GC I/O breakdown | `BLOB_GC_RECLAIM_STATS`+`BLOB_GC_BYTES` | `parse_reclaim` | reclaiming bytes costs several reads + relocation writes |
| M3 foreground timeline | timechart CSV + `BLOB_GC_LATENCY` | `read_timechart`/`parse_latency` | foreground P99 spikes coincide with GC bursts |
| M4 obsolete residency | `BLOCK_CACHE_OBSOLETE_SAMPLE` + timechart | `parse_obsolete_samples` | obsolete-byte ratio stays high over time (standard LRU) |
| M5 entry-vs-byte | `BLOB_GC_FILE_GARBAGE_STATS.entry_ratio/byte_ratio` | `parse_garbage_ratios` | scatter departs from y=x: entry-ratio is a poor proxy for byte-ratio |

Interface (benefit; one-command figures live directly under `paper_figures`, metrics from
statistics tickers in `out/updatex3.txt` + run timechart):

| Fig | Cases | Expected (opt vs base / full vs stock) |
| --- | --- | --- |
| hotness signals / GC I/O / timeline | hotness_base/opt | throughput↑, P99↓, GC read+relocation I/O↓ |
| gc-cache signals / timeline | gc_cache_base/opt | cache hit-rate↑, miss↓, read P99↓, throughput↑ |
| precise signals / GC I/O | precise_base/opt | GC read total↓, write-amp per reclaimed byte↓, throughput ≥ base |
| full-vs-baseline (write) | terarkdb_baseline/full | throughput↑, P99↓, GC total I/O↓, write-amp↓ |
| full-vs-baseline (rw) | terarkdb_baseline_rw/full_rw | throughput↑, read P99↓ (GC-aware cache shows here) |
| cross-engine (write / rw) | rocksdb/blobdb/terarkdb_baseline(_rw)+full(_rw) | TerarkDB+opt competitive; system-level overview |

### 7.4 If a figure does not match the expected shape
1. First confirm it is a data/config issue, not a plot bug: re-read the raw
   ticker/log values behind the figure (cite `path:line`).
2. Common root causes and the lever to pull:
   - gc-cache shows no benefit → cache too large for the working set; lower
     `GC_AWARE_CACHE_SIZE` so eviction actually happens.
   - precise entry-vs-byte not separated → values not all separated or sizes too
     uniform; ensure mixed range is all ≥ `blob_size` with a wide span
     (current: 50% uniform [550,800], 50% fixed 16KB).
   - too few GC events → raise write volume (`UPDATE_REPEAT`) or lower
     `BLOB_GC_RATIO`; confirm run actually entered the run phase, not just load.
   - timeline empty → run-phase timechart missing; check `timechart=true` and the
     non-`load_` CSV exists.
3. Adjust the minimal config, rerun only the affected group
   (`interface.sh run <group>` / `motivation.sh run <mN>`), regenerate figures,
   recheck. Do not hand-edit numbers.

### 7.5 Reporting
- Keep numeric conclusions in the analysis note/report, not in this knowledge base.
- Update this file only if a stable convention changed (case matrix, data source,
  plotting entry, path layout).
