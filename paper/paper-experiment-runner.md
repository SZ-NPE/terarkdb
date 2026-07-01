# Full Paper Experiment Runner Notes

This document records the stable behavior of `test-sh/new-ycsb/run_paper_experiments.sh`: call chain, modes, path layout, suite artifacts, and plotting boundary.

If it disagrees with current code, trust the code and update this file.

---

## 1. Entry, modes, and call chain

Top-level entry:

```bash
bash test-sh/new-ycsb/run_paper_experiments.sh [run|dryrun|list|summarize]
```

Modes are parsed in `test-sh/new-ycsb/run_paper_experiments.sh:50`. The wrapper runs two stages in order:

```text
run_paper_experiments.sh
├── motivation.sh   # compact M1-M5 motivation suite, TerarkDB only
└── interface.sh    # compact interface suite, RocksDB/BlobDB/TerarkDB cases
```

Relevant code:

- wrapper purpose and defaults: `test-sh/new-ycsb/run_paper_experiments.sh:14`
- motivation stage call: `test-sh/new-ycsb/run_paper_experiments.sh:133`
- interface stage call: `test-sh/new-ycsb/run_paper_experiments.sh:150`
- interface dispatch to `final_config.sh` for `terarkdb_full`: `test-sh/new-ycsb/interface.sh:184`

---

## 2. Preflight and top-level paths

For `run` mode, the wrapper checks `STORAGE_ROOT`, prints the plan, runs preflight checks unless `SKIP_PREFLIGHT_CHECKS=true`, creates top-level roots, and then runs motivation plus interface.

Current default paths:

```text
STORAGE_ROOT=$HOME/storage
RESULT_BASE=${WORKSPACE_HOME}/result/paper_full_${RUN_ID}
STORAGE_BASE=${STORAGE_ROOT}/paper_full_${RUN_ID}

${RESULT_BASE}/motivation
${RESULT_BASE}/interface
${STORAGE_BASE}/motivation_db/shared_db
${STORAGE_BASE}/interface_db/shared_db
${STORAGE_BASE}/motivation_backup/shared_backup
${STORAGE_BASE}/interface_backup/shared_backup
```

Relevant code:

- path defaults: `test-sh/new-ycsb/run_paper_experiments.sh:56`
- preflight behavior: `test-sh/new-ycsb/run_paper_experiments.sh:93`
- motivation env handoff: `test-sh/new-ycsb/run_paper_experiments.sh:138`
- interface env handoff: `test-sh/new-ycsb/run_paper_experiments.sh:155`

---

## 3. Suite matrices and defaults

### Motivation

Current cases from `test-sh/new-ycsb/motivation.sh:114` (5 cases):

```text
m1_garbage_cdf
m2_gc_io
m3_gc_timeline
m4_cache_residency
m5_byte_accounting
```

Key defaults:

- `DATASET_GB=100`, `THREADS=32`, `UPDATE_REPEAT=6`
- `CACHE_SIZE=1GiB`, `BLOB_GC_RATIO=0.10`, `TARGET_BLOB_FILE_SIZE=256MiB`
- M1-M4 run on the unoptimized baseline; M4 uses standard LRU + obsolete tracking
- fixed-value cases use 4KB values; mixed-value cases use `uniform_fixed` with 50% uniform 550B-800B values and 50% fixed 16KB values
- M5 enables `value_size_key_correlation=true`, `byte_precise_gc=true`, and `use_separated_value_meta_block=true` (these are the data prerequisite for the byte-ratio comparison, not an applied optimization)

Relevant code: `test-sh/new-ycsb/motivation.sh:60`, `test-sh/new-ycsb/motivation.sh:100`, `test-sh/new-ycsb/motivation.sh:318`.

Suite artifacts:

```text
motivation_suite.env
case_plan.txt
motivation_log_inventory.tsv
motivation_metrics.tsv
```

### Interface

Current cases from `test-sh/new-ycsb/interface.sh:140` (14 cases):

```text
baseline   : rocksdb_baseline, blobdb_baseline, terarkdb_baseline      (write-hotspot random write)
hotness    : hotness_base, hotness_opt
gc-cache   : gc_cache_base, gc_cache_opt
precise    : precise_base, precise_opt
full       : terarkdb_full                                            (write-hotspot random write)
baseline-rw: rocksdb_baseline_rw, blobdb_baseline_rw, terarkdb_baseline_rw (read/write-mixed)
full-rw    : terarkdb_full_rw                                         (read/write-mixed)
```

Key defaults:

- `DATASET_GB=100`, `THREADS=32`, `RUN_DURATION=600`, `UPDATE_REPEAT=3`
- `HOTNESS_WORKLOAD=overwrite-zipf1.2` (fixed value)
- `GC_CACHE_WORKLOAD=workloada` (fixed value, read/write-mixed)
- `MIX_WORKLOAD=overwrite-zipf1.2` (mixed value, for precise)
- `END2END_WRITE_WORKLOAD=overwrite-zipf1.2`, `END2END_RW_WORKLOAD=workloada` (fixed value, used by baseline/full and the `_rw` group)
- `CACHE_SIZE=1GiB`, `GC_AWARE_CACHE_SIZE=4GiB` (gc-cache and all end-to-end cases use the 4GiB cache)
- value distribution: only `precise_*` uses `uniform_fixed` mixed values (50% uniform 550B-800B values and 50% fixed 16KB values; byte-precise GC needs mixed sizes); all other cases use fixed 4KB values for faster runs
- baseline (rocksdb/blobdb/terarkdb_baseline) and full are aligned to the same workload + fixed value so end-to-end comparison is controlled
- TerarkDB wrappers default `hot_warm_blob_gc_max_files` to `ceil(dataset * 4 / target_blob_file_size)`; optimized hotness/full runs inherit it, while `0` disables the hot/warm 100%-garbage policy

Valid targets: `all|baseline|hotness|gc-cache|precise|full|baseline-rw|full-rw`.

Relevant code: `test-sh/new-ycsb/interface.sh:64`, `test-sh/new-ycsb/interface.sh:109`, `test-sh/new-ycsb/interface.sh:307`, `test-sh/new-ycsb/interface.sh:310`, `test-sh/new-ycsb/terarkdb_config.sh:134`.

#### Interface experiment purpose and expected outcome

| Case(s) | Workload / value | Purpose | Expected result |
| --- | --- | --- | --- |
| `hotness_base` vs `hotness_opt` | overwrite-zipf1.2 / fixed | Isolate hot/cold routing + drop-key reverse-lookup acceleration, including background compaction-feedback learning, flush-published cold observations, and hot/warm vSST 100%-garbage policy with file-count fallback | opt: lower space amplification and GC I/O; throughput/P99 must be checked because hotness maintenance can cost CPU on small devbox runs |
| `gc_cache_base` vs `gc_cache_opt` | workloada / fixed | Isolate GC-aware block cache under read/write-mixed load (writes trigger GC + obsolete blocks; reads expose the cache benefit) | opt: higher cache hit rate, fewer misses, lower read P99, higher throughput than base |
| `precise_base` vs `precise_opt` | overwrite-zipf1.2 / mixed | Isolate byte-precise GC + separated-value metadata under mixed-size values | opt: lower GC read total / write-amp per reclaimed byte, equal-or-better throughput than base |
| `terarkdb_baseline` vs `terarkdb_full` | overwrite-zipf1.2 / fixed | End-to-end gain of all 3 opts on write-hotspot random write | full: higher throughput, lower P99, lower GC total I/O and write-amp than stock TerarkDB |
| `terarkdb_baseline_rw` vs `terarkdb_full_rw` | workloada / fixed | End-to-end gain on read/write-mixed (where GC-aware cache read-latency benefit shows) | full_rw: higher throughput, lower read P99 than stock |
| rocksdb/blobdb/terarkdb_baseline (+ full) | overwrite-zipf1.2 / fixed | Cross-engine positioning under random write | TerarkDB+opt competitive vs RocksDB/BlobDB; system-level overview (engines differ, not a controlled ablation) |
| `*_rw` cross-engine set | workloada / fixed | Cross-engine positioning under read/write-mixed | same as above for the mixed load |

> Small-scale devbox run: override the heavy defaults, e.g.
> `DATASET_GB=10 RUN_DURATION=180 CACHE_SIZE=$((1024*1024*1024)) GC_AWARE_CACHE_SIZE=$((1024*1024*1024)) bash test-sh/new-ycsb/interface.sh run all`.
> Lower `GC_AWARE_CACHE_SIZE` so a 10GB dataset still exercises eviction; otherwise a 4GiB cache may hold most of the working set and hide the gc-cache benefit.

Suite artifacts:

```text
run_manifest.txt
case_plan.txt
summary.tsv
```

`summary.tsv` columns are written in `test-sh/new-ycsb/interface.sh:469`:

```text
group case workload ops_sec micros_per_op seconds operations read_p99_us read_p999_us write_p99_us db_size_mb raw_size_mb space_amp flush_gb compact_gb gc_rewrite_gb file
```

---

## 4. Case-level layout and lifecycle

Each case is written under `${RESULT_ROOT}/${figure_or_group}/${case}/` and includes:

```text
case.env
motivation_run.log or interface_run.log
info_log/
<DB_NAME>/
  <DB_NAME>_LOG/
  <DB_NAME>_STATS/
  out/insert.txt
  out/updatex*.txt
  out/*_kvbench_timechart_*.csv
  out/*_report.csv
out/*_log.csv
```

Standalone `rocksdb.sh`, `blobdb.sh`, `terarkdb.sh`, and `final_config.sh`
also default their output and DB info-log roots under `${WORKSPACE_HOME}/result/`
so ad-hoc runs do not create top-level log/output directories outside the result
tree.

The suite-level shared DB/backup directory is cleaned before each case, so cases are intended to be independent even though they reuse the same suite root.

Relevant code:

- motivation case setup: `test-sh/new-ycsb/motivation.sh:324`
- interface case setup: `test-sh/new-ycsb/interface.sh:381`
- runner output layout: `test-sh/new-ycsb/runner_common.sh:11`
- archive/move flow: `test-sh/new-ycsb/runner_common.sh:48`

---

## 5. Topology and resource assumptions

The runner is a single-physical-machine workflow. It does not set explicit `numactl` or `taskset` bindings.

Background worker budget used by motivation and interface:

```text
max_background_jobs=16
max_background_flushes=4
max_background_garbage_collections=4
effective compaction budget=8
```

Relevant code: `test-sh/new-ycsb/motivation.sh:76`, `test-sh/new-ycsb/interface.sh:77`.

---

## 6. Figure and plotting boundary

The runner summarizes but does not plot figures automatically.

Motivation figures:

- M1: vSST garbage-ratio distribution
- M2: Blob GC I/O breakdown
- M3: foreground timeline during GC
- M4: obsolete block-cache residency timeline
- M5: entry-ratio vs byte-ratio accounting

Interface figures (`plot_tools/plot_interface.py`, all driven by statistics tickers, no extra instrumentation):

- hotness / gc-cache / precise base-vs-opt ablation figures;
- end-to-end full-vs-baseline gain, two workloads (`*_write`, `*_rw`);
- cross-engine throughput/latency, two workloads (`*_write`, `*_rw`);
- figures whose cases are missing are skipped automatically.

With the one-command `plot_paper_figures.py` entry, both motivation and interface figures are written directly under the requested `--out-dir` (default: `${RESULT_BASE}/paper_figures`). If `plot_motivation_figures.py` or `plot_interface.py` is run directly without `--out-dir`, those individual plotters keep their own `paper_figures/motivation` or `paper_figures/interface` default subdirectories.

One-command plotting entry:

```bash
python3 plot_tools/plot_paper_figures.py \
  --result-root result/paper_full_<RUN_ID> \
  --out-dir result/paper_full_<RUN_ID>/paper_figures
```

Relevant code: `plot_tools/plot_paper_figures.py:31`, `plot_tools/plot_interface.py:453`, `plot_tools/plot_interface.py:468`.

---

## 7. Stable checklist

Before a run or script edit, confirm:

1. motivation has 5 cases (M1-M5) and interface has 14 cases (incl. `_rw` end-to-end/cross-engine group);
2. mixed-value experiments use `uniform_fixed` rather than Pareto;
3. current third optimization is byte-precise GC with `use_separated_value_meta_block` explicitly enabled;
4. runner summarizes only; plotting is separate;
5. result/storage roots follow the current wrapper defaults.

After a run or edit, update the knowledge base only if a stable convention changed: case matrix, path layout, artifact names, figure data source, mechanism boundary, or plotting entry.
