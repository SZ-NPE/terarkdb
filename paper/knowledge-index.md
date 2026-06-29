# TerarkDB Paper Knowledge Index

This is the entry point for `third-party/terarkdb/paper/`. Keep this file as a map, not as a second copy of every mechanism or experiment detail.

Rule: if this knowledge base conflicts with current scripts/code/results, trust the current scripts/code/results and update the stale document.

Language rule: keep all knowledge-base documents and figure sketches in English.

---

## 1. Current canonical facts

- Paper runner: `test-sh/new-ycsb/run_paper_experiments.sh`.
- Motivation suite: `test-sh/new-ycsb/motivation.sh`, 5 cases mapped to M1-M5.
- Interface suite: `test-sh/new-ycsb/interface.sh`, 14 cases across `baseline`, `hotness`, `gc-cache`, `precise`, `full`, `baseline-rw`, and `full-rw`.
- All-on TerarkDB: `test-sh/new-ycsb/final_config.sh`.
- Mixed-value distribution: `uniform_fixed`; current M5 motivation path also uses key-correlated value size.
- Third paper optimization: byte-precise GC with `use_separated_value_meta_block` explicitly enabled; old middle-value delta-separate and read-by-handle are not part of the current paper path.
- Runner outputs summaries only; figures require a separate `plot_tools/` step.

---

## 2. Document map

| File | Role | Read when |
| --- | --- | --- |
| `paper-experiment-runner.md` | Runner modes, call chain, result/storage layout, suite artifacts | Running/debugging full paper experiments or inspecting `paper_full_*` |
| `experiment-result-analysis-handoff.md` | Finished-batch analysis checklist and plotting entry | Continuing analysis after a run |
| `motivation-test-figures.md` | M1-M5 case-to-figure semantics and diagnostic sources | Explaining/generating motivation figures |
| `terarkdb-kv-separation-and-gc.md` | Mechanism baseline: KV separation, vSST/kSST, Blob GC, precise GC | Explaining or changing engine/paper mechanism wording |
| `three-optimizations-summary.md` | Concise paper-facing narrative for the three optimizations | Writing contribution/design text |

SVG files in this directory are figure sketches only. They do not override current scripts, logs, or measured results.

---

## 3. Recommended reading order

- Mechanism/defaults: `terarkdb-kv-separation-and-gc.md` → current code paths → relevant scripts.
- Motivation figures: `motivation-test-figures.md` → SVG sketches → logs/result batch.
- Runner/result layout: `paper-experiment-runner.md` → `experiment-result-analysis-handoff.md` → current scripts.
- Paper wording: `three-optimizations-summary.md` → mechanism and figure docs for details.

Avoid inferring current truth from historical result-directory names. Always re-check current scripts.

---

## 4. What is intentionally not stored here

- Long dated run dumps and one-off troubleshooting logs.
- Machine-specific failures that do not change reusable conventions.
- Historical matrices that are no longer the default.
- Temporary cleanup notes.

If a batch snapshot is useful for handoff, summarize it compactly in `experiment-result-analysis-handoff.md` and mark it non-authoritative.

---

## 5. Maintenance rule

Before each paper-related task, read this index and then the task-specific document. After each task, decide whether a stable reusable fact changed and update the knowledge base if needed.

Each update must be:

- non-redundant: do not repeat the same fact across multiple documents unless it is an index-level pointer;
- clear: prefer exact paths, cases, and artifact names over vague prose;
- detailed enough to be reusable: include the stable rule, boundary, or data source, not one-off logs.

Update this index itself only when a stable entry point, document responsibility, suite shape, or current paper baseline changes.
