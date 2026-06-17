# TerarkDB Plot Tools

这个目录存放 Blob GC / GC-aware block cache 实验的可视化脚本。脚本面向论文和技术报告图，默认输出 `pdf` 和 `png`，图形风格按 SIGMOD/FAST 常见双栏论文尺寸、色盲友好配色和低噪声网格配置。

## 脚本分工

| 脚本 | 用途 | 输入 | 典型输出 |
| --- | --- | --- | --- |
| `plot_motivation_tests.py` | 第一、第二点动机测试 | 两个可选 CSV：`--motivation1`、`--motivation2` | GC 无效读放大、可跳过比例、cache pressure 下 hit rate / throughput / p99、GC-aware cache 事件 |
| `plot_block_cache_obsolete_timeline.py` | 默认 LRU block cache 失效块动机测试 | TerarkDB INFO LOG | 失效块数量柱状图、compaction/GC 任务并发曲线 |
| `plot_final_validation.py` | 最终收益测试 | 一个 final validation CSV | 归一化吞吐收益、P99 改善、绝对尾延迟、读写/空间/GC 资源效率、cache hit rate |
| `gc_visualizer.py` | 已有 GC INFO_LOG 解析 | TerarkDB INFO_LOG | GC bandwidth、block invalidity、phase latency、timeline |

## 环境依赖

```bash
python3 -m pip install --user matplotlib numpy
```

脚本不依赖 pandas，便于在压测机上直接运行。

## 快速开始

先生成 CSV 模板：

```bash
cd /home/pengzhifeng.002/terarkdb/plot_tools

python3 plot_motivation_tests.py \
  --emit-template \
  --out ./templates

python3 plot_final_validation.py \
  --emit-template \
  --out ./templates
```

填完数据后出图：

```bash
python3 plot_motivation_tests.py \
  --motivation1 ./data/motivation1.csv \
  --motivation2 ./data/motivation2.csv \
  --out ./figures/motivation

python3 plot_final_validation.py \
  --input ./data/final_validation.csv \
  --baseline baseline \
  --out ./figures/final

python3 plot_block_cache_obsolete_timeline.py \
  /path/to/LOG \
  --out ./figures/obsolete_cache
```

每张图都会同时保存为：

```text
*.pdf
*.png
```

论文、slides、技术报告建议优先使用 PDF。

## 动机测试：默认 LRU 中失效块驻留

目标：证明默认 LRU block cache 在常态 overwrite/GC/compaction 运行中会长期驻留属于旧 file number 的 data blocks。一次 compaction 或 Blob GC install 成功后，输入文件的 file number 被标记为 obsolete；此时仍在 block cache 中的对应 data blocks 立即计入失效块。

推荐运行参数：

```bash
--use_gc_aware_block_cache=false
--block_cache_obsolete_tracking=true
--block_cache_obsolete_sample_interval_sec=10
--block_cache_obsolete_topk_files=10
```

INFO LOG 中会出现：

```text
[BLOCK_CACHE_OBSOLETE_EVENT] reason=compaction|gc ...
[BLOCK_CACHE_OBSOLETE_SAMPLE] obsolete_blocks=... obsolete_byte_ratio=...
[BLOCK_CACHE_OBSOLETE_DRAIN] file=... residency_us=...
```

主图 `block_cache_obsolete_blocks_with_job_concurrency` 的左轴是 `obsolete_blocks` 柱状图，表示当前 block cache 中属于旧 file number 的 resident data block 数量；右轴是从 `EVENT_LOG_v1` 的 `compaction_started` / `compaction_finished` 配对还原出的 compaction+GC 任务并发曲线。GC job 通过 tracker 的 `[BLOCK_CACHE_OBSOLETE_EVENT] reason=gc job=...` 识别，并额外画一条 GC 并发虚线。

## 动机测试 1：GC 读无效数据的代价

目标：证明 Blob GC 在 overwrite/zipf 场景下会读到大量已经无效的数据，且无效块比例越高，GC I/O 和耗时越明显。这组图适合回答“为什么需要感知垃圾比例/无效块分布”。

推荐实验变量：

| 变量 | 说明 |
| --- | --- |
| `garbage_ratio_pct` | 横轴，建议取 0、10、20、40、60、80 |
| `system` | `baseline`、`gc_aware` 或你的实现名 |
| `run` | 重复实验编号，脚本会聚合均值和标准误 |

CSV 列：

```csv
run,system,garbage_ratio_pct,live_read_mb,dead_read_mb,relocation_write_mb,skippable_ratio_pct,gc_time_sec
1,baseline,20,1024,420,180,18.2,11.4
1,gc_aware,20,840,210,160,18.2,8.7
```

输出图：

| 图 | 解释 |
| --- | --- |
| `motivation1_gc_io_breakdown` | live read、dead read、relocation write 的堆叠柱状图 |
| `motivation1_skippable_ratio` | 可跳过块比例随垃圾比例变化 |
| `motivation1_gc_time` | GC 时间随垃圾比例变化 |

数据来源建议：

- `gc_visualizer.py` 可先从 INFO_LOG 解析 `BLOB_GC_BLOCK_DIST` 和 GC bytes。
- 如果日志字段和模板不完全一致，先用简单脚本或手工汇总成上述 CSV，再用本脚本出图。

## 动机测试 2：GC 数据污染 block cache

目标：证明 GC 读入的数据块会影响正常读路径的 block cache 命中率和尾延迟，尤其在 cache size 较小或 working set 大于 cache 时更明显。这组图适合回答“为什么需要区分普通用户读和 GC/高垃圾比例读入的数据”。

推荐实验变量：

| 变量 | 说明 |
| --- | --- |
| `cache_size_gb` | 横轴，建议取 0.5、1、2、4、8 |
| `system` | `baseline`、`gc_aware` |
| `run` | 重复实验编号 |

CSV 列：

```csv
run,system,cache_size_gb,cache_hit_rate,throughput_ops,p99_us,gc_demote,gc_low_score_evict
1,baseline,1,0.61,43000,2400,0,0
1,gc_aware,1,0.72,51000,1700,12034,4201
```

输出图：

| 图 | 解释 |
| --- | --- |
| `motivation2_cache_hit_rate` | block cache hit rate 随 cache size 变化 |
| `motivation2_throughput` | 吞吐随 cache size 变化 |
| `motivation2_p99_latency` | P99 延迟随 cache size 变化 |
| `motivation2_gc_aware_events` | demote 和 low-score eviction 事件，说明策略确实在工作 |

## 最终收益测试

目标：在完整 workload 上对比 baseline 与优化版本，展示整体收益而不是单点现象。建议至少覆盖 `overwrite-zipf`，如果时间允许，再覆盖 YCSB A/B/C/F。

CSV 列：

```csv
run,workload,system,throughput_ops,p50_us,p99_us,p999_us,read_amp,write_amp,space_amp,cache_hit_rate,gc_time_sec,gc_bytes_mb,cpu_pct
1,overwrite-zipf,baseline,42000,80,2400,9100,1.00,3.20,1.28,0.61,120,40960,780
1,overwrite-zipf,gc_aware,51000,70,1700,6800,0.78,2.65,1.18,0.72,93,31500,760
```

必填列：

| 列 | 说明 |
| --- | --- |
| `run` | 重复实验编号 |
| `workload` | workload 名，如 `overwrite-zipf`、`workloada` |
| `system` | 系统名，默认 baseline 名为 `baseline` |

其他列可按你实际采集情况填写。脚本会跳过缺失指标。

输出图：

| 图 | 解释 |
| --- | --- |
| `final_normalized_throughput` | 相对 baseline 的吞吐 speedup |
| `final_normalized_p99` | 相对 baseline 的 P99 改善，数值越大表示原始 P99 越低 |
| `final_absolute_tail_latency` | P50/P99/P99.9 绝对值，log y-axis |
| `final_resource_efficiency` | read/write/space/gc bytes 相对 baseline 的效率改善 |
| `final_cache_hit_rate` | 各 workload 的 block cache hit rate |
| `final_validation_summary.csv` | 每个 workload/system/metric 的均值、标准误、baseline ratio |

如果 baseline 名不是 `baseline`，用：

```bash
python3 plot_final_validation.py \
  --input ./data/final_validation.csv \
  --baseline rocksdb \
  --out ./figures/final
```

## 从 kvbench 输出整理数据

`blob.sh` 会把 load/run 输出写到类似：

```text
blob_output/<DB_NAME>/out/updatex3.txt
```

常见指标来源：

| 指标 | 来源 |
| --- | --- |
| `throughput_ops` | db_bench/kvbench 输出里的 `ops/sec` |
| `p50_us`, `p99_us`, `p999_us` | histogram 输出 |
| `cache_hit_rate` | `rocksdb.block.cache.data.hit` 和 miss/read 计数计算 |
| `gc_time_sec` | INFO_LOG 中 GC duration 或 `BLOB_GC_LATENCY` 汇总 |
| `gc_bytes_mb` | `BLOB_GC_BYTES` 或 GC breakdown 汇总 |
| `read_amp`, `write_amp`, `space_amp` | stats、LOG 或外部脚本汇总 |

建议每次实验至少跑 3 次，并保留原始 `out/*.txt`、INFO_LOG 和最终 CSV。画图脚本只负责聚合和可视化，不替代原始数据归档。

## 推荐实验流程

1. 固定数据规模、value size、线程数、cache size、GC 参数。
2. 先跑 baseline，确认 `overwrite-zipf` 能稳定触发 Blob GC。
3. 跑动机测试 1：扫 `garbage_ratio_pct` 或通过不同 overwrite repeat 制造不同垃圾比例。
4. 跑动机测试 2：扫 `cache_size_gb`，观察 cache hit rate、throughput、P99。
5. 跑最终收益测试：固定推荐配置，对比 baseline 和优化版本，至少 3 次重复。
6. 把每次结果整理到 CSV，用本目录脚本出图。

## 图形风格约定

- 所有收益类图默认以 baseline 归一化，`1.0` 虚线表示 baseline。
- 对延迟、资源放大这类“越低越好”的指标，图中展示 improvement，即 `baseline / current`。
- 绝对延迟图使用 log y-axis，避免 P50 被 P99.9 压扁。
- 图标题保留在脚本里，投稿前可以按目标论文版式删除标题，只保留 caption。
