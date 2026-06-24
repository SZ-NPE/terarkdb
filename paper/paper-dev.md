# TerarkDB Optimized Paper Dev Note

日期：2026-06-23
当前状态：实验方案已收敛，物理机正式测试入口已准备好。
相关 benchmark 入口：`/home/pengzhifeng.002/KV_Bench_Env/test-sh/new-ycsb/paper_benchmark.sh`

---

## 0. 本文档的定位

本文档用于沉淀当前 TerarkDB optimized 的最新论文方案，包括：

1. 当前系统问题与论文故事线；
2. 最新优化方案的抽象与组件划分；
3. 每个优化项希望证明的机制；
4. 物理机实验设计与控制变量；
5. full matrix 与 ablation matrix；
6. 论文结构草案与写作重点。

本文档不是最终论文正文，而是写论文与跑实验时的开发版蓝图。最终论文可以以本文档为骨架进一步扩写、压缩和英文润色。

---

## 1. 论文一句话故事

> KV-separated LSM-tree 将大 value 从 compaction 路径中移走，但在更新密集场景下，它把后台维护瓶颈转移到了 Blob GC；本文提出一个跨越写入布局、版本演化和回收执行的 GC-aware 闭环，使系统能够主动塑造更易回收的 Blob，在线物化失效信息，并用更低带宽代价执行 Blob GC，从而提升 TerarkDB 在 skewed overwrite 负载下的吞吐与后台效率。

更短的版本：

> We turn Blob GC from a reactive cleaning task into a feedback-driven lifecycle-aware data management loop.

---

## 2. 论文核心问题

### 2.1 背景

LSM-tree 通过顺序写和后台 compaction 获得高写入吞吐，但大 value 会在 compaction 中被反复搬运，导致严重写放大和后台 I/O 消耗。KV separation 通过将 value 存入独立 Blob 文件、在 LSM 中仅保存引用，显著降低了大 value 随 compaction 被重复重写的成本。

但是，KV separation 并没有消除后台维护成本，而是将其从 LSM compaction 转移到了 Blob GC：

- 更新会持续产生旧 value；
- 旧 value 仍驻留在 Blob 文件中；
- 系统需要周期性识别、迁移仍存活 value，并删除垃圾 Blob；
- GC 期间的扫描、验证、迁移会与前台请求争用 I/O、CPU 和 cache。

### 2.2 当前系统的根本低效

现有 KV-separated LSM 中，Blob GC 通常是一个事后清理过程。其低效不只是因为“有垃圾”，而是因为以下三件事情彼此脱节：

1. **垃圾如何形成**：写入路径通常只追求写入吞吐，不主动塑造不同生命周期 value 的物理布局，导致热点更新 value 与冷 value 混写在同一批 Blob 中。
2. **谁已经失效**：旧 value 的失效事实往往要等到 GC 时通过扫描 Blob 并回查 LSM 才能重新确认，GC 被迫承担大量有效性验证开销。
3. **如何回收最便宜**：Blob 的整体垃圾比例不足以描述活数据空间分布，统一扫描-重写式 GC 容易产生额外无效 I/O。

### 2.3 本文主张

本文主张：Blob GC 不应该被视为一个孤立后台任务，而应该被视为贯穿 **写入路径 -> 版本演化 -> 后台回收** 的闭环协同问题。

对应地，系统需要同时回答：

- 写入阶段如何让未来会快速失效的数据更集中？
- 版本演化阶段如何把对象失效信息在线物化，而不是 GC 时临时重建？
- GC 阶段如何利用布局与失效信息选择更低带宽的回收路径？

---

## 3. 最新方案总览

当前 TerarkDB optimized 方案由四类机制组成，其中 fixed-value 主路径和 mixed-value/Pareto 路径分开评估。

### 3.1 Fixed-value 主路径优化

固定 4KB value、overwrite-zipf 负载下，主优化路径包括：

1. **Hotness Tracker / hot-cold routing**
   - 识别短期内反复覆盖的热点 key；
   - 将更可能快速失效的 value 聚集到更适合回收的 Blob 类别中；
   - 利用 compaction feedback 和 drop-key cache 复用版本演化中产生的失效信息。

2. **Delta Separate / middle blob routing**
   - 通过 `middle_blob_size=8192` 和 `middle_combine_level=2` 调整中等大小 value 的分离与合并策略；
   - 减少不必要的大 value 搬运，并让更新路径下的 Blob 布局更符合后续 GC 的收益模型。

3. **GC-aware Block Cache**
   - 在 Blob GC/compaction 相关读路径上减少无收益数据对 cache 的污染；
   - 通过 admission/demotion 策略降低后台维护任务对前台 cache 命中率的干扰。

4. **Read separated value by handle**
   - 读取分离 value 时尽量利用 handle 路径，降低额外解析和查找开销；
   - 作为 fixed-value 主路径中的低风险性能增强项。

5. **Flush 参数统一调优**
   - devbox 验证表明 optimized 路径容易被 flush backpressure 掩盖；
   - 因此正式实验统一使用更充足的 memtable 和 flush worker 配置，避免优化收益被“3 immutable memtables waiting for flush”类 stall 吞没。

### 3.2 Mixed-value / Pareto 路径优化

`precise_gc` 和 `use_delta_block` 不纳入 fixed-value 主路径，而是单独在 Pareto value-size 消融中验证。

原因：

- fixed 4KB value 下，entry-based garbage ratio 与 byte-based garbage ratio 基本一致；
- `precise_gc` 的核心收益来自 value size 不均匀时，entry 数比例无法准确代表失效字节比例；
- `use_delta_block` 更适合证明 byte-level GC 精度和块级有效性表达带来的收益。

因此，本文将其作为独立机制验证：

```bash
pareto_precise_base:       precise_gc=false, use_delta_block=false
pareto_delta_block_only:   precise_gc=false, use_delta_block=true
pareto_precise_delta:      precise_gc=true,  use_delta_block=true
```

---

## 4. 当前实现配置

### 4.1 TerarkDB optimized fixed-value 配置

正式 optimized 配置位于：

```text
test-sh/new-ycsb/final_config.sh
```

核心开启项：

```bash
enable_hotness_tracker=true
hotness_window_capacity=128MB
hotness_hot_capacity=512MB
hotness_enable_write_window=true
hotness_enable_compaction_feedback=true
hotness_enable_drop_key_cache=true

use_gc_aware_block_cache=true
gc_aware_cache_admission_ratio=0.7
gc_aware_cache_demote_score_threshold=0.05
gc_aware_cache_log_interval=10000
enable_gc_aware_cache_aging=true
gc_aware_cache_aging_interval=10000

read_separated_value_by_handle=true

enable_delta_separate=true
middle_blob_size=8192
middle_combine_level=2

precise_gc=false
use_delta_block=false
```

### 4.2 统一控制变量

正式实验中，四组 full matrix 使用统一控制变量：

```bash
VALUE_SIZE=4096
VALUE_SIZE_DISTRIBUTION_TYPE=fixed
YCSB_RUN_WORKLOAD=overwrite-zipf
UPDATE_REPEAT=20
RUN_DURATION=600
THREADS=1
CACHE_SIZE=1GB

BLOB_THRESHOLD=512
BLOB_FILE_SIZE=512MB
BLOB_GC_RATIO=0.2
BLOBDB_GC_AGE_CUTOFF=0.25

WRITE_BUFFER_SIZE=128MB
MAX_WRITE_BUFFER_NUMBER=8
MAX_BACKGROUND_FLUSHES=4
MAX_BACKGROUND_COMPACTIONS=12      # RocksDB/BlobDB flush+compaction total = 16
MAX_BACKGROUND_GARBAGE_COLLECTIONS=4
MAX_BACKGROUND_JOBS=16             # TerarkDB uses 4 flush + 8 compaction + 4 GC = 16 total background workers
```

控制变量传递路径：

- `paper_benchmark.sh` 定义统一参数；
- full matrix 通过 `interface.sh` 分发到 RocksDB、BlobDB、TerarkDB baseline 和 TerarkDB optimized；
- ablation matrix 直接通过 `run_ablation_case` 分发到 `terarkdb.sh`。

---

## 5. 实验设计

### 5.1 为什么选择 overwrite-zipf

正式主实验选择：

```bash
YCSB_RUN_WORKLOAD=overwrite-zipf
VALUE_SIZE=4096
VALUE_SIZE_DISTRIBUTION_TYPE=fixed
UPDATE_REPEAT=20
```

理由：

1. `overwrite-zipf` 能产生稳定热点 key，最容易触发 Hotness Tracker。
2. skewed overwrite 会让旧 value 快速失效，适合展示 Blob GC feedback 与 drop-key cache 的价值。
3. `updatex20` 让系统经历足够多轮更新、flush、compaction 和 GC，更容易形成稳定的 Blob 生命周期现象。
4. 4KB fixed value 足够大，能稳定进入 value separation 路径；同时又能避免 mixed-value 统计误差干扰 fixed-path 主结论。

### 5.2 Full Matrix

默认 full matrix：

```bash
FULL_CASES="rocksdb_100gb blobdb_100gb terarkdb_100gb terarkdb_opt_100gb"
```

实验组：

| Case | Engine | 作用 |
|---|---|---|
| `rocksdb_100gb` | RocksDB | 传统 LSM baseline |
| `blobdb_100gb` | Integrated BlobDB | RocksDB KV-separated baseline |
| `terarkdb_100gb` | TerarkDB baseline | 当前系统 baseline |
| `terarkdb_opt_100gb` | TerarkDB optimized | 本文方案 |

需要回答的问题：

1. TerarkDB baseline 与 RocksDB/BlobDB 的相对表现如何？
2. 本文优化是否能在同等参数下提升 TerarkDB？
3. 优化后的 TerarkDB 是否在 skewed overwrite 场景下展现更好的吞吐和后台效率？

### 5.3 Ablation Matrix

默认 ablation matrix：

```bash
ABLATION_CASES="baseline_fixed all_on_fixed no_hotness_tracker no_gc_aware_cache no_delta_separate no_read_handle pareto_precise_base pareto_delta_block_only pareto_precise_delta"
```

固定 value 主路径消融：

| Case | 目的 |
|---|---|
| `baseline_fixed` | TerarkDB baseline fixed-value 路径 |
| `all_on_fixed` | fixed-value 全量优化 |
| `no_hotness_tracker` | 衡量 Hotness Tracker / feedback 的贡献 |
| `no_gc_aware_cache` | 衡量 GC-aware cache 的贡献 |
| `no_delta_separate` | 衡量 delta separate / middle routing 的贡献 |
| `no_read_handle` | 衡量 handle-based separated-value read 的贡献 |

Pareto value-size 消融：

| Case | 目的 |
|---|---|
| `pareto_precise_base` | mixed-value 下 entry-based GC baseline |
| `pareto_delta_block_only` | mixed-value 下只开启 delta metadata，用于隔离元数据开销 |
| `pareto_precise_delta` | mixed-value 下 byte-accurate precise GC + delta block |

### 5.4 物理机正式命令

推荐正式运行命令：

```bash
cd /path/to/KV_Bench_Env/test-sh/new-ycsb

PAPER_PRESET=physical \
MODE=all \
REPEAT_COUNT=3 \
SKIP_BACKUP=true \
CLEAN_DB_AFTER_CASE=true \
DB_ROOT=/data0/kvbench_paper_db \
BACKUP_ROOT=/data1/kvbench_paper_backup \
RESULT_ROOT=/data0/kvbench_results/paper_$(date +%Y%m%d_%H%M%S) \
bash ./paper_benchmark.sh
```

结果输出：

```text
${RESULT_ROOT}/summary.tsv
${RESULT_ROOT}/run_manifest.md
${RESULT_ROOT}/full_runN/<case>/
${RESULT_ROOT}/ablation_runN/<case>/
```

---

## 6. Devbox 验证记录

### 6.1 Smoke full matrix

结果文件：

```text
/home/pengzhifeng.002/KV_Bench_Env/experiments/paper_smoke_full_20260623_161758/summary.tsv
```

结果：

| Case | ops/s |
|---|---:|
| `rocksdb_100gb` | 29943 |
| `blobdb_100gb` | 59297 |
| `terarkdb_100gb` | 49816 |
| `terarkdb_opt_100gb` | 53380 |

TerarkDB optimized 相对 TerarkDB baseline：

```text
(53380 - 49816) / 49816 = 7.15%
```

该结果主要用于验证脚本链路和 summary 解析，不作为最终论文结论。

### 6.2 1GB ablation probe

结果文件：

```text
/home/pengzhifeng.002/KV_Bench_Env/experiments/control_probe_1gb_20260623_160513/summary.tsv
```

结果：

| Case | ops/s |
|---|---:|
| `baseline_fixed` | 47798 |
| `all_on_fixed` | 51053 |

收益：

```text
(51053 - 47798) / 47798 = 6.81%
```

### 6.3 2GB / 120s ablation probe

结果文件：

```text
/home/pengzhifeng.002/KV_Bench_Env/experiments/control_probe_2gb_20260623_160746/summary.tsv
```

结果：

| Case | ops/s |
|---|---:|
| `baseline_fixed` | 41646 |
| `all_on_fixed` | 39858 |

收益：

```text
(39858 - 41646) / 41646 = -4.29%
```

解释：devbox 上结果波动大，小数据集和短运行时长不足以稳定展现 Blob 生命周期优势；2GB / 120s 下 all-on 仍受到 stall 影响。正式结论应以物理机 100GB / 600s / updatex20 / repeat=3 的结果为准。

---

## 7. 论文故事线草案

### 7.1 标题方向

候选标题：

1. **Closing the Loop for Blob Garbage Collection in KV-Separated LSM Stores**
2. **Feedback-Driven Blob Garbage Collection for KV-Separated LSM Trees**
3. **Lifecycle-Aware Blob Management for Update-Intensive KV-Separated LSM Stores**
4. **From Reactive Cleaning to Feedback-Driven Blob Management in TerarkDB**

建议优先使用第 1 或第 2 个方向。第 1 个更强调设计哲学，第 2 个更直接表达系统机制。

### 7.2 Abstract 故事线

摘要可以按以下逻辑写：

1. LSM-tree 在大 value 下有高 compaction 写放大；KV separation 缓解了该问题。
2. 但 KV separation 将维护成本转移到 Blob GC，更新密集负载下 GC 会持续占用后台带宽并干扰前台请求。
3. 现有 Blob GC 是 reactive 的：垃圾混合形成、失效信息 GC 时重建、回收执行路径单一。
4. 本文提出 feedback-driven closed-loop Blob management：写路径塑形、版本演化失效物化、GC-aware 执行。
5. 在 TerarkDB 中实现，并通过 full matrix 和 ablation 证明吞吐、GC 带宽、stall、尾延迟等指标改善。

### 7.3 Introduction 主线

Introduction 建议分 7 段：

1. **LSM 与大 value 问题**：LSM 适合写密集，但大 value compaction 代价高。
2. **KV separation 的价值与代价转移**：value 不再随 compaction 重写，但旧 value 在 Blob 中积累，需要 GC。
3. **更新密集场景放大 Blob GC 问题**：overwrite / state refresh / online feature 等场景会持续产生旧 value。
4. **现有 GC 的低效根因**：垃圾形成、失效识别、回收执行三者脱节。
5. **关键观察**：GC 成本不只由垃圾比例决定，还由垃圾是否集中、失效信息是否已物化、活数据空间分布决定。
6. **本文方案**：closed-loop / feedback-driven Blob management。
7. **贡献和实验结论**：在 TerarkDB 上实现，full matrix 与 ablation 证明收益。

### 7.4 Motivation 图表建议

建议准备 3 个 motivation 图：

1. **GC 与前台吞吐竞争图**
   - x 轴：时间；
   - y 轴：foreground ops/s、GC read/write MB/s、stall time；
   - 展示 baseline 中 GC/compaction 活跃时前台吞吐下跌。

2. **Blob 垃圾形成不集中图**
   - 展示 baseline 下多个 Blob 的 garbage ratio 分布；
   - 说明许多 Blob 处于中间状态，GC 回收收益不稳定。

3. **entry-based vs byte-based GC 误差图**
   - 用 Pareto value-size；
   - 展示 entry garbage ratio 与 byte garbage ratio 的偏差；
   - 引出 `precise_gc`。

### 7.5 Design 章节组织

建议 Design 分为四节：

#### 7.5.1 Design Overview

提出闭环模型：

```text
Write-path shaping -> Invalidation materialization -> GC-aware execution
```

强调三个环节不是独立 patch，而是共同作用于 Blob 生命周期。

#### 7.5.2 Hotness-guided Blob Layout

讲 Hotness Tracker：

- 使用 write window 捕捉短期重复更新；
- 使用 hot table 保存热点集合；
- 使用 compaction feedback/drop-key feedback 修正热点判断；
- 将更可能快速死亡的 value 写入特定 Blob 类别，提升垃圾收敛速度。

#### 7.5.3 Invalidation Feedback and GC-aware Cache

讲版本演化中的失效信息：

- compaction/drop old key 时产生失效信号；
- drop-key cache 降低 GC 反查成本；
- GC-aware block cache 减少后台 GC/compaction 数据污染前台 cache。

#### 7.5.4 Precise GC for Heterogeneous Values

讲 Pareto/mixed-value 场景：

- entry-based garbage ratio 在 value size 不均匀时不准确；
- precise byte statistics 更接近真实可回收带宽收益；
- delta block 支撑更细粒度空间表达。

### 7.6 Evaluation 章节组织

建议 Evaluation 回答 5 个问题：

1. **Overall performance**：TerarkDB optimized 相对 TerarkDB baseline、RocksDB、BlobDB 的吞吐提升。
2. **Ablation**：各组件分别贡献多少。
3. **GC efficiency**：GC read/write bytes、GC count、有效迁移比例、空间回收效率。
4. **Interference**：前台 stall time、p99/p99.9 latency、block cache hit/miss。
5. **Sensitivity**：不同 update repeat、不同 value size、不同 Zipf skew、不同 cache size。

### 7.7 FAST 风格强化版故事线

下面这一节以 FAST/OSDI/SOSP 系统论文的审稿口味来重构故事。FAST 更喜欢的不是“我做了几个优化开关”，而是一个清晰的系统性问题、一条不可避免的设计逻辑、一个最小但有力的抽象，以及足够扎实的实验闭环。因此，本文不能被包装成“TerarkDB 参数调优 + 若干工程 patch”，而应该被讲成：**KV-separated LSM 的 Blob GC 生命周期管理缺少反馈闭环，而我们把 GC 从事后清理重构为跨写入、版本演化和回收执行的在线协同机制。**

#### 7.7.1 FAST 风格的核心立论

系统论文的强故事通常需要回答四个问题：

1. **Why now?** 为什么这个问题现在重要？
2. **Why hard?** 为什么现有系统没有自然解决它？
3. **What is the key insight?** 你的关键观察是否足够简单、深刻、可泛化？
4. **Why this design?** 你的设计是否是从观察自然推出，而不是堆功能？

本文可以这样回答：

- **Why now**：现代 KV workloads 越来越多地存储大状态对象、特征对象、缓存对象和在线服务状态。它们不是纯 append，也不是一次写入后长期只读，而是持续 overwrite。KV separation 已经成为处理大 value 的自然选择，但 update-intensive 场景让 Blob GC 从偶发后台任务变成持续性带宽竞争者。
- **Why hard**：Blob GC 的低效不是单个阈值、单个 cache 策略或单个 GC 算法能解决的。垃圾形成在写入阶段，失效语义在 compaction/version 演化阶段确定，而回收代价在 GC 阶段支付。现有系统把这三个阶段拆开处理，所以 GC 只能被动地扫描、验证、搬迁。
- **Key insight**：Blob GC 的成本由三件事共同决定：垃圾是否集中形成、失效信息是否已经被物化、活数据是否以适合低成本搬迁的空间形态存在。单纯的 garbage ratio 只是一个粗糙信号，不能解释真实带宽代价。
- **Design**：因此，我们构建一个 feedback-driven loop：写入阶段做 lifetime-aware layout shaping，version 演化阶段 materialize invalidation，GC 阶段用这些在线信息进行更友好的 cache/admission 和回收决策。

这条线的关键是：**不是先有 hotness tracker、GC-aware cache、precise GC，然后把它们拼起来；而是先有“Blob 生命周期闭环”这个系统抽象，然后这些机制分别落在闭环的不同位置。**

#### 7.7.2 论文应该避免的弱表述

为了更像 FAST 顶会论文，需要避免以下叙述方式：

1. **不要说**：“我们为 TerarkDB 加了 Hotness Tracker、GC-aware block cache、precise GC 等优化。”
   **应该说**：“我们发现 Blob GC 低效来自生命周期信号断裂，因此设计了一个 feedback loop。Hotness tracking、cache admission 和 precise accounting 是这个 loop 的三个执行点。”

2. **不要说**：“我们选择 overwrite-zipf 是因为它对我们有利。”
   **应该说**：“overwrite-zipf 代表 update-intensive state workloads 中常见的 skewed refresh 行为，能够稳定暴露 Blob GC 与 foreground interference 的系统瓶颈。”

3. **不要说**：“我们通过调大 write buffer 获得收益。”
   **应该说**：“为了隔离 GC-aware 机制本身的效果，我们统一配置 flush/compaction 并消除明显的 memtable backpressure，使不同系统在相同资源预算下比较。”

4. **不要说**：“precise GC 是另一个优化项。”
   **应该说**：“precise GC 证明了本文观点的另一侧：当 value size heterogeneous 时，entry-level invalidation view 不足以代表真实 reclaimable bytes，GC 决策需要 byte-level visibility。”

5. **不要说**：“BlobDB、RocksDB 只是 baseline。”
   **应该说**：“RocksDB 表示 non-separated LSM 的大 value 搬运成本，BlobDB 表示工业界 KV separation 的通用路径，TerarkDB baseline 表示目标系统的现有 KV-separated 实现。三者共同界定了本文方案的设计空间。”

#### 7.7.3 推荐的整篇论文叙事节奏

整篇文章建议采用一个递进式叙事：

**第一幕：KV separation 的成功与副作用。**
LSM-tree 原本的问题是 compaction 搬运大 value。KV separation 成功地把大 value 从 compaction 中移走，但这不是“免费午餐”。旧 value 没有消失，只是从 SST compaction 问题变成 Blob lifecycle management 问题。对于 append-heavy workloads，这个问题可能不严重；但对 overwrite-heavy workloads，Blob GC 会变成持续运行的后台带宽消费者。

**第二幕：Blob GC 的真正问题不是垃圾多，而是信息来得太晚。**
传统 GC 在回收时才去理解 Blob：扫描文件、解析对象、回查 LSM、判断对象是否仍活着。这个流程的问题是，GC 试图在最后一刻重建本应在系统运行过程中自然产生的信息。实际上，写入路径已经暴露了对象是否可能很快更新，compaction 已经知道旧版本何时被丢弃，cache 已经能观察哪些后台读不应该污染前台工作集。现有系统没有把这些信息串起来。

**第三幕：提出 Blob lifecycle feedback loop。**
本文的核心设计不是“更聪明的 GC 选择器”，而是把 Blob 的生命周期管理提前并分摊到系统自然执行的路径中：写入时塑形，compaction 时反馈，GC 时消费。这样 GC 不再是一个盲目的清扫器，而是一个消费在线物化信息的执行器。

**第四幕：设计落地在 TerarkDB。**
我们不重写 TerarkDB，而是在现有写入、compaction、Blob GC 和 cache 框架中插入轻量机制。这样强调系统论文常见的工程可信度：设计不是 toy prototype，而是落在真实 KV-separated LSM 上。

**第五幕：实验闭环证明。**
实验不是只看吞吐，而要证明整个 loop 的每个环节都在起作用：

- overall：optimized 比 baseline 更快；
- ablation：去掉每个环节会退化；
- GC metrics：后台读写、stall、GC rewrite 下降；
- interference：前台 tail latency 或 throughput dip 改善；
- sensitivity：在 update intensity、skew、value-size heterogeneity 变化下解释何时收益最大。

#### 7.7.4 可以写进 Introduction 的强版本逻辑

Introduction 可以采用下面的中文逻辑，后续再翻译成英文：

1. **LSM-tree 的基本矛盾**：写优化存储系统通过顺序写获得高吞吐，但把维护复杂性推给后台 compaction。
2. **大 value 放大这个矛盾**：当 value 较大时，compaction 搬运的不再只是元数据，而是大量用户 payload。
3. **KV separation 的突破**：把 value 移出主 LSM，让 compaction 只处理 key 和 pointer。
4. **代价转移**：旧 value 的空间回收仍然需要 Blob GC，尤其 update-heavy workload 下，GC 不再是低频任务。
5. **现有 GC 的盲点**：现有系统把 GC 放在生命周期末端，导致所有关于对象寿命、版本失效和空间分布的信息都需要在回收时重新发现。
6. **核心观察**：低成本 GC 需要三个条件同时成立：垃圾集中、失效可见、活数据空间结构可利用。
7. **本文方案**：一个 feedback-driven loop，将写入阶段、compaction 阶段和 GC 阶段连接起来。
8. **系统实现**：在 TerarkDB 中实现，无需引入独立重型索引或全新存储格式。
9. **实验承诺**：通过 full matrix 和 ablation 证明吞吐、后台 GC 效率和前台干扰均得到改善。

#### 7.7.5 FAST 风格的 Problem Statement

可以把 problem statement 写成下面这种形式：

> In a KV-separated LSM-tree, Blob GC is not merely a file cleaning problem. It is a delayed information problem. The system observes update locality during writes and learns version invalidation during compaction, but the GC path traditionally consumes neither signal. Consequently, GC must rediscover object liveness by scanning Blob files and consulting the main LSM-tree, while reclaiming files whose layout was never optimized for future cleaning. This paper asks whether Blob GC can be made bandwidth-efficient by turning these latent signals into an online feedback loop.

中文解释：

> 在 KV-separated LSM 中，Blob GC 不是单纯的文件清理问题，而是一个“信息延迟”问题。系统在写入时看到了更新局部性，在 compaction 时知道了版本失效，但传统 GC 路径没有消费这些信号。因此，GC 必须在最后一刻重新扫描 Blob、回查主树，并处理一批从未为未来回收优化过的文件。本文要回答的是：能否把这些潜在信号转化为在线反馈闭环，从而让 Blob GC 变得带宽高效？

这个 statement 很适合 FAST 风格，因为它把问题从工程现象提升为系统抽象：**delayed information problem**。

#### 7.7.6 FAST 风格的 Key Insight

建议把 key insight 写成一个非常明确、可引用的段落：

> The cost of Blob GC is determined before GC starts. By the time a Blob file is selected for cleaning, the system has already decided which objects were co-located, which old versions were invalidated, and how live bytes are distributed inside the file. A GC algorithm that only reacts at cleaning time can optimize only the last step. To reduce bandwidth fundamentally, the system must shape Blob files when data is written and materialize invalidation when versions evolve.

中文解释：

> Blob GC 的成本在 GC 开始之前就已经被决定了。当某个 Blob 被选为候选时，哪些对象被放在一起、哪些旧版本已经失效、活数据在文件中如何分布，这些事实都已经形成。一个只在回收时反应的 GC 算法最多只能优化最后一步。若要从根本上降低带宽，系统必须在写入时塑造 Blob，在版本演化时物化失效。

这句话可以作为论文的核心洞察，甚至可以放在 Introduction 的转折段。

#### 7.7.7 FAST 风格的 Design Principle

建议将设计原则写为三个 P：

1. **Predict where garbage will form, but only approximately.**
   不追求精确 lifetime prediction，而是用 hotness/write-window 捕捉足够有效的短期覆盖风险。

2. **Propagate invalidation when the system already knows it.**
   不在 GC 时临时重建 liveness，而是在 compaction/version evolution 自然确定旧版本失效时传播信息。

3. **Protect foreground work from background maintenance.**
   不只减少 GC 的总 I/O，还要通过 GC-aware cache/admission 减少后台任务对前台工作集的污染。

这三个原则比简单列优化项更像系统论文的设计骨架。

#### 7.7.8 组件与故事线的对应关系

| 故事线问题 | 系统机制 | 需要证明的指标 |
|---|---|---|
| 垃圾形成太分散 | Hotness Tracker / hot-cold routing | Blob garbage ratio 分布更极化；GC 候选更高价值 |
| 失效信息来得太晚 | compaction feedback / drop-key cache | GC 反查减少；无效对象识别成本下降 |
| 后台读污染前台 cache | GC-aware block cache | cache hit 更稳定；前台吞吐 dip 更小；tail latency 更低 |
| 中等 value 路径不适合更新 | delta separate / middle routing | write amplification / GC rewrite 降低 |
| entry ratio 无法代表 bytes | precise_gc / delta block | Pareto value 下 candidate selection 更准确；GC bytes 更少 |
| flush backpressure 掩盖优化 | unified flush tuning | 消除非核心瓶颈，保证公平比较 |

这张表建议保留在论文开发文档中，写正文时可以作为 Design Overview 或 Evaluation Plan 的依据。

#### 7.7.9 论文图 1 建议：从 Reactive GC 到 Feedback Loop

FAST 论文很重视 Figure 1。建议 Figure 1 不要画复杂代码结构，而画一个对比图：

左边：Traditional KV-separated LSM

```text
Writes -> Blob files -> later GC scans Blob -> probes LSM -> rewrites live values
          ^ no lifetime shaping
          ^ no online invalidation view
          ^ foreground/cache interference
```

右边：Our Feedback-driven Blob Management

```text
Writes --hotness shaping--> lifetime-aware Blob layout
Compaction --invalidation feedback--> materialized liveness / drop-key info
GC --consumes feedback--> lower-bandwidth cleaning + GC-aware cache
```

图的 caption 可以写：

> Traditional Blob GC reconstructs object liveness after garbage has already formed. Our design closes the loop by shaping Blob layout at write time, materializing invalidation during version evolution, and consuming this feedback during GC.

#### 7.7.10 论文实验 claim 的组织方式

实验结论最好不要只写“吞吐提升 X%”。FAST 风格更喜欢多层 claim：

1. **End-to-end claim**
   在 update-intensive skewed workload 下，TerarkDB optimized 相比 TerarkDB baseline 提升吞吐，降低 stall 或 tail latency。

2. **Mechanism claim**
   Hotness-guided layout 能让垃圾更集中，GC-aware cache 能降低后台读对前台 cache 的污染，precise GC 能在 mixed-value 下更准确选择候选。

3. **Resource claim**
   优化不是通过消耗更多后台资源换吞吐，而是降低无效后台 I/O、减少 GC read/write 或 compaction interference。

4. **Robustness claim**
   在不同 update repeat、skew、value-size distribution 下，收益趋势与设计假设一致。

5. **Fairness claim**
   RocksDB、BlobDB、TerarkDB baseline、TerarkDB optimized 使用统一 write buffer、background jobs、blob threshold、blob file size 和 GC ratio。

#### 7.7.11 如果结果没有非常大收益，故事如何稳住

系统论文不一定只靠一个极大的 throughput number。若物理机结果中吞吐收益没有特别夸张，可以强化以下指标：

- `Cumulative stall` 是否下降；
- `Stalls(count)` 中 memtable slowdown / L0 slowdown 是否下降；
- GC read/write bytes 是否下降；
- compaction rewrite bytes 是否下降；
- p99 或 p99.9 是否更稳定；
- timechart 中吞吐 dip 是否变少；
- ablation 是否能稳定说明每个组件的方向性贡献。

论文故事可以从“吞吐大幅提升”调整为更稳健的：

> The primary benefit of the design is not merely peak throughput, but reducing the background bandwidth tax of Blob GC and making foreground performance more predictable under sustained updates.

中文：

> 本文方案的主要价值不只是提高峰值吞吐，而是降低持续更新下 Blob GC 的后台带宽税，并让前台性能更可预测。

这类表述更稳，也更符合 FAST 对系统行为解释的偏好。

#### 7.7.12 可能的审稿人质疑与预防性回答

**质疑 1：这是不是只对 overwrite-zipf 有效？**
回答思路：overwrite-zipf 是 update-intensive state workloads 的压力代表，用来暴露 Blob lifecycle management 的核心瓶颈。论文可以补充 uniform overwrite、不同 Zipf theta 或不同 update repeat 的 sensitivity，说明收益随更新局部性增强而增强，符合机制预期。

**质疑 2：Hotness tracking 会不会增加前台写开销？**
回答思路：强调 write-window/hot-table 是轻量近似结构，不追求精确 lifetime prediction；实验中报告 CPU overhead、write latency 或吞吐不退化。

**质疑 3：为什么不直接调 GC 阈值？**
回答思路：GC 阈值只解决“何时回收”，不解决“垃圾是否集中形成”和“失效信息是否可见”。本文是跨生命周期设计，而不是单点阈值调参。

**质疑 4：统一调大 write buffer 是否不公平？**
回答思路：所有 full matrix case 使用同一组 write buffer / background jobs 控制变量。调参不是 optimized 独享，而是消除 flush backpressure 这个非研究对象瓶颈。

**质疑 5：precise_gc 为什么不放入主路径？**
回答思路：fixed 4KB value 下 entry-based 与 byte-based 统计几乎等价，将 precise_gc 放入主路径不利于解释机制；Pareto ablation 更能展示其设计价值。

**质疑 6：这是否依赖 TerarkDB 特定实现？**
回答思路：实现落在 TerarkDB，但问题抽象适用于 KV-separated LSM：写入布局、版本失效和 Blob GC 分离是普遍结构。TerarkDB 是一个真实系统载体。

#### 7.7.13 最终论文可采用的章节标题

推荐章节标题：

1. Introduction
2. Background and Motivation
   - LSM-tree and KV Separation
   - Blob GC in Update-intensive Workloads
   - The Delayed Information Problem
3. Design Overview
   - Closing the Blob Lifecycle Loop
4. Hotness-guided Blob Layout
5. Invalidation Feedback and GC-aware Maintenance
6. Precise Garbage Accounting for Heterogeneous Values
7. Implementation in TerarkDB
8. Evaluation
9. Related Work
10. Conclusion

其中 “The Delayed Information Problem” 是一个很强的 framing，可以考虑作为 Motivation 的小节标题。

#### 7.7.14 论文中建议反复出现的关键词

建议全文反复使用并统一术语：

- KV-separated LSM-tree
- Blob lifecycle management
- feedback-driven Blob GC
- reactive cleaning
- delayed information problem
- invalidation materialization
- hotness-guided layout shaping
- foreground interference
- background bandwidth tax
- byte-accurate garbage accounting

统一术语会让论文显得更像一个完整系统，而不是多个 patch 的集合。

#### 7.7.15 更完整的英文 Abstract 草稿

下面是一版更接近 FAST 风格的英文摘要草稿，后续可根据实验数据填数：

> KV separation is a common technique for reducing write amplification in LSM-tree based key-value stores with large values. By moving values out of the main LSM-tree, systems avoid repeatedly rewriting large objects during compaction. However, this optimization shifts the maintenance burden to Blob garbage collection. Under update-intensive workloads, obsolete values accumulate quickly in Blob files, and GC must scan, validate, and migrate objects while competing with foreground requests for device bandwidth and cache capacity.
>
> This paper argues that inefficient Blob GC is fundamentally a delayed information problem. Existing systems observe update locality during writes and learn version invalidation during compaction, but GC typically consumes neither signal until cleaning time. As a result, it reconstructs object liveness by scanning Blob files and probing the main LSM-tree, after the physical layout has already been fixed.
>
> We present a feedback-driven Blob management design for KV-separated LSM-trees. The design closes the loop across the Blob lifecycle: it shapes Blob layout using lightweight hotness tracking at write time, materializes invalidation feedback during version evolution, protects foreground cache residency with GC-aware cache admission, and uses byte-accurate garbage accounting for heterogeneous value sizes. We implement the design in TerarkDB and evaluate it against RocksDB, BlobDB, and unmodified TerarkDB under update-intensive YCSB workloads. Our results show that closing the Blob lifecycle loop reduces the background bandwidth tax of Blob GC and improves foreground performance predictability under sustained overwrites.

#### 7.7.16 更强的中文 Introduction 草稿

下面是一版更完整的中文 Introduction 逻辑，可作为后续英文正文的母稿：

LSM-tree 已经成为现代高性能键值存储的基础结构。它通过将随机写转化为顺序写，在写密集负载下提供高吞吐；但这种优势依赖后台 compaction 持续维护数据有序性和空间效率。当 value 较大时，compaction 不再只是搬运 key 和少量元数据，而会反复读取和重写大量用户 payload，导致显著的写放大、设备带宽消耗和前后台资源竞争。

KV separation 是缓解这一问题的经典方法。系统将大 value 存储在独立 Blob 文件中，而在主 LSM-tree 中仅保留 key、元数据和 value 引用。这样，LSM compaction 只需要处理较小的引用记录，避免大 value 在层级之间反复搬运。WiscKey、Titan、BlobDB 和 TerarkDB 等系统都采用了这一思路。对于大 value 场景，KV separation 显著降低了 compaction 写放大。

然而，KV separation 并没有消除后台维护成本，而是改变了维护成本的形态。被更新或删除的旧 value 不会立即从 Blob 文件中移除，而是作为垃圾继续占用空间。系统必须通过 Blob GC 识别仍然存活的对象、迁移它们并回收旧 Blob 文件。在 update-intensive workload 中，同一批 key 会被持续覆盖，旧 value 快速产生，Blob GC 因而从一个偶发的后台清理任务变成持续运行的带宽消费者。

现有 Blob GC 的核心问题在于它是 reactive 的。写入路径通常不考虑对象未来是否会快速失效，因此不同生命周期的数据被混合写入同一批 Blob。对象失效信息虽然会随着版本演化在 compaction 中逐渐变得确定，但系统通常不会将其在线物化给 GC 使用。最终，当 GC 被触发时，它必须扫描候选 Blob，解析对象，并回查主 LSM-tree 来重新判断对象是否仍然存活。换言之，GC 在生命周期的最后阶段重新发现那些系统早已在写入和 compaction 过程中观察到的信息。

本文的关键观察是：Blob GC 的成本在 GC 开始之前就已经被大体决定。哪些对象被共同写入同一 Blob，哪些旧版本已经失效，活数据在文件中如何分布，这些事实都形成于写入和版本演化阶段。只在 GC 阶段优化扫描和迁移策略，最多只能改善最后一步，而无法从根本上降低后台带宽税。高效的 Blob GC 需要同时满足三个条件：垃圾在 Blob 内部集中形成，失效信息在版本演化过程中被在线物化，回收阶段能够利用活数据空间分布选择低成本执行路径。

基于这一观察，本文提出一种 feedback-driven 的 Blob lifecycle management 机制，并在 TerarkDB 中实现。该机制将 Blob GC 从事后清理任务转化为跨写入、版本演化和后台回收的闭环协同过程。写入阶段，系统利用轻量级 hotness tracking 捕捉短期覆盖风险，并据此塑造更易回收的 Blob 布局。版本演化阶段，系统利用 compaction feedback 和 drop-key 信息在线物化对象失效信号，使 GC 不必在回收时重新构建完整 liveness。回收阶段，系统通过 GC-aware cache admission 和更精确的 byte-level garbage accounting 降低后台维护对前台工作集和设备带宽的干扰。

我们在 TerarkDB 上实现该设计，并通过 RocksDB、BlobDB、TerarkDB baseline 和 TerarkDB optimized 的 full matrix，以及多个组件消融实验进行评估。实验重点不是单一峰值吞吐，而是完整回答：该闭环是否提升前台吞吐，是否降低 GC/compaction 的后台带宽税，是否减少 stall 和 tail latency，是否能通过 ablation 证明各阶段反馈机制的必要性。

---

## 8. 论文贡献写法

建议贡献表述：

1. **Problem framing**：指出 KV-separated LSM 的瓶颈不是简单的 Blob GC 阈值问题，而是垃圾形成、失效信息维护和回收执行之间缺少闭环。
2. **Feedback-driven layout shaping**：提出基于热点更新和 compaction feedback 的 Blob 布局塑形机制，让短生命周期 value 更集中，形成更高收益 GC 候选。
3. **GC-aware invalidation and cache management**：将版本演化中的失效信号前移到在线维护路径，并降低 GC/compaction 对前台 cache 和 I/O 的干扰。
4. **Precise GC for heterogeneous values**：针对 mixed-value 场景引入 byte-accurate garbage accounting，避免 entry-based 统计误导 GC 决策。
5. **TerarkDB prototype and evaluation**：在 TerarkDB 中实现原型，并通过 full matrix 与 ablation 验证该闭环设计对吞吐和后台效率的改善。

---

## 9. 当前实验完成度与下一步

### 9.1 已完成

- 已实现物理机一键入口 `paper_benchmark.sh`。
- 已支持 full matrix 和 ablation matrix。
- 已统一 RocksDB、BlobDB、TerarkDB baseline、TerarkDB optimized 的关键控制变量。
- 已通过 smoke/full 实际运行验证脚本链路。
- 已通过 dry-run 验证参数 override 能正确传播。

### 9.2 待物理机完成

需要正式收集：

1. `MODE=all REPEAT_COUNT=3` 的 `summary.tsv`；
2. full matrix 每个 case 的 `updatex20.txt`；
3. ablation matrix 每个 case 的 `updatex20.txt`；
4. TerarkDB baseline 与 optimized 的 INFO LOG；
5. 如果可能，保留 timechart CSV，用于画吞吐随时间变化图。

### 9.3 结果分析重点

拿到物理机结果后，优先分析：

1. `terarkdb_opt_100gb` vs `terarkdb_100gb` 的平均吞吐提升；
2. `all_on_fixed` vs `baseline_fixed` 的收益；
3. `no_hotness_tracker`、`no_gc_aware_cache`、`no_delta_separate`、`no_read_handle` 相比 `all_on_fixed` 的性能下降；
4. GC/compaction/stall 指标是否支持论文故事线；
5. Pareto case 中 `precise_gc + use_delta_block` 是否能体现 byte-level GC 精度收益。

---

## 10. 可直接放入论文的 Introduction 草稿

LSM-tree has become the foundation of many high-performance key-value stores because it transforms random writes into sequential writes and sustains high throughput under write-intensive workloads. However, this design also introduces significant background maintenance cost. In particular, when values are large, conventional LSM compaction repeatedly reads and rewrites large objects across levels, amplifying device bandwidth consumption and interfering with foreground operations.

KV separation addresses this problem by storing large values in separate Blob files and keeping only keys and references in the main LSM-tree. This design, adopted by systems such as WiscKey, Titan, BlobDB, and TerarkDB, effectively removes large values from the compaction path. Nevertheless, it does not eliminate background maintenance. Instead, it shifts the problem to Blob garbage collection. Under update-intensive workloads, old values quickly become obsolete while still occupying Blob space. The system must eventually identify live objects, migrate them, and reclaim obsolete Blob files.

Unfortunately, existing Blob GC is often reactive. The write path does not actively shape the physical layout according to object lifetime. Obsolete objects are discovered only after GC scans candidate Blob files and validates them against the main LSM-tree. Finally, GC typically applies a uniform scan-and-rewrite procedure even though different Blob files may have very different live-data distributions. As a result, Blob GC spends substantial bandwidth on validation and unnecessary data movement, and its background I/O competes with foreground requests.

The key observation of this paper is that efficient Blob GC depends on a closed loop across three stages: how garbage is formed, how invalidation is materialized, and how reclamation is executed. Garbage ratio alone is not sufficient. A Blob file is cheap to reclaim only when obsolete objects are concentrated, invalidation information is readily available, and live data can be migrated through a bandwidth-efficient path.

This paper presents a feedback-driven Blob management design for KV-separated LSM stores and implements it in TerarkDB. The design uses hotness-guided layout shaping to group short-lived values, compaction feedback to materialize invalidation information, GC-aware cache management to reduce foreground interference, and byte-accurate precise GC for heterogeneous value sizes. Together, these mechanisms turn Blob GC from a reactive cleaning task into a lifecycle-aware feedback loop.

---

## 11. 可直接放入论文的中文摘要草稿

LSM-tree 被广泛用于高性能键值存储系统，但在大 value 场景下，传统 compaction 会反复搬运大对象，造成严重后台 I/O 放大。KV separation 通过将 value 存入独立 Blob 文件、在主 LSM 中仅保留引用，缓解了这一问题。然而，在更新密集负载下，旧 value 会持续在 Blob 中累积，系统必须通过 Blob GC 回收空间。现有 Blob GC 往往是事后清理式的：写入路径不主动塑造数据生命周期布局，失效信息需要 GC 时扫描 Blob 并回查 LSM 重建，回收执行也通常采用统一的扫描-重写流程。这会消耗大量后台带宽，并与前台请求产生显著资源竞争。

本文指出，KV-separated LSM 中 Blob GC 的低效根源在于垃圾形成、失效信息维护和回收执行三者脱节。基于这一观察，本文提出一种 feedback-driven 的 Blob 管理闭环，并在 TerarkDB 中实现原型。该方案通过热点感知布局塑形将更可能快速失效的 value 聚集到更易回收的 Blob 中，通过 compaction feedback 在线物化失效信息，通过 GC-aware cache 降低后台维护对前台 cache 的污染，并针对 value 大小不均匀场景引入 byte-accurate precise GC。实验将通过 RocksDB、BlobDB、TerarkDB baseline 和 TerarkDB optimized 的 full matrix，以及多组组件消融，验证该闭环设计对吞吐、后台 GC 效率和前台服务质量的改善。
