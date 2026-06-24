# TerarkDB 动机测试图表记录

本文档记录用于论文动机部分的 6 幅核心图表。目标是用现有 `test-sh/new-ycsb/terarkdb.sh` 支持的 `load + overwrite`、`workloada/workloadb`、以及 Pareto 不定长 value 负载，分别证明冷热路由与反查加速、GC 感知块缓存淘汰、不定长 value 精确 GC 的问题动机。

## 图 M1：vSST 垃圾率分布 CDF

- **图表形式**：CDF 图。
- **建议负载**：`load + overwrite-zipf`、`load + overwrite-zipf1.2` 或 `load + overwrite-hotspot-0.9`。
- **横轴**：每个 vSST 的 garbage ratio。
- **纵轴**：vSST 文件累计占比。
- **证明的问题**：hot/cold value 混写会使垃圾分散在多个 vSST 中，GC 难以只选择高垃圾文件，导致回收时必须扫描较多有效数据。
- **对应动机**：需要冷热路由，将高更新频率 value 聚集到短生命周期 vSST 中，提高垃圾集中度。

## 图 M2：GC I/O 分解图

- **图表形式**：堆叠柱状图。
- **建议负载**：`load + overwrite-uniform`、`load + overwrite-zipf`、`load + overwrite-zipf1.2`、`load + overwrite-hotspot-0.9`。
- **横轴**：不同 overwrite 分布或不同 skew 程度。
- **纵轴**：GC I/O bytes。
- **堆叠项**：
  - vSST read bytes
  - invalid/dead value read bytes
  - kSST lookup read bytes
  - live value relocation write bytes
- **证明的问题**：Blob GC 的代价不仅是搬迁有效 value，还包括扫描 vSST、读取最终会被丢弃的 dead value、以及执行 kSST 反查。
- **对应动机**：需要减少 GC 无效读带宽浪费，并通过 drop-key cache 等方式降低 kSST 反查开销。

## 图 M3：GC 期间前台性能时间线

- **图表形式**：时间序列图。
- **建议负载**：`load + overwrite-zipf` 后触发 GC，同时运行前台读或短 scan。
- **横轴**：时间。
- **纵轴**：
  - 前台吞吐或 P99 延迟。
  - GC read bandwidth，可拆分为 vSST read bandwidth 和 kSST lookup read bandwidth。
- **证明的问题**：GC 扫描 vSST 和反查 kSST 会与前台读争用 I/O 带宽，造成前台吞吐下降或尾延迟升高。
- **对应动机**：需要降低 GC 后台读 I/O 和反查放大，避免 GC 对前台读造成明显干扰。

## 图 M4：block cache 失效块占比时间线

- **图表形式**：时间序列折线图或面积图。
- **建议负载**：`load + workloada/workloadb`，开启 `block_cache_obsolete_tracking`。
- **横轴**：时间。
- **纵轴**：block cache 中 obsolete block bytes 占比，例如 `obsolete block bytes / tracked block bytes` 或 `obsolete block bytes / total block cache bytes`。
- **证明的问题**：compaction/GC 之后，属于 obsolete vSST/SST 的 blocks 会继续驻留在 LRU block cache 中，并长期保持较高水位。
- **对应动机**：LRU 不感知 block 的 GC 状态和文件失效状态，需要 GC-aware block cache eviction 主动降低失效块优先级。

## 图 M5：entry-based garbage ratio vs byte-based garbage ratio 散点图

- **图表形式**：散点图，带 `y = x` 参考线。
- **建议负载**：`load + overwrite-pareto`，可分别使用 `overwrite-uniform` 和 `overwrite-zipf` 的 key 更新分布。
- **每个点**：一个 vSST。
- **横轴**：entry-based garbage ratio，即 `dead entries / total entries`。
- **纵轴**：byte-based garbage ratio，即 `dead bytes / total bytes`。
- **证明的问题**：在不定长 value 下，失效 key 比例不能准确代表真实可回收字节比例；大量点会偏离 `y = x`。
- **对应动机**：entry-based GC 收益模型在 variable-size value 场景下会误判，需要 byte-precise GC。

## 图 M6：GC scan bytes / reclaimed bytes

- **图表形式**：柱状图或折线图。
- **建议负载**：fixed value 与 Pareto value 对比；也可进一步区分 `overwrite-uniform + pareto` 和 `overwrite-zipf + pareto`。
- **横轴**：value size 分布或 workload 组合。
- **纵轴**：`GC scan bytes / reclaimed bytes`，也可使用 `GC total I/O bytes / reclaimed bytes`。
- **证明的问题**：entry-based GC 在不定长 value 下可能选择回收收益较低的 vSST，导致单位回收成本升高。
- **对应动机**：byte-precise GC 不仅修正统计口径，还能降低错误 GC 选择带来的实际回收效率损失。

## 图表与优化点对应关系

| 优化点 | 图表 | 证明的问题 |
| --- | --- | --- |
| 冷热路由与反查加速 | M1：vSST 垃圾率分布 CDF | hot/cold 混写导致垃圾分散，冷热路由有必要。 |
| 冷热路由与反查加速 | M2：GC I/O 分解图 | GC 存在 vSST 无效扫描和 kSST 反查 I/O。 |
| 冷热路由与反查加速 | M3：GC 期间前台性能时间线 | GC 后台读与前台读争用带宽并造成性能抖动。 |
| GC 感知块缓存淘汰 | M4：block cache 失效块占比时间线 | LRU 下 obsolete blocks 长期驻留并污染 block cache。 |
| 不定长 value 精确 GC | M5：entry vs byte garbage ratio 散点图 | entry-based 垃圾率不能代表真实可回收 bytes。 |
| 不定长 value 精确 GC | M6：GC scan bytes / reclaimed bytes | 错误收益模型会转化为实际 GC 效率损失。 |
