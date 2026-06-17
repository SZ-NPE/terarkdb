# Delta Block / Delta Separate POC 开关记录

本文档记录当前 TerarkDB 原型中与 Delta Block、Delta Separate 相关的 db_bench 开关、运行语义以及 POC 对比测试建议。目标是让实验时可以明确地区分：基础 KV separation、Delta Separate middle-value 策略、Delta Block 元数据以及精确 Blob GC 各自带来的收益和开销。

## 1. 开关总览

| 功能 | db_bench 开关 | 默认值 | 作用 |
| --- | --- | --- | --- |
| 基础 KV separation | `--blob_size` | `size_t(-1)` in db_bench | 控制是否启用 KV separation。`size_t(-1)` 表示关闭；非 `-1` 表示 value 大于等于该阈值时可分离到 Blob。 |
| Delta Separate 总开关 | `--enable_delta_separate` | `true` | 控制 middle-value Delta Separate 策略是否启用。关闭后仍可保留普通 KV separation。 |
| Middle-value 上界 | `--middle_blob_size` | `size_t(-1)` | 当 Delta Separate 启用时，`[blob_size, middle_blob_size)` 区间内的 value 作为 middle separated value 处理。 |
| Middle-value 合并层级 | `--middle_combine_level` | `size_t(-1)` | 当 compaction input level 到达该层级后，将 middle separated value combine 回普通 SST。 |
| Delta Block 元数据 | `--use_delta_block` | `false` | 控制是否在 SST 中写入 delta metadata block，用于保存 separated value 的 per-entry metadata/value_size。 |
| 精确 Blob GC | `--precise_gc` | `false` | 控制 Blob GC 是否使用 byte-precise 垃圾统计。通常需要 Delta Block 提供 value_size 元信息才能发挥完整收益。 |

## 2. Delta Separate 总开关语义

新增的 `--enable_delta_separate` 是一个针对 middle-value Delta Separate 策略的 master gate。

它不替代 `--blob_size`：

- `--blob_size` 控制基础 KV separation 是否开启。
- `--enable_delta_separate` 控制在基础 KV separation 之上，是否启用 middle-value 的“浅层分离、深层合并”策略。

因此，推荐把二者理解为两层开关：

```text
blob_size == size_t(-1)
  => KV separation 整体关闭

blob_size != size_t(-1) && enable_delta_separate=false
  => 普通 KV separation 开启，但 Delta Separate middle-value 策略关闭

blob_size != size_t(-1) && enable_delta_separate=true
  => 普通 KV separation 开启，Delta Separate middle-value 策略也开启
```

这样可以在 POC 中固定 `blob_size`，只切换 `enable_delta_separate`，从而隔离 Delta Separate 策略本身的收益。

## 3. 推荐 POC 对比矩阵

### 3.1 基础 KV separation baseline

用于评估普通 KV separation 的表现，不启用 Delta Separate，不启用 Delta Block，不启用精确 GC。

```bash
--blob_size=<B> \
--enable_delta_separate=false \
--use_delta_block=false \
--precise_gc=false
```

### 3.2 Delta Separate only

用于评估 middle-value Delta Separate 策略是否降低写放大或改善 GC 压力。

```bash
--blob_size=<B> \
--enable_delta_separate=true \
--middle_blob_size=<M> \
--middle_combine_level=<L> \
--use_delta_block=false \
--precise_gc=false
```

建议约束：

```text
B < M
L 为 middle value 希望 combine 回 SST 的 compaction input level
```

### 3.3 Delta Block only

用于评估写入 delta metadata block 的额外开销。此配置不打开精确 GC，主要观察 table build/compaction 路径的元数据写入成本。

```bash
--blob_size=<B> \
--enable_delta_separate=false \
--use_delta_block=true \
--precise_gc=false
```

### 3.4 Delta Separate + Delta Block

用于评估 Delta Separate 与 Delta Block 同时开启时的整体表现，但仍不启用精确 GC。

```bash
--blob_size=<B> \
--enable_delta_separate=true \
--middle_blob_size=<M> \
--middle_combine_level=<L> \
--use_delta_block=true \
--precise_gc=false
```

### 3.5 Delta Separate + Delta Block + Precise GC

用于评估完整设计：Delta Separate 负责 value placement，Delta Block 提供 per-entry metadata，precise GC 使用 byte-precise 垃圾统计。

```bash
--blob_size=<B> \
--enable_delta_separate=true \
--middle_blob_size=<M> \
--middle_combine_level=<L> \
--use_delta_block=true \
--precise_gc=true
```

## 4. 实验注意事项

1. **固定基础 KV separation 阈值**  
   对比 Delta Separate 收益时，应固定 `--blob_size=<B>`，只切换 `--enable_delta_separate`，否则实验结果会混入基础分离阈值变化带来的影响。

2. **区分策略收益与元数据成本**  
   `--enable_delta_separate` 是运行策略开关；`--use_delta_block` 是 SST 元数据开关。建议分别测试，再组合测试。

3. **精确 GC 与 Delta Block 的关系**  
   `--precise_gc=true` 时，系统会尝试使用 per-entry value_size 进行 byte-precise 垃圾统计。`--use_delta_block=true` 能让 compaction 在不读取 Blob value 的情况下获得更精确的 value_size 信息。

4. **关闭 Delta Separate 后的语义**  
   `--enable_delta_separate=false` 不会关闭 KV separation；它只关闭 middle-value Delta Separate 策略。若要完全关闭 KV separation，需要设置：

   ```bash
   --blob_size=18446744073709551615
   ```

   该值等价于 `size_t(-1)`。

## 5. 当前实现入口

- `ColumnFamilyOptions::enable_delta_separate`：控制 Delta Separate 策略总开关。
- `MutableCFOptions::enable_delta_separate`：运行时 mutable option。
- `BlobConfig::enable_delta_separate`：传递给 compaction iterator 的运行配置。
- `CompactionIterator`：使用 `enable_delta_separate` gate middle-value combine / Delta Separate 策略。
- `db_bench`：通过 `--enable_delta_separate` 暴露给 POC 实验。

