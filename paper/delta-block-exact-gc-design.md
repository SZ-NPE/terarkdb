# Delta Block、Delta Separate 与精确 Blob GC：论文表述和实现记录

本文档记录 TerarkDB 中 `delta block`、`delta separate` 与精确 Blob GC 的设计逻辑、论文中可采用的描述方式，以及相关代码文件和具体实现路径。本文档用于论文写作素材整理，不替代主论文草稿。

## 1. 论文中建议如何描述这一点

建议不要把创新点简单表述为“增加了一个 delta block”。单独看，delta block 更像 SST table format 中的一个辅助元数据块；如果只描述为“额外保存 value size”，论文贡献会显得偏工程实现。更合适的论文表述是：

> 我们提出一种面向 KV-separated LSM-tree 的轻量级 SST-side value metadata sidecar。该结构与 normal SST 中的 entry 顺序对齐，为被 KV 分离的 value 保存真实 value size 与可选 value meta，使 Compaction 和 Blob GC 能够在不读取 Blob value 的情况下获得精确的字节级元信息。基于该元信息，系统可在版本演化过程中在线聚合每个 Blob 的 live bytes 与 obsolete bytes，从而实现 byte-accurate garbage accounting，并避免仅基于 KV 数量估算垃圾比例所带来的误判。

如果要进一步强化论文贡献，可以将 `delta block` 与 `delta separate` 合并描述为一个更完整的机制：

> 我们提出一种 metadata-assisted、size- and level-aware 的 value separation 机制。系统通过 delta metadata block 为 separated value 保存精确的 per-entry size/meta 信息，并根据 value size 将数据划分为 inline value、middle blob 和 large blob。Middle value 在浅层被分离以降低写放大，在深层 Compaction 中重新合并回 LSM-tree 以降低长期读放大；large value 则长期保留在 Blob 中，并通过 size-aware GC 回收。该机制在写放大、读放大和空间放大之间提供了更细粒度的折中。

可以将其拆成两个贡献点：

1. **Delta Metadata Block for Exact Garbage Accounting**  
   在 SST 中引入与 entry 顺序对齐的轻量级 sidecar，记录 separated value 的真实大小和 value meta。该结构使后台 Compaction 和 Blob GC 能够不读取 Blob value 就获得字节级元信息，从而支持精确垃圾统计。

2. **Size- and Level-aware Value Placement**  
   根据 value size 和 LSM level 区分 inline value、middle blob 与 large blob。Middle value 在浅层分离、深层合并，large value 长期分离并交给 size-aware Blob GC 处理。

## 2. 背景问题：为什么不能只看 KV 数量

在 KV separation 中，normal SST 不再保存真实 value，而是保存指向 Blob 文件的 value index。Blob GC 需要判断一个 Blob 文件是否值得回收，常见做法是估算其中已经失效的对象比例。如果 value 大小近似均匀，按 entry 数量估算垃圾比例是可接受的：

```text
entry_garbage_ratio = obsolete_entry_count / total_entry_count
```

但在 value size 不均匀的负载中，entry 数量比例不能代表真实可回收空间。例如：

```text
Blob A:
  k1 -> 1 KB   live
  k2 -> 1 KB   live
  k3 -> 1 KB   live
  k4 -> 1 MB   obsolete

entry_garbage_ratio = 1 / 4 = 25%
size_garbage_ratio  ≈ 1 MB / (1 MB + 3 KB) ≈ 99.7%
```

按 entry 数看，Blob A 似乎垃圾不多；按字节看，它几乎全是垃圾，非常值得回收。

反过来：

```text
Blob B:
  k1 -> 1 MB   live
  k2 -> 1 KB   obsolete
  k3 -> 1 KB   obsolete
  k4 -> 1 KB   obsolete

entry_garbage_ratio = 3 / 4 = 75%
size_garbage_ratio  < 1%
```

按 entry 数看，Blob B 似乎很脏；按字节看，真正可回收的数据很少，执行 GC 反而可能需要重写仍然存活的大 value，收益很低。

因此，精确 Blob GC 的核心不是记录“这个文件有多少 KV”，而是记录每个 separated value 的真实大小，并在版本安装过程中按 Blob 文件聚合 live bytes 与 obsolete bytes。

## 3. 核心设计：delta block 记录 per-entry separated value size

`delta block` 是 TerarkDB 在 block-based table format 中增加的额外 meta block。它不存 key，也不存真实 value，而是按照 normal SST 中 entry 的顺序保存附加信息。

核心接口位于：

- `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_builder.h:16`
- `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_builder.h:31`

关键语义如下：

```cpp
// The interface for building delta block.
// Delta block contains no key, only the information needs to be stored,
// its value is in the order of keys.
class DeltaBuilder { ... };

void Add(bool is_separated, uint32_t separated_value_size,
         const std::string* const value_meta = nullptr);
```

每个 SST entry 写入时，delta block 记录：

```text
entry_i:
  is_separated: true / false
  if is_separated:
    separated_value_size
    value_meta(optional)
```

注意：delta block 不是简单记录一个文件级总量，例如“该 SST 所有 value 总大小”；它是记录 per-entry 元数据。只有 per-entry size 才能在后续 Compaction 和 VersionBuilder 中根据实际 still-live reference 聚合到不同 Blob 文件上。

## 4. 精确 GC 的数据流

精确 GC 的数据流可以概括为：

```text
Flush/Compaction 写 SST
  -> normal SST 中保存 value index
  -> delta block 中保存 per-entry value_size / value_meta

Compaction 读取 SST
  -> iterator 从 delta block 获取当前 entry 的 value_size
  -> 输出 SST 的 dependence 记录引用了哪些 Blob 以及引用字节数

VersionBuilder 安装新版本
  -> 汇总所有 normal SST 的 dependence
  -> 计算每个 Blob 的 live bytes
  -> raw bytes - live bytes = obsolete bytes
  -> 写入 FileMetaData::num_antiquation_bytes

GC Picker 选择候选 Blob
  -> precise_gc 开启时使用 num_antiquation_bytes / BlobGcAccountingBytes()
  -> 关闭时退化为 num_antiquation / num_entries
```

对应公式：

```text
live_bytes(blob_i) = Σ size(value_j), where value_j is still referenced by current version
obsolete_bytes(blob_i) = raw_bytes(blob_i) - live_bytes(blob_i)
size_garbage_ratio(blob_i) = obsolete_bytes(blob_i) / raw_bytes(blob_i)
```

其中 `size(value_j)` 来自 delta block 记录的 per-entry separated value size。

## 4.1 基于真实代码的 precise GC / delta block 联动审阅

本节基于当前 TerarkDB 源码补充 `precise_gc` 如何与 delta block 一起运行。核心结论是：**precise GC 本身不直接扫描 delta block；delta block 在 SST 构建和 Compaction 阶段提供 per-entry `value_size`，该值被逐级汇总到 `Dependence::byte_count` / `byte_count_entry_count`、`FileMetaData::num_antiquation_bytes`，最终 GC picker 在 precise 模式下使用 `num_antiquation_bytes / BlobGcAccountingBytes()` 选择候选 Blob。**

### 4.1.1 precise GC 的模式定义与开关语义

当前实现中 `precise_gc` 是布尔 CF option。开启后，GC picker 从 entry-count 垃圾比例切换为 byte-level 垃圾比例；当前 steady-state 路径不会为了补历史缺失 metadata 主动读取 Blob value。

```cpp
bool precise_gc = false;
```

其中：关闭时 GC picker 使用 entry 维度的 `num_antiquation / num_entries`；开启时使用 `num_antiquation_bytes / BlobGcAccountingBytes()`。缺失 value-size metadata 时，由 VersionBuilder 使用平均字节回退估算，而不是读取 Blob value 补齐。

### 4.1.2 precise GC 依赖 delta block 的配置校验

delta block 的 table option 位于 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/terarkdb/table.h:272`：

```cpp
// Use delta block to store value size and value meta
// use_delta_block is immutable and required by precise_gc.
bool use_delta_block = false;
```

这说明当前实现中 precise GC 的精确性依赖 `use_delta_block`。当前 db_bench/sanitize 路径在 `precise_gc=true` 时会确保 block-based table 开启 delta block；不支持的 table 配置会拒绝或关闭 precise GC。配置校验链路有两层：

1. `BlockBasedTableFactory` 限制 delta block 只能用于支持的 index type。若 index type 不是 `kBinarySearch` 或 `kHashSearch`，且 `use_delta_block=true`，会直接返回 InvalidArgument，提示关闭不兼容的 delta/precise-GC 配置。

2. CF option sanitize 时，如果 table factory 不存在或不支持 delta-block-backed precise GC，则直接关闭 `precise_gc`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/column_family.cc:339`：

```cpp
if (!result.table_factory ||
    !result.table_factory->IsExactGarbageCollectionSupported()) {
  result.precise_gc = false;
}
```

`BlockBasedTableFactory::IsExactGarbageCollectionSupported()` 的返回值就是 `table_options_.use_delta_block`。当前实现会在 `precise_gc=true` 时自动开启 `use_delta_block`；若 table 配置仍不支持，则 precise GC 被关闭或拒绝，避免在缺失 metadata 时误用精确语义。

### 4.1.3 delta block 在写 SST 时生成 value_size

当 value 被 KV 分离后，normal SST 中的 value 不再是真实 value，而是 `kTypeValueIndex` 或 `kTypeMergeIndex`。真实 value size 的来源在 `SeparateHelper::TransToSeparate()` 中形成：

- `SeparateHelper::ValueMetaData` 含有 `value_size`、`block_handle` 和 `meta_data`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/dbformat.h:933`；
- `TransToSeparate()` 计算 `separated_value_size = internal_key.internal_key_size() + value.size()`，并写入 `meta->value_size`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/dbformat.cc:212` 和 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/dbformat.cc:215`。

随后，`BlockBasedTableBuilder::Add()` 将该值写入 delta block：

- 解析 key type，判断是否为 `kTypeValueIndex` 或 `kTypeMergeIndex`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc:578`；
- 若是 separated value，则 `value_size = value_meta.value_size`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc:581`；
- data block 正常写 key/value index，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc:605`；
- delta block 同步记录 `is_separated`、`value_size` 和 `value_meta`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc:606`；
- table property 同步维护 `props.raw_key_size` / `props.raw_value_size` 等通用统计；per-entry separated value size 只写入 delta block，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc:452` 到 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc:468`。

SST finish 时，只有在 `use_delta_block=true` 且 delta block 非空时才写出 delta meta block，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc:1148`；写完后在 metaindex 中注册 `kDeltaBlock`，并记录 `props.delta_block_size`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc:1170`。

因此，真实代码路径中 delta block 的角色是：**在 SST 写入时，将每个 separated entry 的真实 `value_size` 固化到 table meta block 中，供后续 Compaction 读取。**

### 4.1.4 Compaction 如何从 delta block 取回 value_size

Compaction 读取 input SST 时，table reader 只在 `for_compaction=true` 且存在 delta index block 时创建 `DeltaBlockReader`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_reader.cc:3567`。普通前台 Get 不依赖 delta block；delta block 主要服务后台 Compaction / GC 元数据维护。

Compaction iterator 获取扩展信息的链路如下：

1. `BlockBasedTableIteratorBase::GetExtendedInfo()` 判断当前 key type 是否为 `kTypeValueIndex` 或 `kTypeMergeIndex`；若是且存在 delta reader，则调用 `delta_block_reader_->GetExtendedInfo(info)`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_reader.cc:3547`。

2. `DeltaBlockReader::GetExtendedInfo()` 将当前位置对应的 `value_size` 和 `value_meta` 填入 `IteratorExtendedInfo`，其定义入口见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_reader.cc:73`。

3. `CompactionIterator` 读取 input value 后调用 `input_->GetExtendedInfo(&info)`，并将 `info.value_size` 写入 `value_meta_.value_size`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_iterator.cc:899` 到 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_iterator.cc:903`。

这条链路说明：**precise GC 不在 GC picker 阶段直接读 delta block；delta block 是在 Compaction 读 SST 时被消费，并把 per-entry value size 传递给 CompactionIterator。**

### 4.1.5 历史 SST 缺失 value_size 时的回退路径

如果历史 SST 缺失 delta block 或 delta block 中没有 value size，precise GC 不能凭空获得精确字节信息。当前实现避免在 steady-state compaction 中为了补齐 metadata 主动读取 Blob value；缺失部分会在 VersionBuilder 中按平均字节回退估算。

Compaction job 中会跟踪 `byte_count_entry_count`，用于表示 `byte_count` 覆盖了多少 entries。该字段小于 `entry_count` 时，说明存在历史/缺失 metadata entries，需要在版本安装时估算。

```cpp
estimated_entries = dependence.entry_count - ExactByteEntryCount(dependence);
dep_bytes = dependence.byte_count + estimated_entries * average_bytes;
```

该模式的语义是：

```text
旧 SST 缺失 value_size
  -> Compaction 仍累计 entry_count
  -> 已知 value_size 的 entries 累计 byte_count / byte_count_entry_count
  -> VersionBuilder 对缺失 entries 使用平均字节估算
  -> GC picker 仍可使用稳定的 byte-level score
```

因此论文中应将其描述为“精确 metadata + 缺失 metadata 的混合估算路径”，而不是 Blob value repair 路径。

### 4.1.6 Compaction 如何写 Dependence::byte_count / byte_count_entry_count

Compaction 主循环中，当前输出 key 如果仍是 `kTypeValueIndex` 或 `kTypeMergeIndex`，说明输出 normal SST 仍引用某个 Blob 文件。此时 `CompactionJob` 会按照 `value.file_number()` 聚合 dependence：

- 判断输出 key type，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_job.cc:2146`；
- 以 Blob file number 为 key 聚合；首次插入时记录 `entry_count=1`；
- 对同一 Blob 的后续引用，entry count 加一；
- 若当前 entry 携带可信 `value_size`，则 `byte_count += c_iter->value_size()` 且 `byte_count_entry_count++`。

输出 SST 完成时，这些聚合结果写入 `meta->prop.dependence`，形成 `Dependence{file_number, entry_count, byte_count, byte_count_entry_count}`。

该 dependence 会被持久化在 table properties 和 MANIFEST 中：

- table properties 编码 dependence separated size：`/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/meta_blocks.cc:153`；
- table properties 解码恢复：`/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/meta_blocks.cc:546`；
- VersionEdit / MANIFEST 编码：`/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_edit.cc:294`；
- VersionEdit / MANIFEST 解码：`/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_edit.cc:502`。

这一步是 delta block 与 precise GC 之间最重要的桥梁：**delta block 的 per-entry value size 被 Compaction 汇总为 per-SST、per-Blob 的 `byte_count` metadata。**

### 4.1.7 VersionBuilder 如何计算 num_antiquation_bytes

`VersionBuilder` 安装新版本时，会根据当前 version 中所有 normal SST 的 dependence 信息计算每个 Blob 文件仍被引用的 entry 数和字节数。

内部结构 `DependenceItem` 维护两个关键字段：

- `entry_depended`：仍被引用的 entry 数；
- `bytes_depended`：仍被引用的字节数，包含精确 `byte_count` 与缺失 metadata entries 的平均字节估算。

定义见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_builder.cc:123` 到 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_builder.cc:132`。

`SetDependence()` 遍历每个 normal SST 的 `prop.dependence`：

- `entry_depended += dependence.entry_count * ratio`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_builder.cc:324`；
- `bytes_depended += ResolveDepBytes(dependence, item)`，其中 `ResolveDepBytes()` 会对缺失 `byte_count` 的 entries 做平均字节估算。

在 `CalculateDependence()` 的 finish 阶段，VersionBuilder 对每个 Blob 计算：

```cpp
uint64_t accounting_bytes = item.f->BlobGcAccountingBytes();
bytes_depended = std::min(accounting_bytes, bytes_depended);
uint64_t num_antiquation_bytes = accounting_bytes - bytes_depended;
```

随后写入 `item.f->num_antiquation_bytes`。

因此，`num_antiquation_bytes` 的语义是：

```text
Blob 文件原始数据字节数 - 当前版本仍引用该 Blob 的字节数
```

这个值不是 GC 时临时扫描 Blob 得到的，而是在版本安装过程中根据 dependence 在线物化得到。

### 4.1.8 GC picker 如何使用 precise score

GC picker 的候选文件封装在 `GarbageFileInfo` 中。构造函数根据 `precise_gc` 选择不同 score：

```cpp
if (precise_gc) {
  score = std::min(1.0, f->num_antiquation_bytes /
                            std::max<double>(1, f->BlobGcAccountingBytes()));
} else {
  score = std::min(
      1.0, f->num_antiquation / std::max<double>(1, f->prop.num_entries));
}
```

对应位置：`/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_picker.cc:47`。

`PickGarbageCollection()` 遍历 hidden level `-1` 的 Blob 文件时，会用 `mutable_cf_options.precise_gc` 构造 `GarbageFileInfo`。如果 dirtiest blob 未被强制标记且 score 小于 `blob_gc_ratio`，则不触发 GC。

GC 执行完成后的日志和 metrics 会输出 entry estimation 与 size/byte estimation。

此外，`VersionStorageInfo::ComputeCompactionScore()` 也维护全局 entry/size 两套垃圾比例：

- `entry_garbage_ratio_ = num_antiquation / num_entries`，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_set.cc:2411`；
- `size_garbage_ratio_ = num_antiquation_bytes / BlobGcAccountingBytes()`；
- precise GC 开启时，`total_garbage_ratio_ = size_garbage_ratio_`；
- precise GC 关闭时，`total_garbage_ratio_ = entry_garbage_ratio_`。

### 4.1.9 测试覆盖与当前缺口

当前代码中已有多类测试覆盖 precise GC 与 delta block 的联动：

- `VersionBuilderTest.PreciseGcByteCountFromDependence` 等测试覆盖 byte_count、fallback 和 accounting denominator。
- `VersionBuilderTest.PreciseGcTriggerUsesPickerAccountingBytes` 构造 mixed metadata 场景，验证 picker 使用 byte-based score 与 `BlobGcAccountingBytes()` 分母。
- `BlockTest.DeltaBlockTest` 直接测试 `DeltaBuilder` 的 value_size 编码和解析，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_test.cc:938`。
- `BlockBasedTableTest.DeltaBlockTest` 验证 table iterator 可通过 `GetExtendedInfo()` 读回 value_size，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/table_test.cc:4685`。
- `DBCompactionTest.DeltaBlockWithValueMetaTest` 覆盖 delta block、value meta extractor 与 compaction filter 的组合路径，见 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/db_compaction_test.cc:5025`。
- `version_edit_test.cc` 覆盖 `Dependence::byte_count` / `byte_count_entry_count` 在 VersionEdit 中的前向兼容编码和解码。
- `version_edit_test.cc` 和 `version_builder_test.cc` 共同覆盖 manifest/table-property 兼容编码、显式 byte_count、legacy fallback 与 precise score denominator。

当前值得补充的测试方向是：更多混合新旧 metadata、异常 byte_count 和不同 value-size 分布下的端到端 GC 触发验证。

### 4.1.10 真实代码链路总结

基于上述代码，precise GC 与 delta block 的真实运行链路如下：

```text
配置阶段：
  ColumnFamilyOptions.precise_gc
    -> sanitize/db_bench 路径确保 use_delta_block=true
    -> 不支持时 precise GC 被拒绝或关闭

写 SST 阶段：
  SeparateHelper::TransToSeparate()
    -> ValueMetaData.value_size = internal_key_size + value.size()
  BlockBasedTableBuilder::Add()
    -> data block 写 key/value index
    -> delta_block.Add(is_separated, value_size, value_meta)
  WriteDeltaBlock()
    -> 写 rocksdb.delta meta block

Compaction 阶段：
  BlockBasedTableIteratorBase::GetExtendedInfo()
    -> DeltaBlockReader::GetExtendedInfo()
    -> IteratorExtendedInfo.value_size
  CompactionIterator
    -> value_meta_.value_size = info.value_size
  CompactionJob
    -> dependence[file_number].byte_count += c_iter->value_size()
    -> dependence[file_number].byte_count_entry_count++

Version 安装阶段：
  VersionBuilder::SetDependence()
    -> item.bytes_depended += exact byte_count + fallback estimated bytes
  VersionBuilder::CalculateDependence()
    -> num_antiquation_bytes = BlobGcAccountingBytes() - bytes_depended

GC 选择阶段：
  GarbageFileInfo(precise_gc=true)
    -> score = num_antiquation_bytes / BlobGcAccountingBytes()
  GarbageFileInfo(precise_gc=false)
    -> score = num_antiquation / num_entries
```

可以在论文中用一句话总结为：

> Delta block makes precise GC possible by materializing per-entry separated value sizes in SSTs. During compaction, these sizes are aggregated into per-SST dependence byte records and then into per-blob obsolete-byte counters during version installation. GC picking can therefore use byte-level garbage ratios rather than entry-count approximations.

中文表述：

> delta block 通过在 SST 中物化每条 separated value 的真实大小，为 precise GC 提供基础事实。Compaction 将这些 per-entry size 聚合到 per-SST dependence byte_count，VersionBuilder 再将 dependence 聚合为 per-Blob obsolete bytes。GC picker 因此可以使用字节级垃圾比例，而不是依赖 entry 数量近似。

## 5. 相关代码文件和具体逻辑

以下文件和逻辑是论文中描述 delta block、delta separate 与 precise GC 时可以引用的实现依据。

### 5.1 Table option 与配置约束

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/terarkdb/table.h`

关键位置：`include/terarkdb/table.h:272`

逻辑：

- `BlockBasedTableOptions::use_delta_block` 控制是否启用 delta block；
- 注释说明 delta block 用于存储 value size 和 value meta；
- 该选项是 immutable；
- 当前 `precise_gc=true` 的路径会要求/自动开启 `use_delta_block`，否则不能声称字节级精确统计。

论文中可描述为：

> Delta block 是 table format 级能力。由于它决定 SST 是否携带 per-entry separated-value metadata，不能对已有文件随意切换；否则历史 SST 缺失 value size，将无法支持 byte-accurate garbage accounting。

**关联源码：** `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/terarkdb/options.h`

关键位置：

- `include/terarkdb/options.h:394`：`blob_size`
- `include/terarkdb/options.h:399`：`middle_blob_size`
- `include/terarkdb/options.h:409`：`middle_combine_level`
- `include/rocksdb/options.h`：`precise_gc`

逻辑：

- `blob_size` 决定小 value 是否留在 LSM；
- `middle_blob_size` 将 separated value 进一步划分为 middle blob 和 large blob；
- `middle_blob_size <= blob_size` 时关闭 delta separate；
- `middle_combine_level` 决定 middle value 在到达某个 Compaction input level 后是否合并回 LSM；
- `precise_gc` 控制是否使用字节级垃圾比例；如果历史数据缺失 `use_delta_block` 记录的 value size，则对缺失 entries 使用平均字节估算。

论文中可描述为：

> Precise GC 依赖 SST 中的 per-entry value size metadata，并对历史缺失 metadata 提供平均字节回退估算。因此，该能力需要与 table format 兼容性绑定，不能在缺失 delta block 的历史数据上无条件宣称完全精确。

### 5.2 Delta block 编码与结构

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_builder.h`

关键位置：

- `table/delta_builder.h:16`
- `table/delta_builder.h:31`
- `table/delta_builder.h:47`
- `table/delta_builder.h:64`

逻辑：

- `DeltaBuilder` 是 delta block 的构建器；
- delta block 不存 key，只按 key 顺序记录元数据；
- `Add()` 记录当前 entry 是否 separated，以及 separated value size / value meta；
- `AddIndexEntry()` 记录 data block 边界；
- 内部使用 bitmap 记录哪些 entry 是 KV separated。

核心意义：

> Delta block 是一个顺序对齐的 sidecar，而不是额外 key-value index。它依赖 normal SST iterator 的当前位置进行同步定位，从而降低索引开销。

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_builder.cc`

关键位置：

- `table/delta_builder.cc:39`：`DeltaBuilder::Add()`
- `table/delta_builder.cc:57`：`AddIndexEntry()`
- `table/delta_builder.cc:82`：`Finish()`
- `table/delta_builder.cc:110`：`FinishMetaBlock()`

逻辑：

- 未分离 value 只在 bitmap 中标记 `false`；
- 分离 value 写入 value size、meta size 和 meta；
- delta block 按 data block 边界组织；
- 完成时生成 delta meta block、delta data block 和 delta index block。

论文中可以强调：

> 通过 bitmap + 顺序编码，系统只为 separated entries 保存额外大小信息，避免对所有 KV 引入高额元数据开销。

### 5.3 写 SST 时生成 delta block

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_builder.cc`

关键位置：

- `table/block_based_table_builder.cc:266`：`BlockBasedTableBuilder::Rep` 持有 `DeltaBuilder delta_block`
- `table/block_based_table_builder.cc:335`：初始化 delta builder
- `table/block_based_table_builder.cc:533`：data block flush 前后记录 delta block index entry
- `table/block_based_table_builder.cc:578`：判断当前 entry 是否为 separated value
- `table/block_based_table_builder.cc:606`：调用 `delta_block.Add(...)`
- `table/block_based_table_builder.cc:1148`：写 delta block
- `table/block_based_table_builder.cc:1170`：将 delta block handle 写入 metaindex
- `table/block_based_table_builder.cc:1171`：记录 `props.delta_block_size`

具体逻辑：

1. `BlockBasedTableBuilder` 初始化时创建 `DeltaBuilder`；
2. 每次写入 data block entry 时，解析 internal key type；
3. 如果 type 是 `kTypeValueIndex` 或 `kTypeMergeIndex`，说明当前 value 已经被 KV 分离；
4. 将 `is_separated`、真实 separated value size、value meta 写入 delta block；
5. SST finish 时把 delta block 作为 meta block 写入 table；
6. metaindex 中记录 delta block handle，table properties 中记录 delta block size。

论文中可以描述为：

> Delta block 在 SST 构建时与 data block 同步生成。主 data block 保存 key 与 value index，delta block 以相同 entry 顺序保存 separated value 的真实大小，从而在不改变主查找路径的情况下补齐后台管理所需的信息。

### 5.4 读取 delta block 并供 Compaction 使用

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/block_based_table_reader.cc`

关键位置：

- `table/block_based_table_reader.cc:1476`：打开 SST 时从 metaindex 查找 delta block
- `table/block_based_table_reader.cc:1484`：调用 `ReadDeltaBlock()`
- `table/block_based_table_reader.cc:1895`：读取 delta index block
- `table/block_based_table_reader.cc:3359`：初始化 delta meta block
- `table/block_based_table_reader.cc:3404`：初始化具体 delta data block
- `table/block_based_table_reader.cc:3530`：从 delta block 读取 `IteratorExtendedInfo`
- `table/block_based_table_reader.cc:3567`：仅在 `for_compaction && delta_index_block` 时创建 `DeltaBlockReader`

逻辑：

- Table reader 打开时识别 SST 是否包含 `rocksdb.delta`；
- 对于 compaction iterator，创建 `DeltaBlockReader`；
- 普通 foreground read 通常不依赖 delta block；
- Compaction iterator 通过 `GetExtendedInfo()` 获取当前 entry 的 value size / value meta。

论文中可以强调：

> Delta block 主要服务后台 Compaction 和 GC 元数据维护，而不是替代前台 Get 的 value 读取路径。前台读取仍通过 value index 定位 Blob，delta block 则让后台流程避免不必要的 Blob value fetch。

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_reader.h` 和 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/table/delta_reader.cc`

关键位置：

- `table/delta_reader.h:16`：`DeltaBlockReader`
- `table/delta_reader.h:38`：按 block index / entry index 定位
- `table/delta_reader.h:47`：`GetExtendedInfo()`
- `table/delta_reader.cc:7`：`SeekForBlockIndex()`
- `table/delta_reader.cc:19`：`SeekForEntryIndexWithinBlock()`
- `table/delta_reader.cc:73`：返回 value size / value meta

逻辑：

- delta reader 与 data block iterator 同步前进；
- 使用 block 编号和 entry 在 block 内的位置定位对应 delta entry；
- 借助 bitmap/rank 找到 separated entry 在 delta data 中的位置；
- 将 value size / value meta 返回给上层 iterator。

### 5.5 Compaction 中消费 delta block 的 value size

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_iterator.cc`

关键位置：

- `db/compaction_iterator.cc:898`：读取 input value
- `db/compaction_iterator.cc:900`：调用 `input_->GetExtendedInfo(&info)`
- `db/compaction_iterator.cc:902`：写入 `value_meta_.value_size` 和 `value_meta_.meta_data`
- `db/compaction_iterator.cc:715`：决定是否保持 inline
- `db/compaction_iterator.cc:722`：将 value 写入新 Blob 并写 value index
- `db/compaction_iterator.cc:761`：处理已经是 value index 的 separated value
- `db/compaction_iterator.cc:788`：middle value 可 combine 回 LSM
- `db/compaction_iterator.cc`：缺少 value size 时保留 entry_count，byte_count 覆盖范围由 `byte_count_entry_count` 标识

具体逻辑：

1. Compaction iterator 从 input iterator 读取 key/value；
2. 调用 `GetExtendedInfo()` 获取 delta block 中的 value size / value meta；
3. 根据 value size、`blob_size`、`middle_blob_size`、`middle_combine_level` 等决定输出形态；
4. 如果保持 separated，则使用 value size 更新 output SST 的 dependence 统计；
5. 如果 delta block 缺失 value size，则在 VersionBuilder 中对缺失 entries 使用平均字节回退估算。

论文中可描述为：

> Delta block 将 Compaction 对 Blob value 的读取需求转化为对 SST-side metadata 的顺序访问。对于保持 separated 的 value，Compaction 可以直接转发 value index 并维护精确字节统计，而无需读取真实 value。

### 5.6 Value index 编码与 value meta 来源

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/dbformat.h` 和 `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/dbformat.cc`

关键位置：

- `db/dbformat.h:126`：`kTypeValueIndex`
- `db/dbformat.h:127`：`kTypeMergeIndex`
- `db/dbformat.h:929`：`SeparateHelper`
- `db/dbformat.h:933`：`SeparateHelper::ValueMetaData`
- `db/dbformat.h:989`：`do_middle_separate()`
- `db/dbformat.cc:206`：`SeparateHelper::TransToSeparate()`
- `db/dbformat.cc:212`：计算 separated value size
- `db/dbformat.cc:216`：调用 value meta extractor
- `db/dbformat.cc:246`：编码 file number + block handle

逻辑：

- 被 KV 分离的 value 在 normal SST 中使用 `kTypeValueIndex` 或 `kTypeMergeIndex`；
- `SeparateHelper::TransToSeparate()` 将真实 value 写入 Blob，并把 normal SST 中的 value 改写为 index；
- `ValueMetaData` 携带 value size、block handle 和 value meta；
- delta block 记录的 value size / value meta 来自这里形成的 metadata。

### 5.7 Dependence 聚合与 precise GC garbage ratio

**关联源码：** `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/terarkdb/types.h`

关键位置：`include/terarkdb/types.h:48`

```cpp
struct Dependence {
  uint64_t file_number;
  uint64_t entry_count;
  uint64_t byte_count;
  uint64_t byte_count_entry_count;
};
```

逻辑：

- `file_number` 表示被 normal SST 引用的 Blob 文件；
- `entry_count` 表示引用了多少条 separated value；
- `byte_count` 表示已知 value-size metadata 覆盖的引用字节数；
- `byte_count_entry_count` 表示 `byte_count` 覆盖的 entry 数，用于识别历史/缺失 metadata entries。

论文中可描述为：

> Dependence 是 normal SST 到 Blob 文件的版本化引用摘要。delta block 提供 per-entry size，dependence 将这些 size 按 Blob file number 聚合为 per-SST live-byte contribution。

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_job.cc`

关键位置：

- `db/compaction_job.cc:2144`：输出 SST 记录对 Blob 的 dependence
- `db/compaction_job.cc:2151`：dependence 中累计 `c_iter->value_size()`
- `db/compaction_job.cc:2402`：`ProcessGarbageCollection()`
- `db/compaction_job.cc:2402`：GC 读取候选 Blob、重写存活 value 并重建 dependence / byte accounting

逻辑：

- Compaction 输出 normal SST 时，根据当前 entry 仍引用的 Blob file number 更新 dependence；
- entry 数累计到 `entry_count`；
- value size 累计到 `byte_count`，对应 entry 数累计到 `byte_count_entry_count`；
- 这些信息后续被 VersionBuilder 用来计算每个 Blob 的 live bytes。

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/version_builder.cc`

关键位置：

- `db/version_builder.cc:302`：`SetDependence()`
- `db/version_builder.cc:342`：`CalculateDependence()`
- `db/version_builder.cc:408`：计算 Blob antiquated size
- `db/version_builder.cc:451`：写入 `num_antiquation`
- `db/version_builder.cc`：写入 `num_antiquation_bytes`

逻辑：

1. VersionBuilder 安装新版本时遍历当前版本中所有 normal SST；
2. 根据每个 SST 的 `prop.dependence` 聚合同一 Blob 被多少 entry、多少字节引用；
3. 对 Blob 文件计算：

   ```text
   num_antiquation_bytes = BlobGcAccountingBytes() - bytes_depended
   ```

4. 将 entry 维度垃圾数写入 `FileMetaData::num_antiquation`；
5. 将字节维度垃圾数写入 `FileMetaData::num_antiquation_bytes`。

论文中可描述为：

> VersionBuilder 是将 per-SST dependence 转化为 per-Blob garbage view 的关键位置。它利用当前 version 的可见文件集合判断哪些 Blob 引用仍然存活，并在线物化 Blob 级 obsolete bytes。

#### `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_picker.cc`

关键位置：

- `db/compaction_picker.cc:37`：`GarbageFileInfo`
- `db/compaction_picker.cc`：precise 模式使用 `num_antiquation_bytes / BlobGcAccountingBytes()`
- `db/compaction_picker.cc`：非 precise 模式使用 `num_antiquation / num_entries`
- `db/compaction_picker.cc:821`：`PickGarbageCollection()`
- `db/compaction_picker.cc:872`：垃圾比例不足时不触发 GC
- `db/compaction_picker.cc:969`：按 GC eligibility、candidate score 与 overlap/adjoining 关系扩展候选集合

逻辑：

- GC picker 为每个候选 Blob 计算 score；
- 如果 `precise_gc` 开启，则 score 是字节维度垃圾比例；
- 如果关闭，则 score 是 entry 维度垃圾比例；
- 分数高且超过 `blob_gc_ratio` 的 Blob 更容易被选为 GC 候选。

论文中可描述为：

> Precise GC 将候选选择从 entry-count based policy 替换为 byte-benefit based policy。它不改变 GC 的文件重写流程，而是提升 GC picker 对回收收益的估计精度。

### 5.8 Delta separate 与 value placement

delta separate 是在基础 KV separation 之上的 value-size-aware placement 策略：系统不再把所有超过 `blob_size` 的 value 都放入同一类 Blob，而是继续按 `middle_blob_size` 将 separated value 划分为 middle value 和 large value。

整体分类如下：

```text
value.size < blob_size
  -> inline value，直接写入 normal SST

blob_size <= value.size < middle_blob_size
  -> middle value，写入 middle Blob

value.size >= middle_blob_size
  -> large value，写入 large Blob
```

其中 middle Blob 和 large Blob 的核心区别是：

| 类型 | value 范围 | 当前实现承载方式 | 生命周期策略 |
| --- | --- | --- | --- |
| middle value | `blob_size <= value.size < middle_blob_size` | 普通 separated Blob + delta metadata 标识 value size | 前期分离以降低上层 compaction 写放大；到达较深层后可合并回 LSM |
| large value | `value.size >= middle_blob_size` | 普通 separated Blob + delta metadata 标识 value size | 长期保持分离，主要通过 Blob GC 回收空间 |

因此，middle Blob 可以理解为“中等 value 的短期 Blob 化”，large Blob 可以理解为“大 value 的长期 Blob 化”。

#### 5.8.1 配置入口：`blob_size`、`middle_blob_size` 与 `middle_combine_level`

**关联源码：** `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/include/terarkdb/options.h`

关键位置：

- `include/terarkdb/options.h:394`：`blob_size`
- `include/terarkdb/options.h:399`：`middle_blob_size`
- `include/terarkdb/options.h:405`：`middle_blob_size <= blob_size` 时关闭 delta separate
- `include/terarkdb/options.h:409`：`middle_combine_level`

核心逻辑：

- `blob_size` 是 KV separation 的基础阈值，小于该阈值的 value 不分离；
- `middle_blob_size` 是 middle / large 的分界线；
- 当 `middle_blob_size <= blob_size` 时，middle 区间不存在，因此关闭 delta separate；
- `middle_combine_level` 控制 middle value 何时可以被合并回 normal SST。

代码注释中已经表达了该语义：

```cpp
// Don't separate Value if value.size < blob_size
size_t blob_size = 512;

// Separate to blob with large value size if value.size >= middle_blob_size &&
// value.size >= std::max(blob_size, key_size / large_key_size_ratio).
//
// Separate to blob with middle value size if value.size < middle-blob_size &&
// value.size >= std::max(blob_size, key_size / large_key_size_ratio)
//
// if middle_blob_size <= blob_size, will turn off the delta separate feature
size_t middle_blob_size = 0;

// For blob with middle value size,will be combine to LSM-tree during
// compaction If compaction-input-level >= middle_combine_level
size_t middle_combine_level = 0;
```

论文中可以描述为：

> Delta separate adds a second size threshold, `middle_blob_size`, on top of the original `blob_size`. Values below `blob_size` remain inline, values between the two thresholds are temporarily separated as middle blobs, and values above `middle_blob_size` are treated as large blobs and remain separated.

#### 5.8.2 当前文件类型建模

当前代码没有为 middle/large value 引入额外的 Blob 文件类型枚举。实现重点是：normal SST 记录 value index，delta metadata 记录每个 separated entry 的 value size / value meta；middle/large 的生命周期决策由 `middle_blob_size`、`middle_combine_level` 和 compaction iterator 的 combine 逻辑表达，而不是通过额外文件类型表达。

#### 5.8.3 middle / large 判断函数

**关联源码：** `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/dbformat.h`

关键位置：`db/dbformat.h:989`

代码：

```cpp
static bool do_middle_separate(int value_size, int middle_blob_size) {
  return value_size < middle_blob_size;
}
```

该函数本身只判断 `value_size < middle_blob_size`。需要注意的是，它通常在“已经决定该 value 需要 KV 分离”的前提下被调用。因此真实语义是：

```text
已满足 value.size >= blob_size 的 separated value 中：

value.size < middle_blob_size
  -> middle value

value.size >= middle_blob_size
  -> large value
```

这也是为什么 `middle_blob_size <= blob_size` 会关闭 delta separate：如果 middle 阈值不大于基础分离阈值，那么不会存在 `blob_size <= value.size < middle_blob_size` 的 middle 区间。

#### 5.8.4 Flush / BuildTable 阶段：按 hotness route 写 Blob，delta metadata 记录 value size

**关联源码：** `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/builder.cc`

关键位置：

- `db/builder.cc`：Flush/BuildTable 阶段的 value separation 与 hotness route 选择
- `db/builder.cc:347`：为 Blob 创建 `TableBuilder`
- `db/builder.cc:364`：向 Blob builder 写入真实 value
- `db/builder.cc:371`：调用 `SeparateHelper::TransToSeparate()` 将 normal SST 中的 value 转换为 value index
- `db/builder.cc:391`：初始化 large helper
- `db/builder.cc:392`：初始化 middle helper
- `db/builder.cc:460`：结束 large Blob 输出
- `db/builder.cc:461`：结束 middle Blob 输出
- `db/builder.cc:471`：normal SST property 记录对 Blob 的 dependence

关键代码片段：

```cpp
bool is_middle = SeparateHelper::do_middle_separate(
    value.size(), mutable_cf_options.middle_blob_size);
BuilderCompactionHelper& separate_helper =
    is_middle ? middle_separate_helper : large_separate_helper;
```

当前实现不根据 `is_middle` 写入额外文件类型。Flush/BuildTable 的布局重点是 hotness route（hot/warm/cold）与 separated value index 的生成；middle/large 的后续差异由 compaction combine 条件和 delta metadata 表达。

```cpp
Flush route -> corresponding separated Blob output;
delta block -> per-entry value_size/value_meta;
```

写入流程可以概括为：

```text
Flush / BuildTable
  -> CompactionIterator 产出 KV
  -> trans_to_separate()
     -> hotness tracker chooses hot/warm/cold separated output
     -> blob_builder->Add(key, value, meta)
     -> SeparateHelper::TransToSeparate(...)
        将 normal SST 中的 value 改写为 value index / value meta
  -> normal SST property 中记录 dependence
```

设计含义：

- Flush 阶段的 Blob 输出由 hotness route 决定；
- normal SST 只保留 value index；
- dependence 记录 normal SST 对这些 Blob 文件的引用关系，后续 GC 和 precise GC 都依赖该引用关系。

#### 5.8.5 Compaction 阶段：middle value 到达深层后可合并回 LSM

**关联源码：** `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_iterator.cc`

关键位置：

- `db/compaction_iterator.cc:103`：读取 compaction 的 separation type
- `db/compaction_iterator.cc:107`：初始化是否执行 value separation
- `db/compaction_iterator.cc:111`：初始化是否执行 blob rebuild
- `db/compaction_iterator.cc:113`：初始化是否执行 combine value
- `db/compaction_iterator.cc:114`：根据 `middle_combine_level` 判断是否需要 combine middle value
- `db/compaction_iterator.cc:709`：处理普通 value 时判断是否需要 combine middle value
- `db/compaction_iterator.cc:715`：小 value 或需要 combine 的 middle value 保持 / 转回 inline
- `db/compaction_iterator.cc:722`：需要分离时写入新 Blob
- `db/compaction_iterator.cc:761`：处理已经 separated 的 value index
- `db/compaction_iterator.cc:765`：对 separated value index 判断是否为 middle value 且需要 combine
- `db/compaction_iterator.cc:788`：将 separated middle value 读出并写回 normal SST

初始化逻辑：

```cpp
need_combine_middle_value_ =
    compaction_ != nullptr &&
    compaction_->level() >= blob_config_.middle_combine_level;
```

处理普通 value 时，如果 value 已经到达 combine level，并且它属于 middle value，则直接写回 normal SST：

```cpp
bool do_combine_value = need_combine_middle_value_ &&
                        SeparateHelper::do_middle_separate(
                            value_.size(), blob_config_.middle_blob_size);

if (value_.size() < blob_config_.blob_size ||
    (current_user_key_.size() << 16) >
        value_.size() * blob_large_key_ratio_lsh16_ ||
    do_combine_value) {
  zero_sequence();
}
```

处理已经 separated 的 value index 时，如果该 value 是 middle value 且达到 combine level，就把 value index 改回普通 value 类型：

```cpp
bool do_combine_value = need_combine_middle_value_ &&
                        SeparateHelper::do_middle_separate(
                            value_size(), blob_config_.middle_blob_size);

if (do_rebuild_blob) {
  ...
} else if (do_combine_value_ || do_combine_value) {
  // read separated value from the blob and write it to output sst.
  ikey_.set_type(ikey_.type() == kTypeValueIndex ? kTypeValue : kTypeMerge);
  current_key_.UpdateInternalKey(ikey_.tag);
  zero_sequence();
} else {
  // keep separated value in the blob and write value index to output sst.
  ...
}
```

这体现了 middle Blob 与 large Blob 生命周期上的根本区别：

- middle value：浅层可分离，深层可合并回 LSM；
- large value：一般继续保持 separated，避免大 value 在 LSM compaction 中反复搬运。

#### 5.8.6 GC picker：按候选 score / overlap 扩展 GC，不按额外文件类型强制分组

**关联源码：** `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_picker.cc`

关键位置：

- `db/compaction_picker.cc:926`：读取目标 Blob 的平均 value size
- `db/compaction_picker.cc`：为候选 Blob 构造 `GarbageFileInfo`
- `db/compaction_picker.cc`：根据 GC eligibility、score、overlap/adjoining 关系扩展候选集合

关键逻辑：

```cpp
if (f->is_gc_permitted() && !f->being_compacted) {
  GarbageFileInfo gc_blob(f, mutable_cf_options.precise_gc);
  ...
}
```

设计含义：

- 未开启 delta separate 时，所有 Blob 基本按同一类处理；
- 当前代码不维护额外文件类型分组不变量；
- value placement 的稳定性主要依赖 compaction iterator 的 middle combine 逻辑与 precise GC 的候选收益估计。

#### 5.8.7 GC job：重写 Blob 并维护 dependence / byte accounting

**关联源码：** `/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb/db/compaction_job.cc`

关键位置：

- `db/compaction_job.cc:2402`：`ProcessGarbageCollection()`
- `db/compaction_job.cc`：`ProcessGarbageCollection()`
- `db/compaction_job.cc`：GC 输出重新生成 normal SST dependence 与 Blob 输出

关键代码：

```cpp
ProcessGarbageCollection()
  -> read live entries from selected Blob candidates
  -> rewrite surviving values as needed
  -> rebuild dependence byte accounting for new outputs
```

输出 Blob builder 继承当前 GC/rewrite 所需的普通 Blob 输出语义：

```cpp
sub_compact->blob_builder.reset(NewTableBuilder(...));
```

设计含义：

- GC 不只是回收空间，也需要维护新 normal SST 到 Blob 的 dependence；
- precise GC 的 byte_count metadata 在 rewrite 后继续为后续候选选择提供收益估计。

#### 5.8.8 测试覆盖

当前仓库没有独立的 delta-separate 单测文件；相关覆盖分布在 compaction、version_builder、block/table delta metadata 与 hotness routing/cache 测试中。

关键位置：

- `VersionBuilderTest.*PreciseGc*`：覆盖 byte_count、fallback、accounting denominator；
- `HotnessTrackerTest` / `AdaptiveHotnessRoutingTest`：覆盖 hotness route 对 flush layout 的影响；
- `GarbageAwareCacheTest`：覆盖 GC-aware block cache admission/aging/obsolete marking；
- delta block reader/builder 相关测试覆盖 per-entry value-size metadata 编解码。

典型测试语义：

- precise GC metadata 的核心测试语义是 byte-level accounting 相比 entry-count accounting 在 mixed-value 场景下更准确；
- hotness routing 的核心测试语义是 repeated-write / compaction-feedback keys 进入 hot route，cold one-hit keys 不污染 hot set；
- GC-aware cache 的核心测试语义是高垃圾比例 data blocks 被降级/淘汰，metadata/high-priority blocks 被保护。

#### 5.8.9 论文中的推荐表述

可以将该设计表述为：

> Delta metadata and delta separate support size-aware value management on top of KV separation. Instead of treating all separated values as equal for GC accounting, the system records per-entry value sizes in SST-side metadata and uses that information during compaction/version installation to compute byte-level live/obsolete bytes. Combined with hotness routing, this lets the implementation optimize both layout and GC/cache decisions without adding foreground Blob reads.

中文版本：

> Delta metadata / delta separate 是建立在 KV separation 之上的 value-size-aware 管理策略。系统不再仅用 entry 数估算 separated value 的 GC 收益，而是在 SST-side metadata 中记录 per-entry value size，并在 compaction / version installation 中聚合为 byte-level live/obsolete bytes。结合 hotness routing 后，当前实现可以在不增加前台 Blob 读取的情况下优化布局、GC 与 cache 决策。

## 6. 论文中的推荐结构

如果把这部分写入论文正文，建议放在设计和实现章节中。

### 6.1 Design 章节建议小节

#### Delta Metadata for Byte-accurate Garbage Accounting

该小节说明：

1. KV separation 后 normal SST 只保存 value pointer，丢失真实 value size；
2. entry-count garbage ratio 在 value size skew 下失真；
3. delta block 以 per-entry sidecar 方式保存 separated value size；
4. VersionBuilder 将 per-entry size 聚合为 per-Blob live bytes；
5. GC picker 使用 byte-level garbage ratio 选择候选 Blob。

可以使用如下段落：

> To avoid entry-count based misprediction under skewed value sizes, we introduce a delta metadata block for each SST. The delta block is aligned with SST entries and stores whether an entry is value-separated and, if so, the actual separated value size and optional value metadata. During compaction, this metadata is propagated into per-SST dependence records. During version installation, the dependence records are aggregated by blob file number to derive per-blob live bytes and obsolete bytes. This enables byte-accurate garbage accounting without fetching blob values during GC candidate selection.

中文版本：

> 为避免 value size 偏斜场景下基于 entry 数量的垃圾比例误判，我们为每个 SST 引入 delta metadata block。该结构与 SST entry 顺序对齐，记录每个 entry 是否为 separated value，以及 separated value 的真实大小和可选 value meta。Compaction 过程中，这些元信息被写入 per-SST dependence；版本安装时，VersionBuilder 再按 Blob file number 聚合 dependence，得到每个 Blob 的 live bytes 与 obsolete bytes。这样，GC picker 可以在不读取 Blob value 的情况下进行字节级精确垃圾统计。

#### Size- and Level-aware Value Placement

该小节说明：

1. 普通 KV separation 一刀切地把大于阈值的 value 全部放入 Blob；
2. 中等 value 长期放 Blob 可能增加读放大和 GC 成本；
3. delta separate 将 value 分成 inline、middle、large 三类；
4. middle value 浅层分离、深层合并；
5. large value 长期分离并交给 precise GC。

可以使用如下段落：

> We further extend value separation with a size- and level-aware placement policy. Values smaller than `blob_size` remain inline in the LSM-tree. Values larger than `middle_blob_size` are treated as large blobs and remain separated. Values between the two thresholds are treated as middle blobs: they are separated in upper levels to reduce early compaction write amplification, but are recombined into the LSM-tree when compaction reaches `middle_combine_level`, reducing long-term read amplification and GC overhead.

## 7. 实验建议

为了让该机制在论文中站得住，需要至少设计以下实验。

### 7.1 Entry-based GC vs precise byte-based GC

对比对象：

```text
A. precise_gc=false，使用 num_antiquation / num_entries
B. precise_gc=true，使用 num_antiquation_bytes / BlobGcAccountingBytes()
```

工作负载：

- value size 高度偏斜；
- 少量大 value 高频更新；
- 大量小 value 高频更新；
- 混合更新。

指标：

- GC picked file quality；
- reclaimed bytes per GC；
- GC write bytes；
- GC read bytes；
- space amplification；
- foreground p99 latency。

预期结论：

> precise GC 在 value size skew 场景下能更准确选择高收益 Blob，减少低收益 GC，提高单位 GC I/O 的空间回收效率。

### 7.2 Delta block 对 Compaction read amplification 的影响

对比对象：

```text
A. use_delta_block=false 或历史文件缺失 value size，VersionBuilder 使用平均字节回退估算
B. use_delta_block=true，Compaction 从 delta block 获取 value size/meta 并形成 byte_count
```

指标：

- Compaction read bytes；
- Blob read bytes during compaction；
- Compaction duration；
- CPU overhead；
- delta block size overhead。

预期结论：

> delta block 以较小的 SST metadata overhead 换取后台 Compaction 中显著减少的 Blob value fetch。

### 7.3 Delta separate 的读写放大折中

对比对象：

```text
A. 无 KV separation
B. 普通 KV separation，单一 blob_size 阈值
C. delta separate，inline + middle blob + large blob
```

指标：

- write amplification；
- read amplification；
- Get latency；
- Compaction bytes；
- GC bytes；
- block cache hit ratio；
- space amplification。

预期结论：

> middle value 浅层分离可减少早期 Compaction 写放大，深层合并可降低长期读放大和 Blob GC 成本。

## 8. 最终建议表述

论文中建议将该机制命名为：

> Delta Metadata Guided Value Separation and Garbage Collection

中文可写为：

> Delta 元数据引导的值分离与垃圾回收机制

或者更强调精确 GC：

> Metadata-assisted Exact Blob Garbage Accounting

中文可写为：

> 元数据辅助的精确 Blob 垃圾统计机制

最终贡献点可以写成：

> 我们提出一种元数据辅助的精确 Blob 垃圾统计机制。该机制在 SST 侧维护与 entry 顺序对齐的 delta metadata block，记录 separated value 的真实大小和 value meta，并在版本安装阶段将 per-entry size 聚合为每个 Blob 的 live bytes 与 obsolete bytes。基于该机制，GC picker 能够使用字节级垃圾比例选择候选 Blob，避免 value size 偏斜场景下基于 entry 数量的误判，同时减少 Compaction 和 GC 对 Blob value 的额外读取。

如果与 delta separate 一起作为更完整的设计，可以写成：

> 我们进一步提出一种 size- and level-aware value separation 策略，将 value 划分为 inline、middle blob 和 large blob。Middle value 在浅层分离以降低写放大，在深层合并回 LSM-tree 以降低长期读放大；large value 长期分离并通过 delta metadata 支撑的 byte-accurate GC 回收。该设计在写放大、读放大和空间放大之间实现了更细粒度的权衡。
