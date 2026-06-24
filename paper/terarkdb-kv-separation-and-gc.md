# TerarkDB 键值分离与 Blob GC 机制梳理

本文记录 TerarkDB 中键值分离策略、分离条件、Flush/Compaction 生成 kSST 与 vSST 的流程、分离 value 的有序存储与短 scan 支持，以及垃圾回收的触发条件和执行策略。

## 1. 总体模型

TerarkDB 的键值分离不是把 value 写入无序 append-only blob log，而是把大 value 写入独立的 SST-like 文件，即 vSST / Blob SST。主 LSM 中保留 kSST，kSST 中的 value 被替换为 value index。

```text
kSST:
  internal key -> value_index(blob_file_number + optional metadata / block handle)

vSST / Blob SST:
  same internal key -> real value
```

value index 对应的 internal key type 是 `kTypeValueIndex` / `kTypeMergeIndex`。读取时先在 kSST 找到 value index，再根据 file number 定位 vSST，并按原 user key 与 sequence 取回真实 value。

关键代码位置：

- `third-party/terarkdb/include/rocksdb/options.h:305`：`blob_size` 控制基础分离阈值。
- `third-party/terarkdb/db/compaction_iterator.cc:864`：`CompactionIterator::PrepareOutput()` 中决定 value 保持 inline、分离、重建或合回。
- `third-party/terarkdb/db/dbformat.cc:203`：`SeparateHelper::TransToSeparate()` 编码 value index。
- `third-party/terarkdb/db/version_set.cc:1321`：`Version::TransToCombined()` 将 value index 转换为 lazy real value。
- `third-party/terarkdb/db/version_set.cc:1279`：`Version::fetch_buffer()` 真正从 vSST 读取 separated value。

## 2. value 分离条件

### 2.1 基础阈值：`blob_size`

`blob_size` 的语义是：当 `value.size < blob_size` 时不分离；设置为 `size_t(-1)` 时禁用键值分离。

```cpp
// Don't separate Value if value.size < blob_size
// Set size_t(-1) to disable Key Value separation
size_t blob_size = 512;
```

位置：`third-party/terarkdb/include/rocksdb/options.h:305`。

因此基础规则是：

```text
value.size < blob_size      -> value 保留在 kSST
value.size >= blob_size     -> value 可以被分离到 vSST
blob_size == size_t(-1)     -> 关闭键值分离
```

### 2.2 key 过大时不分离

TerarkDB 还使用 `blob_large_key_ratio` 避免 key 相对 value 过大时分离收益不足。

```cpp
// Don't separate Value if key.size > value.size * blob_large_key_ratio
double blob_large_key_ratio = 0.25;
```

位置：`third-party/terarkdb/include/rocksdb/options.h:356`。

实际判断在 `CompactionIterator::PrepareOutput()` 中：

```cpp
const bool key_too_large =
    blob_config_.blob_size != 0 && blob_large_key_ratio_lsh16_ > 0 &&
    (current_user_key_.size() << 16) >
        value_.size() * blob_large_key_ratio_lsh16_;
```

位置：`third-party/terarkdb/db/compaction_iterator.cc:882`。

如果 value 太小、key 太大，或者 middle value 到达合回层级，则 value 保持 inline：

```cpp
if (value_.size() < blob_config_.blob_size || key_too_large ||
    do_combine_middle_value) {
  // Keep value combined. value too small or key too large
```

位置：`third-party/terarkdb/db/compaction_iterator.cc:887`。

### 2.3 middle value / delta separate 策略

TerarkDB 支持 middle-value delta separate 策略：

- `enable_delta_separate`：总开关，见 `third-party/terarkdb/include/rocksdb/options.h:310`。
- `[blob_size, middle_blob_size)`：被视为 middle separated value，见 `third-party/terarkdb/include/rocksdb/options.h:315`。
- `middle_combine_level`：compaction 输入层级达到该层后可将 middle value 合回 kSST，见 `third-party/terarkdb/include/rocksdb/options.h:321`。

判断逻辑：

```cpp
const bool do_combine_middle_value =
    need_combine_middle_value_ &&
    SeparateHelper::do_middle_separate(value_.size(),
                                       blob_config_.middle_blob_size);
```

位置：`third-party/terarkdb/db/compaction_iterator.cc:878`。

该策略的意义是：中等大小 value 可以在浅层先分离以降低 compaction 搬迁成本，进入更深层后再合回 kSST，以减少长期 value index / vSST 依赖。

## 3. Flush 如何生成 kSST 和 vSST

Flush 入口是 `FlushJob::WriteLevel0Table()`，它把 memtable iterator 传给 `BuildTable()`。

```cpp
s = BuildTable(..., &meta_, ..., &table_properties_, 0 /* level */, ...);
```

位置：`third-party/terarkdb/db/flush_job.cc:386`。

`BuildTable()` 的核心流程：

1. 创建 normal SST，也就是 kSST。
2. 扫描 memtable 的有序 internal iterator。
3. 对满足分离条件的 value，先写入 vSST / Blob SST。
4. 调用 `SeparateHelper::TransToSeparate()` 将 kSST 中的 value 改写成 value index。
5. kSST 的 table properties 记录其依赖的 vSST。
6. Flush 安装时，`meta_[0]` 进入 L0，其余 vSST 进入 hidden level `-1`。

Flush 侧分离逻辑在 `trans_to_separate` lambda 中，位置：`third-party/terarkdb/db/builder.cc:325`。

### 3.1 创建 vSST

当需要新 vSST 时，会分配新的 file number，并使用普通 `NewTableBuilder()` 创建 builder：

```cpp
blob_meta.fd = FileDescriptor(versions_->NewFileNumber(), ...);
...
bstate.builder.reset(NewTableBuilder(
    ..., int_tbl_prop_collector_factories_for_blob, ..., -1 /* level */, ...,
    true));
```

位置：`third-party/terarkdb/db/builder.cc:365` 与 `third-party/terarkdb/db/builder.cc:385`。

这里的 `level = -1` 表示 hidden blob level。

### 3.2 先写 vSST，再改写 kSST value

真实 value 被写入 vSST：

```cpp
status = blob_builder->Add(key, value);
```

或带 metadata：

```cpp
status = blob_builder->Add(key, value, *value_meta);
```

位置：`third-party/terarkdb/db/builder.cc:393`。

随后将 kSST 中的 value 改成 value index：

```cpp
status = SeparateHelper::TransToSeparate(
    key, value, blob_meta.fd.GetNumber(), value_meta, ...);
```

位置：`third-party/terarkdb/db/builder.cc:419`。

### 3.3 安装位置

Flush 完成后：

```cpp
edit_->AddFile(i == 0 ? 0 : -1, ...)
```

位置：`third-party/terarkdb/db/flush_job.cc:435`。

因此：

- `meta_[0]` 是 kSST，进入 L0。
- `meta_[1..]` 是 vSST / Blob SST，进入 level `-1`。

## 4. Compaction 如何生成 kSST 和 vSST

Compaction 与 Flush 类似，但还需要处理已有 value index、blob rebuild、middle value combine、普通 compaction 与 GC 的交互。

Compaction 侧分离逻辑在 `CompactionJob::ProcessKeyValueCompaction()` 的 `trans_to_separate` lambda 中，位置：`third-party/terarkdb/db/compaction_job.cc:1864`。

### 4.1 写入新的 vSST

当当前 blob builder 超过 `target_blob_file_size` 时，先 finish 当前 vSST：

```cpp
if (blob_builder != nullptr &&
    blob_builder->FileSize() > target_blob_file_size) {
  s = FinishCompactionOutputBlob(...);
}
```

位置：`third-party/terarkdb/db/compaction_job.cc:1869`。

如果没有 blob builder，则打开新的 vSST：

```cpp
s = OpenCompactionOutputBlob(sub_compact);
```

位置：`third-party/terarkdb/db/compaction_job.cc:1874`。

真实 value 写入 vSST：

```cpp
s = blob_builder->Add(key, value);
```

位置：`third-party/terarkdb/db/compaction_job.cc:1879`。

然后 value 被改写为 value index：

```cpp
s = SeparateHelper::TransToSeparate(
    key, value, blob_meta->fd.GetNumber(), value_meta, ...);
```

位置：`third-party/terarkdb/db/compaction_job.cc:1892`。

### 4.2 kSST 记录对 vSST 的依赖

如果 compaction iterator 输出的是 `kTypeValueIndex` 或 `kTypeMergeIndex`，则 output kSST 记录其依赖的 vSST，并统计 entry count 与 byte count：

```cpp
if (c_iter->ikey().type == kTypeValueIndex ||
    c_iter->ikey().type == kTypeMergeIndex) {
  auto& acc = dependence[value.file_number()];
  ++acc.entry_count;
  const uint64_t value_size = c_iter->value_size();
  if (value_size > 0) {
    acc.byte_count += value_size;
    ++acc.byte_count_entry_count;
  }
}
```

位置：`third-party/terarkdb/db/compaction_job.cc:2042`。

这部分是 byte-precise GC 的基础：kSST 不只记录“引用了多少 separated entries”，还记录“引用了多少 separated bytes”。

### 4.3 kSST 写入

最终 normal output builder 写入当前 key/value：

```cpp
status = sub_compact->builder->Add(key, value, c_iter->value_meta());
```

位置：`third-party/terarkdb/db/compaction_job.cc:2083`。

如果 value 已分离，此时写入 kSST 的 value 是 value index，而不是原始 value。

### 4.4 Compaction 输出安装

普通 compaction output 加到 compaction output level：

```cpp
compaction->edit()->AddFile(compaction->output_level(), out.meta);
```

位置：`third-party/terarkdb/db/compaction_job.cc:3317`。

blob output / vSST 加到 hidden level `-1`：

```cpp
compaction->edit()->AddFile(-1, out.meta);
```

位置：`third-party/terarkdb/db/compaction_job.cc:3325`。

## 5. value 分离后如何保持有序并支持短 scan

TerarkDB 的关键设计是：separated value 被写入 SST-like 的 vSST，而不是无序 blob log。因此 value 分离后仍保持 key order。

### 5.1 kSST 保持 key 有序

Flush 与 compaction 的输入 iterator 都是 internal key 有序的。TerarkZipTableBuilder 也显式检查 key 单调递增：

```cpp
if (properties_.num_entries > 0 &&
    ioptions_.internal_comparator.Compare(key, prevKey_.Encode()) <= 0) {
  return Status::Corruption("TerarkZipTableBuilder::Add: overlapping key");
}
```

位置：`third-party/terarkdb/table/terark_zip_table_builder.cc:397`。

value 分离只改变 value 内容与 key type，不改变 key 本身，因此 kSST 的 key order 不受影响。

### 5.2 vSST 也按同一 internal key 顺序写入

Flush 中，value 在同一有序扫描过程中写入 vSST：

```cpp
blob_builder->Add(key, value)
```

位置：`third-party/terarkdb/db/builder.cc:393`。

Compaction 中同理：

```cpp
s = blob_builder->Add(key, value);
```

位置：`third-party/terarkdb/db/compaction_job.cc:1879`。

因此 vSST 内部同样是按 internal key 有序的 table 文件。

### 5.3 scan 路径：扫 kSST，按需 lazy fetch vSST

Iterator 读到 value index 时，会调用 `TransToCombined()` 转换为真实 value：

```cpp
if (pikey.type != kTypeValueIndex && pikey.type != kTypeMergeIndex) {
  return iter_->value();
}
LazyBuffer v = separate_helper_->TransToCombined(...);
```

位置：`third-party/terarkdb/table/iterator.cc:114`。

Get 路径中，`GetContext::SaveValue()` 遇到 `kTypeValueIndex` 后也会转成 combined value：

```cpp
case kTypeValueIndex:
  value = separate_helper_->TransToCombined(user_key_,
                                            parsed_key.sequence, value);
```

位置：`third-party/terarkdb/table/get_context.cc:195`。

`Version::TransToCombined()` 从 value index 解码 file number，并在 dependence map 中找到 vSST：

```cpp
uint64_t file_number = SeparateHelper::DecodeFileNumber(value.slice());
auto find = dependence_map.find(file_number);
```

位置：`third-party/terarkdb/db/version_set.cc:1327`。

真正需要 value 时，`Version::fetch_buffer()` 对对应 vSST 做 table lookup：

```cpp
auto s = table_cache_->Get(
    ReadOptions(), *pair.second, storage_info_.dependence_map(),
    iter_key.GetInternalKey(), &get_context, ...);
```

位置：`third-party/terarkdb/db/version_set.cc:1298`。

因此短 scan 的机制可以表述为：

> Range scan 仍沿着有序 kSST 前进；当扫描结果需要访问 separated value 时，再根据 value index 从有序 vSST 中懒加载真实 value。由于 vSST 本身也是 SST/table，并保持 internal-key order，value 分离不会退化为随机无序 blob log 访问。

### 5.4 block handle 元数据

TerarkDB 还支持将 source data-block handle 编入 value index：

- 配置项 `read_separated_value_by_handle`，见 `third-party/terarkdb/include/rocksdb/options.h:360`。
- block-based builder 写入 block handle，见 `third-party/terarkdb/table/block_based_table_builder.cc:471`。
- value index 编码 block handle，见 `third-party/terarkdb/db/dbformat.cc:283`。
- iterator 可解析 block handle，见 `third-party/terarkdb/table/iterator.cc:196`。

当前主读取路径仍以 `table_cache_->Get()` 按 key 查 vSST 为主，见 `third-party/terarkdb/db/version_set.cc:1298`。因此论文表述建议强调“vSST 有序、短 scan locality 友好、支持 lazy fetch”，如果要强调“直接通过 handle 定位”，需要结合具体实验代码确认读路径是否已完整接入 handle fast path。

## 6. value index 编码格式

`SeparateHelper::TransToSeparate()` 是 value index 编码入口。

最简单情况：

```cpp
value.reset(EncodeFileNumber(file_number), true, file_number);
```

位置：`third-party/terarkdb/db/dbformat.cc:213`。

带 metadata 时：

```cpp
Slice parts[] = {EncodeFileNumber(file_number), value_meta};
value.reset(SliceParts(parts, 2), file_number);
```

位置：`third-party/terarkdb/db/dbformat.cc:230`。

带 block handle 时：

```cpp
meta->block_handle.EncodeTo(&handle_encode);
Slice parts[] = {EncodeFileNumber(file_number), handle_encode};
value.reset(SliceParts(parts, 2), file_number);
```

位置：`third-party/terarkdb/db/dbformat.cc:283`。

可以抽象为：

```text
value_index := blob_file_number || optional_value_metadata
```

其中 optional metadata 可以是 value meta，也可以是 data-block handle。

## 7. GC 触发条件

TerarkDB 的 Blob GC 复用 compaction 框架，类型是 `CompactionType::kGarbageCollection`，而不是独立的 BlobGCJob。

是否需要 GC 由 `ColumnFamilyData::NeedsGarbageCollection()` 决定：

```cpp
bool res = !vstorage->IsPickGarbageCollectionFail() &&
       (vstorage->blob_marked_for_compaction() ||
        vstorage->total_garbage_ratio() >= mutable_cf_options_.blob_gc_ratio);
```

位置：`third-party/terarkdb/db/column_family.cc:1013`。

即：

```text
NeedsGC =
  !IsPickGarbageCollectionFail()
  && (
       blob_marked_for_compaction
       || total_garbage_ratio >= blob_gc_ratio
     )
```

`blob_gc_ratio` 默认是 0.05：

```cpp
double blob_gc_ratio = 0.05;
```

位置：`third-party/terarkdb/include/rocksdb/options.h:365`。

## 8. GC score：entry-based 与 byte-precise

### 8.1 原始 entry-based GC

默认 `precise_gc=false` 时，GC score 按 entry 数计算：

```cpp
return std::min(1.0, f->num_antiquation /
                         std::max<double>(1, f->prop.num_entries));
```

位置：`third-party/terarkdb/db/compaction_picker.cc:91`。

即：

```text
file_garbage_ratio = obsolete_entries / total_entries
```

这对固定 value size 近似可用，但对不定长 value 会产生误判。

### 8.2 byte-precise GC

开启 `precise_gc=true` 后，GC score 按字节计算：

```cpp
return std::min(1.0, f->num_antiquation_bytes /
                         std::max<double>(
                             1, f->BlobGcAccountingBytes()));
```

位置：`third-party/terarkdb/db/compaction_picker.cc:86`。

全局 `total_garbage_ratio_` 也区分 precise / non-precise：

```cpp
total_garbage_ratio_ = std::min(
    1.0, mutable_cf_options.precise_gc
             ? num_antiquation_bytes /
                   std::max<double>(1, blob_accounting_bytes)
             : num_antiquation / std::max<double>(1, num_entries));
```

位置：`third-party/terarkdb/db/version_set.cc:1891`。

byte-precise metadata 的计算来自 kSST 对 vSST 的 dependence byte count。VersionBuilder 根据 live depended bytes 计算 obsolete bytes：

```cpp
uint64_t bytes_depended = ClampPositiveBytesDepended(
    item.bytes_depended, blob_gc_accounting_bytes);
uint64_t num_antiquation_bytes =
    blob_gc_accounting_bytes - bytes_depended;
```

位置：`third-party/terarkdb/db/version_builder.cc:496`。

最后写回 file metadata：

```cpp
item.f->num_antiquation = num_antiquation;
item.f->num_antiquation_bytes = num_antiquation_bytes;
```

位置：`third-party/terarkdb/db/version_builder.cc:534`。

因此不定长 value 的精确 GC 可以总结为：

> 原始 GC 使用 obsolete entry ratio 估算回收收益；precise GC 使用 obsolete byte ratio 估算回收收益，避免小 value 与大 value 在 GC score 中权重相同的问题。

## 9. GC 候选文件选择策略

候选选择入口是 `CompactionPicker::PickGarbageCollection()`，位置：`third-party/terarkdb/db/compaction_picker.cc:862`。

### 9.1 只从 hidden blob level 选择

```cpp
auto& hidden_files = vstorage->LevelFiles(-1);
```

位置：`third-party/terarkdb/db/compaction_picker.cc:885`。

### 9.2 跳过不可 GC 文件

```cpp
if (!f->is_gc_permitted() || f->being_compacted) {
  continue;
}
```

位置：`third-party/terarkdb/db/compaction_picker.cc:900`。

### 9.3 marked-for-compaction 优先，其次 score 最大

```cpp
return (l.f->marked_for_compaction < r.f->marked_for_compaction) ||
       (l.f->marked_for_compaction == r.f->marked_for_compaction &&
        l.score < r.score);
```

位置：`third-party/terarkdb/db/compaction_picker.cc:876`。

### 9.4 score 低于阈值则不 GC

```cpp
if (dirtiest_blob.f == nullptr ||
    (!dirtiest_blob.f->marked_for_compaction &&
     dirtiest_blob.score < mutable_cf_options.blob_gc_ratio)) {
  return nullptr;
}
```

位置：`third-party/terarkdb/db/compaction_picker.cc:912`。

### 9.5 扩展相邻 / overlap vSST

选中最脏 vSST 后，会考虑相邻或 key range overlap 的 vSST：

```cpp
// expand with neighbor blob
std::vector<GarbageFileInfo> candidate_blob_vec;
```

位置：`third-party/terarkdb/db/compaction_picker.cc:927`。

邻居加入条件：

```cpp
if (gc_blob.estimate_size <= fragment_size ||
    gc_blob.score >= mutable_cf_options.blob_gc_ratio ||
    gc_blob.f->marked_for_compaction) {
  candidate_blob_vec.emplace_back(gc_blob);
}
```

位置：`third-party/terarkdb/db/compaction_picker.cc:970`。

### 9.6 单次 GC 文件数量与输出大小限制

单次最多选择 8 个 vSST：

```cpp
while (!candidate_blob_vec.empty() && input.files.size() < 8)
```

位置：`third-party/terarkdb/db/compaction_picker.cc:985`。

估计 live size 不能超过 `target_blob_file_size`：

```cpp
if (total_estimate_size + estimate_size < target_blob_file_size)
```

位置：`third-party/terarkdb/db/compaction_picker.cc:988`。

最终构造 `kGarbageCollection` 类型 compaction，输出 level 仍为 `-1`：

```cpp
params.output_level = -1;
params.compaction_type = kGarbageCollection;
```

位置：`third-party/terarkdb/db/compaction_picker.cc:1005` 与 `third-party/terarkdb/db/compaction_picker.cc:1016`。

## 10. GC 调度与执行策略

### 10.1 调度

当 column family 需要 GC 时，会进入 pending GC queue：

```cpp
if (!cfd->queued_for_garbage_collection() && cfd->NeedsGarbageCollection()) {
  AddToGarbageCollectionQueue(cfd);
  ++unscheduled_garbage_collections_;
}
```

位置：`third-party/terarkdb/db/db_impl_compaction_flush.cc:2037`。

后台线程入口：

```cpp
DBImpl::BGWorkGarbageCollection(...)
```

位置：`third-party/terarkdb/db/db_impl_compaction_flush.cc:2075`。

### 10.2 执行入口

GC 执行函数是：

```cpp
CompactionJob::ProcessGarbageCollection(...)
```

位置：`third-party/terarkdb/db/compaction_job.cc:2315`。

### 10.3 执行流程

GC 执行过程可以概括为：

```text
scan old vSST
  -> 对每条 separated value 反查 kSST
  -> 如果不再被 kSST 引用，则丢弃
  -> 如果仍 live，则重写到新 vSST
  -> 安装新 vSST，旧 vSST 后续删除
```

关键代码如下。

创建输入 iterator，扫描被选中的 vSST：

```cpp
std::unique_ptr<InternalIterator> input(versions_->MakeInputIterator(...));
```

位置：`third-party/terarkdb/db/compaction_job.cc:2330`。

打开新的 vSST：

```cpp
Status status = OpenCompactionOutputBlob(sub_compact);
```

位置：`third-party/terarkdb/db/compaction_job.cc:2384`。

遍历旧 vSST 记录：

```cpp
while (status.ok() && !cfd->IsDropped() && input->Valid()) {
```

位置：`third-party/terarkdb/db/compaction_job.cc:2453`。

对每个 blob record 做反查：

```cpp
input_version->GetKey(ikey.user_key, iter_key.GetInternalKey(), &s, &type,
                      &seq, &value, *blob_meta);
```

位置：`third-party/terarkdb/db/compaction_job.cc:2525`。

如果反查不到、sequence 不匹配、type 不是 value index，则判定为垃圾：

```cpp
if (s.IsNotFound()) { ... }
else if (seq != ikey.sequence ||
         (type != kTypeValueIndex && type != kTypeMergeIndex)) { ... }
```

位置：`third-party/terarkdb/db/compaction_job.cc:2534` 与 `third-party/terarkdb/db/compaction_job.cc:2543`。

如果仍然 live，则重写到新 vSST：

```cpp
status = sub_compact->blob_builder->Add(curr_key, value);
```

位置：`third-party/terarkdb/db/compaction_job.cc:2574`。

## 11. 与三个优化的关系

### 11.1 冷热路由与反查加速

原始 Blob GC 对每条 vSST record 都需要 `GetKey()` 反查 kSST，成本较高。你的 HotnessTracker / drop-key cache 优化在 GC 中加入 fast path：

```cpp
if (drop_key_cache_enabled &&
    hotness_tracker->IsDropped(ikey.user_key, ikey.sequence)) {
  ...
  break;
}
```

位置：`third-party/terarkdb/db/compaction_job.cc:2502`。

含义是：如果 compaction 已经确认某个 `(user_key, sequence)` 死亡，并记录在 drop-key cache 中，则 Blob GC 可以跳过 `GetKey()` 反查，直接判定为垃圾。

### 11.2 GC 感知块缓存淘汰

GC 机制维护了每个 vSST 的垃圾比例，例如 `num_antiquation_bytes`。这些信息可以传播给 block cache，使缓存淘汰策略降低高垃圾比例 block 的保留优先级。其动机是：默认 LRU 可能缓存来自高垃圾比例 vSST 的 obsolete blocks，从而污染 block cache。

### 11.3 不定长 value 的精确 GC

原始 entry-based GC 对不定长 value 不准确，因为一个小 value 和一个大 value 都只算一个 entry。byte-precise GC 通过 kSST-vSST dependence byte count 计算 obsolete bytes，使 GC 选择基于可回收字节数，而不是 obsolete entry 数。

## 12. 论文表述建议

### 键值分离机制

TerarkDB separates large values into ordered value SSTables rather than unordered blob logs. During flush and compaction, records are scanned in internal-key order. For values larger than a configurable threshold, the real value is first written to a value SSTable, while the key SSTable stores a compact value index containing the value SST file number and optional metadata. The key SSTable is installed into the normal LSM tree, whereas value SSTables are installed into a hidden blob level. Range scans still traverse ordered key SSTables, and separated values are lazily fetched from ordered value SSTables when needed.

### GC 机制

TerarkDB triggers Blob GC when the total garbage ratio of value SSTables exceeds a threshold or when a value SSTable is explicitly marked for compaction. The GC picker selects the dirtiest value SSTable from the hidden blob level, optionally expands the input with adjacent or overlapping value SSTables, and runs a special compaction of type garbage collection. During GC, TerarkDB scans old value SSTables and performs reverse lookups into key SSTables to determine whether each separated value is still referenced. Dead values are discarded, while live values are rewritten into new value SSTables.

### 不定长 value 精确 GC

The original GC score is entry-based, which assumes each separated value contributes equally to space reclamation. This assumption breaks under variable-size values. Byte-precise GC maintains byte-level dependency statistics between key SSTables and value SSTables, and computes garbage ratios using obsolete bytes over total value bytes. This enables GC to select files based on reclaimable bytes rather than obsolete entry counts.
