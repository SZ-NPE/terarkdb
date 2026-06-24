# TerarkDB 相对 dev.1.4 的修改与优化审阅记录

- 审阅时间：2026-06-22
- 仓库路径：`/home/pengzhifeng.002/KV_Bench_Env/third-party/terarkdb`
- 当前分支：`release-version`
- 对比范围：`dev.1.4..HEAD`
- 基线提交：`dev.1.4 = 539dcfa63774bde692265a479d55b5a17e2b5629`
- 当前提交：`HEAD = f5adf7c7e`
- 提交数量：33 个非 merge 提交
- 变更规模：113 个文件，310453 行新增，428 行删除
- 排除大日志文件 `log_rocksdb/storage_testdb_LOG` 后：112 个文件，10967 行新增，428 行删除

> 说明：本记录按功能域归纳 `dev.1.4` 之后引入的主要代码修改与优化，重点覆盖 Blob GC、Delta Separate、Hotness Routing、Cache、统计/实验工具以及构建配置。文中的代码引用均为当前 HEAD 的位置。

## 1. 总体结论

从 `dev.1.4` 到当前 `release-version`，TerarkDB 的修改主线不是小规模修补，而是一组围绕 **KV separation / Blob GC 精确化 / 热点感知 / GC-aware cache / 实验可观测性** 的系统性增强：

1. **Precise Blob GC**：将 Blob GC 的垃圾比例判断从 entry-count 估算推进到 byte-level 统计，贯通 table delta metadata、compaction dependence、manifest、version builder、GC picker 和 GC 执行统计。
2. **Delta Block / Delta Separate**：在 BlockBasedTable 中新增 `rocksdb.delta` 元数据块，按 entry 记录 separated value 的 `value_size` 和 meta，使 compaction 与 GC 无需重新读取 value 即可获得精确元数据。
3. **中值 value 分离策略**：新增 `[blob_size, middle_blob_size)` middle-size value 的分离与在指定 level 合回能力，降低长期 Blob 依赖。
4. **Hotness Routing**：新增基于 FIFO 写入窗口 + compaction feedback 的热点识别，在 flush 阶段将 hot/warm value 分流到不同 blob 输出，并复用 dropped-key cache 加速 Blob GC。
5. **GC-aware Block Cache**：新增基于 block 垃圾比例与访问频率评分的 cache 策略，减少高垃圾 vSST data block 对 block cache 的污染。
6. **Obsolete Block Cache Tracker**：跟踪 obsolete file 的 block 在 cache 中的驻留时间、字节和比例，补齐 GC/cache 交互的可观测性。
7. **Subcompaction 边界优化**：从按 SST 文件边界粗分改为基于 table index anchor 的近似字节均衡采样。
8. **实验与可视化闭环**：新增 db_bench 开关、统计 ticker、plot_tools、实验脚本和设计文档。

## 2. 提交概览

| 提交 | 日期 | 主题 | 归类 |
|---|---:|---|---|
| `bcf213408` | 2026-04-30 | Add logs and stats | GC/统计 |
| `f03b7ef4c` | 2026-04-30 | Add Precise GC | Precise GC |
| `156055e15` | 2026-04-19 | implement dual-level hotness routing | Hotness Routing |
| `b22d8fd87` | 2026-04-30 | Add hotness options | Options |
| `d101be6e2` | 2026-05-07 | add three-level for hotness | Hotness Routing |
| `10fd1d1e6` | 2026-05-07 | Implement block-level effective bitmap | Blob GC fast traverse |
| `483608c3f` | 2026-05-07 | Fix chunk-aware value index encoding | Table/Delta metadata |
| `250bdb24c` | 2026-05-07 | Fix bitmap strategy | Delta/GC |
| `e7087e1c7` | 2026-05-07 | Wire chunk-aware GC fast path | Blob GC fast path |
| `cd6aae6f4` | 2026-05-07 | update paper | 文档 |
| `bd0f360bd` | 2026-05-27 | Count time line for BackgroundGarbageCollection | GC 观测 |
| `aa7046c8c` | 2026-05-29 | add gc track | GC 观测 |
| `3dfea7f75` | 2026-05-30 | update python scripts | Plot tools |
| `1ab0cd4b3` | 2026-06-02 | update scripts | 脚本 |
| `c91fe6781` | 2026-06-07 | update hotness routing | Hotness Routing |
| `86bd9ca01` | 2026-06-09 | make blob GC block skipping layout-aware | Blob GC |
| `4243118d9` | 2026-06-15 | fix hotness model | Hotness Routing |
| `7b151fd1f` | 2026-06-16 | death-map based vSST GC fast path | Blob GC |
| `8ca47a281` | 2026-06-16 | UT cover hotness tracker | 单测 |
| `d82506c57` | 2026-06-16 | Simplify hotness routing admission | Hotness Routing |
| `14083c5e9` | 2026-06-16 | clear bitmap skip | Blob GC |
| `3cde9df00` | 2026-06-17 | add blob GC drop-key cache | Blob GC/Hotness |
| `32da803d4` | 2026-06-17 | add option to open | Options |
| `f3f1f8d63` | 2026-06-17 | new block cache | Cache |
| `8bb759adc` | 2026-06-17 | add scripts | 脚本 |
| `e5dbe6bba` | 2026-06-17 | add delta block metadata support | Table/Delta |
| `4175726cd` | 2026-06-17 | complete delta separate middle-value support | Delta Separate |
| `6a4154510` | 2026-06-17 | implement precise blob GC | Precise GC |
| `921e1d927` | 2026-06-17 | add delta separate benchmark switch | Benchmark |
| `d1f937dda` | 2026-06-17 | improve subcompaction boundary sampling | Compaction |
| `13aed5585` | 2026-06-17 | complete precise GC metadata wiring | Precise GC |
| `1b1a15fa3` | 2026-06-17 | add obsolete block cache tracker | Cache/观测 |
| `f5adf7c7e` | 2026-06-18 | add Terark Zip include paths | 构建 |

## 3. Precise Blob GC：byte-level 精确垃圾比例

### 3.1 修改内容

Precise GC 的核心改动是把原本依赖 entry 数的 Blob 文件垃圾比例估算，升级为按 value bytes 统计：

- `Dependence` 从 `(file_number, entry_count)` 扩展为 `(file_number, entry_count, byte_count)`，用于记录 kSST 对 blob SST 的精确字节依赖，入口在 `include/rocksdb/types.h:21`。
- compaction 输出 kSST 时，对 `kTypeValueIndex` / `kTypeMergeIndex` 聚合被引用 blob 文件的 entry 数和 value bytes，相关逻辑在 `db/compaction_job.cc:1941`、`db/compaction_job.cc:1953`、`db/compaction_job.cc:2821`。
- manifest 编解码追加 `byte_count`，并保持旧 manifest 兼容：编码在 `db/version_edit.cc:233`，解码在 `db/version_edit.cc:418`。
- VersionBuilder 使用 `byte_count` 计算 blob live/dead bytes，缺失时 fallback 到 entry 平均估算；关键逻辑在 `db/version_builder.cc:293`、`db/version_builder.cc:351`、`db/version_builder.cc:445`、`db/version_builder.cc:446`。
- `FileMetaData` 新增 `num_antiquation_bytes`，用于保存 obsolete bytes，见 `db/version_edit.h:138`、`db/version_edit.h:139`。
- GC picker 在 `precise_gc=true` 时使用 bytes-based score：`num_antiquation_bytes / file_size`，见 `db/compaction_picker.cc:84`、`db/compaction_picker.cc:86`、`db/compaction_picker.cc:861`。

### 3.2 优化收益

- Blob GC 候选文件选择更精确，避免 entry 数近似导致大 value / 小 value 混合场景下的误判。
- 结合 delta block 记录的 per-entry value size，GC 能在不重新读取外部 value 的情况下做 byte-level 决策。
- GC 统计链路新增 bytes 与 latency 维度，更利于后续论文/实验评估。

### 3.3 关键代码路径

- `include/rocksdb/options.h:360`：新增/暴露 `precise_gc` 配置。
- `db/column_family.cc:329`：开启 `precise_gc` 时自动启用 `use_delta_block`。
- `db/db_impl_open.cc:186`：DB open 路径补齐 `use_delta_block`。
- `tools/db_bench_tool.cc:3552`：db_bench 中 `FLAGS_use_delta_block || FLAGS_precise_gc` 联动。
- `db/compaction_job.cc:2221`：Blob GC 主流程 `CompactionJob::ProcessGarbageCollection()`。
- `db/compaction_job.cc:2261`、`db/compaction_job.cc:2262`、`db/compaction_job.cc:2263`：GC latency/bytes/block 统计采集入口。
- `db/compaction_job.cc:2583`：GC vSST/kSST/invalid/relocation bytes 统计。

### 3.4 工程实现与社区规范角度的优化建议

Precise Blob GC 的目标是面向用户写入 value 大小混合的 KV workload，把“失效 entry 数”升级为“失效 value bytes”，从而让 GC 选择真正能降低空间放大的 blob 文件。当前链路已经形成闭环：compaction iterator 提供 `value_size`，compaction job 聚合 dependence byte count，manifest/table properties 持久化 byte metadata，VersionBuilder 计算 `num_antiquation_bytes`，GC picker 使用 byte-based score。为了稳定拿收益，建议重点遵循以下工程原则：

1. **已优化：byte_count 与分母使用同一语义域**。`dependence.byte_count` 来自 separated value 的 payload size，因此 precise GC score 的分母也应优先使用 blob 文件的 value payload bytes，而不是物理 SST 文件大小。已新增 `FileMetaData::BlobGcAccountingBytes()`，优先返回 `prop.raw_value_size`，缺失时回退到 `fd.GetFileSize()`，见 `db/version_edit.h:180`；VersionBuilder fallback 和递归比例计算改为使用该接口，见 `db/version_builder.cc:297`、`db/version_builder.cc:341`、`db/version_builder.cc:407`；GC picker 的 byte-based score 也改为使用该接口，见 `db/compaction_picker.cc:84`。
2. **收益解释**：在 key/value 大小混合场景下，使用物理文件大小作为分母会把 index/filter/compression block 等非 value bytes 混入 garbage ratio，可能低估或扭曲 value 层面的失效比例；使用 `raw_value_size` 可以让 GC 更贴近“回收用户 value 空间”的目标，减少空间放大。
3. **兼容性策略**：旧 SST/旧 manifest 或 remote-compaction 路径可能没有 `byte_count` 或 `raw_value_size`。当前实现保留 fallback：`byte_count=0` 时按 `entry_count * accounting_bytes / num_entries` 估算，`raw_value_size=0` 时回退 file size，不破坏历史数据可读性。
4. **稳定性建议**：所有 byte 计数都应保持 saturating / clamped 语义，避免依赖 metadata 异常导致负数或超过 100% 的 score。当前 `VersionBuilder` 对 live bytes 做 `min(accounting_bytes, bytes_depended)`，picker 侧 `std::min(1.0, score)`，方向正确。
5. **性能建议**：compaction job 中只有 `precise_gc=true` 时才读取并累加 `c_iter->value_size()`，否则 byte count 置零，避免默认路径额外开销，见 `db/compaction_job.cc:1945`、`db/compaction_job.cc:1956`。后续若要进一步优化，可只在存在 separated value 且输出 kSST 会持久化 dependence 时维护 byte map。
6. **社区规范建议**：把“GC 计分用什么 bytes”收敛为 `FileMetaData::BlobGcAccountingBytes()` 这样的单一 helper，避免在 VersionBuilder、Picker、日志统计中复制公式；新增测试应覆盖显式 byte_count、legacy fallback、raw_value_size 分母三类路径，避免后续改动回退到 file-size 语义。
7. **观测建议**：实验时建议同时记录 entry-based garbage ratio、value-byte garbage ratio、file-size-based ratio、GC 后空间放大变化，证明 precise GC 的收益来自混合 value size 场景下的候选选择改善，而不是单纯增加 GC 频率。

## 4. Delta Block / Delta Separate：SST 侧 separated value 元数据

### 4.1 新增 delta block 元数据

本次新增了 `rocksdb.delta` meta block，用于为 SST 中每个 entry 记录 separated-value 相关元数据：

- `BlockBasedTableOptions::use_delta_block` 位于 `include/rocksdb/table.h:193`。
- delta meta block 名 `kDeltaBlock` 声明在 `include/rocksdb/table_properties.h:80`，定义为 `rocksdb.delta`，见 `table/table_properties.cc:246`。
- builder 写出 delta block 的入口为 `BlockBasedTableBuilder::WriteDeltaBlock()`，见 `table/block_based_table_builder.cc:910`。
- reader 打开时从 meta index 查找 delta block，见 `table/block_based_table_reader.cc:962`；读取逻辑在 `table/block_based_table_reader.cc:1284`。
- `Rep::delta_index_block` 保存 delta index block，见 `table/block_based_table_reader.h:539`。

delta block 的设计并不是按 key 存储，而是按 data block entry 顺序记录 bitmap 与 separated entry metadata：

1. delta index block 的 key 是 fixed32 block number；
2. key `0` 指向 delta meta block；
3. 后续 key 指向 physical delta data block；
4. meta block 记录 restart interval、累计 entry 边界与 separated bitmap；
5. data block 记录 separated entry 的 `value_size` 与可选 `value_meta`。

### 4.2 新增 DeltaBuilder / DeltaReader

- `DeltaBuilder` 定义在 `table/delta_builder.h:25`，核心 `Add()` 在 `table/delta_builder.h:35`、实现见 `table/delta_builder.cc:41`。
- `DeltaBuilder::AddIndexEntry()` 用于记录 data-block 边界，见 `table/delta_builder.h:47`、`table/delta_builder.cc:59`。
- `DeltaBuilder::Finish()` 分阶段输出 physical delta blocks 和 delta index block，见 `table/delta_builder.cc:90`。
- `DeltaBlockReader` 定义在 `table/delta_reader.h:28`，`DeltaBlockInfo` 保存 `value_size` / `value_meta`，见 `table/delta_reader.h:32`。
- reader 初始化 meta block 在 `table/delta_reader.cc:143`，按需加载 physical block 在 `table/delta_reader.cc:210`。
- `IsSeparated()` 通过 bitmap 判断 entry 是否 separated，见 `table/delta_reader.cc:272`；`BitmapRank1()` 将 entry index 映射到 separated metadata 下标，见 `table/delta_reader.cc:287`。

### 4.3 iterator 对齐能力

为让 compaction iterator 能将当前 entry 对齐到 delta metadata，本次扩展了内部 iterator 能力：

- `InternalIterator::GetProperty()` 与 `GetIndex()` 在 `table/internal_iterator.h:76`、`table/internal_iterator.h:80`。
- data block iterator 提供 `GetIndex()`，见 `table/block.cc:110`、`table/block.cc:132`。
- BlockBasedTable iterator 在 `for_compaction_ && delta_index_iter != nullptr` 时创建 `DeltaBlockReader`，见 `table/block_based_table_reader.h:595`。
- `GetIndexPair()` 将当前位置转换为 `(restart_index, entry_index)`，见 `table/block_based_table_reader.h:670`。
- delta metadata 暴露为 property：`rocksdb.delta.is-separated`、`rocksdb.delta.value-size`、`rocksdb.delta.value-meta`，分别见 `table/block_based_table_reader.cc:2467`、`table/block_based_table_reader.cc:2471`、`table/block_based_table_reader.cc:2475`。

### 4.4 兼容性与 gating

这组改动有明确兼容保护：

- 写入侧只有 `use_delta_block=true` 才构建和写出 delta block，配置位于 `include/rocksdb/table.h:200`。
- 打开侧找不到 `rocksdb.delta` 不失败，只是不启用 delta metadata，见 `table/block_based_table_reader.cc:970`。
- iterator 侧只有 compaction iterator 使用 delta reader，见 `table/block_based_table_reader.cc:2489`。
- `precise_gc` 会要求 `use_delta_block`，见 `table/block_based_table_factory.cc:263`。
- index delta encoding 依赖 table properties 判定；缺失 properties 时走兼容旧格式，见 `table/block_based_table_reader.cc:2935`、`table/block_based_table_reader.cc:2938`。

## 5. 中值 value Delta Separate 与 combine

本次引入 middle-size value 的分离策略：

- `blob_size` 以下仍内联；
- `[blob_size, middle_blob_size)` 的 value 可作为 middle separated value；
- 当 compaction input level 达到 `middle_combine_level` 后，middle separated value 合回普通 SST，减少长期 blob 依赖。

关键配置与路径：

- `enable_delta_separate`：`include/rocksdb/options.h:310`。
- `middle_blob_size`：`include/rocksdb/options.h:315`、`options/cf_options.h:145`。
- `middle_combine_level`：`include/rocksdb/options.h:321`、`options/cf_options.h:146`。
- `BlobConfig` 聚合相关配置，见 `options/cf_options.h:142`。
- `MutableCFOptions::get_blob_config()` 在 `options/cf_options.h:203`。
- `SeparateHelper::do_middle_separate()` 在 `db/dbformat.h:858`。
- compaction iterator 中 middle combine 判断在 `db/compaction_iterator.cc:829`、`db/compaction_iterator.cc:881`、合回逻辑在 `db/compaction_iterator.cc:905`。
- C API 暴露在 `db/c.cc:2466`、`db/c.cc:2470`。

优化意义：middle-size value 通常介于“内联成本可接受”和“长期外置收益明显”之间；该策略提供了可配置折中，让短期写放大、长期读/GC 成本可以通过 level 规则调节。

## 6. Hotness Routing：热点识别与 Flush 分流

### 6.1 HotnessTracker

新增 `HotnessTracker`，用 FIFO recent-write window 捕获重复写，命中后晋升到 hot LRU；compaction 丢弃 obsolete version 时也可反馈 hotness：

- `HotnessTracker` 定义在 `util/hotness_tracker.h:25`。
- Flush 路由枚举 `FlushRoute` 在 `util/hotness_tracker.h:27`。
- `HotEntry` 保存 hot 状态与 bounded dropped seqs，见 `util/hotness_tracker.h:37`、`util/hotness_tracker.h:41`。
- 写路径记录入口 `RecordWrite()` 在 `util/hotness_tracker.h:108`。
- compaction feedback 入口在 `util/hotness_tracker.h:126`、`util/hotness_tracker.h:138`。
- dropped-key 查询 `IsDropped()` 在 `util/hotness_tracker.h:158`。
- flush 分类 `ClassifyForFlush()` 在 `util/hotness_tracker.h:180`。

ColumnFamily 层集成：

- `MakeHotnessTrackerOptions()` 在 `db/column_family.cc:427`。
- `hotness_tracker_` 创建在 `db/column_family.cc:477`。
- `ColumnFamilyData::hotness_tracker()` 在 `db/column_family.h:417`。

写入与 compaction 反馈：

- Put 路径记录写入在 `db/write_batch.cc:1227`。
- Delete 路径记录写入在 `db/write_batch.cc:1326`。
- compaction iterator 丢弃 separated old version 时记录 feedback，见 `db/compaction_iterator.cc:218`、`db/compaction_iterator.cc:226`、`db/compaction_iterator.cc:231`。

### 6.2 Flush hot/warm blob 分流

flush 构建 blob SST 时按 hotness 将 value 分流到不同 blob 输出，并设置 write lifetime hint：

- flush 侧 helper 在 `db/builder.cc:176`。
- `BlobOutput blobs[3]` 分别对应 warm / ephemeral / stable，见 `db/builder.cc:179`。
- 默认 route 为 warm，见 `db/builder.cc:310`。
- 通过 `ClassifyForFlush(user_key)` 判断 route，见 `db/builder.cc:313`。
- 设置 write lifetime hint 在 `db/builder.cc:358`。
- hot/warm flush 统计在 `db/builder.cc:382`、`db/builder.cc:386`。

### 6.3 对 Blob GC 的额外收益

HotnessTracker 还复用为 drop-key cache：Blob GC 可通过 exact `(user_key, seq)` 判断某 value 是否已由 compaction 确认死亡，命中时跳过昂贵的 `GetKey()` reverse lookup。

- Blob GC 使用 hotness tracker 的入口在 `db/compaction_job.cc:2227`。
- drop-key cache fast path 在 `db/compaction_job.cc:2401`。
- `drop_key_cache_enabled` 在 `db/compaction_job.cc:2408`。
- `hotness_tracker->IsDropped(...)` 在 `db/compaction_job.cc:2410`。
- 命中/避免 GetKey 的统计在 `db/compaction_job.cc:2572`、`db/compaction_job.cc:2575`。

### 6.4 工程实现与 Google C++ 规范角度的优化建议

从工程开销和稳定运行角度看，Hotness Routing 的整体方向合理：默认总开关关闭；启用后使用 FIFO window 捕获近期重复写、hot LRU 控制热点集合上界、drop-key cache 只做 opportunistic acceleration，miss 会回退到旧路径，不影响正确性。但当前实现仍有若干可优化点：

1. **已修复：容量为 0 时应关闭对应子功能，避免无效写路径开销**。原实现只根据 `enable_write_window` 创建 FIFO window，若 `hotness_window_capacity=0`，每次 `RecordWrite()` 仍可能执行 `Lookup`、`Insert`、分配和立即淘汰，没有任何收益；若 `hotness_hot_capacity=0`，`DropKeyCacheEnabled()` 仍可能返回 true，使 Blob GC 进入永远 miss 的 `IsDropped()` 路径。已将构造期 gating 调整为 `enable_write_window && window_capacity > 0`、`enable_compaction_feedback && hot_capacity > 0`、`enable_drop_key_cache && hot_capacity > 0`，见 `util/hotness_tracker.h:92`；并增加零容量回归测试，见 `db/hotness_routing_test.cc:239`、`db/hotness_routing_test.cc:257`。
2. **建议：降低写路径对象复制和引用计数开销**。`write_batch.cc` 在每次成功写入后调用 `cfd->hotness_tracker()`，该 accessor 返回 `std::shared_ptr<HotnessTracker>`，每次都会产生原子引用计数增减；写路径属于高频路径，建议提供 `HotnessTracker* hotness_tracker_ptr() const` 或在当前 memtable/options 中缓存裸指针，只要生命周期由 `ColumnFamilyData` 保证即可，相关调用点在 `db/write_batch.cc:1227`、`db/write_batch.cc:1326`、`db/write_batch.cc:1552`、`db/write_batch.cc:1572`。
3. **建议：抽取统一的 `MaybeRecordHotnessWrite()` helper**。当前 Put / Delete / Merge 多处重复 `recovering_log_number_ == 0 && cfd && cfd->hotness_tracker()` 判断，且已有缩进不一致的代码块。按 Google C++ 规范，应减少重复控制流、保持一致缩进和早返回风格；可在 `MemTableInserter` 内抽取私有 helper，集中处理 recovery gating 和 tracker 获取。
4. **建议：用轻量环形缓冲替代 `HotEntry::AddDroppedSeq()` 中的 O(N) 搬移**。当前 `HotEntry` 最多保存 8 个 seq，开销可控，但满载时会移动数组元素，见 `util/hotness_tracker.h:57`。若 compaction feedback 非常频繁，可改为 ring buffer + count，避免热路径搬移，并保持 bounded memory。
5. **建议：避免 `ClassifyForFlush()` 在每个 separated value 上刷新 hot LRU**。当前 flush 分类使用 `record_hit=true`，见 `util/hotness_tracker.h:180`；如果 flush 中大量 hot key 被查询，会改变 LRU recency，使“被 flush 查询过”影响热点保留。若只希望写入/compaction feedback 更新热点，flush 查询应使用 `record_hit=false`，或增加独立选项控制是否刷新。
6. **建议：显式记录配置归一化日志或统计**。当用户开启 hotness 但容量为 0 时，功能被关闭是合理的，但为了排查配置，应在 ColumnFamily 初始化或 options sanitize 阶段输出一次 warning，例如 `enable_hotness_tracker=true but hotness_hot_capacity=0 disables hot route and drop-key cache`。
7. **建议：减少 header 内复杂实现**。`util/hotness_tracker.h` 当前包含较多非模板函数实现，会增加编译依赖并暴露实现细节；按 Google C++ 风格，复杂成员函数可移到 `util/hotness_tracker.cc`，头文件保留接口和小型 inline，降低增量编译成本。
8. **建议：补充长稳压测指标**。已有基础并发测试，但还应覆盖高写入速率、极小容量、重复 overwrite、快照/merge operand、GC 与 compaction 并发场景，重点观察写吞吐、CPU、cache mutex contention、hot/cold blob 文件数量和 drop-key false miss 比例。

## 7. Cache 相关优化

### 7.1 公共 Cache API 扩展

本次对 cache 公共接口增加 metadata 传递与 obsolete 文件通知能力：

- `BlockCacheMetadata` 在 `include/rocksdb/cache.h:42`，包含 `is_blob_file`、`is_data_block`、`file_number`、`block_offset`、`block_size`、`garbage_ratio` 等。
- `BlockCacheObsoleteTrackingOptions` 在 `include/rocksdb/cache.h:53`。
- `Cache::InsertWithMetadata(...)` 在 `include/rocksdb/cache.h:271`。
- `Cache::MarkBlockCacheFilesObsolete(...)` 在 `include/rocksdb/cache.h:280`。
- `Cache::LogBlockCacheObsoleteSample(...)` 在 `include/rocksdb/cache.h:286`。
- `Lookup(..., record_hit, Statistics*)` 在 `include/rocksdb/cache.h:300`，支持 lookup 但不刷新命中行为。

### 7.2 FIFO Cache

新增通用 FIFO cache，可独立使用，也被 HotnessTracker 用作 recent-write window：

- `FIFOCacheOptions` 在 `include/rocksdb/cache.h:129`。
- `NewFIFOCache(...)` 在 `include/rocksdb/cache.h:172`、`cache/fifo_cache.cc:604`、`cache/fifo_cache.cc:611`。
- `FIFOHandle` 在 `cache/fifo_cache.h:58`。
- `FIFOHandleTable` 在 `cache/fifo_cache.h:134`。
- `FIFOCacheShardTemplate` 在 `cache/fifo_cache.h:173`。
- `FIFOCacheShardTemplate::Lookup(...)` 支持 `record_hit`，见 `cache/fifo_cache.h:209`。
- FIFO 淘汰入口 `EvictFromFIFO(...)` 在 `cache/fifo_cache.h:254`。
- 诊断版 top-k 支持在 `cache/fifo_cache.h:332`、`cache/fifo_cache.h:337`。

### 7.3 Garbage-aware Cache

新增 `GarbageAwareCache`，对 vSST data block 根据 `score = access_freq * (1 - garbage_ratio)` 做 admission/probation 管理：

- `GarbageAwareCacheOptions` 在 `include/rocksdb/cache.h:203`。
- `NewGarbageAwareCache(...)` 在 `include/rocksdb/cache.h:213`、`cache/garbage_aware_cache.cc:567`、`cache/garbage_aware_cache.cc:576`。
- `GarbageAwareCacheShard` 在 `cache/garbage_aware_cache.h:23`。
- `InsertWithMetadata(...)` 在 `cache/garbage_aware_cache.h:35`。
- `ScoreKey` / `GAHandle` 在 `cache/garbage_aware_cache.h:66`、`cache/garbage_aware_cache.h:80`。
- 插入时识别 vSST data block 并初始化 score，见 `cache/garbage_aware_cache.cc:65`、`cache/garbage_aware_cache.cc:85`、`cache/garbage_aware_cache.cc:88`。
- 命中时增加 access frequency 并更新 score，见 `cache/garbage_aware_cache.cc:142`。
- admission 低分条目降级到 probation，见 `cache/garbage_aware_cache.cc:196`、`cache/garbage_aware_cache.cc:389`。
- 淘汰逻辑优先从 probation 的低 score entry 淘汰，见 `cache/garbage_aware_cache.cc:407`。
- 周期性日志 `[GC_AWARE_BLOCK_CACHE]` 在 `cache/garbage_aware_cache.cc:451`。

### 7.4 Obsolete Block Cache Tracker

新增 obsolete block cache tracker，用于观察 compaction/GC 后旧 file 的 data block 在 cache 中继续驻留的情况：

- `BlockCacheObsoleteTracker` 在 `cache/block_cache_obsolete_tracker.h:16`。
- `RecordInsert()` 在 `cache/block_cache_obsolete_tracker.h:22`、实现见 `cache/block_cache_obsolete_tracker.cc:34`。
- `RecordErase()` 在 `cache/block_cache_obsolete_tracker.h:24`、实现见 `cache/block_cache_obsolete_tracker.cc:62`。
- `MarkFilesObsolete()` 在 `cache/block_cache_obsolete_tracker.h:25`、实现见 `cache/block_cache_obsolete_tracker.cc:99`。
- 事件日志 `[BLOCK_CACHE_OBSOLETE_EVENT]` 在 `cache/block_cache_obsolete_tracker.cc:137`。
- 周期性 sample 在 `cache/block_cache_obsolete_tracker.cc:150`、后台循环在 `cache/block_cache_obsolete_tracker.cc:163`。
- sample 构造与 top obsolete files 在 `cache/block_cache_obsolete_tracker.cc:177`、`cache/block_cache_obsolete_tracker.cc:259`。
- LRU cache 集成在 `cache/lru_cache.h:211`、`cache/lru_cache.cc:380`、`cache/lru_cache.cc:503`、`cache/lru_cache.cc:606`。

### 7.5 工程实现与性能优化建议

这组 Cache API 扩展的方向是正确的：block cache key 由 cache id / file number / offset 等字段构造，vSST GC 或 kSST compaction 生成新文件后，旧文件的 cached data block 对新文件不再可命中，只能依赖 LRU 自然淘汰；在键值分离场景下，大量 obsolete block 会占据 cache 容量，压低有效命中率。因此通过 `InsertWithMetadata()` 记录 file/block metadata，并在 compaction install 后通过 `MarkBlockCacheFilesObsolete()` 标记输入文件，是修复该问题的必要基础。

1. **已优化：避免 mark 不在 cache 中的 obsolete file 时污染 tracker 状态**。原 `MarkFilesObsolete()` 对每个输入 file 使用 `files_[file_number]`，即使该 file 没有任何 resident block，也会创建一个空 `FileResidency`。长时间运行后，频繁 compaction/GC 会让 `files_` 累积大量零 block 文件，增加 sample 遍历成本和内存占用。已改为只对已存在的 resident file 做 obsolete 标记，见 `cache/block_cache_obsolete_tracker.cc:104`；新增回归测试见 `cache/cache_test.cc:784`。
2. **已优化：resident block 清空后及时删除 file 维度状态**。原实现 `RecordErase()` 只递减 counters，obsolete file 即使所有 block 都已被淘汰，也会留在 `files_` 中。已在最后一个 block erase 后删除对应 `FileResidency`，同时保持 drain 日志语义，见 `cache/block_cache_obsolete_tracker.cc:62`、`cache/block_cache_obsolete_tracker.cc:100`；新增回归测试见 `cache/cache_test.cc:799`。
3. **已优化：top obsolete files 从全量排序改为 top-k 选择**。sample 只需要输出前 `topk_files`，原实现对全部 obsolete files 做 `std::sort`，复杂度为 `O(N log N)`。已改为 `std::nth_element + sort(topK)`，将大多数周期性 sample 成本降低为 `O(N + K log K)`，见 `cache/block_cache_obsolete_tracker.cc:268`、`cache/block_cache_obsolete_tracker.cc:290`。
4. **建议：下一步从“观测”升级为“主动逐出”**。当前 API 已能知道 obsolete file number，但 LRU cache 仍没有按 file 维度批量删除 block 的索引；若要直接改善命中率而不仅是观测，应在 tracker 或 LRU shard 内维护 `file_number -> handles` 的轻量倒排索引，在 `MarkBlockCacheFilesObsolete()` 时按 shard 批量 `Erase`/降优先级。需要注意 pinned handle 只能从 hash/LRU 中摘除，不能立即 free，释放仍遵循现有 refcount。
5. **建议：仅对 data block 维护倒排索引**。index/filter block 对读路径价值高，且 metadata 中已有 `is_data_block` 标记；主动逐出应优先限制在 obsolete data block，避免误伤仍可能被 table reader / iterator 使用的高价值元数据 block。
6. **建议：控制额外锁竞争**。`RecordInsert()`/`RecordErase()` 位于 block cache 热路径，当前 tracker 只有在 `obsolete_tracking_options.enabled` 时挂入 LRU，并且只记录 data block，是合理 gating。若继续引入 file->handles 索引，应保持 shard-local 更新，避免所有 cache shard 竞争一个全局 mutex。

## 8. Subcompaction boundary sampling 优化（非创新点）

`GenSubcompactionBoundaries()` 从原来的较粗粒度边界生成，调整为从输入 SST 的 table index 采样 anchors，按近似 range bytes 生成 subcompaction boundaries，使多个 subcompaction 的输入字节更均衡。

关键路径：

- `Compaction::ShouldFormSubcompactions()` 在 `db/compaction.cc:651`。
- `CompactionJob::GenSubcompactionBoundaries()` 在 `db/compaction_job.cc:941`。
- 收集 anchors 的容器在 `db/compaction_job.cc:961`。
- 对依赖文件和普通输入文件调用 `ApproximateKeyAnchors()`，见 `db/compaction_job.cc:985`、`db/compaction_job.cc:994`。
- anchors 按 user key 排序在 `db/compaction_job.cc:1015`。
- 目标 range size 与 boundary 生成在 `db/compaction_job.cc:1048`、`db/compaction_job.cc:1060`、`db/compaction_job.cc:1067`。
- `TableCache::ApproximateKeyAnchors()` 声明与实现位于 `db/table_cache.h:114`、`db/table_cache.cc:659`。
- `TableReader::Anchor` 在 `table/table_reader.h:70`，virtual `ApproximateKeyAnchors()` 在 `table/table_reader.h:80`。
- BlockBasedTable 实现在 `table/block_based_table_reader.cc:3021`，最多 128 anchors，见 `table/block_based_table_reader.cc:3045`。

优化收益：对大 SST 或 key 分布不均的 compaction，subcompaction 切分更接近字节均衡，减少某个 subcompaction 成为长尾。

## 9. 新增配置、C API 与 db_bench 开关

### 9.1 ColumnFamilyOptions

- `enable_delta_separate`：`include/rocksdb/options.h:310`。
- `middle_blob_size`：`include/rocksdb/options.h:319`。
- `middle_combine_level`：`include/rocksdb/options.h:323`。
- `enable_hotness_tracker`：`include/rocksdb/options.h:326`。
- `hotness_window_capacity`：`include/rocksdb/options.h:329`。
- `hotness_hot_capacity`：`include/rocksdb/options.h:332`。
- `hotness_enable_write_window`：`include/rocksdb/options.h:335`。
- `hotness_enable_compaction_feedback`：`include/rocksdb/options.h:338`。
- `hotness_enable_drop_key_cache`：`include/rocksdb/options.h:343`。
- `read_separated_value_by_handle`：`include/rocksdb/options.h:352`。
- `blob_gc_ratio`：`include/rocksdb/options.h:357`。
- `precise_gc`：`include/rocksdb/options.h:360`。
- `target_blob_file_size`：`include/rocksdb/options.h:364`。
- `blob_file_defragment_size`：`include/rocksdb/options.h:368`。
- `max_dependence_blob_overlap`：`include/rocksdb/options.h:372`。

### 9.2 DBOptions

- `blob_gc_collect_block_stats`：`include/rocksdb/options.h:1070`。
- `blob_gc_collect_latency_stats`：`include/rocksdb/options.h:1074`。
- `blob_gc_collect_bytes_stats`：`include/rocksdb/options.h:1078`。
- `block_cache_obsolete_tracking`：`include/rocksdb/options.h:1084`。
- 对应内部字段见 `options/db_options.h:83` 到 `options/db_options.h:88`。

### 9.3 options helper / settable options

新增选项已注册到 options helper：

- Delta separate 与 middle value：`options/options_helper.cc:1975`、`options/options_helper.cc:1979`、`options/options_helper.cc:1983`。
- Hotness tracker：`options/options_helper.cc:1987` 到 `options/options_helper.cc:2002`。
- Precise GC 与 blob 文件参数：`options/options_helper.cc:2017` 到 `options/options_helper.cc:2029`。

### 9.4 db_bench

主要实验开关：

- GC-aware cache：`tools/db_bench_tool.cc:441`、`tools/db_bench_tool.cc:445`、`tools/db_bench_tool.cc:449`、`tools/db_bench_tool.cc:453`。
- Delta separate / middle value：`tools/db_bench_tool.cc:1004`、`tools/db_bench_tool.cc:1008`、`tools/db_bench_tool.cc:1013`。
- Hotness tracker：`tools/db_bench_tool.cc:1017` 到 `tools/db_bench_tool.cc:1027`。
- GC stats 与 obsolete cache tracking：`tools/db_bench_tool.cc:1030` 到 `tools/db_bench_tool.cc:1042`。
- `read_separated_value_by_handle`：`tools/db_bench_tool.cc:1048`。
- `precise_gc` 与 blob 参数：`tools/db_bench_tool.cc:1054` 到 `tools/db_bench_tool.cc:1061`。
- 创建 GC-aware cache 的分支在 `tools/db_bench_tool.cc:2460`。
- 默认 LRU 注入 obsolete tracking options 在 `tools/db_bench_tool.cc:2473`。
- flags 写回 DB options 在 `tools/db_bench_tool.cc:3674`、`tools/db_bench_tool.cc:3677`、`tools/db_bench_tool.cc:3685`、`tools/db_bench_tool.cc:3688`、`tools/db_bench_tool.cc:3698`。

## 10. 统计指标与可观测性

### 10.1 Ticker 新增

新增 ticker 覆盖 drop-key cache、hotness flush、GC pick/bytes、GC-aware cache：

- Drop-key cache：`include/rocksdb/statistics.h:270`、`include/rocksdb/statistics.h:271`、`include/rocksdb/statistics.h:272`。
- Hotness flush：`include/rocksdb/statistics.h:275` 到 `include/rocksdb/statistics.h:278`。
- GC pick / bytes / block：`include/rocksdb/statistics.h:281` 到 `include/rocksdb/statistics.h:292`。
- GC-aware cache：`include/rocksdb/statistics.h:294` 到 `include/rocksdb/statistics.h:298`。

Ticker 名称注册在 `monitoring/statistics.cc:147` 到 `monitoring/statistics.cc:173`。

### 10.2 测试覆盖

- Garbage-aware cache 降级、淘汰、容量、shard 测试在 `cache/cache_test.cc:683`、`cache/cache_test.cc:718`、`cache/cache_test.cc:755`、`cache/cache_test.cc:776`。
- Hotness 相关 ticker 注册与可观测性测试在 `db/hotness_routing_test.cc:295`、`db/hotness_routing_test.cc:320`。
- Hotness routing 新增完整测试文件 `db/hotness_routing_test.cc`。
- dbformat / version_edit / version_builder 等补充了 dependence byte count 兼容与计算相关测试，涉及 `db/dbformat_test.cc`、`db/version_edit_test.cc`、`db/version_builder_test.cc`。

## 11. 实验脚本、绘图工具与文档

### 11.1 plot_tools

新增 `plot_tools`，用于解析 TerarkDB INFO LOG / CSV 并生成实验图：

- 工具总览在 `plot_tools/README.md:7`。
- obsolete block cache tracking 推荐参数在 `plot_tools/README.md:72`，预期日志 tag 在 `plot_tools/README.md:81`。
- Motivation 1/2 与 final validation 说明在 `plot_tools/README.md:88`、`plot_tools/README.md:121`、`plot_tools/README.md:150`。
- GC 日志解析入口 `parse_log()` 在 `plot_tools/gc_visualizer.py:123`。
- obsolete cache 时间线绘图在 `plot_tools/plot_block_cache_obsolete_timeline.py:162`。
- motivation 图包括 GC waste stack 与 cache events，见 `plot_tools/plot_motivation_tests.py:176`、`plot_tools/plot_motivation_tests.py:242`。
- final validation 归一化图在 `plot_tools/plot_final_validation.py:164`。

### 11.2 实验脚本

- 新增 `overwrite-uniform.sh`，默认 DB path 在 `overwrite-uniform.sh:2`，默认 dataset 100GB 在 `overwrite-uniform.sh:3`。
- 脚本包含 `fillseq`、`compact`、`overwrite` 三阶段，见 `overwrite-uniform.sh:17`、`overwrite-uniform.sh:20`、`overwrite-uniform.sh:23`。

### 11.3 设计文档

- `paper/delta-block-exact-gc-design.md:1`：Delta Block / Delta Separate / 精确 Blob GC 设计文档。
- `paper/delta-block-exact-gc-design.md:63`：delta block per-entry separated value size 说明。
- `paper/delta-block-exact-gc-design.md:96`：精确 GC 数据流。
- `paper/delta-block-exact-gc-design.md:1086`：实验建议。
- `paper/delta-separate-poc-switch.md:1`：Delta Block / Delta Separate POC 开关记录。
- `paper/paper-dev.md:53`、`paper/paper-dev.md:81`、`paper/paper-dev.md:107`：论文结构草稿。

## 12. 构建与依赖变更

- `CMakeLists.txt:550`、`CMakeLists.txt:551` 增加 Terark Zip include path。
- `CMakeLists.txt:568` 到 `CMakeLists.txt:570` 纳入 `block_cache_obsolete_tracker.cc`、`fifo_cache.cc`、`garbage_aware_cache.cc`。
- `CMakeLists.txt:677`、`CMakeLists.txt:678` 纳入 `delta_builder.cc`、`delta_reader.cc`。
- `CMakeLists.txt:726` 纳入 `util/hotness_tracker.cc`。
- `src.mk` 同步增加相关源文件。

需要特别注意：`CMakeLists.txt:451`、`CMakeLists.txt:458` 将 release/gnu 编译参数调整为偏调试的 `/Od`、`-O0 -g` 风格，这有利于实验调试，但如果用于正式性能评测或发布构建，需要确认是否符合预期。

## 13. 需要关注的风险与后续建议

1. **大日志文件被纳入版本库**：`log_rocksdb/storage_testdb_LOG` 新增约 299486 行，是本次 diff 行数暴涨的主要来源。若不是刻意保存实验日志，建议移出代码提交或改为 artifact 管理。
2. **Release 构建优化等级变化**：`CMakeLists.txt` 中引入 `-O0 -g` / `/Od` 会明显影响性能结果；若本分支用于 benchmark，对比时需固定并声明编译参数。
3. **新 metadata 与老 SST 兼容**：当前代码已通过 `use_delta_block`、`for_compaction`、properties fallback 做兼容保护，但仍建议在混合新旧 SST、manifest 旧记录缺失 `byte_count`、`precise_gc` 动态开关场景下增加回归测试。
4. **HotnessTracker 内存边界**：hot LRU 与 FIFO window 均受容量控制，但 dropped seqs、写入窗口容量和高写入速率场景下的内存/CPU 开销值得压测。
5. **Garbage-aware cache 策略参数敏感**：`admission_ratio`、`demote_score_threshold` 直接影响 cache hit 与污染控制，应通过 plot_tools 中的 motivation/final validation 脚本形成推荐参数。
6. **Blob GC fast path 正确性**：drop-key cache、death-map、block skipping 属于性能关键路径，应重点验证不会误跳过 live value，尤其是 sequence number、snapshot、merge operand 场景。

## 14. 功能域到文件的快速索引

| 功能域 | 主要文件 |
|---|---|
| Precise Blob GC | `db/compaction_job.cc`, `db/compaction_picker.cc`, `db/version_builder.cc`, `db/version_edit.cc`, `include/rocksdb/types.h` |
| Delta Block Metadata | `table/delta_builder.*`, `table/delta_reader.*`, `table/block_based_table_builder.cc`, `table/block_based_table_reader.cc`, `include/rocksdb/table.h` |
| Middle Delta Separate | `db/dbformat.*`, `db/compaction_iterator.*`, `options/cf_options.*`, `include/rocksdb/options.h` |
| Hotness Routing | `util/hotness_tracker.*`, `db/builder.cc`, `db/write_batch.cc`, `db/compaction_iterator.cc`, `db/column_family.*` |
| Garbage-aware Cache | `cache/garbage_aware_cache.*`, `include/rocksdb/cache.h`, `tools/db_bench_tool.cc` |
| Obsolete Cache Tracker | `cache/block_cache_obsolete_tracker.*`, `cache/lru_cache.*`, `include/rocksdb/cache.h` |
| Subcompaction Sampling | `db/compaction_job.cc`, `db/table_cache.*`, `table/table_reader.h`, `table/block_based_table_reader.cc` |
| 统计与可视化 | `include/rocksdb/statistics.h`, `monitoring/statistics.cc`, `plot_tools/*` |
| 实验开关 | `tools/db_bench_tool.cc`, `options/options_helper.cc`, `db/c.cc` |
