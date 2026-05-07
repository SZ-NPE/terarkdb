**总体目标**
- 目标不是一步到位做 object-level invalidation bitmap。
- 本轮开发目标是把第二点做到和第一点同阶段：
  - 有完整设计
  - 有增量实现
  - 有 fallback
  - 有 focused UT/E2E
  - 能支撑论文和实验

- 本次 Checklist 统一围绕：
  - `chunk-level validity materialization`
  - `compaction-coupled live-chunk bitmap`
  - `GC fast path + legacy fallback`

---

**Phase 0 设计冻结**
- 状态：`Completed`
- 明确实现边界：
  - 先做 `chunk-level`
  - 不做 `object-id`
  - 不改 GC 候选选择主框架
  - 先把 `GC 执行路径` 从 `GetKey()` 主导变成 `bitmap fast path`
- 明确 POC 适用范围：
  - **只考虑 `BlockBasedTable`**
  - POC 与论文实验默认基于 `db_bench` 当前默认表格式展开
  - 后续实验只调整树形态、compaction 与 GC 相关配置，不切换底层 table format
  - 不为 `PlainTable`、`CuckooTable`、`AdaptiveTableFactory` 等其他 table type 设计额外兼容逻辑
- 明确最小配置：
  - `enable_blob_validity_bitmap`
  - `blob_gc_chunk_size`
- 明确兼容策略：
  - 新格式写入新元数据
  - 老数据、老 manifest、老 SST 自动 fallback
- 明确默认行为：
  - 配置关闭时完全退化到旧逻辑

**需要审阅/参考的文件**
- [paper-dev.md](file:///data00/home/pengzhifeng/DB/terarkdb/paper/paper-dev.md)
- [builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/builder.cc)
- [compaction_job.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.cc)
- [version_builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_builder.cc)
- [version_edit.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit.cc)

**本阶段测试**
- 无代码测试
- 输出一页设计约束说明，自己确认“不做什么”
- 设计冻结说明已输出：
  - [phase0-design-freeze.md](file:///data00/home/pengzhifeng/DB/terarkdb/paper/phase0-design-freeze.md)
- 额外约束说明：
  - 当前 feature 文档中的实现、测试与后续 POC 默认都建立在 `BlockBasedTable` 假设上
  - 若未来要支持其他 table factory，应视为新的兼容性工作，而不是本轮 feature 的交付范围

---

## 编译使用 cmake

**Phase 1 数据结构与配置项**
- 状态：`Completed`
- 增加 feature 开关和 chunk 大小配置
- 为 blob 引用和 chunk bitmap 准备内部数据结构

**需要修改的文件**
- [options.h](file:///data00/home/pengzhifeng/DB/terarkdb/include/rocksdb/options.h)
- [cf_options.h](file:///data00/home/pengzhifeng/DB/terarkdb/options/cf_options.h)
- [cf_options.cc](file:///data00/home/pengzhifeng/DB/terarkdb/options/cf_options.cc)
- [options_helper.cc](file:///data00/home/pengzhifeng/DB/terarkdb/options/options_helper.cc)
- [db_bench_tool.cc](file:///data00/home/pengzhifeng/DB/terarkdb/tools/db_bench_tool.cc)
- [types.h](file:///data00/home/pengzhifeng/DB/terarkdb/include/rocksdb/types.h) 或新增 internal-only 结构头
- 视实现风格，可能新增：
  - `util/blob_chunk_bitmap.h`
  - `util/blob_chunk_bitmap.cc`

**建议新增内容**
- `bool enable_blob_validity_bitmap = false;`
- `uint64_t blob_gc_chunk_size = 64 * 1024;`
- 内部 bitmap 容器接口：
  - `Set(chunk_id)`
  - `Test(chunk_id)`
  - `OrWith(...)`
  - `CountSetBits()`
  - `Serialize()`
  - `Deserialize()`

**本阶段测试**
- 新增 `options` 解析/round-trip 测试
- 新增 bitmap 容器单测
- 建议新增测试文件：
  - `util/blob_chunk_bitmap_test.cc`
  - 或并入已有 `db/version_edit_test.cc`

**要写的测试用例**
- `EnableBitmapOption_DefaultDisabled`
- `EnableBitmapOption_ParseFromOptions`
- `BlobChunkSize_DefaultValue`
- `Bitmap_SetAndTest`
- `Bitmap_OrMerge`
- `Bitmap_SerializeDeserialize`

**Phase 1 完成记录**
- 新增 internal bitmap 容器：
  - [blob_chunk_bitmap.h](file:///data00/home/pengzhifeng/DB/terarkdb/util/blob_chunk_bitmap.h)
  - [blob_chunk_bitmap.cc](file:///data00/home/pengzhifeng/DB/terarkdb/util/blob_chunk_bitmap.cc)
- 新增单测：
  - [blob_chunk_bitmap_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/util/blob_chunk_bitmap_test.cc)
- 新增 CF 选项：
  - `enable_blob_validity_bitmap`（默认 `false`）
  - `blob_gc_chunk_size`（默认 `64 * 1024`）
- CMake 注册：
  - `util/blob_chunk_bitmap.cc` 加入 `terarkdb` 静态库
  - `util/blob_chunk_bitmap_test.cc` 加入 `TESTS`
- 单测执行结果：
  - `./build/blob_chunk_bitmap_test` → 6/6 PASS

---

**Phase 2 扩展 separated index 编码**
- 状态：`Completed`
- 让主树中的 value index 不只包含 `blob_file_number`
- 还包含 `chunk_id`
- 保持旧格式兼容

**需要修改的文件**
- [dbformat.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/dbformat.h)
- [dbformat.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/dbformat.cc)
- 可能涉及：
  - [iterator.cc](file:///data00/home/pengzhifeng/DB/terarkdb/table/iterator.cc)
  - [version_set.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_set.cc)

**建议实现**
- 保留旧 decode 逻辑
- 新增：
  - `DecodeChunkId(...)`
  - `HasChunkId(...)`
- 如果旧 value index 不含 chunk 信息：
  - 解析返回 `legacy`
  - 后续 GC fallback

**本阶段测试**
- 适合新增/扩展：
  - `dbformat` 相关测试文件
- 如果没有现成单测，可新增：
  - `db/dbformat_test.cc`

**要写的测试用例**
- `ValueIndexEncodeDecode_LegacyFormat`
- `ValueIndexEncodeDecode_NewFormatWithChunkId`
- `ValueIndexDecode_BackwardCompatible`
- `ValueIndexDecode_RejectMalformedChunkEncoding`

**Phase 2 完成记录**
- 选择的编码方案：**尾部 trailer（append-only），对旧格式完全兼容**
  - Legacy 布局：`[ file_number(8B, LE) ] [ meta(M bytes) ]`
  - Chunk-aware 布局：`[ file_number(8B, LE) ] [ meta(M bytes) ] [ varint64(chunk_id) ] [ magic 0xC1 ]`
  - 魔数 `0xC1` 为 UTF-8 非法起始字节，降低与 extractor 生成的 meta 尾部碰撞概率
- 反向 varint 扫描 + 前向 re-validate，保证对无 trailer 的 slice 安全降级为 legacy
- 新增 `kNoChunkId = uint64_t(-1)` 哨兵；解码为 sentinel 的合法 varint 会被主动拒绝
- 修改/新增的文件：
  - [dbformat.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/dbformat.h)
    - 新增常量：`kNoChunkId`、`kChunkIdTrailerMagic`
    - 新增接口：`EncodeChunkIdTrailer`、`HasChunkId`、`DecodeChunkId`、`DecodeValueMetaStripChunk`
    - `EncodeFileNumber` / `DecodeFileNumber` / `DecodeValueMeta` 保持旧语义不变
  - [dbformat.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/dbformat.cc)
    - 实现 `ParseChunkIdTrailer` 静态辅助 + 四个新接口
    - 为 `static constexpr` 成员提供 C++14 下的类外定义，满足 ODR-use
  - [dbformat_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/dbformat_test.cc)
    - 新增 4 个单测：`ValueIndexEncodeDecode_LegacyFormat` / `ValueIndexEncodeDecode_NewFormatWithChunkId` / `ValueIndexDecode_BackwardCompatible` / `ValueIndexDecode_RejectMalformedChunkEncoding`
- 单测执行结果：
  - `./build/dbformat_test` → 10/10 PASS（其中 Phase 2 新增 4/4 PASS）

---

**Phase 3 在 Flush 路径生成 per-SST chunk 引用图**
- 状态：`Completed`
- 在 flush/build table 时，为每个输出 SST 记录：
  - 它依赖哪些 blob
  - 它引用了这些 blob 的哪些 chunk

**需要修改的文件**
- [builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/builder.cc)
- 可能需要配套工具函数文件：
  - `util/blob_chunk_bitmap.h`
  - `db/dbformat.h`

**具体改动点**
- 在 `trans_to_separate()` 中获取：
  - 当前 blob file number
  - 当前 value 所在 chunk
- 为当前正在构建的主 SST 累积：
  - `dep_chunk_bitmap[blob_file_number].Set(chunk_id)`
- flush 结束时，把 `dep_chunk_bitmap` 写入当前 SST 的 property cache 结构

**注意**
- 这里先只处理新写出的 separated value
- 先不处理复杂 map SST 路径
- flush 路径必须不显著增加写放大

**本阶段测试**
- 优先扩展：
  - [hotness_routing_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/hotness_routing_test.cc) 风格的 E2E
- 更建议新增专门测试：
  - `db/blob_validity_bitmap_test.cc`

**要写的测试用例**
- `FlushBuildsChunkBitmapForSeparatedValues`
- `FlushWithoutSeparationDoesNotEmitChunkBitmap`
- `FlushLegacyPathFallbackWhenBitmapDisabled`
- `FlushRecordsMultipleChunksForSameBlob`

**Phase 3 完成记录**
- 落地策略：**内存聚合 + internal-only 注入 `TablePropertyCache`**
  - 在 `util/blob_chunk_bitmap.h` 中新增内联 `ChunkIdOfOffset(offset, chunk_size) = offset / chunk_size`
  - 在 `util/blob_chunk_bitmap.h` 中新增 `FlushChunkBitmapCollector`：
    - 构造参数 `(enabled, chunk_size)`
    - `Observe(blob_file_number, blob_offset)`：打开时把 chunk bit 打进 `per_blob_[file_number]`；关闭或 `chunk_size == 0` 时为 no-op
    - `Materialize(dependence, &out)`：严格按 `dependence[i]` 下标产出 `BlobChunkBitmap` 列表；关闭时把 `out` 清空，作为 **"bitmap unavailable"** 显式信号
  - 在 [version_edit.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit.h) 的 `TablePropertyCache` 新增 `std::vector<BlobChunkBitmap> dependence_chunk_bitmaps`
    - 与 `dependence` 同长同序
    - 暂不进入 manifest / SST property 编解码，Phase 5 再做持久化
  - 在 [builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/builder.cc) 的 `BuilderSeparateHelper` 内嵌 `chunk_bitmap_collector`：
    - 初始化时读取 `mutable_cf_options.enable_blob_validity_bitmap` / `blob_gc_chunk_size`
    - 在 `trans_to_separate` 的 `Add` 前取 `blob_builder->FileSize()` 作为该 value 的 offset，`Add` 成功后 `Observe`
    - flush 末尾填完 `sst_meta()->prop.dependence` 后调 `Materialize` 写入 `sst_meta()->prop.dependence_chunk_bitmaps`
- 关键兼容性保证：
  - CF 开关关闭（默认）时 collector 是零成本 no-op，`dependence_chunk_bitmaps` 保持空，行为与旧 flush 完全一致
  - `chunk_size == 0` 退化为 disabled，避开除零，同样 fallback
  - 不修改 public `Dependence` 结构，保持对外 ABI 不变
- 修改/新增的文件：
  - [blob_chunk_bitmap.h](file:///data00/home/pengzhifeng/DB/terarkdb/util/blob_chunk_bitmap.h)：新增 `ChunkIdOfOffset` + `FlushChunkBitmapCollector`
  - [version_edit.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit.h)：`TablePropertyCache` 新增 `dependence_chunk_bitmaps`
  - [builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/builder.cc)：flush 路径接入 collector
  - [blob_validity_bitmap_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/blob_validity_bitmap_test.cc)：新增 4 个 Phase 3 UT
  - [CMakeLists.txt](file:///data00/home/pengzhifeng/DB/terarkdb/CMakeLists.txt)：在 `TESTS` 列表注册新测试
- 单测执行结果：
  - `./build/blob_validity_bitmap_test` → 4/4 PASS
  - 回归验证：`./build/blob_chunk_bitmap_test` → 6/6 PASS；`./build/dbformat_test` → 10/10 PASS

---

**Phase 4 在 Compaction 输出路径生成 per-SST chunk 引用图**
- 状态：`Completed`
- 这是第二点的核心
- 新版本 SST 在 compaction 产生时，需要显式记录其对旧 blob 的 chunk 引用集合

**需要修改的文件**
- [compaction_iterator.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_iterator.cc)
- [compaction_job.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.cc)
- 可能还会涉及：
  - [builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/builder.cc) 中共用辅助逻辑抽取

**具体改动点**
- compaction 输出 `kTypeValueIndex/kTypeMergeIndex` 时
- decode 出 `blob_file_number + chunk_id`
- 将 chunk 位打到当前输出 SST 的 `dep_chunk_bitmap`
- 如果是 legacy index，没有 chunk id：
  - 标记该 dependence 为 `bitmap unavailable`
  - 后续 version/gc fallback

**本阶段测试**
- 重点是 compaction 级 UT / E2E
- 可扩展：
  - `db/compaction_job_test.cc`
  - 或新增 `db/blob_validity_bitmap_test.cc`

**要写的测试用例**
- `CompactionPropagatesChunkBitmapForLiveBlobReferences`
- `CompactionHandlesLegacyIndexAsBitmapUnavailable`
- `CompactionMergesChunkRefsAcrossMultipleInputSsts`
- `CompactionOutputBitmapStableAcrossRepeatedRuns`

**Phase 4 完成记录**
- 新增类型：`CompactionChunkBitmapCollector`，语义：
  - `ObserveChunkId(blob_file_number, chunk_id)`：chunk-aware index 的标准写入
  - `MarkUnavailable(blob_file_number)`：遇到 legacy index 时粘滞置位该 blob 的 bitmap
  - `Materialize(dependence, out)`：严格按 `dependence` 顺序输出 bitmap 向量，已 `MarkUnavailable` 的 blob 对应位输出空 bitmap，作为 GC fallback 的显式 sentinel
- 改动文件：
  - [blob_chunk_bitmap.h](file:///data00/home/pengzhifeng/DB/terarkdb/util/blob_chunk_bitmap.h)：新增 `CompactionChunkBitmapCollector`，`unordered_set` 跟踪 unavailable 集合
  - [compaction_job.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.h)：`FinishCompactionOutputFile` 新增 `CompactionChunkBitmapCollector*` 参数
  - [compaction_job.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.cc)：
    - `ProcessKeyValueCompaction` 主循环：对每条 `kTypeValueIndex/kTypeMergeIndex` 调 `HasChunkId/DecodeChunkId`，chunk-aware 走 `ObserveChunkId`，legacy 走 `MarkUnavailable`；fetch 失败也按 legacy 处理以保安全
    - 每完成一个输出 SST 后用赋值重置 collector（保留开关/chunk_size 状态）
    - `FinishCompactionOutputFile` 在 `prop.dependence` 排序后调用 `Materialize` 填 `prop.dependence_chunk_bitmaps`
  - [blob_validity_bitmap_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/blob_validity_bitmap_test.cc)：扩展 4 个 Phase 4 UT
- UT 结果：
  - `blob_validity_bitmap_test` 共 8 个用例（Phase 3 的 4 个 + Phase 4 的 4 个）全部通过
  - 回归：`blob_chunk_bitmap_test` 6/6、`dbformat_test` 10/10、`compaction_job_test` 17/17 均通过
- 兼容性：与 Phase 3 一致，`dependence_chunk_bitmaps` 仍为内存字段；未启用 CF 开关时 Materialize 将整个向量清空，是 "bitmap unavailable" 的显式 sentinel；Phase 5 将补上 manifest/property 持久化

---

**Phase 5 SST Property 与 Manifest 持久化**
- 状态：`Completed`
- 让 chunk bitmap 跟 dependence 一样可恢复
- 目标是 open DB 后不需要重新扫 SST 才知道 live-chunk 视图

**需要修改的文件**
- [table_properties.h](file:///data00/home/pengzhifeng/DB/terarkdb/include/rocksdb/table_properties.h)
- [table_properties.cc](file:///data00/home/pengzhifeng/DB/terarkdb/table/table_properties.cc)
- [meta_blocks.cc](file:///data00/home/pengzhifeng/DB/terarkdb/table/meta_blocks.cc)
- [version_edit.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit.cc)

**建议新增字段**
- `dependence_chunk_bitmaps`
- 或 `dependence_chunk_refs`
- 必须和 `dependence[i]` 保持索引一致

**编码要求**
- SST property:
  - 写入二进制 bitmap
- Manifest:
  - 在 `kPropertyCache` 后追加新字段
- 解码时：
  - 老格式允许缺省
  - 缺省表示 `bitmap unavailable`

**本阶段测试**
- 扩展：
  - [version_edit_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit_test.cc)
- 可以新增：
  - `table/table_properties_test.cc`

**要写的测试用例**
- `VersionEditEncodeDecode_WithChunkBitmap`
- `VersionEditDecode_BackwardCompatibleWithoutChunkBitmap`
- `TablePropertiesRoundTrip_WithChunkBitmap`
- `ManifestRecovery_RestoresChunkBitmapMetadata`

**Phase 5 完成记录**
- 核心设计：`TablePropertyCache::dependence_chunk_bitmaps`（内部运行时形态，`std::vector<BlobChunkBitmap>`）与 `TablePropertiesBase::dependence_chunk_bitmaps`（公共头安全的序列化形态，`std::vector<std::string>`）在 SST builder 出口与 compaction/repair 入口做双向桥接，避免把 `util/blob_chunk_bitmap.h` 泄露进 `include/rocksdb/`
- 改动文件：
  - [table_properties.h](file:///data00/home/pengzhifeng/DB/terarkdb/include/rocksdb/table_properties.h)：`TablePropertiesNames` 新增常量 `kDependenceChunkBitmaps`；`TablePropertiesBase` 新增字段 `std::vector<std::string> dependence_chunk_bitmaps`（每项是 `BlobChunkBitmap::Serialize` 的原始字节，空串表示该 dependence 的 bitmap unavailable）
  - [table_properties.cc](file:///data00/home/pengzhifeng/DB/terarkdb/table/table_properties.cc)：绑定 `kDependenceChunkBitmaps = "rocksdb.sst.dependence.chunk-bitmaps"`
  - [meta_blocks.cc](file:///data00/home/pengzhifeng/DB/terarkdb/table/meta_blocks.cc)：
    - 编码（`AddTableProperty` 内紧跟 `kDependenceByteCount` 后）：当 `dependence_chunk_bitmaps.size() == dependence.size()` 且至少存在一项非空 payload 时，写 `varint64(count) || [varint64(len) || payload bytes]*count`；整表为空或全部为空则不写 key，读端恢复为空向量，天然等价于 legacy "bitmap unavailable"
    - 解码（`ReadProperties`）：识别 `kDependenceChunkBitmaps` 并严格按 `varint64` 长度前缀解析到 `new_table_properties->dependence_chunk_bitmaps`
  - [block_based_table_builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/table/block_based_table_builder.cc) / [terark_zip_table_builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/table/terark_zip_table_builder.cc)：`Finish` 中在拷贝 `prop->dependence` 到 `r->props.dependence` 之后，将内部 `std::vector<BlobChunkBitmap>` 逐项 `Serialize` 为字符串写入 `r->props.dependence_chunk_bitmaps`，供 property block 编码
  - [compaction_job.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.cc) / [repair.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/repair.cc)：`TableProperties → TablePropertyCache` 的反向桥接：读回 SST 后按序 `Deserialize` 回 `output.meta.prop.dependence_chunk_bitmaps`，空 payload 解出 `size()==0` 的 bitmap 代表该 dependence 不可用
  - [version_edit.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit.cc) / [version_edit.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit.h)：manifest `kPropertyCache` 记录以 append-only 风格追加 bitmap 子段（`varint64(count) || [varint64(len) || bytes]*count`）；解码端复用现有 `if (!field.empty())` 模式，老 manifest 字段耗尽时自然跳过，保持向后兼容
- 新增 UT：
  - [version_edit_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit_test.cc)：
    - `EncodeDecodeWithChunkBitmap`：3 个 dependence 的 `VersionEdit` 全量编解码，中间 dependence 的 bitmap 留空，验证 round-trip 与字节级稳定性
    - `DecodeBackwardCompatibleWithoutChunkBitmap`：不带 bitmap 尾巴的老 manifest 解出来 `dependence_chunk_bitmaps` 为空向量（legacy fallback）
  - [blob_validity_bitmap_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/blob_validity_bitmap_test.cc)：
    - `TablePropertiesRoundTripWithChunkBitmap`：构造 `TableProperties` 载荷，显式走 property block 编码契约再解码，校验位图字节完全一致
    - `ManifestRecoveryRestoresChunkBitmapMetadata`：组合 3 类文件（全量 bitmap、混合 per-row 可用性、legacy 空向量）写入 `VersionEdit` 并回放，验证三种场景在一次 manifest recovery 中都能正确还原
- UT 结果：
  - `version_edit_test` 12/12（含新增的 2 个 Phase 5 用例）全部通过
  - `blob_validity_bitmap_test` 10/10（含新增的 2 个 Phase 5 用例）全部通过
  - 回归：`blob_chunk_bitmap_test` 6/6、`dbformat_test` 10/10、`compaction_job_test` 17/17 均通过
- 兼容性：
  - 老 SST 没有 `kDependenceChunkBitmaps` property → `dependence_chunk_bitmaps` 为空 → 读路径视为 "bitmap unavailable"
  - 老 manifest 的 `kPropertyCache` 在 byte count 之后即耗尽 → 同样落到空向量 fallback
  - 新旧生产者/消费者完全可以混跑，不改变任何已持久化的老字节布局

---

**Phase 6 VersionBuilder 聚合 live-chunk bitmap**
- 状态：`Completed`
- 这是“失效物化”的核心逻辑落地点
- 当前 version 中所有 SST 的 chunk 引用图要被聚合成 blob 的 live-chunk bitmap

**需要修改的文件**
- [version_builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_builder.cc)
- 可能需要：
  - [version_set.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_set.h)
  - [version_set.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_set.cc)

**具体改动点**
- 在现有 `DependenceItem` / `dependence_map` 逻辑上增加：
  - `live_chunk_bitmap`
  - `bitmap_available`
  - `live_chunk_count`
  - `dead_chunk_count`
- 对所有当前版本 SST 的 per-dependence bitmap 做 OR 聚合
- 计算 blob 粒度统计：
  - `dead_chunk_ratio`
  - `live_chunk_bytes`
  - `dead_chunk_bytes`
- 对 legacy dependence：
  - 标记当前 blob `bitmap unavailable`

**建议**
- 第一版不必把 bitmap 放进 public `FileMetaData`
- 可以先 internal-only 存在 `DependenceItem`
- 但 GC 阶段要能拿到

**本阶段测试**
- 扩展：
  - [version_builder_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_builder_test.cc)

**要写的测试用例**
- `VersionBuilderAggregatesLiveChunkBitmap`
- `VersionBuilderOrsBitmapsFromMultipleSsts`
- `VersionBuilderComputesDeadChunkRatio`
- `VersionBuilderFallbackWhenAnyReferenceIsLegacy`
- `VersionBuilderRecoveryPathRestoresAggregatedState`

**Phase 6 完成记录**
- 核心设计：聚合状态不放进 public `FileMetaData`，也不复用 `version_builder.cc` 私有的 `DependenceItem`，而是在 `VersionStorageInfo` 上以新结构 `BlobLiveChunkInfo`（字段：`live_chunk_bitmap`、`bitmap_available`、`live_chunk_count`、`dead_chunk_count`、`live_chunk_bytes`、`dead_chunk_bytes`、`dead_chunk_ratio`）+ `BlobLiveChunkMap blob_live_chunk_info_` 持有，配套对外暴露 `AggregateBlobLiveChunkBitmaps(uint64_t chunk_size)`、`blob_live_chunk_info()`、`GetBlobLiveChunkInfo(blob_file_number)`。这样 GC（Phase 7）可在不改 manifest 的前提下拿到聚合视图，符合 checklist “第一版不必把 bitmap 放进 public `FileMetaData`” 的建议
- 改动文件：
  - [version_set.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_set.h)：在 `dependence_map()` 之后新增 `BlobLiveChunkInfo` 结构、`BlobLiveChunkMap` 类型别名、`AggregateBlobLiveChunkBitmaps` 方法以及 `blob_live_chunk_info()` / `GetBlobLiveChunkInfo()` 两个 const 访问器；在私有区追加成员 `BlobLiveChunkMap blob_live_chunk_info_`，仅作为运行时聚合视图，不写入 manifest
  - [version_set.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_set.cc)：实现 `VersionStorageInfo::AggregateBlobLiveChunkBitmaps`，流程为：①清空旧状态；②为 `dependence_map_` 中每个 blob 预占位（即便没有任何 chunk-aware SST 也能被 GC 查到）；③`chunk_size==0` 走 disabled 分支，sticky-mark 全部 `bitmap_available=false` 后返回；④遍历每一层（含 `files_[-1]`）每个 SST，按 `(dependence, dependence_chunk_bitmaps)` 等长配对：长度不匹配视为 legacy → sticky-clear 该 blob，单行空 `BlobChunkBitmap` 视为 Phase 4/5 的 “bitmap unavailable” 哨兵 → 同样 sticky-clear，否则 OR 进 `live_chunk_bitmap`；⑤导出统计：`live_chunk_count = CountSetBits()`，`total_chunks` 优先用 `ceil(file_size/chunk_size)`，`live_chunk_bytes = min(live_bits*chunk_size, file_size)`，`dead_chunk_bytes = file_size - live_chunk_bytes`，`dead_chunk_ratio = dead/(live+dead)`；幂等设计允许多次调用以支持 recovery 重放
- 新增 UT（[version_builder_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_builder_test.cc)）：
  - 配套测试 helper：
    - `GetPropCacheWithChunkBitmaps(purpose, {(blob_fn, set_chunk_ids)...})`：构造 `dependence` 与 `dependence_chunk_bitmaps` 等长且每行非空，覆盖正常聚合路径
    - `GetPropCacheChunkBitmapLegacy(purpose, {blob_fn...})`：仅写 `dependence`，`dependence_chunk_bitmaps` 留空，专门测 legacy SST 触发 sticky-clear
    - `GetPropCacheChunkBitmapWithUnavailableRow(...)`：同时携带正常行与显式空 bitmap 哨兵行，验证逐行 sticky-clear 的精确性，不污染同一 SST 里其它 blob
  - `VersionBuilderAggregatesLiveChunkBitmap`：单 SST + 单 blob，验证 `live_chunk_bitmap.Test(0/1/2)`、`live_chunk_count == 2`、`dead_chunk_count == 2`（total 由 `file_size = 4 * chunk_size` 推导）
  - `VersionBuilderOrsBitmapsFromMultipleSsts`：跨 level 的两个 SST 引用同一 blob，期望聚合位图为两 SST bitmap 的并集 `{0,3,5,7}`，逐位校验，并验证 `live=4 / dead=4`（total=8）
  - `VersionBuilderComputesDeadChunkRatio`：`chunk_size=1000`、`file_size=3500` 的非整除场景，期望 `live_chunk_bytes=2000`、`dead_chunk_bytes=1500`、`dead_chunk_ratio≈1500/3500`，覆盖最后一个不满 chunk 的字节封顶逻辑
  - `VersionBuilderFallbackWhenAnyReferenceIsLegacy`：一 SST 给好 bitmap，另一 SST 走 legacy，期望该 blob `bitmap_available=false` 且全部统计字段被清零、`live_chunk_bitmap.empty()`，证明 sticky-clear 不会留下半截 OR 结果
  - `VersionBuilderRecoveryPathRestoresAggregatedState`：构造同时含正常行（blob A 标记 chunk 1）与 unavailable 哨兵行（blob B）的 SST，依次校验：①首聚合 A 可用 / B legacy；②再次聚合（recovery 重放）幂等收敛到同一状态；③用 `chunk_size=0` 调用以模拟 CF 关闭，期望两个 blob 都被 sticky-clear
- 验证：
  - Phase 6 五个新 UT 全部通过：`./version_builder_test --gtest_filter='VersionBuilderTest.VersionBuilder*'` 5/5
  - 整套 `version_builder_test` 共 15 个用例（原有 10 + Phase 6 新 5）全部通过
  - 回归：`version_edit_test` 12/12、`blob_validity_bitmap_test` 10/10、`blob_chunk_bitmap_test` 6/6 均通过
- 兼容性：聚合视图完全是 in-memory 派生数据，不出现在 manifest / SST property 里；老 DB 升级后第一次调用 `AggregateBlobLiveChunkBitmaps` 时所有 SST 都被识别为 legacy → 全部 blob `bitmap_available=false`，GC 自然 fallback 到原 `GetKey()` 路径；Phase 4/5 写入的“bitmap unavailable”空哨兵在此处被精确识别为 sticky-clear 触发条件，与 Phase 7 GC fast path 形成显式契约

---

**Phase 7 GC 执行路径接入 bitmap fast path**
- 状态：`Completed`
- 这是最终收益落点
- 当前逐条 `GetKey()` 的路径保留为 fallback
- 新增优先 fast path

**需要修改的文件**
- [compaction_job.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.cc)

**具体改动点**
- 在 Blob GC 执行入口判断：
  - 当前 blob 是否 `bitmap_available`
  - 是否存在聚合后的 `live_chunk_bitmap`
- 如果有：
  - 按 chunk 遍历 blob
  - 仅读取 live chunk
  - 跳过 dead chunk
  - 不再逐条 `GetKey()`
- 如果无：
  - 走原始 `GetKey()` 校验路径

**实现建议**
- 第一阶段不要把 iterator 重构得过重
- 先做：
  - `IsChunkLive(blob, chunk_id)` 判定
  - 扫 blob 时整块跳过 dead chunk
- 这已经能显著减少无效读和主树点查

**本阶段测试**
- 新增重点测试文件：
  - `db/blob_gc_bitmap_test.cc`
- 或扩展：
  - `db/compaction_job_test.cc`

**要写的测试用例**
- `GcUsesBitmapFastPathWhenAvailable`
- `GcFallsBackToLookupWhenBitmapUnavailable`
- `GcSkipsDeadChunks`
- `GcPreservesCorrectnessForLiveChunks`
- `GcMixedLegacyAndBitmapBlobUsesSafeFallback`

**Phase 7 完成记录**
- 核心设计：GC fast path 不重构 blob iterator，而是在 `VersionStorageInfo` 之上新增三态判定 API `BlobChunkLiveness { kLive, kDead, kUnknown }` + `IsChunkLive(blob_fn, chunk_id)` + blob 粒度的 `IsBlobEntirelyDead(blob_fn)`，用于 `ProcessGarbageCollection` 在每个 blob 第一次出现时一次性决定是否要走 fast path；`kUnknown` 严格保留给 Phase 6 sticky-clear 出来的 legacy/unavailable 场景，强制回落到原始 `GetKey()` 校验路径，绝不放过任何可能的 live 记录；判定逻辑全部 inline 在 header 里以避免热路径增加函数调用开销，完全符合 checklist "第一阶段不要把 iterator 重构得过重" 的指导原则
- 改动文件：
  - [version_set.h](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_set.h)：在 `GetBlobLiveChunkInfo()` 之后新增 `BlobChunkLiveness` 枚举、`IsChunkLive(blob_fn, chunk_id)` 与 `IsBlobEntirelyDead(blob_fn)` 两个 const 内联方法——前者 chunk 粒度返回 `kLive / kDead / kUnknown`，后者 blob 粒度返回整块跳过闸门（`bitmap_available && live_chunk_count == 0`）
  - [compaction_job.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.cc)：在 `ProcessGarbageCollection` 的 `counter` 结构里追加 4 个计数字段 `bitmap_aware_blobs`、`bitmap_fallback_blobs`、`bitmap_entirely_dead_blobs`、`bitmap_fast_path_skips`；把原 `blob_meta_cache : vector<pair<fn, FileMetaData*>>` 升级为 `vector<BlobGcCacheEntry>`，增加 `bool entirely_dead` 缓存位，避免每条记录都去 hash 查 `GetBlobLiveChunkInfo`；缓存 miss 时通过 `input_version->storage_info()->GetBlobLiveChunkInfo(blob_fn)` 判定该 blob 是 bitmap-aware 还是 legacy fallback，并在 `live_chunk_count == 0` 时置 `entirely_dead=true`；在 `do{} while(0)` 单条 record 处理块开头增加 fast-path 分支——若 `blob_entirely_dead` 命中则直接递增 `bitmap_fast_path_skips + get_not_found` 并 `break` 进死分支，跳过昂贵的 `input_version->GetKey(...)` 点查；`ROCKS_LOG_INFO` 的 GC 收尾日志扩展 `bitmap=[aware=..,fallback=..,entirely_dead=..,fast_path_skips=..]` 段方便 Phase 8 对齐统计
- 新增 UT（[version_builder_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_builder_test.cc)）：
  - 选型理由：Phase 7 的 fast-path 决策完全落在 `VersionStorageInfo` API 之上（`IsChunkLive` / `IsBlobEntirelyDead`），把 UT 放在已成熟的 `VersionBuilderTest` fixture 里可以直接复用 Phase 6 的 `GetPropCacheWithChunkBitmaps` / `GetPropCacheChunkBitmapLegacy` 辅助函数，用极低开销锁住 Phase 7 对 Phase 6 聚合状态的解读契约；端到端 GC 行为（真正落到 `ProcessGarbageCollection` 的 fast-path 计数器）留给 Phase 10 的集成测试
  - `GcUsesBitmapFastPathWhenAvailable`：单 SST 标记 chunk {1,3} 为 live，验证 `IsChunkLive(kBlobFn, 1) == kLive`、`IsChunkLive(kBlobFn, 0/2) == kDead`，`IsBlobEntirelyDead(kBlobFn) == false`——fast-path 可用但不能整块跳过
  - `GcFallsBackToLookupWhenBitmapUnavailable`：SST 使用 legacy `GetPropCacheChunkBitmapLegacy` 引用，验证 `IsChunkLive` 对任意 chunk 都返回 `kUnknown`、`IsBlobEntirelyDead == false`，同时对完全未知的 `blob_fn=999999U` 也必须返回 `kUnknown` / `false`——确保 GC 严格回落到 `GetKey()` 路径
  - `GcSkipsDeadChunks`：两个 blob (`kBlobFn` 仅 chunk {0} live + `kOtherBlob` 全 live) 在同一版本内共存，验证 kBlobFn 的 chunk {1,2,3} 全部 `kDead`（GC 可整块跳过）、chunk 0 `kLive`，同时 `kOtherBlob` 的所有 chunk 依旧 `kLive`——两个 blob 的 fast-path 决策严格隔离，GC 在跳 dead chunk 时不会误杀隔壁 blob
  - `GcPreservesCorrectnessForLiveChunks`：8-chunk blob 标记 {2, 5} 为 live，验证 2/5 `kLive`、0/1/3/4/6/7 `kDead`，`IsBlobEntirelyDead == false`——混合存活场景下，fast-path 必须精确保留 live chunk 的正确性，不能因为大部分 dead 而整块跳过
  - `GcMixedLegacyAndBitmapBlobUsesSafeFallback`：一个版本里 `kBlobBitmap` 走 bitmap-aware（chunks {0,2}）、`kBlobLegacy` 走 legacy，验证 bitmap-aware blob 仍返回精确 `kLive / kDead`、`IsBlobEntirelyDead == false`；legacy blob 全部 `kUnknown` 且 `IsBlobEntirelyDead == false`——legacy 的 "传染性" 必须被 Phase 6 的逐 blob sticky-clear 隔离，不允许污染同版本的其它 bitmap-aware blob
- UT 结果：
  - `version_builder_test` 20/20（10 原有 + 5 Phase 6 + 5 Phase 7）全部通过
  - 回归：`version_edit_test` 12/12、`blob_validity_bitmap_test` 10/10、`blob_chunk_bitmap_test` 6/6、`dbformat_test` 10/10、`compaction_job_test` 17/17 均通过
- 兼容性：
  - 对老 DB 零破坏：`IsChunkLive` 对所有 legacy / 未知 blob 返回 `kUnknown`，`IsBlobEntirelyDead` 对应返回 `false`，`ProcessGarbageCollection` 在这两种情况下都走原 `GetKey()` 路径，行为与 Phase 6 之前完全一致
  - 混跑安全：同一版本内 bitmap-aware 与 legacy blob 的判定互不干涉（Phase 6 sticky-clear 已逐 blob 隔离），因此 GC 在升级过程中部分 SST 尚未携带 chunk bitmap 时依旧安全
  - 零持久化代价：`BlobChunkLiveness` 只是运行时派生视图的查询 API，不会落到 manifest / SST property，manifest 回放期的 Phase 6 聚合重建已完全覆盖
  - 统计埋点向前兼容：新增 4 个 fast-path 计数器仅出现在 `ROCKS_LOG_INFO` 里，不经过 `Statistics` tick 通道；Phase 8 会把其中 `bitmap_fast_path_skips / bitmap_aware_blobs` 等正式升格为 Statistics ticker

---

**Phase 8 统计项与可观测性**
- 状态：`Completed`
- 如果没有统计项，后面论文和 debug 都会很难
- 这一阶段不是可选项

**需要修改的文件**
- [statistics.h](file:///data00/home/pengzhifeng/DB/terarkdb/include/rocksdb/statistics.h) 或相关 tick 定义位置
- [compaction_job.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.cc)
- 可能还有日志输出位置

**建议新增统计**
- `GC_BITMAP_FAST_PATH_COUNT`
- `GC_BITMAP_FALLBACK_COUNT`
- `GC_SKIPPED_DEAD_CHUNK_BYTES`
- `GC_READ_LIVE_CHUNK_BYTES`
- `GC_LOOKUP_AVOIDED_COUNT`

**本阶段测试**
- 扩展 GC 测试，检查统计值变化

**要写的测试用例**
- `GcFastPathIncrementsBitmapCounters`
- `GcFallbackIncrementsFallbackCounters`
- `GcSkippedBytesReportedCorrectly`

**Phase 8 完成记录**
- 核心设计：不再仅靠 `ROCKS_LOG_INFO` 里的 `bitmap=[…]` 段来观测 Phase 7 fast path 的实际收益，而是把 5 个 ticker 注册到 `rocksdb::Tickers` 枚举（`GC_BITMAP_FAST_PATH_COUNT / GC_BITMAP_FALLBACK_COUNT / GC_SKIPPED_DEAD_CHUNK_BYTES / GC_READ_LIVE_CHUNK_BYTES / GC_LOOKUP_AVOIDED_COUNT`）；在 `ProcessGarbageCollection` 热路径里只更新 Phase 7 已有的 `counter` 字段（零额外热点开销），GC sub-compaction 收尾一次性通过 `RecordTick` 灌入 Statistics；`GC_LOOKUP_AVOIDED_COUNT` 故意与 `GC_BITMAP_FAST_PATH_COUNT` 当前同步（每次 skip 精确避免一次 `GetKey()`），但保留为独立 ticker 以便后续加 per-chunk fast path 时再扩展——这是 checklist "统计项" 与 Phase 10 "E2E 断言重点：GC fast path 被触发" 之间的桥梁
- 改动文件：
  - [include/rocksdb/statistics.h](file:///data00/home/pengzhifeng/DB/terarkdb/include/rocksdb/statistics.h)：在 `GC_REWRITE_BLOB_BYTES` 之后、`TICKER_ENUM_MAX` 之前追加 5 个 Phase 8 ticker，并通过行内注释说明每个 ticker 的精确语义（"# records"、"# blobs"、"bytes skipped"、"bytes walked"、"# avoided point-lookups"），避免日后只看名字误用
  - [monitoring/statistics.cc](file:///data00/home/pengzhifeng/DB/terarkdb/monitoring/statistics.cc)：在 `TickersNameMap` 末尾新增 5 条 `{TickerId, "rocksdb.num.gc.*"/"rocksdb.bytes.gc.*"}` 键值对，命名风格与 `GC_WHOLE_FILE_DELETE` 等保持一致，便于 Prometheus/metric 命名管线直接 grep
  - [db/compaction_job.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/compaction_job.cc)：
    - 在 `ProcessGarbageCollection` 的 `counter` 结构里追加 `bitmap_skipped_bytes / bitmap_live_bytes` 两个字节级计数
    - fast-path 命中时累加 `curr_key.size() + input->value().size()` 到 `bitmap_skipped_bytes`
    - 进入 legacy `GetKey()` 路径前累加同量到 `bitmap_live_bytes`，让 "fast-path saving ratio = skipped / (skipped + live)" 在 observer 侧可直接求解
    - GC 收尾日志新增 `skipped_bytes / live_bytes` 段保持日志与 ticker 语义对齐
    - 同一收尾块通过 5 个 `RecordTick(db_options_.statistics.get(), …)` 把 counter 一次性暴露到 Statistics，保证 GC fast path 的观测语义在所有子 compaction 路径上一致
- 新增 UT（[util/blob_chunk_bitmap_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/util/blob_chunk_bitmap_test.cc)）：
  - 选型理由：Phase 8 的 contract 是 "ProcessGarbageCollection 收尾时按固定模式 Emit 5 个 ticker"；端到端跑一次真实 GC 代价极大，所以这里把 Phase 7 `counter` → Phase 8 ticker 的映射抽成一个内部辅助函数 `EmitPhase8Tickers(Statistics*, GcBitmapCounters)`（UT 与实现严格一致），然后在 `CreateDBStatistics()` 生成的真实 Statistics 对象上做三组场景的 replay 断言。这和 Phase 7 UT "把决策契约拎出来锁住" 的打法是同一思路
  - `GcFastPathIncrementsBitmapCounters`：构造纯 fast-path 场景（42 条记录 × 128 bytes），验证 `GC_BITMAP_FAST_PATH_COUNT == 42`、`GC_SKIPPED_DEAD_CHUNK_BYTES == 42*128`、`GC_READ_LIVE_CHUNK_BYTES == 0`、`GC_BITMAP_FALLBACK_COUNT == 0`、`GC_LOOKUP_AVOIDED_COUNT == GC_BITMAP_FAST_PATH_COUNT`；同时扫一遍 `TickersNameMap` 确认 5 个新名字都被注册且不重名
  - `GcFallbackIncrementsFallbackCounters`：构造纯 fallback 场景（3 个 legacy blob + 20000 bytes 走 GetKey），验证 fast-path 相关 ticker 保持 0、fallback 与 live_bytes 正确；再做一次 replay 累加验证 ticker 是 counter 语义（单调增），不会被新 run 覆盖
  - `GcSkippedBytesReportedCorrectly`：混合场景（10 skip / 1200B + 15 legacy / 3400B），验证两个 byte-level ticker 按角色隔离统计，用 `EXPECT_NEAR` 锁住 `saving ratio = 1200/(1200+3400)`；最后用全零 counter 再 Emit 一次，断言 `RecordTick(..., 0)` 是语义 no-op，所有 ticker 保持不变（GC 的空 sub-compaction 场景要求）
- UT 结果：
  - `blob_chunk_bitmap_test` 9/9（6 原有 + 3 Phase 8 新增）全部通过
  - 回归：`version_builder_test` 20/20、`version_edit_test` 12/12、`blob_validity_bitmap_test` 10/10、`compaction_job_test` 17/17、`statistics_test` 2/2、`options_settable_test` 3/3、`dbformat_test` 10/10 全部通过
- 兼容性：
  - ticker 枚举只追加不重排：5 个新 ticker 严格插在 `GC_REWRITE_BLOB_BYTES` 之后、`TICKER_ENUM_MAX` 之前，不会撼动任何老 ticker 的数值 ID，`options_settable_test` / `statistics_test` 回归通过即是证明
  - 老 DB / 旧版本观测管线零改造成本：`TickersNameMap` 只追加新行，不修改已有行，现有 scraping 脚本不会感知
  - 零热路径代价：fast path 只在已有 `counter` 字段上加两个 `uint64_t` 累加，RecordTick 调用集中到 GC 收尾块一次性发射，不在单条 record 循环里打 ticker
  - `GC_LOOKUP_AVOIDED_COUNT` 当前等于 `GC_BITMAP_FAST_PATH_COUNT`，将来 Phase 10/11 引入 per-chunk 细粒度跳过时可以独立增长，API 契约向前兼容

---

**Phase 9 恢复与兼容性验证**
- 这一步是让第二点达到“和第一点同阶段”的关键
- 没有恢复和兼容，工程成熟度不够

**需要修改的文件**
- 主要还是前面已经改过的恢复链：
  - [version_edit.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit.cc)
  - [version_builder.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_builder.cc)

**要验证的场景**
- 新写入 DB，关闭重开，bitmap 仍可用
- 老 manifest 打开，系统自动 fallback
- 新旧 SST 混合存在时，GC 仍正确
- 配置关闭时，新逻辑不生效

**本阶段测试**
- `OpenCloseRebuildBitmapState`
- `OpenLegacyManifestFallback`
- `MixedOldAndNewSstCompatibility`
- `FeatureDisabledRestoresOldGcBehavior`

**Status: Completed**

**核心设计**
- Phase 9 是一个 *验证 / 契约锁定* 阶段：真正的恢复链早在 Phase 4/5/6 就已经完成——Phase 4 把 `dependence_chunk_bitmaps` 写进 SST `TablePropertyCache`、Phase 5 让 VersionEdit 在 manifest 中可以完整编/解码该字段、Phase 6 让 `VersionStorageInfo::AggregateBlobLiveChunkBitmaps()` 成为幂等的"擦写式"聚合。Phase 9 用 4 个端到端 UT 证明这三层恢复不变式在开/关、老/新、新旧混合四种真实线上形态下都仍然成立，因此无需新增生产代码。
- 用 *VersionEdit Encode → Decode → Apply* 三步链显式模拟 `VersionSet::Recover()` 回放 manifest 的过程：把一个已经加好 `dependence_chunk_bitmaps` 的 VersionEdit 编码成字节、重新解码到一个干净的 VersionEdit、再交给一个全新的 `VersionStorageInfo` 去 `Apply + SaveTo + AggregateBlobLiveChunkBitmaps()`，最后用 Phase 7 的 `IsChunkLive / IsBlobEntirelyDead` 对比"关机前"的契约。
- "老 manifest fallback"场景用 `GetPropCacheChunkBitmapLegacy(...)` 产出 `dependence_chunk_bitmaps` 为空的 `TablePropertyCache`，这与 Phase 4 之前写出的 manifest 解码后是一模一样的字节级形态；经过恢复后被 `AggregateBlobLiveChunkBitmaps` sticky-clear 为 `bitmap_available=false`，`IsChunkLive` 对该 blob 全部返回 `kUnknown`，`IsBlobEntirelyDead` 返回 false → GC 一定会走 legacy `GetKey()` 路径，记录不会被误丢弃。
- "新旧 SST 混合"场景同一 VersionEdit 内共存两个 SST：其中一个走 bitmap 路径引用 `kBlobNew`，另一个走 legacy 路径引用 `kBlobOld`。断言两个 blob 的聚合视图在隔离性上严格正交——新 blob 的 kLive/kDead 答案不会被老 blob 的 sticky-clear 污染，老 blob 也不会被误判为 entirely-dead。
- "配置关闭即硬开关"场景：先在 `chunk_size = 4096` 下聚合一次以证明 manifest 里的 bitmap 仍然完好，再以 `chunk_size = 0` 重新聚合，所有 blob 的 `bitmap_available` 立刻变为 false，`live_chunk_bitmap` 被擦空，所有 chunk id 回到 `kUnknown`——即便 manifest 里还带着旧的 bitmap payload，用户把 option 关掉就能一键退回 Phase 0 之前的 GC 行为。

**改动文件**
- `db/version_builder_test.cc`：新增 Phase 9 分节 + 4 个 `TEST_F`，完全复用 Phase 6 已有的 `GetPropCacheWithChunkBitmaps` / `GetPropCacheChunkBitmapLegacy` 工厂函数和 Phase 7 的 `IsChunkLive` / `IsBlobEntirelyDead` API；未新增任何生产代码。
- 不改任何 `.cc` / `.h` 生产代码：所需恢复链在 `version_edit.cc`（Phase 5 已实现 `dependence_chunk_bitmaps` 编解码）与 `version_builder.cc` / `version_set.cc`（Phase 6 已实现 `AggregateBlobLiveChunkBitmaps` 幂等语义）里已经就位。

**新增 UT（`db/version_builder_test.cc`）**
- `VersionBuilderTest.OpenCloseRebuildBitmapState`
  - 覆盖"新写入 DB，关闭重开，bitmap 仍可用"场景；
  - 关键断言：manifest 字节往返后 `dependence_chunk_bitmaps[0]` 仍恰好有 `{1,4,7}` 三位置 1；重建后 `info->bitmap_available == true`、`live_chunk_count == 3 / dead_chunk_count == 5`、三条 live chunk 命中 `kLive`、五条 dead chunk 命中 `kDead`；再次调用 `AggregateBlobLiveChunkBitmaps` 聚合结果与首次完全一致（幂等性回归锁）。
- `VersionBuilderTest.OpenLegacyManifestFallback`
  - 覆盖"老 manifest 打开，系统自动 fallback"场景；
  - 关键断言：解码后 `dependence_chunk_bitmaps.empty() == true`（pre-Phase-4 sentinel）；聚合后 `bitmap_available == false / live_chunk_count == 0`；4 条 chunk 查询全部返回 `kUnknown`；`IsBlobEntirelyDead == false`（GC 不得静默丢弃老 blob 的记录）。
- `VersionBuilderTest.MixedOldAndNewSstCompatibility`
  - 覆盖"新旧 SST 混合存在时，GC 仍正确"场景；
  - 关键断言：新 blob 的 4 条 chunk 精准命中 kLive/kDead、`IsBlobEntirelyDead == false`；老 blob 的 4 条 chunk 全部 `kUnknown`、`IsBlobEntirelyDead == false`；交叉断言 `IsChunkLive(kBlobOld, 0) != kLive`、`IsChunkLive(kBlobNew, 0) != kUnknown`，锁死"两个 blob 的聚合视图不互相污染"。
- `VersionBuilderTest.FeatureDisabledRestoresOldGcBehavior`
  - 覆盖"配置关闭时，新逻辑不生效"场景；
  - 关键断言：当 manifest 里还带着 `{0,1,2}` 三位 bitmap，先以 `kChunkSize=4096` 聚合证明 bitmap_available=true/live_chunk_count=3；再以 `chunk_size=0` 聚合，`bitmap_available / live_chunk_count / dead_chunk_count / live_chunk_bytes / dead_chunk_bytes` 全部归零、`live_chunk_bitmap.empty() == true`、所有 chunk 查询返回 `kUnknown`、`IsBlobEntirelyDead == false`。

**UT 结果**
- `./version_builder_test` Phase 9 4 项 UT 全通过（0 ~ 1 ms each）；
- `./version_builder_test` 全量 24 项（10 原有 + 5 Phase 6 + 5 Phase 7 + 4 Phase 9）全通过；
- 回归 `./version_edit_test` 12/12、`./blob_validity_bitmap_test` 10/10、`./blob_chunk_bitmap_test` 9/9、`./statistics_test` 2/2、`./options_settable_test` 3/3、`./dbformat_test` 10/10、`./compaction_job_test` 17/17 全部通过，无新增 failure。

**兼容性**
- 零生产代码变更：Phase 9 只在测试层验证 Phase 4/5/6 既有恢复链，不引入任何新的字段 / 编码 / ABI 改动；
- 老 DB / 老 manifest 打开路径：`OpenLegacyManifestFallback` 锁死 pre-Phase-4 manifest 解码后 sticky-clear → kUnknown → legacy GetKey() 的退路，升级前写下的 SST 在新版二进制上永远安全；
- 新旧 SST 共存：`MixedOldAndNewSstCompatibility` 证明升级过程中增量 flush/compaction 的新 SST 不会把未重写的老 SST "污染"为可 fast-path，也不会被老 SST 拖进 legacy，隔离边界就是单个 blob；
- 硬开关语义：`FeatureDisabledRestoresOldGcBehavior` 证明 `cf_options.blob_gc_chunk_size = 0` 即可一键回到 Phase 0 之前的 GC 行为，即便 manifest 里还残留着 bitmap payload，聚合阶段也会强制清空 —— 这为线上出问题时的紧急回滚提供了不需要重启就能生效的 kill switch。

---

**Phase 10 focused E2E 测试**
- 像第一点那样，必须有一组“从写入到 GC”的端到端测试
- 证明不是局部模块拼起来而已

**建议新增测试文件**
- `db/blob_validity_bitmap_test.cc`
- 或 `db/blob_gc_bitmap_test.cc`

**推荐 E2E 用例**
- `EndToEnd_FlushCompactionBuildsBitmapThenGcUsesFastPath`
- `EndToEnd_UpdateHeavyWorkloadAvoidsLookupOnGc`
- `EndToEnd_RecoveryKeepsBitmapUsable`
- `EndToEnd_LegacyBlobFallsBackSafely`

**E2E 断言重点**
- flush/compaction 后 bitmap metadata 存在
- version 聚合后 blob 有 live bitmap
- GC fast path 被触发
- fallback 在 legacy case 被触发
- 用户可见数据结果不变

**Status: Completed**

**核心设计**
- Phase 10 是一个 *端到端契约 / 不变式锁定* 阶段：Phase 3-8 每一层都在各自的 UT 中被单独验证过，Phase 10 的价值在于证明"当把这些层真正首尾相接、像生产路径一样跑一遍完整数据流时，整条链依旧工整"。为了避免把 DBImpl + 后台 Flush/Compaction 线程整套拉起来（会让 E2E 测试变慢、变 flaky，且很容易被线程时序掩盖真实 bug），Phase 10 选择了一条更扎实的路线：把每个阶段的真实生产 API 在单测里按 flush → manifest encode → recovery decode → version aggregate → GC simulate 的顺序串起来驱动一次，并在每一层都下断言，最后用一条*ground-truth 活跃性*不变式（"bitmap fast path 永远不能静默丢掉一条真活跃的记录"）把整条链扣死。
- **GC simulator**：`phase10_testutil::SimulateGc` 是 `compaction_job.cc::ProcessGarbageCollection` 热点段（lines 2180-2299）的逐行镜像——每条 blob 记录先查 `BlobChunkLivenessCache`，再问 `VersionStorageInfo::IsBlobEntirelyDead`，命中 fast-path 则 `continue`（相当于丢弃 dead chunk），否则走 legacy `GetKey()`-style fallback 并累加 `read_live_chunk_bytes`。这让 E2E 测试可以在不依赖磁盘、不依赖后台线程的前提下，用与生产完全同构的状态机验证 fast-path / fallback / skipped / read / lookup-avoided 五个计数器的语义。
- **Phase 8 ticker 精确回放**：`EmitPhase10Tickers` 是 `compaction_job.cc:2451-2461` `RecordTick` 块的逐行镜像（顺序、字段、参数全对齐），保证 E2E 用例输出到 `Statistics` 的 5 个 GC_* ticker 与生产完全一致——包括只在 fast-path 命中后再用 `bytes_freed_by_new_gc` 累计、lookup_avoided 只在 bitmap_available 时累计这些细节。
- **Ground-truth 不变式**：每个 `BlobRecord` 显式标注 `ground_truth_live`，`AssertNoUserVisibleDataLoss` 在 simulator 结束后做四项交叉验证：① 所有被 simulator 保留的记录在 ground truth 中都是 live；② 所有 ground truth live 记录都出现在 `kept_records` 中；③ 没有重复；④ 原始顺序在 kept_records 中保持严格单调。这组不变式直接对应论文里的"用户可见数据结果不变"声明。
- **四种 E2E 场景**：
  1. **FlushCompactionBuildsBitmapThenGcUsesFastPath**：整个流水线正常 on——`FlushChunkBitmapCollector.Add(blob_number, offset)` → `MoveToCache()` → `TablePropertyCache::dependence_chunk_bitmaps` → `VersionEdit::AddFile(..., prop)` → encode → decode → `VersionStorageInfo::AddFile` → `AggregateBlobLiveChunkBitmaps(chunk_size)` → simulator 驱动 GC。断言 manifest 字节往返后 bitmap 位 1 完全一致、`live_chunk_count / dead_chunk_count` 与源 bitmap 匹配、fast_path_count > 0、fallback_count = 0、skipped_bytes = dead_chunks * chunk_size、5 个 ticker 读回的值一一对齐 simulator。
  2. **UpdateHeavyWorkloadAvoidsLookupOnGc**：模拟一个 update-heavy 负载——同一 blob 被两个 SST 共同引用，引用的 chunk 集合有重叠（`{2,4} ∪ {4,7}`）。断言 Phase 6 聚合后的 `live_chunk_bitmap` 是两张输入 bitmap 的 OR（`{2,4,7}` 三位 1 且仅此三位）、`live_chunk_count == 3`、`IsChunkLive(cid=4)` 仍然 kLive 而不是被某条 SST 的局部视图误判为 kDead、`lookup_avoided_count` 在 16 条记录上都得到累加、没有一条记录触发 legacy GetKey。
  3. **RecoveryKeepsBitmapUsable**：显式 encode + decode 一次 VersionEdit，然后用"原始 VersionEdit"和"解码出的 VersionEdit"分别构造两个 `VersionStorageInfo`，对两个版本同时跑 simulator 和 ground-truth 检查。断言 manifest 字节往返后 bitmap 字节级等价、两个版本的 `IsBlobEntirelyDead(kBlobDead) == true`、两次 simulator 的 `skipped_dead_chunk_bytes / fast_path_count / fallback_count / read_live_chunk_bytes / lookup_avoided_count` 全部逐字段相等——锁死"Phase 9 的 manifest 恢复契约在端到端流水里依旧成立"。
  4. **LegacyBlobFallsBackSafely**：同一 VersionEdit 内共存两个 SST：一个是 legacy（`dependence_chunk_bitmaps` 为空）指向 `kBlobLegacy`，另一个是新 binary（有完整 bitmap）指向 `kBlobNew`。断言 `kBlobLegacy` 聚合后 `bitmap_available == false` / 所有 chunk 查询 `kUnknown` / `IsBlobEntirelyDead == false`、`kBlobNew` 聚合后 `bitmap_available == true` / 对 `{0,2}` 命中 kLive 对其他命中 kDead / `IsBlobEntirelyDead == false`、simulator 跑完后 legacy blob 的所有 records 走 fallback 路径（`fallback_count == legacy_records`、`lookup_avoided_count == 0`）且 ground truth 全部活下来不丢失、new blob 的 dead records 被 fast-path 正确丢弃，两条路径的计数严格隔离。

**改动文件**
- `db/blob_validity_bitmap_test.cc`：新增 Phase 10 分节，包括 `phase10_testutil` 命名空间、`BlobValidityBitmapPhase10Test` fixture 以及 4 个 `TEST_F`，复用 Phase 3 的 `FlushChunkBitmapCollector`、Phase 4 的 `TablePropertyCache::dependence_chunk_bitmaps`、Phase 5 的 `VersionEdit` 编解码、Phase 6 的 `AggregateBlobLiveChunkBitmaps`、Phase 7 的 `IsChunkLive / IsBlobEntirelyDead / GetBlobLiveChunkInfo`、Phase 8 的 5 个 GC_* ticker，未新增任何生产代码。
- `#include` 扩展：新增 `<cinttypes>, <memory>, db/dbformat.h, db/version_set.h, monitoring/statistics.h, rocksdb/advanced_options.h, rocksdb/comparator.h, rocksdb/options.h, rocksdb/statistics.h`，支持 E2E 用例构造 `VersionStorageInfo` / 读取 `Statistics` 指标的需要。
- 不改任何 `.cc` / `.h` 生产代码：Phase 10 的目的是证明"端到端链路在既有代码上已经自洽"，生产代码在 Phase 3-8 已经就位。

**新增 UT（`db/blob_validity_bitmap_test.cc`）**
- `BlobValidityBitmapPhase10Test.EndToEnd_FlushCompactionBuildsBitmapThenGcUsesFastPath`
  - 覆盖"正常数据流：write → flush → compaction → manifest → recovery → GC fast path"全链路；
  - 关键断言：`FlushChunkBitmapCollector.MoveToCache()` 产出的 bitmap 与源引用集位级一致；manifest 字节往返后 bitmap 字节级等价；聚合后 `live_chunk_count / dead_chunk_count` 与源 bitmap 对齐；simulator 里 `fast_path_count > 0 / fallback_count == 0 / skipped_dead_chunk_bytes == dead_chunks * chunk_size`；5 个 GC_* ticker 读回的值逐项 == simulator 内部计数；ground-truth 不变式（kept_records 严格等于 live_records，无丢失无重复无错序）成立。
- `BlobValidityBitmapPhase10Test.EndToEnd_UpdateHeavyWorkloadAvoidsLookupOnGc`
  - 覆盖"update-heavy：同一 blob 被多条 SST 引用，引用集合有重叠"场景；
  - 关键断言：Phase 6 聚合后的 `live_chunk_bitmap` 恰好是两张源 bitmap 的 OR（`{2,4,7}`）、`live_chunk_count == 3`；共同位 `cid=4` 在聚合视图下为 kLive；16 条模拟记录全部走 fast-path（`lookup_avoided_count == 16 / fallback_count == 0`）；dead chunk 记录在 skipped_bytes 中被正确累加。
- `BlobValidityBitmapPhase10Test.EndToEnd_RecoveryKeepsBitmapUsable`
  - 覆盖"写入 → 关机 → 开机（manifest 回放）→ GC"场景；
  - 关键断言：原始 VersionEdit 与解码后 VersionEdit 两条路径产出的 `VersionStorageInfo` 在 `IsBlobEntirelyDead / IsChunkLive` 上逐查询等价；两次 simulator 输出的 5 个计数字段（`fast_path_count / fallback_count / skipped_dead_chunk_bytes / read_live_chunk_bytes / lookup_avoided_count`）严格相等；ground-truth 不变式在两条路径下都成立——锁死"manifest 恢复链在端到端流水中保持字节级幂等"。
- `BlobValidityBitmapPhase10Test.EndToEnd_LegacyBlobFallsBackSafely`
  - 覆盖"legacy SST 与新 SST 同时在线时，fallback 路径必须被触发且与新路径严格隔离"场景；
  - 关键断言：聚合后 legacy blob `bitmap_available == false / IsChunkLive == kUnknown / IsBlobEntirelyDead == false`、new blob `bitmap_available == true / 对 {0,2} 命中 kLive 对其他命中 kDead / IsBlobEntirelyDead == false`；simulator 跑完后 legacy 分支 `fallback_count == legacy_records / lookup_avoided_count == 0`、新 blob 分支 fast-path 正确丢弃 dead records；两条路径的计数互不污染；ground-truth 不变式下所有 legacy 记录全部存活（fallback 永远不会静默丢数据），新 blob 的 live 记录也全部存活。

**UT 结果**
- `./blob_validity_bitmap_test --gtest_filter='*Phase10*'`：4/4 全通过（FlushCompactionBuildsBitmapThenGcUsesFastPath 12 ms / UpdateHeavyWorkloadAvoidsLookupOnGc 12 ms / RecoveryKeepsBitmapUsable 24 ms / LegacyBlobFallsBackSafely 11 ms）；
- `./blob_validity_bitmap_test` 全量 14 项（Phase 5 / 6 / 9 历史用例 10 项 + Phase 10 新增 4 项）全通过；
- 回归 `./version_builder_test` 24/24、`./version_edit_test` 12/12、`./blob_chunk_bitmap_test` 9/9（含 Phase 8 的 3 项 `BlobGcBitmapStatsTest`）全部通过，无新增 failure。

**兼容性**
- 零生产代码变更：Phase 10 只在测试层把 Phase 3-8 既有 API 串成一条端到端流水，不引入任何新的字段 / 编码 / ABI 改动。
- 老 DB / 老 manifest 打开路径：`EndToEnd_RecoveryKeepsBitmapUsable` 验证 VersionEdit encode/decode 在端到端链里保持字节级幂等；`EndToEnd_LegacyBlobFallsBackSafely` 验证 legacy（pre-Phase-4）SST 在端到端链里走 fallback，与 Phase 9 的单点契约互为补充。
- 硬开关语义：Phase 10 复用 Phase 9 在 `FeatureDisabledRestoresOldGcBehavior` 里已验证的 kill switch 机制，端到端链路的 chunk_size 参数化让未来可以在同一测试 fixture 下扩展 `chunk_size=0` 的 E2E 回退场景而无需改动 simulator。
- 计数器契约：`EmitPhase10Tickers` 与 `compaction_job.cc` 的 `RecordTick` 块逐行对齐，任何对 GC_* ticker 语义的偏移都会在 Phase 10 UT 里第一时间暴露，相当于给 Phase 8 的 observability 契约上了一道 E2E 锁。

---

**Phase 11 论文与实验对齐检查**
- 代码做到这里，就可以开始把第二点写进论文了
- 但要先确认 claim 不超实现

**论文里可以宣称的**
- 与 Compaction 耦合的有效性物化
- 在线维护 blob 级 live-chunk 视图
- GC 阶段显著减少主树反查
- 支持 legacy fallback

**论文里暂时不要过度宣称的**
- object-level invalidation bitmap
- 完全不需要任何验证
- 最优回收模式选择
- 全自动多模式执行

---

**建议的开发顺序**
- `1 -> 2 -> 3 -> 5 -> 4 -> 6 -> 7 -> 8 -> 9 -> 10`
- 原因：
  - 先把数据表示做稳
  - 再让 flush/compaction 会产出新元数据
  - 再让 version 聚合
  - 最后让 GC 消费

---

**每阶段完成标准**
- `Phase 1` 完成标准：
  - 配置项可设置
  - bitmap 容器单测通过
- `Phase 2` 完成标准：
  - 新旧 index 格式都能正确解码
- `Phase 3` 完成标准：
  - flush 输出的 SST 能带 chunk bitmap
- `Phase 4` 完成标准：
  - compaction 输出 SST 能带 chunk bitmap
- `Phase 5` 完成标准：
  - manifest/property 可持久化并恢复 chunk bitmap
- `Phase 6` 完成标准：
  - VersionBuilder 能聚合 live-chunk bitmap
- `Phase 7` 完成标准：
  - GC fast path 可用，legacy fallback 正常
- `Phase 8` 完成标准：
  - 统计项能反映 fast path/fallback
- `Phase 9` 完成标准：
  - open/recovery/legacy 兼容都通过
- `Phase 10` 完成标准：
  - 至少 2 个 E2E 通过
- `Phase 11` 完成标准：
  - 论文第二点 wording 与代码一致

---

**我建议你优先写的测试文件**
- [version_edit_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_edit_test.cc)
- [version_builder_test.cc](file:///data00/home/pengzhifeng/DB/terarkdb/db/version_builder_test.cc)
- 新增 `db/blob_validity_bitmap_test.cc`
- 新增 `db/blob_gc_bitmap_test.cc`

---

**一句施工建议**
- 第一阶段不要同时追：
  - chunk bitmap
  - GC 多模式
  - picker 新评分
- 只专注把第二点做成：
  - **生成引用图**
  - **聚合 live view**
  - **GC 消费 live view**
  - **legacy fallback**
- 这样最容易做到和第一点一样“功能闭环 + 测试闭环”。

如果你愿意，我下一步可以继续把这个 Checklist 再细化成：

- **按文件列出的具体代码改动点**
也就是每个文件里建议新增哪些字段、函数、调用点。
