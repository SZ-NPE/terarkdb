# Task 1: 染色删除次数统计 (Whole-File Deletion Statistics)

## 1. 任务目标
实现染色删除（Whole-File Deletion）次数和节省空间的精确统计。此任务为整个 KV 分离优化的基础观测工具，用于量化后续任务带来的收益。

## 2. 需新增的 Ticker 指标
在 `include/rocksdb/statistics.h` 的 `Tickers` 枚举中，于 `READ_BLOB_INVALID` 之后、`TICKER_ENUM_MAX` 之前新增：
```cpp
GC_WHOLE_FILE_DELETE,           // GC 输出为空，整文件直接删除的次数
GC_WHOLE_FILE_DELETE_BYTES,     // 染色删除释放的磁盘空间（字节）
GC_REWRITE_BLOB_BYTES,          // 传统 GC 重写新 Blob 的数据量（用于对比）
```

## 3. 代码修改点

### 3.1 埋点 A：染色删除计数
**位置**：`db/compaction_job.cc` 里的 `ProcessGarbageCollection` 逻辑结束后，通过检查 `meta.prop.num_entries == 0` 来触发。
**修改逻辑**：
```cpp
if (meta.prop.num_entries == 0) {
    RecordTick(stats_, GC_WHOLE_FILE_DELETE);
    RecordTick(stats_, GC_WHOLE_FILE_DELETE_BYTES, /* 对应被删除的 Blob 大小 */);
    ROCKS_LOG_INFO(/* logger */, "★染色删除★ Blob ...");
}
```

### 3.2 埋点 B：传统 GC 重写统计
**位置**：`db/compaction_job.cc` 里的 `FinishCompactionOutputBlob` 结束后。
**修改逻辑**：
```cpp
if (!sub_compact->blob_outputs.empty()) {
    RecordTick(stats_, GC_REWRITE_BLOB_BYTES, current_bytes);
}
```

### 3.3 统计注册与同步
* **TickersNameMap 注册**：在 `monitoring/statistics.cc` 追加相应的字符串名称映射。

## 4. 验收标准
* 运行 `db_bench` 后，能够通过 `--statistics` 正确输出新增的三个指标。