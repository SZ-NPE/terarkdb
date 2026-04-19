# Task 2.3: 写入路径与 Flush 路由的分流改造

## 1. 任务目标
利用前一个子任务（Task 2.2）产出的 `HotnessTracker`，将其嵌入 TerarkDB 的写入路径与 Flush 落盘阶段，从而物理分流出 Hot-Blob 与 Cold-Blob 文件。

## 2. 核心架构设计

### 2.1 生命周期绑定与前台写入拦截
*   **ColumnFamilyData 挂载**：在 `db/column_family.h` 中，实例化 `HotnessTracker` 成员，并将其挂载在 `ColumnFamilyData` 的生命周期上。
*   **写入路径拦截 (`MemTableInserter`)**：
    *   在 `db/memtable.cc` 的 `MemTableInserter::PutCFImpl` 等相关路径（包含单条写入和 WriteBatch 批量写入）中进行拦截。
    *   每次检测到写入新记录时，提取 `user_key` 并调用 `hotness_tracker->RecordHotness(user_key, hash)`。

### 2.2 Flush 路由判断 (`db/builder.cc`)
*   在 `db/builder.cc` 进行数据刷盘并创建 Blob 文件时，**只查 Cache2** 以判断当前 Key 是否是真正的热键。
*   **查找接口约束**：必须调用一个特殊的查找接口，以防止在 Flush 阶段的查询动作延长了该 Key 在 Cache2 中的生命周期。

### 2.3 GC 存活晋升与硬件协同 (Age-based Promotion & SSD Hints)
*   **GC 强制下沉**：在后台 GC 重写旧 Blob 时（如 `db/compaction_job.cc`），强制跳过热度查询，将所有存活数据全部写入新的 Cold-Blob。
*   **SSD Hints 传递**：
    *   **热区 (`WLTH_SHORT`)**：Flush 创建 Hot-Blob 时，传递 `Env::WLTH_SHORT` 给底层的 `Env`，提示 SSD 快速回收。
    *   **冷区 (`WLTH_EXTREME`)**：Flush 和 GC 创建 Cold-Blob 时，传递 `Env::WLTH_EXTREME` 给底层的 `Env`，减少 SSD 物理块搬移。

## 3. 验收标准
*   `MemTableInserter` 中成功嵌入 `HotnessTracker`。
*   Flush 阶段能根据双级缓存的结果成功分离 Hot-Blob / Cold-Blob 文件。
*   单条写入和 WriteBatch 写入双路径均实现拦截。
*   `WLTH_SHORT` 和 `WLTH_EXTREME` 硬件协同参数在对应的场景下成功传递给底层的 `Env`。