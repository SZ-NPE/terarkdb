# Task 2: 基于 Dual-Level Cache 的前台自适应冷热分离 (Adaptive Hotness Routing)

## 1. 任务目标
解决时间窗口与更新频率不匹配的问题。利用双级 Sharded Cache（FIFO + LRU），在 MemTable Flush 落盘阶段将高频更新（热）和低频更新（冷）数据进行物理隔离。

## 2. 核心架构设计

### 2.1 Cache1 (FIFO Cache / Window Cache)
* 负责记录首次写入，充当“观察期窗口”。容量可根据 `Write_Rate * Target_Time_Window` 动态调整。
* **现有代码修改**：由于目前没有 FIFO 的实现，你需要基于 `LRUCache` 改写一个 `fifo_cache.cc`。
  * **必须修改点**：(1) 将 LRU Cache 被读后增加 key 生命周期的逻辑删除。(2) 提供一个无锁或读写锁实现的读取接口（无需互斥锁）。

### 2.2 Cache2 (LRU Cache / Hot Cache)
* 负责存储在观察期内被再次更新的高频更新（真正热点）。
* **零 Value 内存开销**：两个 Cache 均退化为纯粹的 Set（Value 设为 `nullptr`），并提供 `NoopDeleter` 实现零开销释放。

### 2.3 延迟消亡策略 (Age-out)
* 对于命中 Cache1 的 Key，我们在将其晋升到 Cache2 时，**绝对不调用 Erase 从 Cache1 中删除它**，允许它在 FIFO 队列内随着新数据的写入自然淘汰，完美保留读路径的无锁化潜力。

### 2.4 Hash Key 共用
* Cache1 和 Cache2 可以共用同一个 hash key 的计算结果，以降低重复计算的开销。

## 3. 代码修改点

### 3.1 写入时拦截 (Promotion Logic)
在写入路径（包括单条写入和 WriteBatch 写入，如 `MemTableInserter::PutCFImpl`）进行拦截：
1. 先查 Cache2，命中则更新 LRU（维持热度）。
2. 若未命中 Cache2，再查 Cache1。若命中 Cache1，**晋升插入 Cache2**。
3. 若均未命中，仅插入 Cache1 进行观察。

### 3.2 Flush 路由判断 (Routing)
在 Flush 阶段查找 Cache2（LRU Cache）判断 Key 是否为热键：
* 必须使用特殊的查找接口，**绝对不增加**该 Key 在 Cache2 中的生命周期。
* 根据是否命中 Cache2 将数据分离至 Hot-Blob 或 Cold-Blob。

### 3.3 GC 存活晋升与硬件协同 (Age-based Promotion & SSD Hints)
* **GC 存活晋升**：后台 GC 重写旧 Blob 时，强制跳过热度查询，将存活数据全部写入新的 Cold-Blob。
* **硬件协同**：
  * **热区**：Flush 创建 Hot-Blob 时，传递 `WLTH_SHORT` 给底层的 `Env`，提示 SSD 快速回收。
  * **冷区**：Flush 和 GC 创建 Cold-Blob 时，传递 `WLTH_EXTREME`，减少 SSD 物理块搬移。

## 4. 验收标准
* `fifo_cache.cc` 的实现必须符合要求，没有无用的锁竞争。
* `HotnessTracker` 实现双级准入，无锁晋升（没有 Erase Cache1 的操作）。
* Flush 阶段成功根据双级缓存的结果分流出 Hot-Blob / Cold-Blob。
* 单条写入和 WriteBatch 写入双路径均实现拦截。
* `WLTH_SHORT` 和 `WLTH_EXTREME` 参数成功传递。