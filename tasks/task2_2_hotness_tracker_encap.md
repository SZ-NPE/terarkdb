# Task 2.2: HotnessTracker 组件封装与双级晋升逻辑

## 1. 任务目标
利用前一个子任务（Task 2.1）产出的 `FIFO Cache` 以及系统现有的 `LRU Cache`，封装出一个供整个引擎使用的 `HotnessTracker` 热度追踪器。并实现核心的双级准入与无锁晋升拦截。

## 2. 核心架构设计

### 2.1 双级缓存封装 (`util/hotness_tracker.h`)
*   **Cache1 (Window Cache)**：使用 `FIFO Cache` 实例，负责记录首次写入（观察期窗口）。
*   **Cache2 (Hot Cache)**：使用原生 `LRU Cache` 实例，负责存储确认的高频更新热点。
*   **Hash Key 共用**：由于 Cache1 和 Cache2 分属两个对象，提供一个接口计算统一的 `Slice` 的 Hash 值，避免在查 Cache2 和 Cache1 时重复计算。
*   **零内存开销**：缓存全退化为纯 Set，Value 强制传入 `nullptr`，并附带空实现的 `NoopDeleter`。

### 2.2 延迟消亡晋升逻辑 (Age-out)
*   编写核心拦截函数 `RecordHotness(const Slice& key, uint32_t hash)`，其逻辑如下：
    1.  先查 Cache2，命中则更新 LRU（说明已经是热点，维持热度）。
    2.  若未命中 Cache2，再查 Cache1（使用无锁接口）。若命中 Cache1，说明该 Key 在预期的时间窗口内被再次更新，**确认为热点，晋升插入 Cache2**。
    3.  若均未命中，说明是首次写入（或间隔太久已被淘汰出 Cache1），则仅插入 Cache1 进行观察。
*   **绝对红线**：当从 Cache1 晋升到 Cache2 时，**绝对禁止调用 Erase 从 Cache1 中删除它**。允许它在 FIFO 队列内自然淘汰（Age-out），以避免由于 Erase 操作将底层的读锁升级为写锁，从而保留读路径的无锁化。

## 3. 验收标准
*   `HotnessTracker` 封装符合规范，包含 `RecordHotness`。
*   明确不包含 `Erase` Cache1 的操作。
*   两个 Cache 的 `Deleter` 均使用了 `nullptr` 友好的安全销毁逻辑。