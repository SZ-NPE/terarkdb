# Task 2.1: FIFO Cache 实现与无锁读取接口改造

## 1. 任务目标
为双级缓存架构提供底层的基建支持。TerarkDB 原生只有 LRU Cache，本任务需要基于 `lru_cache.cc` 剥离并改造出一个纯粹的 FIFO Cache。

## 2. 核心修改点

### 2.1 创建 `fifo_cache.cc` 和 `fifo_cache.h`
* 将 `cache/lru_cache.cc` 复制为 `cache/fifo_cache.cc`，并在 `cache/` 目录下提供头文件，对齐 TerarkDB 的命名空间和代码风格。

### 2.2 剥离 LRU 逻辑 (改为纯 FIFO)
* 找到原生 `LRUCache` 中每次 `Lookup` 或被读取后，将节点移动到链表头部（提升生命周期）的逻辑。
* **删除该逻辑**，确保数据一旦插入，只能随着容量写满被新数据自然淘汰出队列，绝对不因为被读取而延长生命周期。

### 2.3 提供无锁读取接口
* `LRUCache::Lookup` 内部包含了一个 `std::mutex` 互斥锁。
* 由于 FIFO 不需要在读取时修改链表结构，因此可以实现一个**完全无互斥锁（或使用读写锁的读锁）的查找接口**。
* 目标：保证前台线程在查 Cache1（Window Cache）时，不会发生 Write Stall。

## 3. 验收标准
* `fifo_cache.cc` 成功编译。
* 代码中没有因为读操作修改链表结构导致的并发错误。
* 提供并暴露了一个没有互斥锁开销的查找接口（如 `LookupWithoutLock`）。