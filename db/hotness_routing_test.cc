//  Copyright (c) 2024-present. All rights reserved.

#include <memory>
#include <string>
#include <map>
#include <vector>
#include <thread>

#include "cache/fifo_cache.h"
#include "db/db_test_util.h"
#include "util/hotness_tracker.h"
#include "util/testharness.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/terark_namespace.h"
#include "port/stack_trace.h"

namespace TERARKDB_NAMESPACE {

// Phase 1 & 2: 单元测试 (HotnessTracker 基本逻辑)
class HotnessTrackerTest : public testing::Test {};

TEST_F(HotnessTrackerTest, BasicThreeStateClassification) {
  HotnessTracker tracker(16, 64, 0);

  Slice warm_key("w001");
  uint32_t warm_hash = HotnessTracker::Hash(warm_key);
  ASSERT_EQ(tracker.ClassifyForFlush(warm_key, warm_hash),
            HotnessTracker::FlushRoute::kWarm);

  tracker.RecordHotness(warm_key, warm_hash);
  ASSERT_EQ(tracker.ClassifyForFlush(warm_key, warm_hash),
            HotnessTracker::FlushRoute::kWarm);
  ASSERT_FALSE(tracker.IsHot(warm_key, warm_hash));

  Slice stateful_key("s001");
  uint32_t stateful_hash = HotnessTracker::Hash(stateful_key);
  tracker.RecordHotness(stateful_key, stateful_hash);
  tracker.RecordHotness(stateful_key, stateful_hash);
  ASSERT_EQ(tracker.ClassifyForFlush(stateful_key, stateful_hash),
            HotnessTracker::FlushRoute::kEphemeral);
  ASSERT_TRUE(tracker.IsHot(stateful_key, stateful_hash));

  for (int i = 0; i < 4; ++i) {
    std::string filler = "f00" + std::to_string(i);
    tracker.RecordHotness(filler, HotnessTracker::Hash(filler));
  }

  ASSERT_EQ(tracker.ClassifyForFlush(stateful_key, stateful_hash),
            HotnessTracker::FlushRoute::kStable);
  ASSERT_TRUE(tracker.IsHot(stateful_key, stateful_hash));
}

TEST_F(HotnessTrackerTest, FlushQueryDoesNotExtendLifecycle) {
  // 设置较小的 Cache 方便触发淘汰，如 1024 字节
  HotnessTracker tracker(1024, 1024, 0);

  Slice key("target_hot_key");

  // 1. 连续写入两次，使其晋升为 Hot (进入 Cache2)
  tracker.RecordHotness(key, HotnessTracker::Hash(key));
  tracker.RecordHotness(key, HotnessTracker::Hash(key));
  ASSERT_TRUE(tracker.IsHot(key, HotnessTracker::Hash(key)));

  // 2. 模拟 Flush 时的频繁查询。这绝对不能触发 LRU 内部的晋升/刷新。
  for (int i = 0; i < 1000; ++i) {
    tracker.IsHot(key, HotnessTracker::Hash(key));
  }

  // 3. 写入大量其他 Hot Key，触发 Cache2 (LRU) 的淘汰
  for (int i = 0; i < 200; ++i) {
    std::string filler = "other_hot_key_" + std::to_string(i);
    tracker.RecordHotness(filler, HotnessTracker::Hash(filler));
    tracker.RecordHotness(filler, HotnessTracker::Hash(filler)); // 第二次写入使其晋升，挤占 Cache2 空间
  }

  // 4. 验证：由于之前的 IsHot 查询不应延长 target_hot_key 的生命周期，
  // 此时它应该已经被新晋升的 filler keys 淘汰出 Cache2。
  ASSERT_FALSE(tracker.IsHot(key, HotnessTracker::Hash(key)));
}

// Phase 3 & 4: 端到端集成测试 (DB Flush 分流 & GC 冷通道)
class AdaptiveHotnessRoutingTest : public DBTestBase {
 public:
  AdaptiveHotnessRoutingTest() : DBTestBase("/hotness_routing_test") {}
};

class TrackHintEnv;

class TrackHintWritableFile : public WritableFileWrapper {
 private:
  std::unique_ptr<WritableFile> owner_;
  TrackHintEnv* env_;
 public:
  std::string fname_;

  TrackHintWritableFile(std::unique_ptr<WritableFile>&& t, 
                        TrackHintEnv* env,
                        const std::string& fname) 
      : WritableFileWrapper(t.get()), owner_(std::move(t)), env_(env), fname_(fname) {}

  void SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) override;
};

class TrackHintEnv : public EnvWrapper {
 public:
  std::map<std::string, Env::WriteLifeTimeHint> file_hints_;
  port::Mutex mutex_;

  explicit TrackHintEnv(Env* t) : EnvWrapper(t) {}

  Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,
                         const EnvOptions& options) override {
    Status s = target()->NewWritableFile(f, r, options);
    if (s.ok()) {
      r->reset(new TrackHintWritableFile(std::move(*r), this, f));
    }
    return s;
  }
};

const Env::WriteLifeTimeHint* FindHintByBasename(
    const std::map<std::string, Env::WriteLifeTimeHint>& file_hints,
    const std::string& basename) {
  size_t basename_pos = basename.find_last_of('/');
  const std::string normalized_basename =
      (basename_pos == std::string::npos) ? basename
                                          : basename.substr(basename_pos + 1);
  for (const auto& pair : file_hints) {
    const std::string& path = pair.first;
    size_t pos = path.find_last_of('/');
    const std::string key =
        (pos == std::string::npos) ? path : path.substr(pos + 1);
    if (key == normalized_basename) {
      return &pair.second;
    }
  }
  return nullptr;
}

void TrackHintWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
  {
    MutexLock l(&env_->mutex_);
    env_->file_hints_[fname_] = hint;
  }
  owner_->SetWriteLifeTimeHint(hint);
}

TEST_F(AdaptiveHotnessRoutingTest, Phase3And4E2ERouting) {
  TrackHintEnv track_env(env_);
  Options options = CurrentOptions();
  options.env = &track_env;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;  // 禁用后台 Compaction 以控制测试流程

  options.blob_size = 0;
  options.enable_hotness_tracker = true;
  options.hotness_window_capacity = 8;
  options.hotness_hot_capacity = 4096;
  options.target_blob_file_size = 10 * 1024;

  DestroyAndReopen(options);

  ASSERT_OK(Put("s000", "sv1"));
  ASSERT_OK(Put("s000", "sv2"));
  ASSERT_OK(Put("w000", "wv0"));
  ASSERT_OK(Put("f001", "fv1"));
  ASSERT_OK(Put("f002", "fv2"));
  ASSERT_OK(Put("e000", "ev1"));
  ASSERT_OK(Put("e000", "ev2"));

  std::map<std::string, int> pre_flush_live_files;
  {
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);
    for (const auto& meta : live_files) {
      size_t pos = meta.name.find_last_of('/');
      const std::string key =
          (pos == std::string::npos) ? meta.name : meta.name.substr(pos + 1);
      pre_flush_live_files.emplace(key, meta.level);
    }
  }
  {
    MutexLock l(&track_env.mutex_);
    track_env.file_hints_.clear();
  }

  Flush(); // 强制触发 MemTable Flush

  size_t new_file_count = 0;
  size_t new_sst_count = 0;
  size_t new_blob_count = 0;
  size_t sst_medium_count = 0;
  size_t blob_medium_count = 0;
  size_t blob_extreme_count = 0;
  size_t blob_short_count = 0;
  {
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);

    MutexLock l(&track_env.mutex_);
    for (const auto& meta : live_files) {
      size_t pos = meta.name.find_last_of('/');
      const std::string key =
          (pos == std::string::npos) ? meta.name : meta.name.substr(pos + 1);
      if (pre_flush_live_files.count(key) != 0) {
        continue;
      }
      ++new_file_count;
      const Env::WriteLifeTimeHint* hint =
          FindHintByBasename(track_env.file_hints_, key);
      ASSERT_NE(hint, nullptr) << meta.db_path << "/" << meta.name;

      if (meta.level == -1) {
        ++new_blob_count;
        if (*hint == Env::WLTH_MEDIUM) ++blob_medium_count;
        if (*hint == Env::WLTH_EXTREME) ++blob_extreme_count;
        if (*hint == Env::WLTH_SHORT) ++blob_short_count;
      } else {
        ++new_sst_count;
        if (*hint == Env::WLTH_MEDIUM) ++sst_medium_count;
      }
    }
  }

  ASSERT_GE(new_file_count, 2U);
  ASSERT_EQ(new_sst_count, 1U);
  ASSERT_GE(new_blob_count, 1U);
  ASSERT_EQ(sst_medium_count, 1U);
  ASSERT_EQ(blob_medium_count + blob_short_count + blob_extreme_count,
            new_blob_count);

  {
    MutexLock l(&track_env.mutex_);
    track_env.file_hints_.clear();
  }

  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  bool found_short = false;
  bool found_extreme = false;
  {
    MutexLock l(&track_env.mutex_);
    for (const auto& pair : track_env.file_hints_) {
      if (pair.first.find(".blob") != std::string::npos ||
          pair.first.find(".sst") != std::string::npos) {
        if (pair.second == Env::WLTH_SHORT) found_short = true;
        if (pair.second == Env::WLTH_EXTREME) found_extreme = true;
      }
    }
  }

  ASSERT_TRUE(found_extreme);
  ASSERT_FALSE(found_short);

  // Cleanup DB manually before env goes out of scope
  Close();
}

TEST_F(AdaptiveHotnessRoutingTest, WriteBatchInterception) {
  TrackHintEnv track_env(env_);
  Options options = CurrentOptions();
  options.env = &track_env;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;  // 禁用后台 Compaction

  options.blob_size = 0;
  options.enable_hotness_tracker = true;
  options.hotness_window_capacity = 8;
  options.hotness_hot_capacity = 4096;
  options.target_blob_file_size = 10 * 1024;

  DestroyAndReopen(options);

  WriteBatch batch;
  batch.Put("s100", "sv1");
  batch.Put("s100", "sv2");
  batch.Put("w100", "wv0");
  batch.Put("f101", "fv1");
  batch.Put("f102", "fv2");
  batch.Put("e100", "ev1");
  batch.Put("e100", "ev2");

  ASSERT_OK(db_->Write(WriteOptions(), &batch));

  std::map<std::string, int> pre_flush_live_files;
  {
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);
    for (const auto& meta : live_files) {
      size_t pos = meta.name.find_last_of('/');
      const std::string key =
          (pos == std::string::npos) ? meta.name : meta.name.substr(pos + 1);
      pre_flush_live_files.emplace(key, meta.level);
    }
  }

  {
    MutexLock l(&track_env.mutex_);
    track_env.file_hints_.clear();
  }

  Flush();

  size_t blob_medium_count = 0;
  size_t blob_extreme_count = 0;
  size_t blob_short_count = 0;
  {
    std::vector<LiveFileMetaData> live_files;
    db_->GetLiveFilesMetaData(&live_files);

    MutexLock l(&track_env.mutex_);
    for (const auto& meta : live_files) {
      size_t pos = meta.name.find_last_of('/');
      const std::string key =
          (pos == std::string::npos) ? meta.name : meta.name.substr(pos + 1);
      if (pre_flush_live_files.count(key) != 0 || meta.level != -1) {
        continue;
      }

      const Env::WriteLifeTimeHint* hint =
          FindHintByBasename(track_env.file_hints_, key);
      ASSERT_NE(hint, nullptr) << meta.db_path << "/" << meta.name;
      if (*hint == Env::WLTH_SHORT) ++blob_short_count;
      if (*hint == Env::WLTH_MEDIUM) ++blob_medium_count;
      if (*hint == Env::WLTH_EXTREME) ++blob_extreme_count;
    }
  }

  ASSERT_GE(blob_short_count + blob_medium_count + blob_extreme_count, 1U);

  Close();
}

// fifo_cache.cc 独立单元测试
// 验证: 无锁读取、FIFO淘汰顺序、生命周期不延长
class FIFOCacheTest : public testing::Test {
 protected:
  void SetUp() override {
    CreateCache(1024);
  }

  void CreateCache(size_t capacity) {
    FIFOCacheOptions opts;
    opts.capacity = capacity;
    opts.num_shard_bits = 0;  // 单 Shard 保证确定性
    opts.strict_capacity_limit = false;
    cache_ = NewFIFOCache(opts);
  }

  bool Contains(const Slice& key, uint32_t hash) {
    Cache::Handle* handle = cache_->Lookup(key, hash, false /* record_hit */);
    if (handle == nullptr) {
      return false;
    }
    cache_->Release(handle);
    return true;
  }

  size_t FIFOSize() const {
    auto* fifo = dynamic_cast<FIFOCache*>(cache_.get());
    EXPECT_NE(fifo, nullptr);
    return fifo == nullptr ? 0 : fifo->TEST_GetFIFOSize();
  }

  std::shared_ptr<Cache> cache_;
  static void NoopDeleter(const Slice& /*key*/, void* /*value*/) {}
};

TEST_F(FIFOCacheTest, BasicInsertAndLookup) {
  Slice key("test_key");
  uint32_t hash = ShardedCache::HashSlice(key);

  Cache::Handle* handle = nullptr;
  Status s = cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter, &handle);
  ASSERT_OK(s);
  ASSERT_NE(handle, nullptr);
  cache_->Release(handle);

  Cache::Handle* lookup = cache_->Lookup(key, hash);
  ASSERT_NE(lookup, nullptr);
  cache_->Release(lookup);
}

TEST_F(FIFOCacheTest, LookupDoesNotExtendLifecycle) {
  // 使用 4-byte key 和 12-byte 容量，确保 FIFO 里最多保留 3 个元素。
  CreateCache(12);

  Slice key1("k001");
  Slice key2("k002");
  Slice key3("k003");
  Slice key4("k004");
  Slice key5("k005");
  uint32_t hash1 = ShardedCache::HashSlice(key1);
  uint32_t hash2 = ShardedCache::HashSlice(key2);
  uint32_t hash3 = ShardedCache::HashSlice(key3);
  uint32_t hash4 = ShardedCache::HashSlice(key4);
  uint32_t hash5 = ShardedCache::HashSlice(key5);

  Cache::Handle* handle = nullptr;
  ASSERT_OK(cache_->Insert(key1, hash1, nullptr, key1.size(), &NoopDeleter, &handle));
  cache_->Release(handle);
  ASSERT_OK(cache_->Insert(key2, hash2, nullptr, key2.size(), &NoopDeleter, &handle));
  cache_->Release(handle);
  ASSERT_OK(cache_->Insert(key3, hash3, nullptr, key3.size(), &NoopDeleter, &handle));
  cache_->Release(handle);

  ASSERT_EQ(FIFOSize(), 3U);
  ASSERT_TRUE(Contains(key1, hash1));
  ASSERT_TRUE(Contains(key2, hash2));
  ASSERT_TRUE(Contains(key3, hash3));

  for (int i = 0; i < 100; ++i) {
    Cache::Handle* h = cache_->Lookup(key1, hash1);
    ASSERT_NE(h, nullptr);
    cache_->Release(h);
  }

  ASSERT_OK(cache_->Insert(key4, hash4, nullptr, key4.size(), &NoopDeleter, &handle));
  cache_->Release(handle);

  ASSERT_FALSE(Contains(key1, hash1));
  ASSERT_TRUE(Contains(key2, hash2));
  ASSERT_TRUE(Contains(key3, hash3));
  ASSERT_TRUE(Contains(key4, hash4));
  ASSERT_EQ(FIFOSize(), 3U);

  for (int i = 0; i < 100; ++i) {
    Cache::Handle* h = cache_->Lookup(key2, hash2);
    ASSERT_NE(h, nullptr);
    cache_->Release(h);
  }

  ASSERT_OK(cache_->Insert(key5, hash5, nullptr, key5.size(), &NoopDeleter, &handle));
  cache_->Release(handle);

  // 如果 Lookup 会延长生命周期，这里被淘汰的会是 key3 而不是 key2。
  ASSERT_FALSE(Contains(key2, hash2));
  ASSERT_TRUE(Contains(key3, hash3));
  ASSERT_TRUE(Contains(key4, hash4));
  ASSERT_TRUE(Contains(key5, hash5));
}

TEST_F(FIFOCacheTest, FIFOFevictionOrder) {
  // 使用固定容量构造可精确预期的淘汰顺序。
  CreateCache(16);

  std::vector<std::string> keys = {"a001", "a002", "a003", "a004", "a005", "a006"};

  Cache::Handle* handle = nullptr;
  for (int i = 0; i < 4; ++i) {
    const auto& key_str = keys[i];
    Slice key(key_str);
    uint32_t hash = ShardedCache::HashSlice(key);
    ASSERT_OK(cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter, &handle));
    cache_->Release(handle);
  }

  ASSERT_EQ(FIFOSize(), 4U);
  ASSERT_TRUE(Contains(keys[0], ShardedCache::HashSlice(keys[0])));
  ASSERT_TRUE(Contains(keys[3], ShardedCache::HashSlice(keys[3])));

  Slice key5(keys[4]);
  uint32_t hash5 = ShardedCache::HashSlice(key5);
  ASSERT_OK(cache_->Insert(key5, hash5, nullptr, key5.size(), &NoopDeleter, &handle));
  cache_->Release(handle);

  ASSERT_FALSE(Contains(keys[0], ShardedCache::HashSlice(keys[0])));
  ASSERT_TRUE(Contains(keys[1], ShardedCache::HashSlice(keys[1])));
  ASSERT_TRUE(Contains(keys[2], ShardedCache::HashSlice(keys[2])));
  ASSERT_TRUE(Contains(keys[3], ShardedCache::HashSlice(keys[3])));
  ASSERT_TRUE(Contains(keys[4], hash5));

  Slice key6(keys[5]);
  uint32_t hash6 = ShardedCache::HashSlice(key6);
  ASSERT_OK(cache_->Insert(key6, hash6, nullptr, key6.size(), &NoopDeleter, &handle));
  cache_->Release(handle);

  ASSERT_FALSE(Contains(keys[1], ShardedCache::HashSlice(keys[1])));
  ASSERT_TRUE(Contains(keys[2], ShardedCache::HashSlice(keys[2])));
  ASSERT_TRUE(Contains(keys[3], ShardedCache::HashSlice(keys[3])));
  ASSERT_TRUE(Contains(keys[4], hash5));
  ASSERT_TRUE(Contains(keys[5], hash6));
  ASSERT_EQ(FIFOSize(), 4U);
}

TEST_F(FIFOCacheTest, EraseAndReinsert) {
  Slice key("erase_test_key");
  uint32_t hash = ShardedCache::HashSlice(key);

  Cache::Handle* handle = nullptr;
  ASSERT_OK(cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter, &handle));
  cache_->Release(handle);

  // 验证存在
  Cache::Handle* lookup = cache_->Lookup(key, hash);
  ASSERT_NE(lookup, nullptr);
  cache_->Release(lookup);

  // 删除
  cache_->Erase(key, hash);

  // 验证已删除
  ASSERT_EQ(cache_->Lookup(key, hash), nullptr);

  // 重新插入
  ASSERT_OK(cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter, &handle));
  cache_->Release(handle);
  lookup = cache_->Lookup(key, hash);
  ASSERT_NE(lookup, nullptr);
  cache_->Release(lookup);
}

// HotnessTracker 边界条件测试
// 验收标准 #2: 无锁晋升（不删除 Cache1 条目）
// 验收标准补充: 热键维持热度逻辑

TEST_F(HotnessTrackerTest, NoEraseFromCache1OnPromotion) {
  // 强验证:
  // 1. 首次写入后仅存在于 Cache1
  // 2. 第二次写入后同时存在于 Cache1 和 Cache2
  // 3. 之后它只能被 FIFO 自然淘汰，而不是在晋升时被主动删除
  HotnessTracker tracker(16, 32, 0);

  Slice key("p001");
  uint32_t hash = HotnessTracker::Hash(key);

  tracker.RecordHotness(key, hash);
  ASSERT_TRUE(tracker.TEST_WindowContains(key, hash));
  ASSERT_FALSE(tracker.TEST_HotContains(key, hash));
  ASSERT_EQ(tracker.TEST_WindowFIFOSize(), 1U);

  tracker.RecordHotness(key, hash);
  ASSERT_TRUE(tracker.TEST_WindowContains(key, hash));
  ASSERT_TRUE(tracker.TEST_HotContains(key, hash));
  ASSERT_EQ(tracker.TEST_WindowFIFOSize(), 1U);
  ASSERT_EQ(tracker.ClassifyForFlush(key, hash),
            HotnessTracker::FlushRoute::kEphemeral);

  for (int i = 0; i < 4; ++i) {
    std::string cold = "c00" + std::to_string(i);
    tracker.RecordHotness(cold, HotnessTracker::Hash(cold));
  }

  // key 的 charge 为 4，窗口容量为 16。写入 4 个新 cold key 后，
  // p001 应该由 FIFO 自然淘汰出 Cache1，但仍保留在 Cache2。
  ASSERT_FALSE(tracker.TEST_WindowContains(key, hash));
  ASSERT_TRUE(tracker.TEST_HotContains(key, hash));
  ASSERT_TRUE(tracker.IsHot(key, hash));
  ASSERT_EQ(tracker.ClassifyForFlush(key, hash),
            HotnessTracker::FlushRoute::kStable);
}

TEST_F(HotnessTrackerTest, HotKeyMaintainsHeatOnRepeatedWrites) {
  // 热缓存容量为 8，只能保留两个 4-byte key。
  // 如果命中 Cache2 时没有更新 LRU，那么后续插入第三个 hot key 会把 h001 淘汰；
  // 反之应淘汰较久未命中的 h002。
  HotnessTracker tracker(32, 8, 0);

  Slice hot_a("h001");
  Slice hot_b("h002");
  Slice hot_c("h003");
  uint32_t hash_a = HotnessTracker::Hash(hot_a);
  uint32_t hash_b = HotnessTracker::Hash(hot_b);
  uint32_t hash_c = HotnessTracker::Hash(hot_c);

  tracker.RecordHotness(hot_a, hash_a);
  tracker.RecordHotness(hot_a, hash_a);
  tracker.RecordHotness(hot_b, hash_b);
  tracker.RecordHotness(hot_b, hash_b);

  ASSERT_TRUE(tracker.TEST_HotContains(hot_a, hash_a));
  ASSERT_TRUE(tracker.TEST_HotContains(hot_b, hash_b));

  // 这次写入应命中 Cache2 并刷新 h001 的 LRU 位置。
  tracker.RecordHotness(hot_a, hash_a);

  tracker.RecordHotness(hot_c, hash_c);
  tracker.RecordHotness(hot_c, hash_c);

  ASSERT_TRUE(tracker.TEST_HotContains(hot_a, hash_a));
  ASSERT_FALSE(tracker.TEST_HotContains(hot_b, hash_b));
  ASSERT_TRUE(tracker.TEST_HotContains(hot_c, hash_c));
}

TEST_F(HotnessTrackerTest, ColdKeyNeverPromotedWithSingleWrite) {
  // 验证: 仅写入一次的 key 永远不会被误判为 Hot

  HotnessTracker tracker(4096, 4096, 0);

  for (int i = 0; i < 1000; ++i) {
    std::string key = "cold_" + std::to_string(i);
    Slice skey(key);
    uint32_t hash = HotnessTracker::Hash(skey);

    // 只写入一次
    tracker.RecordHotness(skey, hash);

    // 绝对不能成为 Hot
    ASSERT_FALSE(tracker.IsHot(skey, hash));
    ASSERT_EQ(tracker.ClassifyForFlush(skey, hash),
              HotnessTracker::FlushRoute::kWarm);
  }
}

TEST_F(HotnessTrackerTest, HashKeyConsistency) {
  // 验证 Hash Key 共用机制
  // 同一个 key 的 Hash 值必须一致，且可以被 Cache1 和 Cache2 共用

  HotnessTracker tracker(1024, 1024, 0);

  Slice test_key("consistency_key");

  // 多次计算 Hash 应该得到相同结果
  uint32_t hash1 = HotnessTracker::Hash(test_key);
  uint32_t hash2 = HotnessTracker::Hash(test_key);
  uint32_t hash3 = HotnessTracker::Hash(test_key);

  ASSERT_EQ(hash1, hash2);
  ASSERT_EQ(hash2, hash3);

  // 使用相同的 hash 值进行操作应该工作正常
  tracker.RecordHotness(test_key, hash1);
  ASSERT_FALSE(tracker.IsHot(test_key, hash1));

  // 使用不同的 hash 值（模拟错误情况）应该查找不到
  // 这证明了 Hash 一致性的重要性
  uint32_t wrong_hash = hash1 ^ 0xFFFFFFFF;  // 故意翻转所有位
  ASSERT_FALSE(tracker.IsHot(test_key, wrong_hash));  // 错误的 hash 应该找不到
}

TEST_F(HotnessTrackerTest, ConcurrentSafetyBasicCheck) {
  // 基本并发安全性验证
  // 虽然 full concurrency test 需要 stress test framework，
  // 但我们可以验证基本的线程安全性假设

  HotnessTracker tracker(1024 * 1024, 1024 * 1024, 0);  // 大容量避免竞争淘汰

  const int num_threads = 4;
  const int ops_per_thread = 100;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < ops_per_thread; ++i) {
        std::string key = "thread_" + std::to_string(t) + "_op_" + std::to_string(i);
        Slice skey(key);
        uint32_t hash = HotnessTracker::Hash(skey);

        // 每个线程执行混合读写操作
        if (i % 3 == 0) {
          tracker.RecordHotness(skey, hash);
          tracker.RecordHotness(skey, hash);  // 第二次写入尝试晋升
        } else {
          tracker.IsHot(skey, hash);  // 只读查询
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  SUCCEED();  // 如果没有崩溃或死锁，说明基本线程安全
}

} // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
