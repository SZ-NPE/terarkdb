//  Copyright (c) 2024-present. All rights reserved.

#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "cache/fifo_cache.h"
#include "db/db_test_util.h"
#include "port/stack_trace.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/terark_namespace.h"
#include "util/hotness_tracker.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

namespace {

// Builds a HotnessTracker::Options with generous FIFO/LRU capacities, suitable
// for unit tests.
HotnessTracker::Options MakeTestOptions() {
  HotnessTracker::Options options;
  options.window_capacity = 1 << 20;
  options.hot_capacity = 1 << 20;
  options.enable_write_window = true;
  options.enable_compaction_feedback = true;
  return options;
}

}  // namespace

// Unit tests for the FIFO + hot-LRU based HotnessTracker.
class HotnessTrackerTest : public testing::Test {};

// (a) A single write only enters the FIFO observation window.
TEST_F(HotnessTrackerTest, SingleWriteStaysBelowThreshold) {
  HotnessTracker tracker(MakeTestOptions(), 0 /* num_shard_bits */);

  Slice key("single_write_key");
  tracker.RecordWrite(key);

  ASSERT_TRUE(tracker.TEST_RecentWindowContains(key));
  ASSERT_FALSE(tracker.TEST_HotCacheContains(key));
  ASSERT_EQ(tracker.ClassifyForFlush(key),
            HotnessTracker::FlushRoute::kWarm);
}

// (b) Repeated writes inside the FIFO window promote the key to hot LRU.
TEST_F(HotnessTrackerTest, RepeatedWritesBecomeHot) {
  HotnessTracker tracker(MakeTestOptions(), 0 /* num_shard_bits */);

  Slice key("repeat_write_key");
  ASSERT_EQ(tracker.ClassifyForFlush(key),
            HotnessTracker::FlushRoute::kWarm);

  // First write only inserts into the window. The second write is an overwrite
  // inside the observation window and promotes the key directly to hot LRU.
  tracker.RecordWrite(key);
  tracker.RecordWrite(key);

  ASSERT_TRUE(tracker.TEST_HotCacheContains(key));
  ASSERT_EQ(tracker.ClassifyForFlush(key),
            HotnessTracker::FlushRoute::kEphemeral);
}

// (c) Compaction feedback is confirmed overwrite evidence and promotes directly.
TEST_F(HotnessTrackerTest, CompactionFeedbackBecomesHot) {
  HotnessTracker tracker(MakeTestOptions(), 0 /* num_shard_bits */);

  Slice key("compaction_feedback_key");

  tracker.RecordCompactionFeedback(key);

  ASSERT_TRUE(tracker.TEST_HotCacheContains(key));
  ASSERT_EQ(tracker.ClassifyForFlush(key),
            HotnessTracker::FlushRoute::kEphemeral);
}

// (c2) Compaction feedback with a sequence also promotes the key into the hot
// LRU (same hot-routing semantics as the no-seq overload).
TEST_F(HotnessTrackerTest, CompactionFeedbackWithSeqBecomesHot) {
  HotnessTracker tracker(MakeTestOptions(), 0 /* num_shard_bits */);

  Slice key("compaction_feedback_seq_key");

  tracker.RecordCompactionFeedback(key, 42 /* seq */);

  ASSERT_TRUE(tracker.TEST_HotCacheContains(key));
  ASSERT_EQ(tracker.ClassifyForFlush(key),
            HotnessTracker::FlushRoute::kEphemeral);
}

// (c3) IsDropped only hits the exact recorded sequence. Other sequences under
// the same user key, and unrecorded keys, must miss (no false hits).
TEST_F(HotnessTrackerTest, IsDroppedMatchesOnlyRecordedSequence) {
  HotnessTracker tracker(MakeTestOptions(), 0 /* num_shard_bits */);

  Slice key("dropped_key");
  ASSERT_FALSE(tracker.IsDropped(key, 100));

  tracker.RecordCompactionFeedback(key, 100 /* seq */);
  tracker.RecordCompactionFeedback(key, 200 /* seq */);

  ASSERT_TRUE(tracker.IsDropped(key, 100));
  ASSERT_TRUE(tracker.IsDropped(key, 200));
  // Same user key, different (unrecorded) sequence must not match.
  ASSERT_FALSE(tracker.IsDropped(key, 150));
  // A different user key must not match.
  ASSERT_FALSE(tracker.IsDropped(Slice("other_key"), 100));
}

// (c4) Once the hot entry is evicted by the LRU, its dropped-sequence info is
// allowed to be lost; IsDropped then returns false (a miss falls back to
// GetKey()).
TEST_F(HotnessTrackerTest, IsDroppedLostAfterEviction) {
  HotnessTracker::Options options = MakeTestOptions();
  options.hot_capacity = 20;
  HotnessTracker tracker(options, 0 /* num_shard_bits */);

  Slice first("drop_key_1");
  Slice second("drop_key_2");
  Slice third("drop_key_3");

  tracker.RecordCompactionFeedback(first, 1 /* seq */);
  tracker.RecordCompactionFeedback(second, 2 /* seq */);
  tracker.RecordCompactionFeedback(third, 3 /* seq */);

  // `first` was evicted by the small-capacity LRU; its dropped seq is gone.
  ASSERT_FALSE(tracker.TEST_HotCacheContains(first));
  ASSERT_FALSE(tracker.IsDropped(first, 1));
  // Survivors still report their dropped sequences.
  ASSERT_TRUE(tracker.IsDropped(second, 2));
  ASSERT_TRUE(tracker.IsDropped(third, 3));
}

// (c5) When compaction feedback is disabled, no dropped sequence is recorded.
TEST_F(HotnessTrackerTest, IsDroppedDisabledWhenFeedbackOff) {
  HotnessTracker::Options options = MakeTestOptions();
  options.enable_compaction_feedback = false;
  HotnessTracker tracker(options, 0 /* num_shard_bits */);

  Slice key("no_feedback_drop_key");
  tracker.RecordCompactionFeedback(key, 7 /* seq */);

  ASSERT_FALSE(tracker.IsDropped(key, 7));
}

// (d) Hot LRU capacity bounds the promoted hot set.
TEST_F(HotnessTrackerTest, HotLruCapacityEvictsOldEntries) {
  HotnessTracker::Options options = MakeTestOptions();
  options.hot_capacity = 20;
  HotnessTracker tracker(options, 0 /* num_shard_bits */);

  Slice first("hot_key_1");
  Slice second("hot_key_2");
  Slice third("hot_key_3");

  tracker.RecordCompactionFeedback(first);
  tracker.RecordCompactionFeedback(second);
  tracker.RecordCompactionFeedback(third);

  ASSERT_FALSE(tracker.TEST_HotCacheContains(first));
  ASSERT_TRUE(tracker.TEST_HotCacheContains(second));
  ASSERT_TRUE(tracker.TEST_HotCacheContains(third));
}

TEST_F(HotnessTrackerTest, HotLruDoesNotExpireWithoutEviction) {
  HotnessTracker tracker(MakeTestOptions(), 0 /* num_shard_bits */);

  Slice key("persistent_hot_key");
  tracker.RecordCompactionFeedback(key);
  ASSERT_TRUE(tracker.TEST_HotCacheContains(key));

  for (int i = 0; i < 1000; ++i) {
    std::string filler = "filler_" + std::to_string(i);
    tracker.RecordWrite(filler);
  }

  ASSERT_TRUE(tracker.TEST_HotCacheContains(key));
  ASSERT_EQ(tracker.ClassifyForFlush(key),
            HotnessTracker::FlushRoute::kEphemeral);
}

TEST_F(HotnessTrackerTest, WriteWindowDisabledDoesNotPromoteRepeatedWrites) {
  HotnessTracker::Options options = MakeTestOptions();
  options.enable_write_window = false;
  HotnessTracker tracker(options, 0 /* num_shard_bits */);

  Slice key("no_window_key");
  for (int i = 0; i < 100; ++i) {
    tracker.RecordWrite(key);
  }

  ASSERT_FALSE(tracker.TEST_HotCacheContains(key));
  ASSERT_EQ(tracker.ClassifyForFlush(key),
            HotnessTracker::FlushRoute::kWarm);
}

TEST_F(HotnessTrackerTest, CompactionFeedbackDisabledDoesNotPromote) {
  HotnessTracker::Options options = MakeTestOptions();
  options.enable_compaction_feedback = false;
  HotnessTracker tracker(options, 0 /* num_shard_bits */);

  Slice key("no_feedback_key");
  for (int i = 0; i < 100; ++i) {
    tracker.RecordCompactionFeedback(key);
  }

  ASSERT_FALSE(tracker.TEST_HotCacheContains(key));
  ASSERT_EQ(tracker.ClassifyForFlush(key),
            HotnessTracker::FlushRoute::kWarm);
}

// Cold keys written only once must never be promoted to the hot route.
TEST_F(HotnessTrackerTest, ColdKeysNeverPromoted) {
  HotnessTracker tracker(MakeTestOptions(), 0 /* num_shard_bits */);

  for (int i = 0; i < 1000; ++i) {
    std::string key = "cold_" + std::to_string(i);
    tracker.RecordWrite(key);
    ASSERT_EQ(tracker.ClassifyForFlush(key),
              HotnessTracker::FlushRoute::kWarm);
  }
}

TEST_F(HotnessTrackerTest, ConcurrentSafetyBasicCheck) {
  HotnessTracker tracker(MakeTestOptions(), 0 /* num_shard_bits */);

  const int num_threads = 4;
  const int ops_per_thread = 100;
  std::vector<std::thread> threads;

  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < ops_per_thread; ++i) {
        std::string key =
            "thread_" + std::to_string(t) + "_op_" + std::to_string(i);
        if (i % 3 == 0) {
          tracker.RecordWrite(key);
          tracker.RecordWrite(key);
        } else {
            tracker.ClassifyForFlush(key);
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  SUCCEED();
}

// End-to-end routing tests exercising the full DB write/flush path.
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

  TrackHintWritableFile(std::unique_ptr<WritableFile>&& t, TrackHintEnv* env,
                        const std::string& fname)
      : WritableFileWrapper(t.get()),
        owner_(std::move(t)),
        env_(env),
        fname_(fname) {}

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

std::string BlobValue(const std::string& tag) {
  return tag + std::string(256, 'x');
}

void CollectSstHints(TrackHintEnv* track_env, bool* found_short,
                     bool* found_medium) {
  *found_short = false;
  *found_medium = false;
  MutexLock l(&track_env->mutex_);
  for (const auto& pair : track_env->file_hints_) {
    if (pair.first.find(".sst") == std::string::npos) {
      continue;
    }
    if (pair.second == Env::WLTH_SHORT) {
      *found_short = true;
    }
    if (pair.second == Env::WLTH_MEDIUM) {
      *found_medium = true;
    }
  }
}

void TrackHintWritableFile::SetWriteLifeTimeHint(Env::WriteLifeTimeHint hint) {
  {
    MutexLock l(&env_->mutex_);
    env_->file_hints_[fname_] = hint;
  }
  owner_->SetWriteLifeTimeHint(hint);
}

// Repeatedly-written keys should be flushed to the hot (short-lived) route,
// while a key written only once stays on the cold (medium) route.
TEST_F(AdaptiveHotnessRoutingTest, RepeatedWritesRouteToHotBlob) {
  TrackHintEnv track_env(env_);
  Options options = CurrentOptions();
  options.env = &track_env;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  options.blob_size = 0;
  options.enable_hotness_tracker = true;
  options.hotness_window_capacity = 1 << 20;
  options.hotness_hot_capacity = 1 << 20;
  options.hotness_enable_write_window = true;
  options.hotness_enable_compaction_feedback = true;
  options.target_blob_file_size = 10 * 1024;

  DestroyAndReopen(options);

  // Hot keys: written at least twice so the second write promotes them.
  for (int i = 0; i < 8; ++i) {
    std::string key = "hot" + std::to_string(i);
    ASSERT_OK(Put(key, BlobValue("v1")));
    ASSERT_OK(Put(key, BlobValue("v2")));
    ASSERT_OK(Put(key, BlobValue("v3")));
  }
  // Cold keys: written once.
  for (int i = 0; i < 8; ++i) {
    std::string key = "cold" + std::to_string(i);
    ASSERT_OK(Put(key, BlobValue("v1")));
  }

  {
    MutexLock l(&track_env.mutex_);
    track_env.file_hints_.clear();
  }

  Flush();

  bool found_short = false;
  bool found_medium = false;
  CollectSstHints(&track_env, &found_short, &found_medium);

  // Hot keys produce a short-lived blob; cold keys produce a medium blob.
  Close();
  ASSERT_TRUE(found_short);
  ASSERT_TRUE(found_medium);
}

// With the hotness tracker disabled the DB must behave exactly as before:
// writes succeed, flush succeeds and data is readable.
TEST_F(AdaptiveHotnessRoutingTest, DisabledTrackerBehavesNormally) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.disable_auto_compactions = true;
  options.blob_size = 0;
  options.enable_hotness_tracker = false;
  options.target_blob_file_size = 10 * 1024;

  DestroyAndReopen(options);

  for (int i = 0; i < 8; ++i) {
    std::string key = "k" + std::to_string(i);
    ASSERT_OK(Put(key, BlobValue("v1")));
    ASSERT_OK(Put(key, BlobValue("v2")));
  }

  Flush();

  for (int i = 0; i < 8; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string value;
    ASSERT_OK(db_->Get(ReadOptions(), key, &value));
    ASSERT_EQ(value, BlobValue("v2"));
  }

  Close();
}

// Compaction feedback should mark overwrite-heavy keys as hot even when the
// write-window signal is disabled. This proves the hot-key record path in
// CompactionIterator feeds into the next flush routing decision.
TEST_F(AdaptiveHotnessRoutingTest, CompactionFeedbackRoutesNextFlushToHotBlob) {
  TrackHintEnv track_env(env_);
  Options options = CurrentOptions();
  options.env = &track_env;
  options.create_if_missing = true;
  options.disable_auto_compactions = true;

  options.blob_size = 0;
  options.enable_hotness_tracker = true;
  options.hotness_window_capacity = 1 << 20;
  options.hotness_hot_capacity = 1 << 20;
  options.hotness_enable_write_window = false;
  options.hotness_enable_compaction_feedback = true;
  options.target_blob_file_size = 10 * 1024;

  DestroyAndReopen(options);

  ASSERT_OK(Put("compaction_hot", BlobValue("old_value")));
  Flush();
  ASSERT_OK(Put("compaction_hot", BlobValue("new_value")));
  Flush();

  CompactRangeOptions compact_options;
  ASSERT_OK(db_->CompactRange(compact_options, nullptr, nullptr));

  {
    MutexLock l(&track_env.mutex_);
    track_env.file_hints_.clear();
  }

  ASSERT_OK(Put("compaction_hot", BlobValue("after_feedback")));
  ASSERT_OK(Put("still_cold", BlobValue("single_write")));
  Flush();

  bool found_short = false;
  bool found_medium = false;
  CollectSstHints(&track_env, &found_short, &found_medium);

  Close();
  ASSERT_TRUE(found_short);
  ASSERT_TRUE(found_medium);
}

// fifo_cache.cc 独立单元测试
// 验证: 无锁读取、FIFO淘汰顺序、生命周期不延长
class FIFOCacheTest : public testing::Test {
 protected:
  void SetUp() override { CreateCache(1024); }

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
  Status s =
      cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter, &handle);
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
  ASSERT_OK(
      cache_->Insert(key1, hash1, nullptr, key1.size(), &NoopDeleter, &handle));
  cache_->Release(handle);
  ASSERT_OK(
      cache_->Insert(key2, hash2, nullptr, key2.size(), &NoopDeleter, &handle));
  cache_->Release(handle);
  ASSERT_OK(
      cache_->Insert(key3, hash3, nullptr, key3.size(), &NoopDeleter, &handle));
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

  ASSERT_OK(
      cache_->Insert(key4, hash4, nullptr, key4.size(), &NoopDeleter, &handle));
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

  ASSERT_OK(
      cache_->Insert(key5, hash5, nullptr, key5.size(), &NoopDeleter, &handle));
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

  std::vector<std::string> keys = {"a001", "a002", "a003",
                                   "a004", "a005", "a006"};

  Cache::Handle* handle = nullptr;
  for (int i = 0; i < 4; ++i) {
    const auto& key_str = keys[i];
    Slice key(key_str);
    uint32_t hash = ShardedCache::HashSlice(key);
    ASSERT_OK(
        cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter, &handle));
    cache_->Release(handle);
  }

  ASSERT_EQ(FIFOSize(), 4U);
  ASSERT_TRUE(Contains(keys[0], ShardedCache::HashSlice(keys[0])));
  ASSERT_TRUE(Contains(keys[3], ShardedCache::HashSlice(keys[3])));

  Slice key5(keys[4]);
  uint32_t hash5 = ShardedCache::HashSlice(key5);
  ASSERT_OK(
      cache_->Insert(key5, hash5, nullptr, key5.size(), &NoopDeleter, &handle));
  cache_->Release(handle);

  ASSERT_FALSE(Contains(keys[0], ShardedCache::HashSlice(keys[0])));
  ASSERT_TRUE(Contains(keys[1], ShardedCache::HashSlice(keys[1])));
  ASSERT_TRUE(Contains(keys[2], ShardedCache::HashSlice(keys[2])));
  ASSERT_TRUE(Contains(keys[3], ShardedCache::HashSlice(keys[3])));
  ASSERT_TRUE(Contains(keys[4], hash5));

  Slice key6(keys[5]);
  uint32_t hash6 = ShardedCache::HashSlice(key6);
  ASSERT_OK(
      cache_->Insert(key6, hash6, nullptr, key6.size(), &NoopDeleter, &handle));
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
  ASSERT_OK(
      cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter, &handle));
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
  ASSERT_OK(
      cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter, &handle));
  cache_->Release(handle);
  lookup = cache_->Lookup(key, hash);
  ASSERT_NE(lookup, nullptr);
  cache_->Release(lookup);
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  TERARKDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
