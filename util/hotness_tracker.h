#pragma once

#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "cache/fifo_cache.h"
#include "cache/sharded_cache.h"
#include <memory>

namespace TERARKDB_NAMESPACE {

class HotnessTracker {
 public:
  enum class FlushRoute {
    kWarm = 0,
    kEphemeral = 1,
    kStable = 2,
  };

  HotnessTracker(size_t window_cache_capacity, size_t hot_cache_capacity,
                 int num_shard_bits = 6) {
    window_cache_ = NewFIFOCache(window_cache_capacity, num_shard_bits, false, 0.0);
    hot_cache_ = NewLRUCache(hot_cache_capacity, num_shard_bits, false, 0.0);
  }

  void RecordHotness(const Slice& key, uint32_t hash) {
    Cache::Handle* handle = hot_cache_->Lookup(key, hash);
    if (handle != nullptr) {
      hot_cache_->Release(handle);
      return;
    }

    handle = window_cache_->Lookup(key, hash);
    if (handle != nullptr) {
      window_cache_->Release(handle);
      hot_cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter);
    } else {
      window_cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter);
    }
  }

  bool IsHot(const Slice& key, uint32_t hash) const {
    return CacheContains(hot_cache_, key, hash);
  }

  FlushRoute ClassifyForFlush(const Slice& key, uint32_t hash) const {
    if (!CacheContains(hot_cache_, key, hash)) {
      return FlushRoute::kWarm;
    }
    // Keys that are still in the observation window remain in the
    // high-overwrite route. Only keys that were previously promoted and have
    // since aged out of the window can enter the stable route.
    if (CacheContains(window_cache_, key, hash)) {
      return FlushRoute::kEphemeral;
    }
    return FlushRoute::kStable;
  }

  static void NoopDeleter(const Slice& /*key*/, void* /*value*/) {
    // Intentionally empty because the cache stores keys as a set only.
  }

  static uint32_t Hash(const Slice& key) {
    return ShardedCache::HashSlice(key);
  }

  bool TEST_WindowContains(const Slice& key, uint32_t hash) const {
    Cache::Handle* handle =
        window_cache_->Lookup(key, hash, false /* record_hit */);
    if (handle == nullptr) {
      return false;
    }
    window_cache_->Release(handle);
    return true;
  }

  bool TEST_HotContains(const Slice& key, uint32_t hash) const {
    Cache::Handle* handle =
        hot_cache_->Lookup(key, hash, false /* record_hit */);
    if (handle == nullptr) {
      return false;
    }
    hot_cache_->Release(handle);
    return true;
  }

  size_t TEST_WindowFIFOSize() const {
    auto* fifo = dynamic_cast<FIFOCache*>(window_cache_.get());
    return fifo == nullptr ? 0 : fifo->TEST_GetFIFOSize();
  }

 private:
  static bool CacheContains(const std::shared_ptr<Cache>& cache, const Slice& key,
                            uint32_t hash) {
    Cache::Handle* handle = cache->Lookup(key, hash, false /* record_hit */);
    if (handle == nullptr) {
      return false;
    }
    cache->Release(handle);
    return true;
  }

  std::shared_ptr<Cache> window_cache_;
  std::shared_ptr<Cache> hot_cache_;
};

}  // namespace TERARKDB_NAMESPACE
