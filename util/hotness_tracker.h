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
    Cache::Handle* handle = hot_cache_->Lookup(key, hash, false /* record_hit */);
    if (handle != nullptr) {
      hot_cache_->Release(handle);
      return true;
    }
    return false;
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
  std::shared_ptr<Cache> window_cache_;
  std::shared_ptr<Cache> hot_cache_;
};

}  // namespace TERARKDB_NAMESPACE
