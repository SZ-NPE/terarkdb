#pragma once

#include <array>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "cache/sharded_cache.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/types.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace TERARKDB_NAMESPACE {

// HotnessTracker is owned by ColumnFamilyData, so one tracker instance belongs
// to one column family. Within that CF, the tracker estimates routing hotness so
// that a flush can route a key to a hot/warm/cold vSST. It does not classify
// block-cache blocks as hot: a hot key does not imply every data block
// containing or neighboring that key is hot. Foreground write-window learning
// has been removed; hotness now comes from background compaction feedback
// batches. The unified module keeps two internal index surfaces fed by that same
// compaction feedback stream: an approximate hash-bucket routing surface and an
// exact (user_key, sequence) drop-key surface for GC reverse-lookup avoidance.
class HotnessTracker {
 public:
  enum class FlushRoute {
    kWarm = 0,       // uncertain/default route
    kEphemeral = 1,  // strong-hot route
    kCold = 2,       // strong-cold route
  };

  // Compaction feedback staged by each sub-compaction before the compaction
  // commit publishes it to the tracker. Each user key maps to the exact set of
  // dropped separated-value sequence numbers observed for that sub-compaction.
  using DroppedSeqsByKey =
      std::unordered_map<std::string, std::vector<SequenceNumber>>;

  struct CompactionFeedback {
    std::string user_key;
    std::vector<SequenceNumber> dropped_seqs;
    uint32_t drop_count = 0;
  };

  // Metadata stored as the value of every exact drop-key cache entry. It is not
  // used for routing hotness; routing is represented by fixed hash buckets.
  struct HotEntry {
    // Per-key upper bound on retained dropped sequences. Bounded so memory
    // stays controlled; on overflow the oldest sequence is discarded. This
    // can only cause a miss (fall back to GetKey()), never a false hit.
    static constexpr size_t kMaxDroppedSeqs = 32;

    mutable port::Mutex mu;
    // Sequences of separated-value versions confirmed dead by compaction,
    // in append order ([0] is oldest). Linear search; deduplicated.
    std::array<SequenceNumber, kMaxDroppedSeqs> dropped_seqs;
    size_t num_dropped_seqs = 0;

    void AddDroppedSeq(SequenceNumber seq) {
      MutexLock l(&mu);
      for (size_t i = 0; i < num_dropped_seqs; ++i) {
        if (dropped_seqs[i] == seq) {
          return;
        }
      }
      if (num_dropped_seqs >= kMaxDroppedSeqs) {
        for (size_t i = 1; i < num_dropped_seqs; ++i) {
          dropped_seqs[i - 1] = dropped_seqs[i];
        }
        dropped_seqs[num_dropped_seqs - 1] = seq;
        return;
      }
      dropped_seqs[num_dropped_seqs++] = seq;
    }

    bool ContainsDroppedSeq(SequenceNumber seq) const {
      MutexLock l(&mu);
      for (size_t i = 0; i < num_dropped_seqs; ++i) {
        if (dropped_seqs[i] == seq) {
          return true;
        }
      }
      return false;
    }

  };

  struct Options {
    // Total capacity budget shared by the approximate routing buckets and the
    // exact drop-key cache.
    size_t hot_capacity = 0;
    // Whether compaction obsolete-version feedback contributes to hotness.
    bool enable_compaction_feedback = true;
    // Whether compaction obsolete-version feedback records exact dropped
    // sequence numbers for blob GC GetKey() short-circuiting.
    bool enable_drop_key_cache = true;
    // Simple epoch decay. Every `decay_interval` background batch publishes
    // advances one global epoch. 0 disables decay by default for compatibility.
    uint64_t decay_interval = 0;
    // If non-zero, an admitted hot key whose last hotness observation is older
    // than this many epochs is considered cold again.
    uint64_t decay_window = 0;
  };

  explicit HotnessTracker(const Options& options, int num_shard_bits = 6)
      : enable_compaction_feedback_(options.enable_compaction_feedback &&
                                    options.hot_capacity > 0),
        enable_drop_key_cache_(options.enable_drop_key_cache &&
                               options.hot_capacity > 0),
        decay_interval_(options.decay_interval),
        decay_window_(options.decay_window) {
    if (options.hot_capacity > 0) {
      routing_bucket_count_ = ComputeRoutingBucketCount(options.hot_capacity);
      routing_buckets_.reset(new RoutingBucket[routing_bucket_count_]);
      cold_current_.assign(routing_bucket_count_, 0);
      cold_previous_.assign(routing_bucket_count_, 0);
    }
    if (enable_drop_key_cache_) {
      const size_t drop_key_capacity =
          std::max<size_t>(1, options.hot_capacity / 2);
      drop_key_cache_ =
          NewLRUCache(drop_key_capacity, num_shard_bits, false, 0.0);
    }
  }

  void ApplyCompactionFeedbackBatch(
      const std::vector<CompactionFeedback>& feedbacks) {
    ApplyCompactionFeedbackBatch(feedbacks.data(), feedbacks.size());
  }

  void ApplyCompactionFeedbackBatch(const CompactionFeedback* feedbacks,
                                    size_t count) {
    if (feedbacks == nullptr || count == 0 ||
        (!enable_compaction_feedback_ && !enable_drop_key_cache_)) {
      return;
    }
    const uint64_t epoch = AdvanceAndGetEpoch();
    std::unordered_map<uint32_t, uint32_t> routing_delta_by_bucket;
    if (enable_compaction_feedback_ && routing_bucket_count_ > 0) {
      routing_delta_by_bucket.reserve(count);
    }
    for (size_t i = 0; i < count; ++i) {
      const CompactionFeedback& feedback = feedbacks[i];
      if (feedback.user_key.empty()) {
        continue;
      }
      Slice key(feedback.user_key);
      const uint32_t routing_bucket = RoutingBucketForKey(key);
      if (enable_compaction_feedback_) {
        const uint32_t signal = std::max<uint32_t>(1, feedback.drop_count);
        uint32_t& delta = routing_delta_by_bucket[routing_bucket];
        const uint32_t room = UINT32_MAX - delta;
        delta += std::min<uint32_t>(signal, room);
        ResetColdBucket(routing_bucket);
      }
      if (enable_drop_key_cache_ && !feedback.dropped_seqs.empty()) {
        Cache::Handle* handle = GetOrCreateDropEntryHandle(key);
        if (handle == nullptr) {
          continue;
        }
        auto* entry = static_cast<HotEntry*>(drop_key_cache_->Value(handle));
        if (entry != nullptr) {
          for (SequenceNumber seq : feedback.dropped_seqs) {
            if (seq != 0) {
              entry->AddDroppedSeq(seq);
            }
          }
        }
        drop_key_cache_->Release(handle);
      }
    }
    for (const auto& delta : routing_delta_by_bucket) {
      MarkRoutingBucketHot(delta.first, delta.second, epoch);
    }
  }

  void ApplyColdObservationBatch(const std::vector<uint32_t>& buckets) {
    if (routing_bucket_count_ == 0 || buckets.empty()) {
      return;
    }
    AdvanceAndGetEpoch();
    MutexLock l(&cold_mu_);
    std::swap(cold_previous_, cold_current_);
    std::fill(cold_current_.begin(), cold_current_.end(), 0);
    for (uint32_t bucket : buckets) {
      if (bucket < routing_bucket_count_) {
        cold_current_[bucket] = 1;
      }
    }
  }

  // Returns whether the separated-value version identified by (key, seq) was
  // recorded as dropped. Both the user key and the sequence must match;
  // matching only the user key is not enough (would be a false hit). A miss
  // (including after LRU eviction) is allowed and falls back to GetKey().
  bool IsDropped(const Slice& key, SequenceNumber seq) const {
    if (!enable_drop_key_cache_ || drop_key_cache_ == nullptr || seq == 0) {
      return false;
    }
    uint32_t hash = Hash(key);
    Cache::Handle* handle =
        drop_key_cache_->Lookup(key, hash, false /* record_hit */);
    if (handle == nullptr) {
      return false;
    }
    auto* entry = static_cast<HotEntry*>(drop_key_cache_->Value(handle));
    bool dropped = entry != nullptr && entry->ContainsDroppedSeq(seq);
    drop_key_cache_->Release(handle);
    return dropped;
  }

  FlushRoute ClassifyForRouting(const Slice& key) const {
    const uint32_t bucket = RoutingBucketForKey(key);
    if (IsRoutingBucketHot(bucket)) {
      return FlushRoute::kEphemeral;
    }
    if (IsRepeatedColdMiss(bucket)) {
      return FlushRoute::kCold;
    }
    return FlushRoute::kWarm;
  }

  bool DropKeyCacheEnabled() const { return enable_drop_key_cache_; }

  FlushRoute ClassifyForFlush(const Slice& key) const {
    // Flush classification should consume the hotness signal without refreshing
    // LRU recency. Hotness is advanced by explicit activity observations, not by
    // background flush scans.
    return ClassifyForRouting(key);
  }

  static uint32_t Hash(const Slice& key) {
    return ShardedCache::HashSlice(key);
  }

  uint32_t RoutingBucketForKey(const Slice& key) const {
    if (routing_bucket_count_ == 0) {
      return 0;
    }
    return Hash(key) % static_cast<uint32_t>(routing_bucket_count_);
  }

  // --- Test-only helpers -------------------------------------------------

  bool TEST_HotCacheContains(const Slice& key) const {
    return HasAnyHotSignal(key);
  }

  bool TEST_DropKeyCacheContains(const Slice& key) const {
    return CacheContainsAny(drop_key_cache_, key, false /* record_hit */);
  }

  bool TEST_RoutingBucketHot(const Slice& key) const {
    return IsRoutingBucketHot(RoutingBucketForKey(key));
  }

 private:
  static constexpr uint32_t kStrongHotRoutingScore = 2;

  struct RoutingBucket {
    std::atomic<uint32_t> hot_score{0};
    std::atomic<uint64_t> last_update_epoch{0};
  };

  static size_t ComputeRoutingBucketCount(size_t hot_capacity) {
    size_t buckets = std::max<size_t>(64, hot_capacity / 64);
    buckets = std::min<size_t>(buckets, 64 * 1024);
    return buckets;
  }

  static void DeleteHotEntry(const Slice& /*key*/, void* value) {
    delete static_cast<HotEntry*>(value);
  }

  // Looks up an exact drop-key entry for key, inserting a fresh one if absent.
  // Caller must Release the returned handle.
  Cache::Handle* GetOrCreateDropEntryHandle(const Slice& key) const {
    if (drop_key_cache_ == nullptr) {
      return nullptr;
    }
    uint32_t hash = Hash(key);
    Cache::Handle* handle =
        drop_key_cache_->Lookup(key, hash, true /* record_hit */);
    if (handle != nullptr) {
      return handle;
    }
    size_t charge = key.size() + sizeof(HotEntry);
    if (charge == 0) {
      charge = 1;
    }
    HotEntry* entry = new HotEntry();
    Status s = drop_key_cache_->Insert(key, hash, entry, charge,
                                       &DeleteHotEntry, &handle);
    if (!s.ok() || handle == nullptr) {
      // Insert refused the entry without taking ownership (e.g. a strict
      // capacity limit). Free it ourselves to avoid leaking.
      delete entry;
      return nullptr;
    }
    return handle;
  }

  uint64_t AdvanceAndGetEpoch() {
    if (decay_interval_ == 0) {
      return 0;
    }
    const uint64_t tick = batch_tick_.fetch_add(1, std::memory_order_relaxed) + 1;
    return tick / decay_interval_;
  }

  uint64_t CurrentEpoch() const {
    return decay_interval_ == 0
               ? 0
               : batch_tick_.load(std::memory_order_relaxed) / decay_interval_;
  }

  bool CacheContainsAny(const std::shared_ptr<Cache>& cache, const Slice& key,
                        bool record_hit) const {
    if (cache == nullptr) {
      return false;
    }
    uint32_t hash = Hash(key);
    Cache::Handle* handle = cache->Lookup(key, hash, record_hit);
    if (handle == nullptr) {
      return false;
    }
    cache->Release(handle);
    return true;
  }

  bool HasAnyHotSignal(const Slice& key) const {
    return IsRoutingBucketHot(RoutingBucketForKey(key));
  }

  void MarkRoutingBucketHot(uint32_t bucket, uint32_t signal, uint64_t epoch) {
    if (routing_bucket_count_ == 0 || bucket >= routing_bucket_count_ ||
        routing_buckets_ == nullptr || signal == 0) {
      return;
    }
    RoutingBucket& b = routing_buckets_[bucket];
    uint32_t old = b.hot_score.load(std::memory_order_relaxed);
    while (old < UINT32_MAX &&
           !b.hot_score.compare_exchange_weak(
               old, std::min<uint32_t>(UINT32_MAX, old + signal),
               std::memory_order_relaxed, std::memory_order_relaxed)) {
    }
    b.last_update_epoch.store(epoch, std::memory_order_relaxed);
  }

  bool IsRoutingBucketHot(uint32_t bucket) const {
    if (routing_bucket_count_ == 0 || bucket >= routing_bucket_count_ ||
        routing_buckets_ == nullptr) {
      return false;
    }
    const RoutingBucket& b = routing_buckets_[bucket];
    const uint32_t score = b.hot_score.load(std::memory_order_relaxed);
    if (score < kStrongHotRoutingScore) {
      return false;
    }
    if (decay_interval_ > 0 && decay_window_ > 0) {
      const uint64_t epoch = CurrentEpoch();
      const uint64_t last = b.last_update_epoch.load(std::memory_order_relaxed);
      if (epoch > last && epoch - last > decay_window_) {
        return false;
      }
    }
    return true;
  }

  bool IsRepeatedColdMiss(uint32_t bucket) const {
    if (routing_bucket_count_ == 0 || bucket >= routing_bucket_count_) {
      return false;
    }
    MutexLock l(&cold_mu_);
    return cold_current_[bucket] != 0 && cold_previous_[bucket] != 0;
  }

  void ResetColdBucket(uint32_t bucket) {
    if (routing_bucket_count_ == 0 || bucket >= routing_bucket_count_) {
      return;
    }
    MutexLock l(&cold_mu_);
    cold_current_[bucket] = 0;
    cold_previous_[bucket] = 0;
  }

  const bool enable_compaction_feedback_;
  const bool enable_drop_key_cache_;
  const uint64_t decay_interval_;
  const uint64_t decay_window_;
  mutable std::atomic<uint64_t> batch_tick_{0};

  size_t routing_bucket_count_ = 0;
  std::unique_ptr<RoutingBucket[]> routing_buckets_;
  mutable port::Mutex cold_mu_;
  mutable std::vector<uint8_t> cold_current_;
  mutable std::vector<uint8_t> cold_previous_;
  mutable std::shared_ptr<Cache> drop_key_cache_;
};

}  // namespace TERARKDB_NAMESPACE
