#pragma once

#include <atomic>
#include <cmath>
#include <cstdint>
#include <memory>

#include "cache/fifo_cache.h"
#include "cache/sharded_cache.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "util/hash.h"

namespace TERARKDB_NAMESPACE {

// HotnessTracker estimates per-key write hotness so that a flush can route a
// key to a hot or cold vSST. The estimation is built on three signals:
//   1. A recent write window (a fixed-capacity FIFO set). A repeated write to a
//      key that is still inside the window is considered an overwrite and
//      contributes to the key's hotness.
//   2. A Count-Min Sketch (CMS) that accumulates the hotness weights. The CMS
//      keeps memory bounded regardless of the key cardinality.
//   3. Optional compaction feedback: when a compaction confirms that an old
//      version of a key was dropped, the key receives an additional weight.
//
// Hotness scores decay over time (measured in writes) so that historically hot
// keys cool down when they stop being written.
class HotnessTracker {
 public:
  enum class FlushRoute {
    kWarm = 0,       // cold route
    kEphemeral = 1,  // hot route
    kStable = 2,     // reserved, currently unused
  };

  struct Options {
    // Capacity (in bytes) of the recent write window FIFO set.
    size_t window_capacity = 0;
    // Whether repeated writes inside the window contribute to hotness.
    bool enable_write_window = true;
    // Whether compaction obsolete-version feedback contributes to hotness.
    bool enable_compaction_feedback = true;
    // Count-Min Sketch geometry.
    uint64_t sketch_width = 0;
    uint32_t sketch_depth = 0;
    // Hotness increment when the write window observes a repeated key.
    uint32_t write_repeat_weight = 1;
    // Hotness increment when compaction drops an obsolete version.
    uint32_t compaction_feedback_weight = 2;
    // Minimum estimated hotness score for routing a key to the hot route.
    uint32_t threshold = 1;
    // Number of writes between decay operations. 0 disables decay.
    uint64_t decay_interval = 0;
    // Half-life in writes for decayed hotness scores. 0 disables decay.
    uint64_t half_life_writes = 0;
  };

  explicit HotnessTracker(const Options& options, int num_shard_bits = 6)
      : enable_write_window_(options.enable_write_window),
        enable_compaction_feedback_(options.enable_compaction_feedback),
        sketch_width_(options.sketch_width),
        sketch_depth_(options.sketch_depth),
        write_repeat_weight_(options.write_repeat_weight),
        compaction_feedback_weight_(options.compaction_feedback_weight),
        threshold_(options.threshold),
        decay_interval_(options.decay_interval),
        half_life_writes_(options.half_life_writes),
        writes_(0) {
    if (enable_write_window_) {
      window_cache_ =
          NewFIFOCache(options.window_capacity, num_shard_bits, false, 0.0);
    }
    if (sketch_width_ > 0 && sketch_depth_ > 0) {
      table_.reset(new std::atomic<uint32_t>[ TableSize() ]);
      for (size_t i = 0; i < TableSize(); ++i) {
        table_[i].store(0, std::memory_order_relaxed);
      }
    }
  }

  // Records a write of key. When the write window is disabled this only
  // maintains the write counter used for decay and does not change hotness.
  void RecordWrite(const Slice& key) {
    if (enable_write_window_ && window_cache_ != nullptr) {
      uint32_t hash = Hash(key);
      Cache::Handle* handle =
          window_cache_->Lookup(key, hash, false /* record_hit */);
      if (handle != nullptr) {
        // Repeated write while still inside the recent window: an overwrite.
        window_cache_->Release(handle);
        CmsAdd(key, write_repeat_weight_);
      }
      // Insert (or refresh) the key in the recent window.
      window_cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter);
    }
    MaybeDecay();
  }

  // Records that compaction dropped an obsolete version of key. This must only
  // be called from places where the version is known to be dead, otherwise it
  // would mistake ordinary scans for hotness.
  void RecordCompactionFeedback(const Slice& key) {
    if (!enable_compaction_feedback_) {
      return;
    }
    CmsAdd(key, compaction_feedback_weight_);
  }

  // Returns the estimated hotness score for key.
  uint32_t Estimate(const Slice& key) const { return CmsEstimate(key); }

  FlushRoute ClassifyForFlush(const Slice& key) const {
    if (Estimate(key) >= threshold_) {
      return FlushRoute::kEphemeral;  // hot route
    }
    return FlushRoute::kWarm;  // cold route
  }

  static void NoopDeleter(const Slice& /*key*/, void* /*value*/) {
    // Intentionally empty because the window cache stores keys as a set only.
  }

  static uint32_t Hash(const Slice& key) {
    return ShardedCache::HashSlice(key);
  }

  // --- Test-only helpers -------------------------------------------------

  uint64_t TEST_WriteCount() const {
    return writes_.load(std::memory_order_relaxed);
  }

  void TEST_ForceDecay() {
    Decay(DecayEnabled() ? decay_interval_ : 1);
  }

  bool TEST_RecentWindowContains(const Slice& key) const {
    if (window_cache_ == nullptr) {
      return false;
    }
    Cache::Handle* handle =
        window_cache_->Lookup(key, Hash(key), false /* record_hit */);
    if (handle == nullptr) {
      return false;
    }
    window_cache_->Release(handle);
    return true;
  }

 private:
  size_t TableSize() const {
    return static_cast<size_t>(sketch_depth_) *
           static_cast<size_t>(sketch_width_);
  }

  // Derives an independent hash for sketch row i using double hashing.
  uint32_t RowHash(const Slice& key, uint32_t row) const {
    return TERARKDB_NAMESPACE::Hash(key.data(), key.size(),
                                    0x9747b28cu + row * 0xc2b2ae35u);
  }

  void CmsAdd(const Slice& key, uint32_t weight) {
    if (table_ == nullptr || weight == 0) {
      return;
    }
    for (uint32_t row = 0; row < sketch_depth_; ++row) {
      uint32_t h = RowHash(key, row);
      size_t idx = static_cast<size_t>(row) * sketch_width_ +
                   (h % sketch_width_);
      table_[idx].fetch_add(weight, std::memory_order_relaxed);
    }
  }

  uint32_t CmsEstimate(const Slice& key) const {
    if (table_ == nullptr) {
      return 0;
    }
    uint32_t min_value = UINT32_MAX;
    for (uint32_t row = 0; row < sketch_depth_; ++row) {
      uint32_t h = RowHash(key, row);
      size_t idx = static_cast<size_t>(row) * sketch_width_ +
                   (h % sketch_width_);
      uint32_t value = table_[idx].load(std::memory_order_relaxed);
      if (value < min_value) {
        min_value = value;
      }
    }
    return min_value;
  }

  bool DecayEnabled() const {
    return decay_interval_ > 0 && half_life_writes_ > 0;
  }

  void MaybeDecay() {
    if (!DecayEnabled()) {
      writes_.fetch_add(1, std::memory_order_relaxed);
      return;
    }
    uint64_t now = writes_.fetch_add(1, std::memory_order_relaxed) + 1;
    if (now % decay_interval_ == 0) {
      Decay(decay_interval_);
    }
  }

  uint32_t DecayedValue(uint32_t value, uint64_t elapsed_writes) const {
    if (value == 0) {
      return 0;
    }
    if (!DecayEnabled()) {
      return value / 2;
    }
    const double exponent =
        -static_cast<double>(elapsed_writes) /
        static_cast<double>(half_life_writes_);
    const double factor = std::exp2(exponent);
    return static_cast<uint32_t>(static_cast<double>(value) * factor);
  }

  // Apply half-life decay: counter *= 2^(-elapsed_writes / half_life_writes).
  void Decay(uint64_t elapsed_writes) {
    if (table_ == nullptr) {
      return;
    }
    for (size_t i = 0; i < TableSize(); ++i) {
      uint32_t value = table_[i].load(std::memory_order_relaxed);
      while (true) {
        uint32_t decayed = DecayedValue(value, elapsed_writes);
        if (table_[i].compare_exchange_weak(value, decayed,
                                            std::memory_order_relaxed,
                                            std::memory_order_relaxed)) {
          break;
        }
      }
    }
  }

  const bool enable_write_window_;
  const bool enable_compaction_feedback_;
  const uint64_t sketch_width_;
  const uint32_t sketch_depth_;
  const uint32_t write_repeat_weight_;
  const uint32_t compaction_feedback_weight_;
  const uint32_t threshold_;
  const uint64_t decay_interval_;
  const uint64_t half_life_writes_;

  std::atomic<uint64_t> writes_;
  std::shared_ptr<Cache> window_cache_;
  std::unique_ptr<std::atomic<uint32_t>[]> table_;
};

}  // namespace TERARKDB_NAMESPACE
