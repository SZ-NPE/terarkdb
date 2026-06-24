#pragma once

#include <array>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>

#include "cache/fifo_cache.h"
#include "cache/sharded_cache.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/types.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace TERARKDB_NAMESPACE {

// HotnessTracker estimates per-key update hotness so that a flush can route a
// key to a hot or cold vSST. The write path uses a FIFO recent-write window only
// as an observation window. A repeated write inside that window promotes the key
// into the hot LRU. Compaction feedback is stronger evidence: when compaction
// drops an obsolete version, the key is promoted directly into the hot LRU.
// Flush routing only consults the hot LRU.
class HotnessTracker {
 public:
  enum class FlushRoute {
    kWarm = 0,       // cold route
    kEphemeral = 1,  // hot route
    kStable = 2,     // reserved, currently unused
  };

  // Metadata stored as the value of every LRU entry. `admitted_hot` controls
  // flush routing, while the optional drop-key acceleration data controls blob
  // GC GetKey() avoidance. They share the same user-key entry so the cache does
  // not duplicate key memory; eviction only causes a conservative miss.
  struct HotEntry {
    // Per-key upper bound on retained dropped sequences. Bounded so memory
    // stays controlled; on overflow the oldest sequence is discarded. This
    // can only cause a miss (fall back to GetKey()), never a false hit.
    static constexpr size_t kMaxDroppedSeqs = 8;

    mutable port::Mutex mu;
    bool admitted_hot = false;
    uint32_t write_count = 0;
    uint64_t last_update_epoch = 0;
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
    // Capacity (in bytes) of the recent write window FIFO set.
    size_t window_capacity = 0;
    // Capacity (in bytes) of the promoted hot-key LRU set.
    size_t hot_capacity = 0;
    // Whether repeated writes inside the window contribute to hotness.
    bool enable_write_window = true;
    // Whether compaction obsolete-version feedback contributes to hotness.
    bool enable_compaction_feedback = true;
    // Whether compaction obsolete-version feedback records exact dropped
    // sequence numbers for blob GC GetKey() short-circuiting.
    bool enable_drop_key_cache = true;
    // Number of writes within the recent window needed before a key is admitted
    // to the hot flush route. Default 2 preserves the legacy behavior: first
    // write enters the window, second write becomes hot.
    uint32_t admit_threshold = 2;
    // Simple epoch decay. Every `decay_interval` RecordWrite calls advances one
    // global epoch. 0 disables decay by default for compatibility.
    uint64_t decay_interval = 0;
    // If non-zero, an admitted hot key whose last write is older than this many
    // epochs is considered cold again and its write counter is decayed.
    uint64_t decay_window = 0;
  };

  explicit HotnessTracker(const Options& options, int num_shard_bits = 6)
      : enable_write_window_(options.enable_write_window &&
                             options.window_capacity > 0 &&
                             options.hot_capacity > 0),
        enable_compaction_feedback_(options.enable_compaction_feedback &&
                                    options.hot_capacity > 0),
        enable_drop_key_cache_(options.enable_drop_key_cache &&
                               options.hot_capacity > 0),
        admit_threshold_(std::max<uint32_t>(1, options.admit_threshold)),
        decay_interval_(options.decay_interval),
        decay_window_(options.decay_window) {
    if (enable_write_window_) {
      window_cache_ =
          NewFIFOCache(options.window_capacity, num_shard_bits, false, 0.0);
    }
    if (options.hot_capacity > 0 &&
        (enable_write_window_ || enable_compaction_feedback_ ||
         enable_drop_key_cache_)) {
      hot_cache_ =
          NewLRUCache(options.hot_capacity, num_shard_bits, false, 0.0);
    }
  }

  // Records a write of key. A repeated write inside the FIFO observation
  // window increases the key's write counter. The key is admitted into the hot
  // LRU only after the counter reaches admit_threshold_.
  void RecordWrite(const Slice& key) {
    const uint64_t epoch = AdvanceAndGetEpoch();
    if (enable_write_window_ && window_cache_ != nullptr) {
      uint32_t hash = Hash(key);
      Cache::Handle* handle =
          window_cache_->Lookup(key, hash, false /* record_hit */);
      if (handle != nullptr) {
        // Repeated write while still inside the recent window: an overwrite.
        window_cache_->Release(handle);
        RecordWindowWrite(key, epoch, true /* repeated */);
      }
      // Insert (or refresh) the key in the recent window.
      window_cache_->Insert(key, hash, nullptr, key.size(), &NoopDeleter);
    }
  }

  // Records that compaction dropped an obsolete version of key. This must only
  // be called from places where the version is known to be dead, otherwise it
  // would mistake ordinary scans for hotness.
  void RecordCompactionFeedback(const Slice& key) {
    if (!enable_compaction_feedback_) {
      return;
    }
    AdmitHotKey(key);
  }

  // Overload that, in addition to promoting the key into the hot LRU, records
  // that the separated-value version identified by (key, seq) was confirmed
  // dead by compaction. Blob GC can later short-circuit its GetKey() reverse
  // lookup when (key, seq) is found in this cache. Purely opportunistic: a
  // miss simply falls back to the legacy GetKey() path.
  void RecordCompactionFeedback(const Slice& key, SequenceNumber seq) {
    if (seq == 0) {
      return;
    }
    if (!enable_compaction_feedback_ && !enable_drop_key_cache_) {
      return;
    }
    Cache::Handle* handle =
        GetOrCreateHotEntryHandle(key, enable_compaction_feedback_);
    if (handle == nullptr) {
      return;
    }
    auto* entry = static_cast<HotEntry*>(hot_cache_->Value(handle));
    if (enable_drop_key_cache_ && entry != nullptr) {
      entry->AddDroppedSeq(seq);
    }
    hot_cache_->Release(handle);
  }

  // Returns whether the separated-value version identified by (key, seq) was
  // recorded as dropped. Both the user key and the sequence must match;
  // matching only the user key is not enough (would be a false hit). A miss
  // (including after LRU eviction) is allowed and falls back to GetKey().
  bool IsDropped(const Slice& key, SequenceNumber seq) const {
    if (!enable_drop_key_cache_ || hot_cache_ == nullptr || seq == 0) {
      return false;
    }
    uint32_t hash = Hash(key);
    Cache::Handle* handle = hot_cache_->Lookup(key, hash, false /* record_hit */);
    if (handle == nullptr) {
      return false;
    }
    auto* entry = static_cast<HotEntry*>(hot_cache_->Value(handle));
    bool dropped = entry != nullptr && entry->ContainsDroppedSeq(seq);
    hot_cache_->Release(handle);
    return dropped;
  }

  // Returns whether the key is currently admitted into the hot LRU.
  bool IsHot(const Slice& key) const {
    return HotCacheContainsAdmitted(key, false /* record_hit */);
  }

  bool DropKeyCacheEnabled() const { return enable_drop_key_cache_; }

  FlushRoute ClassifyForFlush(const Slice& key) const {
    // Flush classification should consume the hotness signal without refreshing
    // LRU recency. Hotness is advanced by writes and compaction feedback, not by
    // background flush scans.
    if (HotCacheContainsAdmitted(key, false /* record_hit */)) {
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

  bool TEST_HotCacheContains(const Slice& key) const {
    return HotCacheContainsAdmitted(key, false /* record_hit */);
  }

  bool TEST_DropKeyCacheContains(const Slice& key) const {
    return HotCacheContainsAny(key, false /* record_hit */);
  }

 private:
  static void DeleteHotEntry(const Slice& /*key*/, void* value) {
    delete static_cast<HotEntry*>(value);
  }

  // Looks up the hot entry for key, inserting a fresh one if absent, and
  // returns a referenced handle (or nullptr when the hot LRU is disabled).
  // Looking up an existing entry refreshes its LRU position and preserves any
  // dropped sequences it already holds. Caller must Release the handle.
  Cache::Handle* GetOrCreateHotEntryHandle(const Slice& key, bool admit_hot,
                                           uint32_t initial_write_count = 0) {
    if (hot_cache_ == nullptr) {
      return nullptr;
    }
    uint32_t hash = Hash(key);
    Cache::Handle* handle = hot_cache_->Lookup(key, hash, true /* record_hit */);
    if (handle != nullptr) {
      if (admit_hot) {
        auto* entry = static_cast<HotEntry*>(hot_cache_->Value(handle));
        if (entry != nullptr) {
          MutexLock l(&entry->mu);
          entry->admitted_hot = true;
          entry->write_count = std::max(entry->write_count, admit_threshold_);
          entry->last_update_epoch = CurrentEpoch();
        }
      }
      return handle;
    }
    size_t charge = key.size() + sizeof(HotEntry);
    if (charge == 0) {
      charge = 1;
    }
    HotEntry* entry = new HotEntry();
    entry->admitted_hot = admit_hot;
    entry->write_count = admit_hot ? admit_threshold_ : initial_write_count;
    entry->last_update_epoch = CurrentEpoch();
    Status s = hot_cache_->Insert(key, hash, entry, charge, &DeleteHotEntry,
                                  &handle);
    if (!s.ok() || handle == nullptr) {
      // Insert refused the entry without taking ownership (e.g. a strict
      // capacity limit). Free it ourselves to avoid leaking.
      delete entry;
      return nullptr;
    }
    return handle;
  }

  void AdmitHotKey(const Slice& key) {
    Cache::Handle* handle = GetOrCreateHotEntryHandle(key, true /* admit_hot */);
    if (handle != nullptr) {
      hot_cache_->Release(handle);
    }
  }

  uint64_t AdvanceAndGetEpoch() {
    if (decay_interval_ == 0) {
      return 0;
    }
    const uint64_t tick = write_tick_.fetch_add(1, std::memory_order_relaxed) + 1;
    return tick / decay_interval_;
  }

  uint64_t CurrentEpoch() const {
    return decay_interval_ == 0
               ? 0
               : write_tick_.load(std::memory_order_relaxed) / decay_interval_;
  }

  void RecordWindowWrite(const Slice& key, uint64_t epoch, bool repeated) {
    if (!repeated) {
      return;
    }
    // The FIFO window is the observation structure for one-hit keys.  Only a
    // repeated write is admitted into the hot LRU candidate set, so long-tail
    // cold keys do not evict truly hot keys or drop-key feedback entries.
    Cache::Handle* handle = GetOrCreateHotEntryHandle(
        key, false /* admit_hot */, 1 /* initial_write_count */);
    if (handle == nullptr) {
      return;
    }
    auto* entry = static_cast<HotEntry*>(hot_cache_->Value(handle));
    if (entry != nullptr) {
      MutexLock l(&entry->mu);
      if (IsExpiredLocked(*entry, epoch)) {
        entry->admitted_hot = false;
        entry->write_count = 0;
      }
      if (repeated && entry->write_count == 0) {
        // The first write was represented by the FIFO hit; count it when an
        // existing non-admitted entry came from drop-key feedback or was reset
        // by decay.
        entry->write_count = 1;
      }
      if (entry->write_count == 0 || repeated) {
        if (entry->write_count < admit_threshold_) {
          ++entry->write_count;
        }
      }
      entry->last_update_epoch = epoch;
      if (entry->write_count >= admit_threshold_) {
        entry->admitted_hot = true;
      }
    }
    hot_cache_->Release(handle);
  }

  bool IsExpiredLocked(const HotEntry& entry, uint64_t epoch) const {
    return decay_interval_ > 0 && decay_window_ > 0 &&
           epoch > entry.last_update_epoch &&
           epoch - entry.last_update_epoch > decay_window_;
  }

  bool HotCacheContainsAny(const Slice& key, bool record_hit) const {
    if (hot_cache_ == nullptr) {
      return false;
    }
    uint32_t hash = Hash(key);
    Cache::Handle* handle = hot_cache_->Lookup(key, hash, record_hit);
    if (handle == nullptr) {
      return false;
    }
    hot_cache_->Release(handle);
    return true;
  }

  bool HotCacheContainsAdmitted(const Slice& key, bool record_hit) const {
    if (hot_cache_ == nullptr) {
      return false;
    }
    uint32_t hash = Hash(key);
    Cache::Handle* handle = hot_cache_->Lookup(key, hash, record_hit);
    if (handle == nullptr) {
      return false;
    }
    auto* entry = static_cast<HotEntry*>(hot_cache_->Value(handle));
    bool hot = false;
    if (entry != nullptr) {
      MutexLock l(&entry->mu);
      if (IsExpiredLocked(*entry, CurrentEpoch())) {
        entry->admitted_hot = false;
        entry->write_count = 0;
      }
      hot = entry->admitted_hot;
    }
    hot_cache_->Release(handle);
    return hot;
  }

  const bool enable_write_window_;
  const bool enable_compaction_feedback_;
  const bool enable_drop_key_cache_;
  const uint32_t admit_threshold_;
  const uint64_t decay_interval_;
  const uint64_t decay_window_;
  mutable std::atomic<uint64_t> write_tick_{0};

  std::shared_ptr<Cache> window_cache_;
  mutable std::shared_ptr<Cache> hot_cache_;
};

}  // namespace TERARKDB_NAMESPACE
