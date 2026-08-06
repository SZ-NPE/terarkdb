#pragma once

#include <stdint.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include "rocksdb/terark_namespace.h"
#include "table/internal_iterator.h"

namespace TERARKDB_NAMESPACE {

class FilePrefetchBuffer;
class RandomAccessFileReader;

class ReadaheadLaneManager final : public ReadaheadLaneProvider {
 public:
  struct Snapshot {
    size_t budget = 0;
    size_t registered_owners = 0;
    size_t active_owners = 0;
    size_t manager_owned = 0;
    size_t in_flight = 0;
    size_t total_live = 0;
    size_t peak_manager_owned = 0;
    size_t peak_in_flight = 0;
    size_t peak_total_live = 0;
    uint64_t grants = 0;
    uint64_t revocations = 0;
    uint64_t denials = 0;
  };

  explicit ReadaheadLaneManager(size_t budget);

  uint64_t RegisterOwner(Statistics* statistics,
                         size_t source_upper_bound) override;
  void ReleaseOwner(uint64_t owner) override;
  std::shared_ptr<FilePrefetchBuffer> Acquire(
      uint64_t owner, uint64_t source, RandomAccessFileReader* file_reader,
      size_t initial_readahead_size,
      size_t maximum_readahead_size) override;
  void FinishOperation(uint64_t owner, uint64_t source) override;
  void Release(uint64_t owner, uint64_t source) override;

  Snapshot GetSnapshot() const;

 private:
  struct LaneKey {
    uint64_t owner;
    uint64_t source;

    bool operator==(const LaneKey& other) const {
      return owner == other.owner && source == other.source;
    }
  };

  struct LaneKeyHash {
    size_t operator()(const LaneKey& key) const {
      return static_cast<size_t>(
          key.owner * 0x9e3779b97f4a7c15ULL ^ key.source);
    }
  };

  struct Lane {
    std::shared_ptr<FilePrefetchBuffer> buffer;
    uint64_t last_use = 0;
    uint64_t granted_clock = 0;
    size_t in_flight = 0;
  };

  struct Owner {
    Statistics* statistics = nullptr;
    size_t source_upper_bound = 0;
    bool eligibility_initialized = false;
    bool enabled = false;
  };

  size_t OwnerCapacityLocked(uint64_t owner) const;
  size_t OwnerLaneCountLocked(uint64_t owner) const;
  size_t EnabledOwnerCountLocked() const;
  void RecomputeOwnerEligibilityLocked();
  void RevokeOwnerLanesLocked(uint64_t owner, Statistics* statistics);
  bool EvictLocked(uint64_t requesting_owner);
  void UpdatePeaksLocked();

  mutable std::mutex mutex_;
  static constexpr uint64_t kMinimumLeaseOperations = 64;
  static constexpr size_t kMaximumSourceToLaneRatio = 4;
  const size_t budget_;
  uint64_t next_owner_ = 1;
  uint64_t clock_ = 0;
  std::unordered_map<uint64_t, Owner> owners_;
  std::unordered_map<LaneKey, Lane, LaneKeyHash> lanes_;
  size_t in_flight_ = 0;
  size_t orphaned_in_flight_ = 0;
  size_t peak_manager_owned_ = 0;
  size_t peak_in_flight_ = 0;
  size_t peak_total_live_ = 0;
  uint64_t grants_ = 0;
  uint64_t revocations_ = 0;
  uint64_t denials_ = 0;
};

}  // namespace TERARKDB_NAMESPACE
