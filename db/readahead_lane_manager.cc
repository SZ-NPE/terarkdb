#include "db/readahead_lane_manager.h"

#include <algorithm>
#include <limits>

#include "monitoring/statistics.h"
#include "util/file_reader_writer.h"

namespace TERARKDB_NAMESPACE {

ReadaheadLaneManager::ReadaheadLaneManager(size_t budget)
    : budget_(budget) {}

uint64_t ReadaheadLaneManager::RegisterOwner(Statistics* statistics,
                                             size_t source_upper_bound) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t owner = next_owner_++;
  owners_[owner].statistics = statistics;
  owners_[owner].source_upper_bound = source_upper_bound;
  RecomputeOwnerEligibilityLocked();
  return owner;
}

void ReadaheadLaneManager::ReleaseOwner(uint64_t owner) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto owner_find = owners_.find(owner);
  Statistics* statistics =
      owner_find == owners_.end() ? nullptr : owner_find->second.statistics;
  RevokeOwnerLanesLocked(owner, statistics);
  owners_.erase(owner);
  RecomputeOwnerEligibilityLocked();
  UpdatePeaksLocked();
}

size_t ReadaheadLaneManager::OwnerCapacityLocked(uint64_t owner) const {
  auto find = owners_.find(owner);
  if (budget_ == 0 || find == owners_.end() || !find->second.enabled) {
    return 0;
  }
  const size_t active_owners = EnabledOwnerCountLocked();
  assert(active_owners != 0);
  return std::max<size_t>(
      1, (budget_ + active_owners - 1) / active_owners);
}

size_t ReadaheadLaneManager::OwnerLaneCountLocked(uint64_t owner) const {
  size_t count = 0;
  for (const auto& entry : lanes_) {
    if (entry.first.owner == owner) {
      ++count;
    }
  }
  return count;
}

size_t ReadaheadLaneManager::EnabledOwnerCountLocked() const {
  size_t count = 0;
  for (const auto& owner : owners_) {
    if (owner.second.enabled) {
      ++count;
    }
  }
  return count;
}

void ReadaheadLaneManager::RevokeOwnerLanesLocked(
    uint64_t owner, Statistics* statistics) {
  for (auto iterator = lanes_.begin(); iterator != lanes_.end();) {
    if (iterator->first.owner != owner) {
      ++iterator;
      continue;
    }
    if (iterator->second.in_flight != 0) {
      orphaned_in_flight_ += iterator->second.in_flight;
    }
    iterator = lanes_.erase(iterator);
    ++revocations_;
    RecordTick(statistics, SCAN_FUSE_LANE_REVOCATION);
  }
}

void ReadaheadLaneManager::RecomputeOwnerEligibilityLocked() {
  size_t source_upper_bound = 0;
  for (const auto& owner : owners_) {
    source_upper_bound += owner.second.source_upper_bound;
  }
  const bool enabled =
      budget_ != 0 &&
      source_upper_bound <= budget_ * kMaximumSourceToLaneRatio;
  for (auto& owner : owners_) {
    if (owner.second.eligibility_initialized &&
        owner.second.enabled == enabled) {
      continue;
    }
    owner.second.eligibility_initialized = true;
    owner.second.enabled = enabled;
    if (!enabled) {
      RecordTick(owner.second.statistics, SCAN_FUSE_LANE_SUPPRESSION);
      RevokeOwnerLanesLocked(owner.first, owner.second.statistics);
    }
  }
}

bool ReadaheadLaneManager::EvictLocked(uint64_t requesting_owner) {
  auto victim = lanes_.end();
  uint64_t oldest = std::numeric_limits<uint64_t>::max();
  const size_t requesting_count = OwnerLaneCountLocked(requesting_owner);
  const size_t requesting_capacity = OwnerCapacityLocked(requesting_owner);
  if (requesting_count >= requesting_capacity) {
    return false;
  }
  for (auto iterator = lanes_.begin(); iterator != lanes_.end(); ++iterator) {
    if (iterator->first.owner == requesting_owner ||
        iterator->second.in_flight != 0) {
      continue;
    }
    const size_t owner_count = OwnerLaneCountLocked(iterator->first.owner);
    const size_t owner_capacity = OwnerCapacityLocked(iterator->first.owner);
    const bool victim_is_over_capacity = owner_count > owner_capacity;
    const bool lease_is_old =
        clock_ - iterator->second.granted_clock >= kMinimumLeaseOperations;
    if (!victim_is_over_capacity && !lease_is_old) {
      continue;
    }
    if (iterator->second.last_use < oldest) {
      oldest = iterator->second.last_use;
      victim = iterator;
    }
  }
  if (victim == lanes_.end()) {
    return false;
  }
  Statistics* statistics = owners_[victim->first.owner].statistics;
  lanes_.erase(victim);
  ++revocations_;
  RecordTick(statistics, SCAN_FUSE_LANE_REVOCATION);
  return true;
}

std::shared_ptr<FilePrefetchBuffer> ReadaheadLaneManager::Acquire(
    uint64_t owner, uint64_t source, RandomAccessFileReader* file_reader,
    size_t initial_readahead_size, size_t maximum_readahead_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++clock_;
  auto owner_find = owners_.find(owner);
  if (budget_ == 0 || owner_find == owners_.end() ||
      !owner_find->second.enabled) {
    ++denials_;
    RecordTick(owner_find == owners_.end() ? nullptr
                                          : owner_find->second.statistics,
               SCAN_FUSE_LANE_DENIAL);
    return nullptr;
  }
  const LaneKey key{owner, source};
  auto find = lanes_.find(key);
  if (find == lanes_.end()) {
    const size_t owner_capacity = OwnerCapacityLocked(owner);
    const bool owner_is_full =
        OwnerLaneCountLocked(owner) >= owner_capacity;
    if (owner_is_full ||
        (lanes_.size() == budget_ && !EvictLocked(owner))) {
      ++denials_;
      RecordTick(owners_[owner].statistics, SCAN_FUSE_LANE_DENIAL);
      return nullptr;
    }
    Lane lane;
    lane.buffer = std::make_shared<FilePrefetchBuffer>(
        file_reader, initial_readahead_size, maximum_readahead_size);
    lane.granted_clock = clock_;
    find = lanes_.emplace(key, std::move(lane)).first;
    ++grants_;
    RecordTick(owners_[owner].statistics, SCAN_FUSE_LANE_GRANT);
  }
  find->second.last_use = clock_;
  ++find->second.in_flight;
  ++in_flight_;
  const size_t previous_manager_peak = peak_manager_owned_;
  const size_t previous_in_flight_peak = peak_in_flight_;
  const size_t previous_total_peak = peak_total_live_;
  UpdatePeaksLocked();
  Statistics* statistics = owners_[owner].statistics;
  if (peak_manager_owned_ > previous_manager_peak) {
    RecordTick(statistics, SCAN_FUSE_LANE_MANAGER_OWNED_PEAK,
               peak_manager_owned_ - previous_manager_peak);
  }
  if (peak_in_flight_ > previous_in_flight_peak) {
    RecordTick(statistics, SCAN_FUSE_LANE_IN_FLIGHT_PEAK,
               peak_in_flight_ - previous_in_flight_peak);
  }
  if (peak_total_live_ > previous_total_peak) {
    RecordTick(statistics, SCAN_FUSE_LANE_TOTAL_LIVE_PEAK,
               peak_total_live_ - previous_total_peak);
  }
  return find->second.buffer;
}

void ReadaheadLaneManager::FinishOperation(uint64_t owner, uint64_t source) {
  std::lock_guard<std::mutex> lock(mutex_);
  const LaneKey key{owner, source};
  auto find = lanes_.find(key);
  if (find != lanes_.end() && find->second.in_flight != 0) {
    --find->second.in_flight;
  } else if (orphaned_in_flight_ != 0) {
    --orphaned_in_flight_;
  }
  if (in_flight_ != 0) {
    --in_flight_;
  }
}

void ReadaheadLaneManager::Release(uint64_t owner, uint64_t source) {
  std::lock_guard<std::mutex> lock(mutex_);
  const LaneKey key{owner, source};
  auto find = lanes_.find(key);
  if (find != lanes_.end()) {
    if (find->second.in_flight != 0) {
      orphaned_in_flight_ += find->second.in_flight;
    }
    lanes_.erase(find);
    ++revocations_;
    auto owner_find = owners_.find(owner);
    RecordTick(owner_find == owners_.end() ? nullptr
                                          : owner_find->second.statistics,
               SCAN_FUSE_LANE_REVOCATION);
  }
}

void ReadaheadLaneManager::UpdatePeaksLocked() {
  peak_manager_owned_ = std::max(peak_manager_owned_, lanes_.size());
  peak_in_flight_ = std::max(peak_in_flight_, in_flight_);
  peak_total_live_ =
      std::max(peak_total_live_, lanes_.size() + orphaned_in_flight_);
}

ReadaheadLaneManager::Snapshot ReadaheadLaneManager::GetSnapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  Snapshot snapshot;
  snapshot.budget = budget_;
  snapshot.registered_owners = owners_.size();
  snapshot.active_owners = EnabledOwnerCountLocked();
  snapshot.manager_owned = lanes_.size();
  snapshot.in_flight = in_flight_;
  snapshot.total_live = lanes_.size() + orphaned_in_flight_;
  snapshot.peak_manager_owned = peak_manager_owned_;
  snapshot.peak_in_flight = peak_in_flight_;
  snapshot.peak_total_live = peak_total_live_;
  snapshot.grants = grants_;
  snapshot.revocations = revocations_;
  snapshot.denials = denials_;
  return snapshot;
}

}  // namespace TERARKDB_NAMESPACE
