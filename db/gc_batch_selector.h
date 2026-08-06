#pragma once

#include <stdint.h>

#include <algorithm>
#include <limits>
#include <unordered_set>
#include <vector>

#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

struct GarbageCollectionReferenceCost {
  uint64_t file_number = 0;
  uint64_t scan_bytes = 0;
};

struct GarbageCollectionBatchCandidate {
  uint64_t file_number = 0;
  uint64_t reclaimable_bytes = 0;
  uint64_t live_bytes = 0;
  bool batch_eligible = false;
  bool primary_eligible = false;
  bool forced = false;
  std::vector<GarbageCollectionReferenceCost> reference_costs;
};

struct GarbageCollectionBatchSelection {
  std::vector<size_t> candidate_indexes;
  uint64_t reclaimable_bytes = 0;
  uint64_t live_bytes = 0;
  uint64_t reference_scan_bytes = 0;
};

inline bool IsGarbageCollectionStreamingEfficient(
    uint64_t candidate_entries, uint64_t reference_entries) {
  constexpr uint64_t kMaxReferenceEntryAmplification = 8;
  const uint64_t normalized_candidate_entries =
      std::max<uint64_t>(1, candidate_entries);
  return normalized_candidate_entries >
             std::numeric_limits<uint64_t>::max() /
                 kMaxReferenceEntryAmplification ||
         reference_entries <=
             normalized_candidate_entries *
                 kMaxReferenceEntryAmplification;
}

inline uint64_t SaturatingGarbageCollectionCostAdd(uint64_t left,
                                                   uint64_t right) {
  return left > std::numeric_limits<uint64_t>::max() - right
             ? std::numeric_limits<uint64_t>::max()
             : left + right;
}

inline GarbageCollectionBatchSelection SelectGarbageCollectionBatch(
    const std::vector<GarbageCollectionBatchCandidate>& candidates,
    size_t max_files, uint64_t max_live_bytes) {
  GarbageCollectionBatchSelection selection;
  if (max_files == 0 || candidates.empty()) {
    return selection;
  }

  std::vector<bool> selected(candidates.size(), false);
  std::unordered_set<uint64_t> selected_reference_files;
  while (selection.candidate_indexes.size() < max_files) {
    size_t best_index = candidates.size();
    bool best_forced = false;
    long double best_utility = -1;
    uint64_t best_reclaimable_bytes = 0;
    uint64_t best_marginal_scan_bytes = 0;

    for (size_t index = 0; index < candidates.size(); ++index) {
      if (selected[index]) {
        continue;
      }
      const auto& candidate = candidates[index];
      if (!candidate.batch_eligible) {
        continue;
      }
      if (selection.candidate_indexes.empty() &&
          !candidate.primary_eligible) {
        continue;
      }
      const bool exceeds_live_budget =
          candidate.live_bytes > max_live_bytes ||
          selection.live_bytes > max_live_bytes - candidate.live_bytes;
      if (exceeds_live_budget &&
          !(selection.candidate_indexes.empty() && candidate.forced)) {
        continue;
      }
      if (!selection.candidate_indexes.empty() &&
          !selected_reference_files.empty()) {
        bool shares_reference_scan = false;
        for (const auto& reference : candidate.reference_costs) {
          if (selected_reference_files.count(reference.file_number) != 0) {
            shares_reference_scan = true;
            break;
          }
        }
        if (!shares_reference_scan) {
          continue;
        }
      }

      uint64_t marginal_scan_bytes = 0;
      std::unordered_set<uint64_t> candidate_reference_files;
      for (const auto& reference : candidate.reference_costs) {
        if (selected_reference_files.count(reference.file_number) != 0 ||
            !candidate_reference_files.insert(reference.file_number).second) {
          continue;
        }
        if (marginal_scan_bytes >
            std::numeric_limits<uint64_t>::max() - reference.scan_bytes) {
          marginal_scan_bytes = std::numeric_limits<uint64_t>::max();
          break;
        }
        marginal_scan_bytes += reference.scan_bytes;
      }

      uint64_t marginal_cost = candidate.live_bytes;
      if (marginal_cost >
          std::numeric_limits<uint64_t>::max() - marginal_scan_bytes) {
        marginal_cost = std::numeric_limits<uint64_t>::max();
      } else {
        marginal_cost += marginal_scan_bytes;
      }
      const long double utility =
          static_cast<long double>(candidate.reclaimable_bytes) /
          std::max<uint64_t>(1, marginal_cost);
      if (!selection.candidate_indexes.empty()) {
        const uint64_t current_cost = SaturatingGarbageCollectionCostAdd(
            selection.live_bytes, selection.reference_scan_bytes);
        const uint64_t combined_reclaimable =
            SaturatingGarbageCollectionCostAdd(
                selection.reclaimable_bytes, candidate.reclaimable_bytes);
        const uint64_t combined_live = SaturatingGarbageCollectionCostAdd(
            selection.live_bytes, candidate.live_bytes);
        const uint64_t combined_scan = SaturatingGarbageCollectionCostAdd(
            selection.reference_scan_bytes, marginal_scan_bytes);
        const uint64_t combined_cost =
            SaturatingGarbageCollectionCostAdd(combined_live, combined_scan);
        const long double current_utility =
            static_cast<long double>(selection.reclaimable_bytes) /
            std::max<uint64_t>(1, current_cost);
        const long double combined_utility =
            static_cast<long double>(combined_reclaimable) /
            std::max<uint64_t>(1, combined_cost);
        if (combined_utility < current_utility) {
          continue;
        }
      }
      if (best_index == candidates.size() ||
          candidate.forced > best_forced ||
          (candidate.forced == best_forced && utility > best_utility) ||
          (candidate.forced == best_forced && utility == best_utility &&
           candidate.reclaimable_bytes > best_reclaimable_bytes) ||
          (candidate.forced == best_forced && utility == best_utility &&
           candidate.reclaimable_bytes == best_reclaimable_bytes &&
           candidate.file_number < candidates[best_index].file_number)) {
        best_index = index;
        best_forced = candidate.forced;
        best_utility = utility;
        best_reclaimable_bytes = candidate.reclaimable_bytes;
        best_marginal_scan_bytes = marginal_scan_bytes;
      }
    }

    if (best_index == candidates.size()) {
      break;
    }
    const auto& candidate = candidates[best_index];
    selected[best_index] = true;
    selection.candidate_indexes.push_back(best_index);
    selection.reclaimable_bytes = SaturatingGarbageCollectionCostAdd(
        selection.reclaimable_bytes, candidate.reclaimable_bytes);
    selection.live_bytes = SaturatingGarbageCollectionCostAdd(
        selection.live_bytes, candidate.live_bytes);
    selection.reference_scan_bytes = SaturatingGarbageCollectionCostAdd(
        selection.reference_scan_bytes, best_marginal_scan_bytes);
    for (const auto& reference : candidate.reference_costs) {
      selected_reference_files.insert(reference.file_number);
    }
  }
  return selection;
}

}  // namespace TERARKDB_NAMESPACE
