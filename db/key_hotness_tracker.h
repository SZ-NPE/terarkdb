//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "util/hash.h"

namespace TERARKDB_NAMESPACE {

// Estimates per-key overwrite frequency without allocating one object per key.
// Count-Min Sketch collisions can only overestimate hotness, which may admit a
// cold key into the write buffer but cannot lose an update.
class KeyHotnessTracker {
 public:
  static constexpr size_t kRows = 4;

  struct Options {
    uint32_t sketch_columns = 1U << 20;
    uint32_t admission_threshold = 16;
    uint64_t decay_interval = 1U << 20;
  };

  explicit KeyHotnessTracker(const Options& options)
      : counters_(NormalizeColumnCount(options.sketch_columns)),
        admission_threshold_(
            std::max<uint32_t>(1, options.admission_threshold)),
        decay_interval_(std::max<uint64_t>(1, options.decay_interval)) {}

  KeyHotnessTracker(const KeyHotnessTracker&) = delete;
  KeyHotnessTracker& operator=(const KeyHotnessTracker&) = delete;

  void RecordOverwrite(const Slice& key) {
    RecordAndShouldAdmit(key);
  }

  bool RecordAndShouldAdmit(const Slice& key) {
    const uint64_t report =
        counters_.reports.fetch_add(1, std::memory_order_relaxed) + 1;
    const uint32_t generation =
        static_cast<uint32_t>(report / decay_interval_);
    std::array<std::atomic<uint64_t>*, kRows> selected_counters;
    std::array<uint32_t, kRows> values;
    uint32_t estimate = std::numeric_limits<uint32_t>::max();
    const auto& seeds = HashSeeds();
    for (size_t row = 0; row < kRows; ++row) {
      const uint32_t hash = Hash(key.data(), key.size(), seeds[row]);
      selected_counters[row] =
          &counters_.rows[row][hash & counters_.column_mask];
      values[row] = DecayedValue(
          selected_counters[row]->load(std::memory_order_relaxed),
          generation);
      estimate = std::min(estimate, values[row]);
    }

    for (size_t row = 0; row < kRows; ++row) {
      if (values[row] == estimate) {
        SaturatingIncrement(selected_counters[row], generation);
      }
    }

    return estimate == std::numeric_limits<uint32_t>::max() ||
           estimate + 1 >= admission_threshold_;
  }

  uint32_t Estimate(const Slice& key) const {
    const uint32_t generation = static_cast<uint32_t>(
        counters_.reports.load(std::memory_order_relaxed) / decay_interval_);
    uint32_t estimate = std::numeric_limits<uint32_t>::max();
    const auto& seeds = HashSeeds();
    for (size_t row = 0; row < kRows; ++row) {
      const uint32_t hash = Hash(key.data(), key.size(), seeds[row]);
      estimate = std::min(
          estimate, DecayedValue(
                        counters_.rows[row][hash & counters_.column_mask].load(
                            std::memory_order_relaxed),
                        generation));
    }
    return estimate;
  }

  bool ShouldAdmit(const Slice& key) const {
    return Estimate(key) >= admission_threshold_;
  }

 private:
  struct CounterTable {
    explicit CounterTable(uint32_t column_count)
        : columns(column_count),
          column_mask(column_count - 1),
          rows(kRows) {
      for (auto& row : rows) {
        row = std::vector<std::atomic<uint64_t>>(columns);
        for (auto& counter : row) {
          counter.store(0, std::memory_order_relaxed);
        }
      }
    }

    const uint32_t columns;
    const uint32_t column_mask;
    std::vector<std::vector<std::atomic<uint64_t>>> rows;
    std::atomic<uint64_t> reports{0};
  };

  static uint32_t NormalizeColumnCount(uint32_t columns) {
    if (columns == 0 || (columns & (columns - 1)) != 0) {
      return 1U << 20;
    }
    return columns;
  }

  static const std::array<uint32_t, kRows>& HashSeeds() {
    static const std::array<uint32_t, kRows> seeds = {
        {0x9e3779b9U, 0x85ebca6bU, 0xc2b2ae35U, 0x27d4eb2fU}};
    return seeds;
  }

  static uint64_t PackCounter(uint32_t generation, uint32_t value) {
    return (static_cast<uint64_t>(generation) << 32) | value;
  }

  static uint32_t DecayedValue(uint64_t counter, uint32_t generation) {
    const uint32_t stored_generation = static_cast<uint32_t>(counter >> 32);
    const uint32_t value = static_cast<uint32_t>(counter);
    if (stored_generation >= generation) {
      return value;
    }
    const uint32_t elapsed_generations = generation - stored_generation;
    return elapsed_generations >= 32 ? 0 : value >> elapsed_generations;
  }

  static void SaturatingIncrement(std::atomic<uint64_t>* counter,
                                  uint32_t generation) {
    uint64_t packed = counter->load(std::memory_order_relaxed);
    while (true) {
      const uint32_t stored_generation =
          static_cast<uint32_t>(packed >> 32);
      const uint32_t target_generation =
          std::max(generation, stored_generation);
      const uint32_t value = DecayedValue(packed, target_generation);
      if (value == std::numeric_limits<uint32_t>::max()) {
        return;
      }
      const uint64_t desired = PackCounter(target_generation, value + 1);
      if (counter->compare_exchange_weak(
              packed, desired, std::memory_order_relaxed,
              std::memory_order_relaxed)) {
        return;
      }
    }
  }

  CounterTable counters_;
  const uint32_t admission_threshold_;
  const uint64_t decay_interval_;
};

}  // namespace TERARKDB_NAMESPACE
