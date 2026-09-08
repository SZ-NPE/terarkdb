//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/hot_region.h"

#include <atomic>
#include <limits>
#include <thread>

#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

TEST(HotRegionTest, PreservesAdmissionAndOverwriteSemantics) {
  HotRegion::Options options;
  options.capacity = 1U << 20;
  options.max_value_size = 4096;
  options.doorkeeper_slots = 16U << 10;
  options.admission_threshold = 2;
  options.rotation_interval = std::numeric_limits<uint64_t>::max();
  HotRegion region(options);

  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("hot", "first", 1, 1, false));
  EXPECT_EQ(HotRegion::PutResult::kInserted,
            region.TryPut("hot", "second", 2, 1, false));
  EXPECT_EQ(HotRegion::PutResult::kUpdatedInPlace,
            region.TryPut("hot", "latest", 3, 1, false));

  std::string value;
  SequenceNumber sequence = 0;
  ASSERT_TRUE(region.Lookup("hot", kMaxSequenceNumber, 1, &value, &sequence));
  EXPECT_EQ("latest", value);
  EXPECT_EQ(3U, sequence);
  EXPECT_EQ(1U, region.entry_count());
}

TEST(HotRegionTest, RotatesAdmissionHistory) {
  HotRegion::Options options;
  options.capacity = 1U << 20;
  options.max_value_size = 4096;
  options.doorkeeper_slots = 16U << 10;
  options.admission_threshold = 2;
  options.rotation_interval = 2;
  HotRegion region(options);

  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("old", "value", 1, 1, false));
  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("other", "value", 2, 1, false));
  EXPECT_EQ(HotRegion::PutResult::kInserted,
            region.TryPut("old", "value", 3, 1, false));

  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("expired", "value", 4, 1, false));
  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("advance-1", "value", 5, 1, false));
  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("advance-2", "value", 6, 1, false));
  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("advance-3", "value", 7, 1, false));
  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("expired", "value", 8, 1, false));
}

TEST(HotRegionTest, CountsReportsWithoutPerThreadWindowSkips) {
  HotRegion::Options options;
  options.capacity = 1U << 20;
  options.max_value_size = 4096;
  options.doorkeeper_slots = 16U << 10;
  options.admission_threshold = 3;
  options.rotation_interval = 4;
  HotRegion region(options);

  EXPECT_EQ(HotRegion::PutResult::kBypass,
            region.TryPut("hot", "first", 1, 1, false));
  HotRegion::PutResult second = HotRegion::PutResult::kInserted;
  std::thread second_writer([&]() {
    second = region.TryPut("hot", "second", 2, 1, false);
  });
  second_writer.join();
  EXPECT_EQ(HotRegion::PutResult::kBypass, second);

  HotRegion::PutResult third = HotRegion::PutResult::kBypass;
  std::thread third_writer([&]() {
    third = region.TryPut("hot", "third", 3, 1, false);
  });
  third_writer.join();
  EXPECT_EQ(HotRegion::PutResult::kInserted, third);
}

TEST(HotRegionTest, KeepsAllComponentsWithinCapacity) {
  constexpr size_t kCapacity = 1U << 20;
  HotRegion::Options options;
  options.capacity = kCapacity;
  options.max_value_size = 4096;
  options.doorkeeper_slots = 16U << 10;
  options.admission_threshold = 1;
  options.rotation_interval = 1U << 20;
  HotRegion region(options);

  const std::string value(4096, 'v');
  for (SequenceNumber sequence = 1; sequence <= 4096; ++sequence) {
    region.TryPut("key-" + std::to_string(sequence), value, sequence, 1,
                  false);
    ASSERT_LE(region.memory_usage(), region.capacity());
  }

  const size_t expected_doorkeeper_bytes =
      ((options.doorkeeper_slots + 5) / 6) *
      sizeof(std::atomic<uint64_t>);
  EXPECT_EQ(expected_doorkeeper_bytes, region.doorkeeper_memory_usage());
  EXPECT_EQ(kCapacity, region.doorkeeper_memory_usage() +
                           region.pending_capacity() +
                           region.resident_capacity());
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
