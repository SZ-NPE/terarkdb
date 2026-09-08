//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/key_hotness_tracker.h"

#include <limits>

#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

TEST(KeyHotnessTrackerTest, AdmitsOnlyAfterRepeatedOverwrites) {
  KeyHotnessTracker::Options options;
  options.sketch_columns = 1024;
  options.admission_threshold = 3;
  options.decay_interval = std::numeric_limits<uint64_t>::max();
  KeyHotnessTracker tracker(options);

  EXPECT_FALSE(tracker.ShouldAdmit("hot"));
  tracker.RecordOverwrite("hot");
  tracker.RecordOverwrite("hot");
  EXPECT_FALSE(tracker.ShouldAdmit("hot"));
  tracker.RecordOverwrite("hot");
  EXPECT_TRUE(tracker.ShouldAdmit("hot"));
  EXPECT_FALSE(tracker.ShouldAdmit("cold"));
}

TEST(KeyHotnessTrackerTest, DecaysHistoricalOverwrites) {
  KeyHotnessTracker::Options options;
  options.sketch_columns = 1024;
  options.admission_threshold = 4;
  options.decay_interval = 4;
  KeyHotnessTracker tracker(options);

  for (size_t update = 0; update < 4; ++update) {
    tracker.RecordOverwrite("hot");
  }
  EXPECT_LT(tracker.Estimate("hot"), 4U);
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
