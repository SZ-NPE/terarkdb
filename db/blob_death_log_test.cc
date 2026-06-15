//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob_death_log.h"

#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class BlobDeathLogTest : public testing::Test {};

namespace {
ValueLocation MakeLoc(uint64_t fn, uint64_t block, uint64_t slot) {
  ValueLocation loc;
  loc.file_number = fn;
  loc.layout_id = fn;  // physical == logical for these tests
  loc.block_id = block;
  loc.slot_id = slot;
  return loc;
}
}  // namespace

// RecordDeath is idempotent: recording the same location twice counts once.
TEST_F(BlobDeathLogTest, RecordDeathIdempotent) {
  BlobDeathLog log(64 << 20);
  log.OnVsstCreated(10);
  log.RecordDeath(MakeLoc(10, 0, 0));
  log.RecordDeath(MakeLoc(10, 0, 0));  // duplicate
  log.RecordDeath(MakeLoc(10, 0, 1));
  ASSERT_EQ(2u, log.records_emitted());

  BlobDeathMap m = log.BuildDeathMap(10);
  ASSERT_TRUE(m.complete);
  ASSERT_TRUE(m.IsValueDead(0, 0));
  ASSERT_TRUE(m.IsValueDead(0, 1));
  ASSERT_FALSE(m.IsValueDead(0, 2));
  ASSERT_EQ(2u, m.dead_count_per_block[0]);
}

// A vSST never created in this process yields an incomplete (empty) map,
// so GC conservatively falls back.
TEST_F(BlobDeathLogTest, UntrackedVsstIsIncomplete) {
  BlobDeathLog log(64 << 20);
  log.RecordDeath(MakeLoc(20, 1, 0));  // recorded but never OnVsstCreated
  BlobDeathMap m = log.BuildDeathMap(20);
  ASSERT_FALSE(m.complete);
  // The dead record is still present (recording does not require created).
  ASSERT_TRUE(m.IsValueDead(1, 0));

  BlobDeathMap missing = log.BuildDeathMap(999);
  ASSERT_FALSE(missing.complete);
  ASSERT_TRUE(missing.dead_slots.empty());
}

// Invalid locations (legacy/v1 without slot) are never recorded.
TEST_F(BlobDeathLogTest, InvalidLocationIgnored) {
  BlobDeathLog log(64 << 20);
  log.OnVsstCreated(40);
  ValueLocation bad;  // all sentinels -> !valid()
  log.RecordDeath(bad);
  ASSERT_EQ(0u, log.records_emitted());
  BlobDeathMap m = log.BuildDeathMap(40);
  ASSERT_TRUE(m.dead_slots.empty());
}

// OnVsstObsolete drops the per-vSST state.
TEST_F(BlobDeathLogTest, ObsoleteDropsState) {
  BlobDeathLog log(64 << 20);
  log.OnVsstCreated(50);
  log.RecordDeath(MakeLoc(50, 0, 0));
  ASSERT_TRUE(log.BuildDeathMap(50).IsValueDead(0, 0));
  log.OnVsstObsolete(50);
  BlobDeathMap m = log.BuildDeathMap(50);
  ASSERT_FALSE(m.complete);
  ASSERT_TRUE(m.dead_slots.empty());
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
