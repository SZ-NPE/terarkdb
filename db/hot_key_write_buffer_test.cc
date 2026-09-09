//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/hot_key_write_buffer.h"

#include <algorithm>
#include <thread>
#include <vector>

#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

HotKeyWriteBuffer::Options MakeWriteBufferOptions(size_t capacity = 1024) {
  HotKeyWriteBuffer::Options options;
  options.capacity = capacity;
  options.max_value_size = 512;
  options.max_pending_memory = capacity;
  return options;
}

TEST(HotKeyWriteBufferTest, CollapsesRepeatedOverwrites) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));

  EXPECT_STREQ("LRUCache", buffer.cache_name());
  EXPECT_EQ(HotKeyWriteBuffer::PutResult::kBypass,
            buffer.TryPut("key", "first", 1, 7, false));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("key", "second", 2, 7, true));
  for (SequenceNumber sequence = 3; sequence <= 11; ++sequence) {
    ASSERT_EQ(HotKeyWriteBuffer::PutResult::kUpdatedInPlace,
              buffer.TryPut("key", std::to_string(sequence), sequence, 7,
                            false));
  }

  EXPECT_EQ(1U, buffer.entry_count());
  std::string value;
  SequenceNumber sequence = 0;
  ASSERT_TRUE(buffer.Lookup("key", 11, 7, &value, &sequence));
  EXPECT_FALSE(buffer.Lookup("key", 11, 8, &value, &sequence));
  EXPECT_EQ("11", value);
  EXPECT_EQ(11U, sequence);

  auto writes = buffer.GetAll();
  ASSERT_EQ(1U, writes.size());
  EXPECT_EQ("key", writes[0].key);
  EXPECT_EQ("11", writes[0].value);
  EXPECT_EQ(11U, writes[0].sequence);
  HotKeyWriteBuffer::BufferedWrite removed;
  ASSERT_TRUE(buffer.Remove("key", &removed));
  EXPECT_TRUE(buffer.empty());
}

TEST(HotKeyWriteBufferTest, RequiresExplicitAdmissionForNewKey) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));

  EXPECT_EQ(HotKeyWriteBuffer::PutResult::kBypass,
            buffer.TryPut("key", "first", 1, 7, false));
  EXPECT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("key", "second", 2, 7, true));
  EXPECT_EQ(1U, buffer.entry_count());
}

TEST(HotKeyWriteBufferTest, BypassesAbsentKeyWithoutRetainingMembership) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  size_t writes = 0;

  ASSERT_TRUE(buffer.ApplyBypassMutation(
      "cold-key", 1, [&writes]() {
        ++writes;
        return true;
      }));
  EXPECT_EQ(1U, writes);
  EXPECT_FALSE(buffer.IsKeyMaybePresent("cold-key"));
  EXPECT_TRUE(buffer.empty());
}

TEST(HotKeyWriteBufferTest, RemovesMembershipWithResidentEntry) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("key", "value", 1, 7, true));
  ASSERT_TRUE(buffer.IsKeyMaybePresent("key"));

  ASSERT_TRUE(buffer.Remove("key", nullptr));
  EXPECT_FALSE(buffer.IsKeyMaybePresent("key"));
}

TEST(HotKeyWriteBufferTest, RemoveReturnsNewestVersion) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("key", "first", 1, 7, true));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kUpdatedInPlace,
            buffer.TryPut("key", "second", 2, 7, false));

  HotKeyWriteBuffer::BufferedWrite write;
  ASSERT_TRUE(buffer.Remove("key", &write));
  EXPECT_EQ("second", write.value);
  EXPECT_EQ(2U, write.sequence);
  EXPECT_TRUE(buffer.empty());
}

TEST(HotKeyWriteBufferTest, KeepsMaximumConcurrentSequence) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("key", "0", 1, 7, true));

  constexpr size_t kThreadCount = 8;
  constexpr size_t kWritesPerThread = 1000;
  std::vector<std::thread> threads;
  for (size_t thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&buffer, thread]() {
      for (size_t write = 0; write < kWritesPerThread; ++write) {
        const SequenceNumber sequence =
            thread * kWritesPerThread + write + 2;
        buffer.TryPut("key", std::to_string(sequence), sequence, 7, false);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  std::string value;
  SequenceNumber sequence = 0;
  ASSERT_TRUE(
      buffer.Lookup("key", kMaxSequenceNumber, 7, &value, &sequence));
  EXPECT_EQ(kThreadCount * kWritesPerThread + 1, sequence);
  EXPECT_EQ(std::to_string(sequence), value);
}

TEST(HotKeyWriteBufferTest, StoresLatestPutOrDeleteMutation) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("key", "value", 1, 7, true));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kUpdatedInPlace,
            buffer.TryDelete("key", kTypeDeletion, 2, 7));

  std::string value;
  SequenceNumber sequence = 0;
  ValueType type = kTypeValue;
  ASSERT_TRUE(buffer.Lookup("key", kMaxSequenceNumber, 7, &value,
                            &sequence, &type));
  EXPECT_TRUE(value.empty());
  EXPECT_EQ(kTypeDeletion, type);
  EXPECT_EQ(2U, sequence);

  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kUpdatedInPlace,
            buffer.TryPut("key", "new-value", 3, 7, false));
  ASSERT_TRUE(buffer.Lookup("key", kMaxSequenceNumber, 7, &value,
                            &sequence, &type));
  EXPECT_EQ("new-value", value);
  EXPECT_EQ(kTypeValue, type);
  EXPECT_EQ(3U, sequence);
}

TEST(HotKeyWriteBufferTest, SerializesConcurrentPutAndDeleteBySequence) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("key", "initial", 1, 7, true));

  constexpr size_t kThreadCount = 8;
  constexpr size_t kWritesPerThread = 1000;
  std::vector<std::thread> threads;
  for (size_t thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&buffer, thread]() {
      for (size_t write = 0; write < kWritesPerThread; ++write) {
        const SequenceNumber sequence =
            thread * kWritesPerThread + write + 2;
        if (sequence % 2 == 0) {
          buffer.TryDelete("key", kTypeDeletion, sequence, 7);
        } else {
          buffer.TryPut("key", std::to_string(sequence), sequence, 7, false);
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  std::string value;
  SequenceNumber sequence = 0;
  ValueType type = kTypeValue;
  ASSERT_TRUE(buffer.Lookup("key", kMaxSequenceNumber, 7, &value,
                            &sequence, &type));
  EXPECT_EQ(kThreadCount * kWritesPerThread + 1, sequence);
  EXPECT_EQ(kTypeValue, type);
  EXPECT_EQ(std::to_string(sequence), value);
}

TEST(HotKeyWriteBufferTest, LargerValueReplacesEntryWithoutMaterializingOld) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("key", "small", 1, 7, true));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kReplaced,
            buffer.TryPut("key", std::string(64, 'x'), 2, 7, false));
  EXPECT_FALSE(buffer.HasPendingEvictions());

  std::string value;
  SequenceNumber sequence = 0;
  ASSERT_TRUE(
      buffer.Lookup("key", kMaxSequenceNumber, 7, &value, &sequence));
  EXPECT_EQ(std::string(64, 'x'), value);
  EXPECT_EQ(2U, sequence);
}

TEST(HotKeyWriteBufferTest, LruEvictsColdEntryAndRetainsRecentEntry) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  const std::string value_bytes(64, 'v');
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("cold", value_bytes, 1, 7, true));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("hot", value_bytes, 2, 7, true));

  std::string value;
  SequenceNumber sequence = 3;
  while (!buffer.HasPendingEvictions()) {
    ASSERT_TRUE(buffer.Lookup("hot", kMaxSequenceNumber, 7, &value, nullptr));
    ASSERT_EQ(
        HotKeyWriteBuffer::PutResult::kInserted,
        buffer.TryPut("new-" + std::to_string(sequence),
                      value_bytes, sequence, 7, true));
    ++sequence;
    ASSERT_LT(sequence, 10000U);
  }

  const auto evicted = buffer.GetPendingEvictions();
  ASSERT_FALSE(evicted.empty());
  EXPECT_TRUE(buffer.Lookup("hot", kMaxSequenceNumber, 7, &value, nullptr));
  EXPECT_TRUE(buffer.Lookup("new-" + std::to_string(sequence - 1),
                            kMaxSequenceNumber, 7, &value, nullptr));
  EXPECT_TRUE(std::any_of(
      evicted.begin(), evicted.end(),
      [](const HotKeyWriteBuffer::BufferedWrite& write) {
        return write.key == "cold";
      }));
  for (const auto& write : evicted) {
    EXPECT_EQ(
        HotKeyWriteBuffer::MaterializeResult::kMaterialized,
        buffer.MaterializeKey(
            write.key,
            [](const HotKeyWriteBuffer::BufferedWrite&) { return true; },
            nullptr));
  }
  EXPECT_FALSE(buffer.HasPendingEvictions());
}

TEST(HotKeyWriteBufferTest, UpdatesEvictingEntryBeforeMaterialization) {
  auto options = MakeWriteBufferOptions(16U << 10);
  options.max_pending_memory = 16U << 10;
  HotKeyWriteBuffer buffer(options);
  const std::string value(512, 'v');
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("target", value, 1, 7, true));

  SequenceNumber sequence = 2;
  while (!buffer.HasPendingEvictions()) {
    ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
              buffer.TryPut("key-" + std::to_string(sequence), value,
                            sequence, 7, true));
    ++sequence;
    ASSERT_LT(sequence, 1000U);
  }

  const std::string latest(512, 'n');
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kUpdatedInPlace,
            buffer.TryPut("target", latest, sequence, 7, false));

  HotKeyWriteBuffer::BufferedWrite materialized;
  ASSERT_EQ(
      HotKeyWriteBuffer::MaterializeResult::kMaterialized,
      buffer.MaterializeKey(
          "target",
          [&](const HotKeyWriteBuffer::BufferedWrite& mutation) {
            materialized = mutation;
            return true;
          },
          nullptr));
  EXPECT_EQ(latest, materialized.value);
  EXPECT_EQ(sequence, materialized.sequence);
}

TEST(HotKeyWriteBufferTest, PreparesResidentsWithinPendingBudget) {
  auto options = MakeWriteBufferOptions(4U << 20);
  options.max_value_size = 4096;
  options.max_pending_memory = 64U << 10;
  options.max_pending_entry_memory = 32U << 10;
  HotKeyWriteBuffer buffer(options);
  const std::string value(4096, 'v');
  constexpr SequenceNumber kEntryCount = 256;
  for (SequenceNumber sequence = 1; sequence <= kEntryCount; ++sequence) {
    ASSERT_EQ(
        HotKeyWriteBuffer::PutResult::kInserted,
        buffer.TryPut("key-" + std::to_string(sequence), value, sequence, 7,
                      true));
  }

  size_t batches = 0;
  while (!buffer.empty()) {
    buffer.PrepareAllForMaterialization();
    const auto pending = buffer.GetPendingEvictions();
    ASSERT_FALSE(pending.empty());
    ASSERT_LE(pending.size(), 16U);
    for (const auto& write : pending) {
      ASSERT_EQ(
          HotKeyWriteBuffer::MaterializeResult::kMaterialized,
          buffer.MaterializeKey(
              write.key,
              [](const HotKeyWriteBuffer::BufferedWrite&) { return true; },
              nullptr));
    }
    ++batches;
  }
  EXPECT_GT(batches, 1U);
}

TEST(HotKeyWriteBufferTest, EvictsAcrossShardsWithoutGlobalSerialization) {
  auto options = MakeWriteBufferOptions(1U << 20);
  options.max_value_size = 1024;
  options.max_pending_memory = 1U << 20;
  options.max_pending_entry_memory = 512U << 10;
  HotKeyWriteBuffer buffer(options);

  constexpr size_t kThreadCount = 16;
  constexpr size_t kWritesPerThread = 1000;
  std::vector<std::thread> threads;
  for (size_t thread = 0; thread < kThreadCount; ++thread) {
    threads.emplace_back([&, thread] {
      const std::string value(1024, static_cast<char>(thread + 1));
      for (size_t write = 0; write < kWritesPerThread; ++write) {
        const SequenceNumber sequence =
            thread * kWritesPerThread + write + 1;
        buffer.TryPut("key-" + std::to_string(thread) + "-" +
                          std::to_string(write),
                      value, sequence, 7, true);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_TRUE(buffer.HasPendingEvictions());
  size_t materialized = 0;
  while (buffer.HasPendingEvictions()) {
    const auto pending = buffer.GetPendingEvictions(64U << 10, 128);
    ASSERT_FALSE(pending.empty());
    for (const auto& write : pending) {
      ASSERT_EQ(
          HotKeyWriteBuffer::MaterializeResult::kMaterialized,
          buffer.MaterializeKey(
              write.key,
              [](const HotKeyWriteBuffer::BufferedWrite&) { return true; },
              nullptr));
      ++materialized;
    }
  }
  EXPECT_GT(materialized, 0U);

  const auto residents = buffer.GetAll();
  for (const auto& resident : residents) {
    ASSERT_TRUE(buffer.Remove(resident.key, nullptr));
  }
  EXPECT_TRUE(buffer.empty());
}

TEST(HotKeyWriteBufferTest, RebindsMemtableAndTracksOldestHotWal) {
  HotKeyWriteBuffer buffer(MakeWriteBufferOptions(1U << 20));
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("first", "value", 1, 7, true, 11));
  ASSERT_EQ(11U, buffer.OldestHotWalNumber());
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kUpdatedInPlace,
            buffer.TryPut("first", "new", 2, 7, false, 12));
  ASSERT_EQ(12U, buffer.OldestHotWalNumber());
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kInserted,
            buffer.TryPut("second", "value", 3, 7, true, 10));
  ASSERT_EQ(10U, buffer.OldestHotWalNumber());

  buffer.RebindMemtable(8, 4);
  std::string value;
  SequenceNumber sequence = 0;
  EXPECT_FALSE(
      buffer.Lookup("first", kMaxSequenceNumber, 7, &value, nullptr));
  EXPECT_TRUE(
      buffer.Lookup("first", kMaxSequenceNumber, 8, &value, &sequence));
  EXPECT_EQ(4U, sequence);
  ASSERT_EQ(HotKeyWriteBuffer::PutResult::kUpdatedInPlace,
            buffer.TryPut("first", "latest", 5, 8, false, 13));
  const auto residents = buffer.GetAll();
  ASSERT_EQ(2U, residents.size());
  EXPECT_TRUE(buffer.Lookup("second", kMaxSequenceNumber, 8, &value,
                            &sequence));
  EXPECT_EQ(4U, sequence);

  ASSERT_TRUE(buffer.Remove("second", nullptr));
  EXPECT_EQ(13U, buffer.OldestHotWalNumber());
  buffer.RebindMemtable(9, 6);
  ASSERT_TRUE(buffer.Remove("first", nullptr));
  EXPECT_EQ(0U, buffer.OldestHotWalNumber());
}

TEST(HotKeyWriteBufferTest, ChargesRepeatedValuesPerEntry) {
  auto options = MakeWriteBufferOptions(1U << 20);
  options.max_value_size = 4096;
  HotKeyWriteBuffer buffer(options);
  const std::string shared_value(4096, 'a');
  constexpr SequenceNumber kEntryCount = 100;
  for (SequenceNumber sequence = 1; sequence <= kEntryCount; ++sequence) {
    ASSERT_EQ(
        HotKeyWriteBuffer::PutResult::kInserted,
        buffer.TryPut("key-" + std::to_string(sequence), shared_value,
                      sequence, 7, true));
  }

  EXPECT_EQ(kEntryCount, buffer.entry_count());
  EXPECT_GT(buffer.memory_usage(), shared_value.size() * kEntryCount);

  std::string value;
  ASSERT_TRUE(buffer.Lookup("key-42", kMaxSequenceNumber, 7, &value,
                            nullptr));
  EXPECT_EQ(shared_value, value);
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
