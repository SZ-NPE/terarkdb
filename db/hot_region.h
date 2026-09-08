//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "db/hot_key_write_buffer.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class HotRegion {
 public:
  struct Options {
    size_t capacity = 64U << 20;
    size_t max_value_size = 1U << 20;
    size_t doorkeeper_slots = 1U << 20;
    uint32_t admission_threshold = 16;
    uint64_t rotation_interval = 1U << 20;
  };

  using BufferedWrite = HotKeyWriteBuffer::BufferedWrite;
  using BypassWriter = HotKeyWriteBuffer::BypassWriter;
  using Materializer = HotKeyWriteBuffer::Materializer;
  using MaterializeResult = HotKeyWriteBuffer::MaterializeResult;
  using PutResult = HotKeyWriteBuffer::PutResult;

  explicit HotRegion(const Options& options);

  HotRegion(const HotRegion&) = delete;
  HotRegion& operator=(const HotRegion&) = delete;

  bool Lookup(const Slice& key, SequenceNumber snapshot, uint64_t memtable_id,
              std::string* value, SequenceNumber* sequence,
              ValueType* type = nullptr) const;

  bool CanBuffer(const Slice& value) const;

  PutResult TryPut(const Slice& key, const Slice& value,
                   SequenceNumber sequence, uint64_t memtable_id,
                   bool admit_new_key, uint64_t wal_number = 0,
                   bool* found_in_region = nullptr);

  PutResult TryDelete(const Slice& key, ValueType type,
                      SequenceNumber sequence, uint64_t memtable_id,
                      uint64_t wal_number = 0,
                      bool* found_in_region = nullptr);

  MaterializeResult MaterializeKey(const Slice& key,
                                   const Materializer& materializer,
                                   BufferedWrite* materialized_write);

  bool ApplyBypassMutation(const Slice& key, SequenceNumber sequence,
                           const BypassWriter& writer);

  bool Remove(const Slice& key, BufferedWrite* write);
  void RebindMemtable(uint64_t memtable_id, SequenceNumber sequence);
  uint64_t OldestWalNumber() const;
  std::vector<BufferedWrite> GetAll();
  void PrepareAllForMaterialization();
  std::vector<BufferedWrite> GetPendingEvictions(
      size_t max_value_bytes = std::numeric_limits<size_t>::max(),
      size_t max_entries = std::numeric_limits<size_t>::max()) const;
  bool HasPendingEvictions() const;

  bool empty() const;
  size_t entry_count() const;
  size_t memory_usage() const;
  size_t capacity() const { return capacity_; }
  size_t doorkeeper_memory_usage() const;
  size_t resident_capacity() const { return resident_capacity_; }
  size_t pending_capacity() const { return pending_capacity_; }

 private:
  class RotatingDoorkeeper {
   public:
    RotatingDoorkeeper(size_t memory_bytes, uint32_t admission_threshold,
                       uint64_t rotation_interval);

    bool RecordAndShouldAdmit(const Slice& key);
    size_t memory_usage() const {
      return words_.size() * sizeof(words_[0]);
    }

   private:
    static constexpr uint64_t kCounterMask = 0xf;
    static constexpr uint64_t kCurrentMask = 0xffffffULL;
    static constexpr uint64_t kPreviousMask = 0xffffffULL << 24;
    static constexpr uint64_t kGenerationMask = 0xffffULL << 48;

    static uint16_t Generation(uint64_t word);
    static uint64_t Normalize(uint64_t word, uint16_t generation);
    static uint8_t CurrentCount(uint64_t word, size_t slot);
    static uint8_t PreviousCount(uint64_t word, size_t slot);
    static uint64_t SetCurrentCount(uint64_t word, size_t slot,
                                    uint8_t count);

    std::vector<std::atomic<uint64_t>> words_;
    const uint32_t admission_threshold_;
    const uint64_t rotation_interval_;
    std::atomic<uint64_t> reports_{0};
  };

  static size_t CalculateDoorkeeperBytes(size_t capacity,
                                         size_t requested_slots);
  static size_t CalculatePendingBytes(size_t capacity,
                                      size_t doorkeeper_bytes);

  const size_t capacity_;
  const size_t doorkeeper_bytes_;
  const size_t pending_capacity_;
  const size_t resident_capacity_;
  RotatingDoorkeeper doorkeeper_;
  HotKeyWriteBuffer write_buffer_;
};

}  // namespace TERARKDB_NAMESPACE
