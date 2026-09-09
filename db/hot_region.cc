//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/hot_region.h"

#include <algorithm>

#include "util/hash.h"

namespace TERARKDB_NAMESPACE {

namespace {

constexpr size_t kMaxPendingBytes = 16U << 20;
constexpr size_t kDoorkeeperSlotsPerWord = 6;

HotKeyWriteBuffer::Options MakeWriteBufferOptions(
    size_t resident_capacity, size_t pending_capacity, size_t max_value_size) {
  HotKeyWriteBuffer::Options buffer_options;
  buffer_options.capacity = resident_capacity;
  buffer_options.max_value_size = max_value_size;
  buffer_options.max_pending_memory = pending_capacity;
  buffer_options.max_pending_entry_memory =
      std::max<size_t>(1, pending_capacity / 2);
  return buffer_options;
}

}  // namespace

HotRegion::HotRegion(const Options& options)
    : capacity_(std::max<size_t>(1, options.capacity)),
      doorkeeper_bytes_(
          CalculateDoorkeeperBytes(capacity_, options.doorkeeper_slots)),
      pending_capacity_(
          CalculatePendingBytes(capacity_, doorkeeper_bytes_)),
      resident_capacity_(
          std::max<size_t>(1, capacity_ - doorkeeper_bytes_ -
                                 pending_capacity_)),
      doorkeeper_(doorkeeper_bytes_, options.admission_threshold,
                  options.rotation_interval),
      write_buffer_(MakeWriteBufferOptions(
          resident_capacity_, pending_capacity_, options.max_value_size)) {}

HotRegion::RotatingDoorkeeper::RotatingDoorkeeper(
    size_t memory_bytes, uint32_t admission_threshold,
    uint64_t rotation_interval)
    : words_(std::max<size_t>(1, memory_bytes / sizeof(words_[0]))),
      admission_threshold_(
          std::min<uint32_t>(16, std::max<uint32_t>(1, admission_threshold))),
      rotation_interval_(std::max<uint64_t>(1, rotation_interval)) {
  for (auto& word : words_) {
    word.store(0, std::memory_order_relaxed);
  }
}

uint16_t HotRegion::RotatingDoorkeeper::Generation(uint64_t word) {
  return static_cast<uint16_t>((word & kGenerationMask) >> 48);
}

uint64_t HotRegion::RotatingDoorkeeper::Normalize(uint64_t word,
                                                  uint16_t generation) {
  const uint16_t stored_generation = Generation(word);
  const int16_t elapsed =
      static_cast<int16_t>(generation - stored_generation);
  if (elapsed <= 0) {
    return word;
  }

  uint64_t previous = 0;
  if (elapsed == 1) {
    previous = (word & kCurrentMask) << 24;
  }
  return previous | (static_cast<uint64_t>(generation) << 48);
}

uint8_t HotRegion::RotatingDoorkeeper::CurrentCount(uint64_t word,
                                                    size_t slot) {
  return static_cast<uint8_t>(
      (word >> (slot * 4)) & kCounterMask);
}

uint8_t HotRegion::RotatingDoorkeeper::PreviousCount(uint64_t word,
                                                     size_t slot) {
  return static_cast<uint8_t>(
      (word >> (24 + slot * 4)) & kCounterMask);
}

uint64_t HotRegion::RotatingDoorkeeper::SetCurrentCount(
    uint64_t word, size_t slot, uint8_t count) {
  const size_t shift = slot * 4;
  word &= ~(kCounterMask << shift);
  return word | (static_cast<uint64_t>(count) << shift);
}

bool HotRegion::RotatingDoorkeeper::RecordAndShouldAdmit(
    const Slice& key) {
  if (admission_threshold_ == 1) {
    return true;
  }

  const uint64_t report = reports_.fetch_add(1, std::memory_order_relaxed);
  const uint16_t generation =
      static_cast<uint16_t>(report / rotation_interval_);
  const uint32_t hash = Hash(key.data(), key.size(), 0x85ebca6bU);
  const size_t slot = hash % kDoorkeeperSlotsPerWord;
  std::atomic<uint64_t>& target =
      words_[(hash / kDoorkeeperSlotsPerWord) % words_.size()];

  uint64_t observed = target.load(std::memory_order_relaxed);
  while (true) {
    const uint64_t normalized = Normalize(observed, generation);
    const uint8_t current = CurrentCount(normalized, slot);
    const uint32_t estimate = current + PreviousCount(normalized, slot);
    const uint8_t next =
        static_cast<uint8_t>(std::min<uint32_t>(15, current + 1));
    const uint64_t desired = SetCurrentCount(normalized, slot, next);
    if (target.compare_exchange_weak(
            observed, desired, std::memory_order_relaxed,
            std::memory_order_relaxed)) {
      return estimate + 1 >= admission_threshold_;
    }
  }
}

size_t HotRegion::CalculateDoorkeeperBytes(size_t capacity,
                                           size_t requested_slots) {
  const size_t requested_words =
      (std::max<size_t>(1, requested_slots) + kDoorkeeperSlotsPerWord - 1) /
      kDoorkeeperSlotsPerWord;
  const size_t requested_bytes =
      requested_words * sizeof(std::atomic<uint64_t>);
  return std::min(requested_bytes, std::max<size_t>(1, capacity / 8));
}

size_t HotRegion::CalculatePendingBytes(size_t capacity,
                                        size_t doorkeeper_bytes) {
  const size_t available = capacity - doorkeeper_bytes;
  if (available < kMaxPendingBytes * 4) {
    return available / 2;
  }
  return std::min(kMaxPendingBytes, available / 8);
}

bool HotRegion::Lookup(const Slice& key, SequenceNumber snapshot,
                       uint64_t memtable_id, std::string* value,
                       SequenceNumber* sequence, ValueType* type) const {
  return write_buffer_.Lookup(
      key, snapshot, memtable_id, value, sequence, type);
}

bool HotRegion::CanBuffer(const Slice& value) const {
  return write_buffer_.CanBuffer(value);
}

bool HotRegion::ShouldRoutePutToHotWal(const Slice& key, const Slice& value,
                                       uint64_t memtable_id) {
  if (!CanBuffer(value)) {
    return false;
  }
  return write_buffer_.Contains(key, memtable_id) ||
         doorkeeper_.RecordAndShouldAdmit(key);
}

bool HotRegion::ShouldRouteDeleteToHotWal(const Slice& key,
                                          uint64_t memtable_id) const {
  return write_buffer_.Contains(key, memtable_id);
}

HotRegion::PutResult HotRegion::TryPut(const Slice& key, const Slice& value,
                                       SequenceNumber sequence,
                                       uint64_t memtable_id,
                                       bool admit_new_key, uint64_t wal_number,
                                       bool* found_in_region,
                                       bool pre_admitted) {
  admit_new_key = admit_new_key || pre_admitted;
  if (!admit_new_key && !write_buffer_.IsKeyMaybePresent(key)) {
    if (!doorkeeper_.RecordAndShouldAdmit(key)) {
      return PutResult::kBypass;
    }
    admit_new_key = true;
  }

  bool found = false;
  PutResult result = write_buffer_.TryPut(
      key, value, sequence, memtable_id, admit_new_key, wal_number, &found);
  if (result == PutResult::kBypass && !found && !admit_new_key &&
      doorkeeper_.RecordAndShouldAdmit(key)) {
    result = write_buffer_.TryPut(
        key, value, sequence, memtable_id, true, wal_number, &found);
  }
  if (found_in_region != nullptr) {
    *found_in_region = found;
  }
  return result;
}

HotRegion::PutResult HotRegion::TryDelete(
    const Slice& key, ValueType type, SequenceNumber sequence,
    uint64_t memtable_id, uint64_t wal_number, bool* found_in_region) {
  return write_buffer_.TryDelete(
      key, type, sequence, memtable_id, wal_number, found_in_region);
}

HotRegion::MaterializeResult HotRegion::MaterializeKey(
    const Slice& key, const Materializer& materializer,
    BufferedWrite* materialized_write) {
  return write_buffer_.MaterializeKey(
      key, materializer, materialized_write);
}

bool HotRegion::ApplyBypassMutation(const Slice& key,
                                    SequenceNumber sequence,
                                    const BypassWriter& writer) {
  return write_buffer_.ApplyBypassMutation(key, sequence, writer);
}

bool HotRegion::Remove(const Slice& key, BufferedWrite* write) {
  return write_buffer_.Remove(key, write);
}

void HotRegion::RebindMemtable(uint64_t memtable_id,
                               SequenceNumber sequence) {
  write_buffer_.RebindMemtable(memtable_id, sequence);
}

uint64_t HotRegion::OldestHotWalNumber() const {
  return write_buffer_.OldestHotWalNumber();
}

std::vector<HotRegion::BufferedWrite> HotRegion::GetAll() {
  return write_buffer_.GetAll();
}

void HotRegion::PrepareAllForMaterialization() {
  write_buffer_.PrepareAllForMaterialization();
}

std::vector<HotRegion::BufferedWrite> HotRegion::GetPendingEvictions(
    size_t max_value_bytes, size_t max_entries) const {
  return write_buffer_.GetPendingEvictions(max_value_bytes, max_entries);
}

bool HotRegion::HasPendingEvictions() const {
  return write_buffer_.HasPendingEvictions();
}

bool HotRegion::empty() const {
  return write_buffer_.empty();
}

size_t HotRegion::entry_count() const {
  return write_buffer_.entry_count();
}

size_t HotRegion::memory_usage() const {
  return doorkeeper_memory_usage() + write_buffer_.memory_usage();
}

size_t HotRegion::doorkeeper_memory_usage() const {
  return doorkeeper_.memory_usage();
}

}  // namespace TERARKDB_NAMESPACE
