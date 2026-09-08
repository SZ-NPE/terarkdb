//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"
#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "util/mutexlock.h"

namespace TERARKDB_NAMESPACE {

// Stores the newest unflushed value of admitted hot keys in RocksDB's existing
// LRU cache. Same-sized and smaller overwrites update the resident cache entry
// in place. Capacity eviction queues only the newest version for MemTable
// materialization, so intermediate hot versions never enter a vSST.
class HotKeyWriteBuffer {
 public:
  struct Options {
    size_t capacity = 64U << 20;
    size_t max_value_size = 1U << 20;
    size_t max_pending_memory = 16U << 20;
    size_t max_pending_entry_memory = 8U << 20;
  };

  enum class PutResult {
    kInserted,
    kUpdatedInPlace,
    kReplaced,
    kSuperseded,
    kBypass,
  };

  struct BufferedWrite {
    std::string key;
    std::string value;
    ValueType type = kTypeValue;
    SequenceNumber sequence = 0;
    uint64_t memtable_id = 0;
    uint64_t wal_number = 0;
    size_t charge = 0;
  };

  enum class MaterializeResult {
    kNotFound,
    kMaterialized,
    kRetry,
  };

  using Materializer = std::function<bool(const BufferedWrite&)>;
  using BypassWriter = std::function<bool()>;

  explicit HotKeyWriteBuffer(const Options& options);
  ~HotKeyWriteBuffer();

  HotKeyWriteBuffer(const HotKeyWriteBuffer&) = delete;
  HotKeyWriteBuffer& operator=(const HotKeyWriteBuffer&) = delete;

  bool Lookup(const Slice& key, SequenceNumber snapshot, uint64_t memtable_id,
              std::string* value, SequenceNumber* sequence,
              ValueType* type = nullptr) const;

  bool CanBuffer(const Slice& value) const {
    return value.size() <= max_value_size_;
  }

  void MarkKeyPresent(const Slice& key);
  bool IsKeyMaybePresent(const Slice& key) const;

  PutResult TryPut(const Slice& key, const Slice& value,
                   SequenceNumber sequence, uint64_t memtable_id,
                   bool admit_new_key, uint64_t wal_number = 0,
                   bool* found_in_buffer = nullptr);

  PutResult TryDelete(const Slice& key, ValueType type,
                      SequenceNumber sequence, uint64_t memtable_id,
                      uint64_t wal_number = 0,
                      bool* found_in_buffer = nullptr);

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

  bool empty() const {
    return entry_count() == 0;
  }

  size_t entry_count() const {
    return resident_entry_count_.load(std::memory_order_relaxed) +
           pending_entry_count_.load(std::memory_order_relaxed);
  }

  size_t memory_usage() const {
    return cache_->GetUsage() +
           membership_counters_.size() * sizeof(membership_counters_[0]) +
           pending_memory_usage_.load(std::memory_order_relaxed);
  }

  const char* cache_name() const { return cache_->Name(); }

 private:
  struct Entry;

  struct PendingShard {
    mutable port::Mutex mutex;
    std::unordered_multimap<uint32_t, std::shared_ptr<Entry>> entries;
  };

  struct PendingReservationContext {
    HotKeyWriteBuffer* owner = nullptr;
    size_t remaining = 0;
  };

  static thread_local PendingReservationContext
      pending_reservation_context_;

  struct WalReference {
    explicit WalReference(uint64_t wal) : wal_number(wal) {}

    const uint64_t wal_number;
    std::atomic<size_t> entry_count{0};
  };

  struct Entry {
    enum class State {
      kResident,
      kEvicting,
    };

    Entry(HotKeyWriteBuffer* entry_owner, const Slice& entry_key,
          const Slice& entry_value, ValueType entry_type,
          SequenceNumber entry_sequence, uint64_t entry_memtable_id,
          uint64_t entry_wal_number);
    ~Entry();

    void CopyTo(BufferedWrite* write);
    void CopyToUnlocked(BufferedWrite* write);
    Slice value_slice() const { return Slice(value); }
    bool CanUpdateMutationInPlace(const Slice& new_value,
                                  ValueType new_type) const;
    void UpdateMutation(const Slice& new_value, ValueType new_type);
    size_t CalculateCharge() const;

    HotKeyWriteBuffer* owner;
    mutable SpinMutex mutex;
    std::string key;
    std::string value;
    ValueType type = kTypeValue;
    SequenceNumber sequence = 0;
    uint64_t memtable_id = 0;
    WalReference* wal_reference = nullptr;
    size_t charge = 0;
    std::atomic<bool> materialize_on_delete{true};
    State state = State::kResident;
    bool pending_memory_reserved = false;
    bool retired = false;
  };

  static void DeleteEntry(const Slice& key, void* value);
  static void SnapshotEntry(void* value, size_t charge);

  static size_t CalculateMembershipFilterBytes(size_t capacity);
  static size_t CalculateResidentCacheCapacity(size_t capacity);
  static uint32_t MembershipHash(const Slice& key, uint32_t seed);
  size_t InsertionShardIndex(const Slice& key) const;
  size_t MembershipIndex(uint32_t hash) const;
  void IncrementMembershipCounter(size_t index);
  void DecrementMembershipCounter(size_t index);
  void MarkKeyAbsent(const Slice& key);
  bool ReservePendingMemory(size_t charge);
  void ReleasePendingReservation(size_t charge);
  void ConvertPendingReservation(size_t charge);
  size_t CalculateInsertionReservation(size_t shard_index,
                                       size_t entry_charge) const;
  void UpdateMaxResidentCharge(size_t shard_index, size_t charge);
  port::Mutex* KeyMutex(const Slice& key);
  bool RebindEntryIfNeeded(Entry* entry, uint64_t memtable_id);
  void RebindEntryToCurrentMemtable(Entry* entry);
  WalReference* RegisterWalReference(uint64_t wal_number);
  void UnregisterWalReference(WalReference* reference);
  WalReference* UpdateWalReference(WalReference* old_reference,
                                   uint64_t new_wal_number);
  bool InsertEntry(std::unique_ptr<Entry> entry);
  PutResult TryMutation(const Slice& key, const Slice& value, ValueType type,
                        SequenceNumber sequence, uint64_t memtable_id,
                        bool admit_new_key, uint64_t wal_number,
                        bool* found_in_buffer);
  PutResult TryUpdatePending(const Slice& key, const Slice& value,
                             ValueType type, SequenceNumber sequence,
                             uint64_t memtable_id, uint64_t wal_number,
                             bool* found);
  std::shared_ptr<Entry> FindPending(const Slice& key) const;
  std::shared_ptr<Entry> FindPending(const Slice& key,
                                     uint32_t pending_hash) const;
  bool RemovePending(const Slice& key, const std::shared_ptr<Entry>& entry,
                     SequenceNumber expected_sequence);
  void OnEntryDeleted(Entry* entry);
  void AppendSnapshot(Entry* entry);

  std::shared_ptr<Cache> cache_;
  const size_t resident_capacity_;
  const size_t max_value_size_;
  const size_t max_pending_memory_;
  const size_t max_pending_entry_memory_;
  const int resident_shard_bits_;
  const size_t resident_shard_count_;
  const size_t resident_shard_capacity_;
  std::vector<std::atomic<uint8_t>> membership_counters_;
  std::array<port::Mutex, 256> key_mutexes_;
  std::array<std::mutex, 64> insertion_mutexes_;
  std::array<std::atomic<size_t>, 64> resident_shard_usage_{};
  std::array<std::atomic<size_t>, 64> max_resident_charge_{};
  std::atomic<size_t> resident_entry_count_{0};
  std::atomic<size_t> pending_entry_count_{0};
  std::atomic<size_t> pending_memory_usage_{0};
  std::atomic<size_t> pending_reserved_memory_{0};
  std::atomic<size_t> pending_budget_usage_{0};
  std::atomic<bool> shutting_down_{false};

  static constexpr size_t kPendingShardCount = 64;
  std::array<PendingShard, kPendingShardCount> pending_shards_;

  port::Mutex snapshot_mutex_;
  std::vector<BufferedWrite>* active_snapshot_ = nullptr;
  std::atomic<uint64_t> current_memtable_id_{0};
  std::atomic<SequenceNumber> current_memtable_sequence_{0};

  mutable port::Mutex wal_mutex_;
  std::map<uint64_t, std::unique_ptr<WalReference>> wal_references_;
  std::atomic<WalReference*> current_wal_reference_{nullptr};

};

}  // namespace TERARKDB_NAMESPACE
