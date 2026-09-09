//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/hot_key_write_buffer.h"

#include <algorithm>
#include <cassert>
#include <cstring>
#include <utility>

#include "cache/sharded_cache.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace TERARKDB_NAMESPACE {

namespace {

constexpr size_t kMaxMembershipFilterBytes = 8U << 20;

}  // namespace

thread_local HotKeyWriteBuffer::PendingReservationContext
    HotKeyWriteBuffer::pending_reservation_context_;

HotKeyWriteBuffer::Entry::Entry(HotKeyWriteBuffer* entry_owner,
                                const Slice& entry_key,
                                const Slice& entry_value,
                                ValueType entry_type,
                                SequenceNumber entry_sequence,
                                uint64_t entry_memtable_id,
                                uint64_t entry_wal_number)
    : owner(entry_owner),
      key(entry_key.ToString()),
      type(entry_type),
      sequence(entry_sequence),
      memtable_id(entry_memtable_id) {
  UpdateMutation(entry_value, entry_type);
  charge = CalculateCharge();
  hot_wal_reference = owner->RegisterHotWalReference(entry_wal_number);
}

HotKeyWriteBuffer::Entry::~Entry() {
  owner->UnregisterHotWalReference(hot_wal_reference);
}

void HotKeyWriteBuffer::Entry::CopyTo(BufferedWrite* write) {
  std::lock_guard<SpinMutex> lock(mutex);
  owner->RebindEntryToCurrentMemtable(this);
  CopyToUnlocked(write);
}

void HotKeyWriteBuffer::Entry::CopyToUnlocked(BufferedWrite* write) {
  write->key = key;
  const Slice current_value = value_slice();
  write->value.assign(current_value.data(), current_value.size());
  write->type = type;
  write->sequence = sequence;
  write->memtable_id = memtable_id;
  write->hot_wal_number =
      hot_wal_reference == nullptr ? 0 : hot_wal_reference->wal_number;
  write->charge = charge;
}

bool HotKeyWriteBuffer::Entry::CanUpdateMutationInPlace(
    const Slice& new_value, ValueType new_type) const {
  if (new_type != kTypeValue) {
    return true;
  }
  return new_value.size() <= value.capacity();
}

void HotKeyWriteBuffer::Entry::UpdateMutation(
    const Slice& new_value, ValueType new_type) {
  type = new_type;
  if (new_type != kTypeValue) {
    value.clear();
    return;
  }
  value.assign(new_value.data(), new_value.size());
}

size_t HotKeyWriteBuffer::Entry::CalculateCharge() const {
  return sizeof(Entry) + key.size() + value.capacity() +
         sizeof(std::pair<const uint32_t, std::shared_ptr<Entry>>) +
         64;
}

HotKeyWriteBuffer::HotKeyWriteBuffer(const Options& options)
    : cache_(NewLRUCache(
          CalculateResidentCacheCapacity(options.capacity),
          GetDefaultCacheShardBits(
              CalculateResidentCacheCapacity(options.capacity)),
          true)),
      resident_capacity_(CalculateResidentCacheCapacity(options.capacity)),
      max_value_size_(std::max<size_t>(1, options.max_value_size)),
      max_pending_memory_(
          std::max<size_t>(1, options.max_pending_memory)),
      max_pending_entry_memory_(
          std::max<size_t>(
              1, std::min(options.max_pending_entry_memory,
                          max_pending_memory_ / 2))),
      resident_shard_bits_(GetDefaultCacheShardBits(resident_capacity_)),
      resident_shard_count_(1U << resident_shard_bits_),
      resident_shard_capacity_(
          (resident_capacity_ + resident_shard_count_ - 1) /
          resident_shard_count_),
      membership_counters_(
          CalculateMembershipFilterBytes(options.capacity)) {
  assert(cache_ != nullptr);
  for (auto& counter : membership_counters_) {
    counter.store(0, std::memory_order_relaxed);
  }
  for (auto& usage : resident_shard_usage_) {
    usage.store(0, std::memory_order_relaxed);
  }
  for (auto& charge : max_resident_charge_) {
    charge.store(0, std::memory_order_relaxed);
  }
}

HotKeyWriteBuffer::~HotKeyWriteBuffer() {
  shutting_down_.store(true, std::memory_order_release);
  cache_->EraseUnRefEntries();
  cache_.reset();
  for (auto& shard : pending_shards_) {
    MutexLock lock(&shard.mutex);
    shard.entries.clear();
  }
}

bool HotKeyWriteBuffer::Lookup(const Slice& key, SequenceNumber snapshot,
                               uint64_t memtable_id, std::string* value,
                               SequenceNumber* sequence,
                               ValueType* type) const {
  Cache::Handle* handle = cache_->Lookup(key);
  if (handle != nullptr) {
    Entry* entry = static_cast<Entry*>(cache_->Value(handle));
    bool found = false;
    {
      std::lock_guard<SpinMutex> lock(entry->mutex);
      if (!entry->retired &&
          const_cast<HotKeyWriteBuffer*>(this)->RebindEntryIfNeeded(
              entry, memtable_id) &&
          entry->sequence <= snapshot) {
        if (value != nullptr) {
          const Slice current_value = entry->value_slice();
          value->assign(current_value.data(), current_value.size());
        }
        if (sequence != nullptr) {
          *sequence = entry->sequence;
        }
        if (type != nullptr) {
          *type = entry->type;
        }
        found = true;
      }
    }
    cache_->Release(handle);
    if (found) {
      return true;
    }
  }

  std::shared_ptr<Entry> pending = FindPending(key);
  if (pending == nullptr) {
    return false;
  }
  std::lock_guard<SpinMutex> lock(pending->mutex);
  if (pending->state != Entry::State::kEvicting || pending->retired ||
      !const_cast<HotKeyWriteBuffer*>(this)->RebindEntryIfNeeded(
          pending.get(), memtable_id) ||
      pending->sequence > snapshot) {
    return false;
  }
  if (value != nullptr) {
    const Slice current_value = pending->value_slice();
    value->assign(current_value.data(), current_value.size());
  }
  if (sequence != nullptr) {
    *sequence = pending->sequence;
  }
  if (type != nullptr) {
    *type = pending->type;
  }
  return true;
}

bool HotKeyWriteBuffer::Contains(const Slice& key,
                                 uint64_t memtable_id) const {
  return Lookup(key, kMaxSequenceNumber, memtable_id, nullptr, nullptr,
                nullptr);
}

HotKeyWriteBuffer::PutResult HotKeyWriteBuffer::TryPut(
    const Slice& key, const Slice& value, SequenceNumber sequence,
    uint64_t memtable_id, bool admit_new_key, uint64_t hot_wal_number,
    bool* found_in_buffer) {
  return TryMutation(key, value, kTypeValue, sequence, memtable_id,
                     admit_new_key, hot_wal_number, found_in_buffer);
}

HotKeyWriteBuffer::PutResult HotKeyWriteBuffer::TryDelete(
    const Slice& key, ValueType type, SequenceNumber sequence,
    uint64_t memtable_id, uint64_t hot_wal_number, bool* found_in_buffer) {
  assert(type == kTypeDeletion);
  return TryMutation(key, Slice(), type, sequence, memtable_id,
                     false, hot_wal_number, found_in_buffer);
}

HotKeyWriteBuffer::PutResult HotKeyWriteBuffer::TryMutation(
    const Slice& key, const Slice& value, ValueType type,
    SequenceNumber sequence, uint64_t memtable_id, bool admit_new_key,
    uint64_t hot_wal_number, bool* found_in_buffer) {
  if (type == kTypeValue && value.size() > max_value_size_) {
    return PutResult::kBypass;
  }
  auto update_resident = [&](Cache::Handle* handle, bool allow_replacement,
                             bool* requires_key_lock) {
    Entry* resident = static_cast<Entry*>(cache_->Value(handle));
    bool replace_entry = false;
    PutResult result = PutResult::kBypass;
    {
      std::lock_guard<SpinMutex> lock(resident->mutex);
      if (resident->retired) {
        return PutResult::kBypass;
      }
      RebindEntryIfNeeded(resident, memtable_id);
      if (sequence <= resident->sequence) {
        result = PutResult::kSuperseded;
      } else {
        if (!resident->CanUpdateMutationInPlace(value, type)) {
          if (!allow_replacement) {
            *requires_key_lock = true;
          } else {
            resident->retired = true;
            resident->materialize_on_delete.store(
                false, std::memory_order_release);
            if (resident->pending_memory_reserved) {
              const size_t previous = pending_reserved_memory_.fetch_sub(
                  resident->charge, std::memory_order_relaxed);
              assert(previous >= resident->charge);
              resident->pending_memory_reserved = false;
              ReleasePendingReservation(resident->charge);
            }
            replace_entry = true;
          }
        } else {
          resident->UpdateMutation(value, type);
          resident->sequence = sequence;
          resident->hot_wal_reference = UpdateHotWalReference(
              resident->hot_wal_reference, hot_wal_number);
          result = PutResult::kUpdatedInPlace;
        }
      }
    }

    if (replace_entry) {
      std::unique_ptr<Entry> replacement(
          new Entry(this, key, value, type, sequence, memtable_id,
                    hot_wal_number));
      if (InsertEntry(std::move(replacement))) {
        result = PutResult::kReplaced;
      } else {
        cache_->Erase(key);
        result = PutResult::kBypass;
      }
    }
    return result;
  };

  Cache::Handle* handle = cache_->Lookup(key);
  if (handle != nullptr) {
    if (found_in_buffer != nullptr) {
      *found_in_buffer = true;
    }
    bool requires_key_lock = false;
    const PutResult result =
        update_resident(handle, false, &requires_key_lock);
    cache_->Release(handle);
    if (!requires_key_lock) {
      return result;
    }
  }

  bool found_pending = false;
  PutResult pending_result =
      TryUpdatePending(key, value, type, sequence, memtable_id, hot_wal_number,
                       &found_pending);
  if (found_pending) {
    if (found_in_buffer != nullptr) {
      *found_in_buffer = true;
    }
    return pending_result;
  }

  MutexLock key_lock(KeyMutex(key));
  handle = cache_->Lookup(key);
  if (handle != nullptr) {
    if (found_in_buffer != nullptr) {
      *found_in_buffer = true;
    }
    bool unused = false;
    const PutResult result = update_resident(handle, true, &unused);
    cache_->Release(handle);
    return result;
  }
  pending_result =
      TryUpdatePending(key, value, type, sequence, memtable_id, hot_wal_number,
                       &found_pending);
  if (found_pending) {
    if (found_in_buffer != nullptr) {
      *found_in_buffer = true;
    }
    return pending_result;
  }
  if (!admit_new_key) {
    return PutResult::kBypass;
  }
  std::unique_ptr<Entry> entry(
      new Entry(this, key, value, type, sequence, memtable_id,
                hot_wal_number));
  return InsertEntry(std::move(entry)) ? PutResult::kInserted
                                       : PutResult::kBypass;
}

HotKeyWriteBuffer::MaterializeResult HotKeyWriteBuffer::MaterializeKey(
    const Slice& key, const Materializer& materializer,
    BufferedWrite* materialized_write) {
  MutexLock key_lock(KeyMutex(key));
  Cache::Handle* handle = cache_->Lookup(key);
  if (handle != nullptr) {
    Entry* entry = static_cast<Entry*>(cache_->Value(handle));
    BufferedWrite write;
    bool success = false;
    {
      std::lock_guard<SpinMutex> lock(entry->mutex);
      if (!entry->retired) {
        RebindEntryToCurrentMemtable(entry);
        entry->CopyToUnlocked(&write);
        success = materializer(write);
        if (success) {
          entry->retired = true;
          entry->materialize_on_delete.store(false,
                                             std::memory_order_release);
          if (entry->pending_memory_reserved) {
            const size_t previous = pending_reserved_memory_.fetch_sub(
                entry->charge, std::memory_order_relaxed);
            assert(previous >= entry->charge);
            entry->pending_memory_reserved = false;
            ReleasePendingReservation(entry->charge);
          }
        }
      }
    }
    if (success) {
      cache_->Erase(key);
    }
    cache_->Release(handle);
    if (!success) {
      return MaterializeResult::kRetry;
    }
    if (materialized_write != nullptr) {
      *materialized_write = std::move(write);
    }
    return MaterializeResult::kMaterialized;
  }

  std::shared_ptr<Entry> pending = FindPending(key);
  if (pending == nullptr) {
    return MaterializeResult::kNotFound;
  }
  BufferedWrite write;
  {
    std::lock_guard<SpinMutex> lock(pending->mutex);
    if (pending->state != Entry::State::kEvicting || pending->retired) {
      return MaterializeResult::kNotFound;
    }
    RebindEntryToCurrentMemtable(pending.get());
    pending->CopyToUnlocked(&write);
    if (!materializer(write)) {
      return MaterializeResult::kRetry;
    }
    pending->retired = true;
  }
  if (!RemovePending(key, pending, write.sequence)) {
    return MaterializeResult::kRetry;
  }
  if (materialized_write != nullptr) {
    *materialized_write = std::move(write);
  }
  return MaterializeResult::kMaterialized;
}

bool HotKeyWriteBuffer::ApplyBypassMutation(
    const Slice& key, SequenceNumber sequence, const BypassWriter& writer) {
  MutexLock key_lock(KeyMutex(key));
  if (!IsKeyMaybePresent(key)) {
    return writer();
  }
  if (!writer()) {
    return false;
  }

  Cache::Handle* handle = cache_->Lookup(key);
  if (handle != nullptr) {
    Entry* entry = static_cast<Entry*>(cache_->Value(handle));
    bool retire = false;
    {
      std::lock_guard<SpinMutex> entry_lock(entry->mutex);
      if (!entry->retired && sequence > entry->sequence) {
        entry->retired = true;
        entry->materialize_on_delete.store(false,
                                           std::memory_order_release);
        if (entry->pending_memory_reserved) {
          const size_t previous = pending_reserved_memory_.fetch_sub(
              entry->charge, std::memory_order_relaxed);
          assert(previous >= entry->charge);
          entry->pending_memory_reserved = false;
          ReleasePendingReservation(entry->charge);
        }
        retire = true;
      }
    }
    if (retire) {
      cache_->Erase(key);
    }
    cache_->Release(handle);
    return true;
  }

  std::shared_ptr<Entry> pending = FindPending(key);
  if (pending == nullptr) {
    return true;
  }
  BufferedWrite removed;
  bool retire = false;
  {
    std::lock_guard<SpinMutex> entry_lock(pending->mutex);
    if (pending->state == Entry::State::kEvicting && !pending->retired &&
        sequence > pending->sequence) {
      RebindEntryToCurrentMemtable(pending.get());
      pending->CopyToUnlocked(&removed);
      pending->retired = true;
      retire = true;
    }
  }
  if (retire) {
    if (!RemovePending(key, pending, removed.sequence)) {
      return false;
    }
  }
  return true;
}

bool HotKeyWriteBuffer::Remove(const Slice& key, BufferedWrite* write) {
  MutexLock key_lock(KeyMutex(key));
  Cache::Handle* handle = cache_->Lookup(key);
  if (handle != nullptr) {
    Entry* entry = static_cast<Entry*>(cache_->Value(handle));
    {
      std::lock_guard<SpinMutex> lock(entry->mutex);
      if (entry->retired) {
        cache_->Release(handle);
        return false;
      }
      if (write != nullptr) {
        RebindEntryToCurrentMemtable(entry);
        entry->CopyToUnlocked(write);
      }
      entry->retired = true;
      entry->materialize_on_delete.store(false, std::memory_order_release);
      if (entry->pending_memory_reserved) {
        const size_t previous = pending_reserved_memory_.fetch_sub(
            entry->charge, std::memory_order_relaxed);
        assert(previous >= entry->charge);
        entry->pending_memory_reserved = false;
        ReleasePendingReservation(entry->charge);
      }
    }
    cache_->Erase(key);
    cache_->Release(handle);
    return true;
  }

  std::shared_ptr<Entry> pending = FindPending(key);
  if (pending == nullptr) {
    return false;
  }
  SequenceNumber sequence;
  {
    std::lock_guard<SpinMutex> lock(pending->mutex);
    if (pending->state != Entry::State::kEvicting || pending->retired) {
      return false;
    }
    RebindEntryToCurrentMemtable(pending.get());
    if (write != nullptr) {
      pending->CopyToUnlocked(write);
    }
    sequence = pending->sequence;
    pending->retired = true;
  }
  return RemovePending(key, pending, sequence);
}

void HotKeyWriteBuffer::RebindMemtable(uint64_t memtable_id,
                                       SequenceNumber sequence) {
  current_memtable_sequence_.store(sequence, std::memory_order_relaxed);
  current_memtable_id_.store(memtable_id, std::memory_order_release);
}

uint64_t HotKeyWriteBuffer::OldestHotWalNumber() const {
  MutexLock lock(&hot_wal_mutex_);
  for (const auto& reference : hot_wal_references_) {
    if (reference.second->entry_count.load(std::memory_order_relaxed) != 0) {
      return reference.first;
    }
  }
  return 0;
}

std::vector<HotKeyWriteBuffer::BufferedWrite> HotKeyWriteBuffer::GetAll() {
  std::vector<BufferedWrite> writes;
  MutexLock lock(&snapshot_mutex_);
  active_snapshot_ = &writes;
  cache_->ApplyToAllCacheEntries(&HotKeyWriteBuffer::SnapshotEntry,
                                 true /* thread_safe */);
  active_snapshot_ = nullptr;
  return writes;
}

void HotKeyWriteBuffer::PrepareAllForMaterialization() {
  if (pending_entry_count_.load(std::memory_order_acquire) != 0 ||
      pending_reserved_memory_.load(std::memory_order_acquire) != 0) {
    return;
  }

  const auto residents = GetAll();
  size_t reserved_memory = 0;
  for (const auto& resident : residents) {
    MutexLock key_lock(KeyMutex(resident.key));
    const size_t insertion_shard = InsertionShardIndex(resident.key);
    std::lock_guard<std::mutex> insertion_lock(
        insertion_mutexes_[insertion_shard]);
    Cache::Handle* handle = cache_->Lookup(resident.key);
    if (handle == nullptr) {
      continue;
    }

    Entry* entry = static_cast<Entry*>(cache_->Value(handle));
    bool prepare = false;
    {
      std::lock_guard<SpinMutex> entry_lock(entry->mutex);
      if (!entry->retired &&
          entry->state == Entry::State::kResident &&
          !entry->pending_memory_reserved &&
          entry->sequence == resident.sequence &&
          entry->charge <= max_pending_entry_memory_ &&
          entry->charge <= max_pending_memory_ - reserved_memory &&
          ReservePendingMemory(entry->charge)) {
        entry->pending_memory_reserved = true;
        pending_reserved_memory_.fetch_add(entry->charge,
                                           std::memory_order_relaxed);
        reserved_memory += entry->charge;
        prepare = true;
      }
    }
    if (prepare) {
      cache_->Erase(resident.key);
    }
    cache_->Release(handle);
    if (reserved_memory == max_pending_memory_) {
      break;
    }
  }
}

std::vector<HotKeyWriteBuffer::BufferedWrite>
HotKeyWriteBuffer::GetPendingEvictions(size_t max_value_bytes,
                                       size_t max_entries) const {
  std::vector<BufferedWrite> writes;
  size_t value_bytes = 0;
  for (const auto& shard : pending_shards_) {
    MutexLock lock(&shard.mutex);
    for (const auto& item : shard.entries) {
      writes.emplace_back();
      item.second->CopyTo(&writes.back());
      value_bytes += writes.back().value.size();
      if (value_bytes >= max_value_bytes || writes.size() >= max_entries) {
        return writes;
      }
    }
  }
  return writes;
}

bool HotKeyWriteBuffer::HasPendingEvictions() const {
  return pending_entry_count_.load(std::memory_order_relaxed) != 0;
}

void HotKeyWriteBuffer::DeleteEntry(const Slice& /*key*/, void* value) {
  Entry* entry = static_cast<Entry*>(value);
  entry->owner->OnEntryDeleted(entry);
}

void HotKeyWriteBuffer::SnapshotEntry(void* value, size_t /*charge*/) {
  Entry* entry = static_cast<Entry*>(value);
  entry->owner->AppendSnapshot(entry);
}

size_t HotKeyWriteBuffer::CalculateMembershipFilterBytes(size_t capacity) {
  const size_t normalized_capacity = std::max<size_t>(1, capacity);
  return std::max<size_t>(
      1, std::min(kMaxMembershipFilterBytes, normalized_capacity / 32));
}

size_t HotKeyWriteBuffer::CalculateResidentCacheCapacity(size_t capacity) {
  const size_t normalized_capacity = std::max<size_t>(1, capacity);
  const size_t membership_bytes =
      CalculateMembershipFilterBytes(normalized_capacity);
  return std::max<size_t>(1, normalized_capacity - membership_bytes);
}

uint32_t HotKeyWriteBuffer::MembershipHash(const Slice& key, uint32_t seed) {
  return Hash(key.data(), key.size(), seed);
}

size_t HotKeyWriteBuffer::MembershipIndex(uint32_t hash) const {
  return hash % membership_counters_.size();
}

size_t HotKeyWriteBuffer::InsertionShardIndex(const Slice& key) const {
  const uint32_t hash = Hash(key.data(), key.size(), 0);
  return resident_shard_bits_ == 0
             ? 0
             : hash >> (32 - resident_shard_bits_);
}

void HotKeyWriteBuffer::IncrementMembershipCounter(size_t index) {
  auto& counter = membership_counters_[index];
  uint8_t current = counter.load(std::memory_order_relaxed);
  while (current != std::numeric_limits<uint8_t>::max() &&
         !counter.compare_exchange_weak(
             current, static_cast<uint8_t>(current + 1),
             std::memory_order_release, std::memory_order_relaxed)) {
  }
}

void HotKeyWriteBuffer::DecrementMembershipCounter(size_t index) {
  auto& counter = membership_counters_[index];
  uint8_t current = counter.load(std::memory_order_relaxed);
  while (current != std::numeric_limits<uint8_t>::max()) {
    assert(current > 0);
    if (counter.compare_exchange_weak(
            current, static_cast<uint8_t>(current - 1),
            std::memory_order_release, std::memory_order_relaxed)) {
      return;
    }
  }
}

void HotKeyWriteBuffer::MarkKeyAbsent(const Slice& key) {
  const size_t first =
      MembershipIndex(MembershipHash(key, 0x9e3779b9U));
  const size_t second =
      MembershipIndex(MembershipHash(key, 0x85ebca6bU));
  DecrementMembershipCounter(first);
  if (second != first) {
    DecrementMembershipCounter(second);
  }
}

bool HotKeyWriteBuffer::ReservePendingMemory(size_t charge) {
  if (charge == 0) {
    return true;
  }
  size_t current = pending_budget_usage_.load(std::memory_order_relaxed);
  while (current <= max_pending_memory_ &&
         charge <= max_pending_memory_ - current) {
    if (pending_budget_usage_.compare_exchange_weak(
            current, current + charge, std::memory_order_acq_rel,
            std::memory_order_relaxed)) {
      return true;
    }
  }
  return false;
}

void HotKeyWriteBuffer::ReleasePendingReservation(size_t charge) {
  if (charge == 0) {
    return;
  }
  const size_t previous =
      pending_budget_usage_.fetch_sub(charge, std::memory_order_acq_rel);
  assert(previous >= charge);
}

void HotKeyWriteBuffer::ConvertPendingReservation(size_t charge) {
  assert(pending_reservation_context_.owner == this);
  assert(pending_reservation_context_.remaining >= charge);
  pending_reservation_context_.remaining -= charge;
}

size_t HotKeyWriteBuffer::CalculateInsertionReservation(
    size_t shard_index, size_t entry_charge) const {
  const size_t usage =
      resident_shard_usage_[shard_index].load(std::memory_order_relaxed);
  if (entry_charge <= resident_shard_capacity_ &&
      usage <= resident_shard_capacity_ - entry_charge) {
    return 0;
  }
  const size_t overflow =
      entry_charge > resident_shard_capacity_
          ? usage
          : usage - (resident_shard_capacity_ - entry_charge);
  const size_t max_charge =
      max_resident_charge_[shard_index].load(std::memory_order_relaxed);
  return std::min(
      usage, overflow > usage - std::min(usage, max_charge)
                 ? usage
                 : overflow + max_charge);
}

void HotKeyWriteBuffer::UpdateMaxResidentCharge(size_t shard_index,
                                                size_t charge) {
  auto& maximum = max_resident_charge_[shard_index];
  size_t current = maximum.load(std::memory_order_relaxed);
  while (current < charge &&
         !maximum.compare_exchange_weak(
             current, charge, std::memory_order_relaxed,
             std::memory_order_relaxed)) {
  }
}

port::Mutex* HotKeyWriteBuffer::KeyMutex(const Slice& key) {
  const uint32_t hash = Hash(key.data(), key.size(), 0);
  return &key_mutexes_[hash & (key_mutexes_.size() - 1)];
}

bool HotKeyWriteBuffer::RebindEntryIfNeeded(Entry* entry,
                                            uint64_t memtable_id) {
  const uint64_t current_memtable_id =
      current_memtable_id_.load(std::memory_order_acquire);
  if (current_memtable_id != 0 && memtable_id != current_memtable_id) {
    return false;
  }
  if (entry->memtable_id == memtable_id) {
    return true;
  }
  if (current_memtable_id != 0 && memtable_id == current_memtable_id) {
    entry->memtable_id = memtable_id;
    entry->sequence =
        current_memtable_sequence_.load(std::memory_order_relaxed);
    return true;
  }
  return false;
}

void HotKeyWriteBuffer::RebindEntryToCurrentMemtable(Entry* entry) {
  RebindEntryIfNeeded(
      entry, current_memtable_id_.load(std::memory_order_acquire));
}

HotKeyWriteBuffer::HotWalReference*
HotKeyWriteBuffer::RegisterHotWalReference(uint64_t wal_number) {
  if (wal_number == 0) {
    return nullptr;
  }
  HotWalReference* reference =
      current_hot_wal_reference_.load(std::memory_order_acquire);
  if (reference != nullptr && reference->wal_number == wal_number) {
    reference->entry_count.fetch_add(1, std::memory_order_relaxed);
    return reference;
  }
  MutexLock lock(&hot_wal_mutex_);
  auto& slot = hot_wal_references_[wal_number];
  if (slot == nullptr) {
    slot.reset(new HotWalReference(wal_number));
  }
  reference = slot.get();
  current_hot_wal_reference_.store(reference, std::memory_order_release);
  reference->entry_count.fetch_add(1, std::memory_order_relaxed);
  return reference;
}

void HotKeyWriteBuffer::UnregisterHotWalReference(
    HotWalReference* reference) {
  if (reference == nullptr) {
    return;
  }
  const size_t previous =
      reference->entry_count.fetch_sub(1, std::memory_order_relaxed);
  assert(previous > 0);
}

HotKeyWriteBuffer::HotWalReference*
HotKeyWriteBuffer::UpdateHotWalReference(
    HotWalReference* old_reference, uint64_t new_wal_number) {
  if ((old_reference == nullptr && new_wal_number == 0) ||
      (old_reference != nullptr &&
       old_reference->wal_number == new_wal_number)) {
    return old_reference;
  }
  HotWalReference* new_reference = RegisterHotWalReference(new_wal_number);
  UnregisterHotWalReference(old_reference);
  return new_reference;
}

bool HotKeyWriteBuffer::InsertEntry(std::unique_ptr<Entry> entry) {
  if (entry->charge > max_pending_entry_memory_) {
    return false;
  }
  const size_t insertion_shard = InsertionShardIndex(entry->key);
  std::lock_guard<std::mutex> insertion_lock(
      insertion_mutexes_[insertion_shard]);
  const size_t reservation =
      CalculateInsertionReservation(insertion_shard, entry->charge);
  if (!ReservePendingMemory(reservation)) {
    return false;
  }
  assert(pending_reservation_context_.owner == nullptr);
  pending_reservation_context_ = {this, reservation};

  Cache::Handle* handle = nullptr;
  MarkKeyPresent(entry->key);
  Status status = cache_->Insert(entry->key, entry.get(), entry->charge,
                                 &HotKeyWriteBuffer::DeleteEntry, &handle);
  const size_t unused_reservation =
      pending_reservation_context_.remaining;
  pending_reservation_context_ = {};
  ReleasePendingReservation(unused_reservation);
  if (!status.ok() || handle == nullptr) {
    MarkKeyAbsent(entry->key);
    return false;
  }
  resident_shard_usage_[insertion_shard].fetch_add(
      entry->charge, std::memory_order_relaxed);
  UpdateMaxResidentCharge(insertion_shard, entry->charge);
  entry.release();
  resident_entry_count_.fetch_add(1, std::memory_order_relaxed);
  cache_->Release(handle);
  return true;
}

HotKeyWriteBuffer::PutResult HotKeyWriteBuffer::TryUpdatePending(
    const Slice& key, const Slice& value, ValueType type,
    SequenceNumber sequence, uint64_t memtable_id, uint64_t hot_wal_number,
    bool* found) {
  std::shared_ptr<Entry> entry = FindPending(key);
  if (entry == nullptr) {
    *found = false;
    return PutResult::kBypass;
  }
  *found = true;
  std::lock_guard<SpinMutex> entry_lock(entry->mutex);
  if (entry->state != Entry::State::kEvicting || entry->retired) {
    *found = false;
    return PutResult::kBypass;
  }
  RebindEntryIfNeeded(entry.get(), memtable_id);
  if (sequence <= entry->sequence) {
    return PutResult::kSuperseded;
  }
  const size_t previous_charge = entry->charge;
  if (!entry->CanUpdateMutationInPlace(value, type)) {
    *found = false;
    return PutResult::kBypass;
  }
  entry->UpdateMutation(value, type);
  entry->sequence = sequence;
  entry->hot_wal_reference =
      UpdateHotWalReference(entry->hot_wal_reference, hot_wal_number);
  entry->charge = entry->CalculateCharge();
  assert(entry->charge <= previous_charge);
  if (entry->charge < previous_charge) {
    const size_t released = previous_charge - entry->charge;
    pending_memory_usage_.fetch_sub(released, std::memory_order_relaxed);
    ReleasePendingReservation(released);
  }
  return PutResult::kUpdatedInPlace;
}

std::shared_ptr<HotKeyWriteBuffer::Entry>
HotKeyWriteBuffer::FindPending(const Slice& key) const {
  if (!IsKeyMaybePresent(key)) {
    return nullptr;
  }
  return FindPending(key, Hash(key.data(), key.size(), 0xd1b54a35U));
}

std::shared_ptr<HotKeyWriteBuffer::Entry>
HotKeyWriteBuffer::FindPending(const Slice& key,
                               uint32_t pending_hash) const {
  const PendingShard& shard =
      pending_shards_[pending_hash & (kPendingShardCount - 1)];
  MutexLock lock(&shard.mutex);
  const auto range = shard.entries.equal_range(pending_hash);
  for (auto entry = range.first; entry != range.second; ++entry) {
    if (entry->second->key.size() == key.size() &&
        std::memcmp(entry->second->key.data(), key.data(), key.size()) == 0) {
      return entry->second;
    }
  }
  return nullptr;
}

bool HotKeyWriteBuffer::RemovePending(
    const Slice& key, const std::shared_ptr<Entry>& entry,
    SequenceNumber expected_sequence) {
  const uint32_t pending_hash =
      Hash(key.data(), key.size(), 0xd1b54a35U);
  PendingShard& shard =
      pending_shards_[pending_hash & (kPendingShardCount - 1)];
  MutexLock lock(&shard.mutex);
  const auto range = shard.entries.equal_range(pending_hash);
  auto current = range.first;
  while (current != range.second && current->second.get() != entry.get()) {
    ++current;
  }
  if (current == range.second) {
    return false;
  }
  {
    std::lock_guard<SpinMutex> entry_lock(entry->mutex);
    if (entry->sequence != expected_sequence) {
      return false;
    }
  }
  pending_memory_usage_.fetch_sub(entry->charge,
                                  std::memory_order_relaxed);
  pending_entry_count_.fetch_sub(1, std::memory_order_relaxed);
  ReleasePendingReservation(entry->charge);
  MarkKeyAbsent(entry->key);
  shard.entries.erase(current);
  return true;
}

void HotKeyWriteBuffer::OnEntryDeleted(Entry* entry) {
  resident_entry_count_.fetch_sub(1, std::memory_order_relaxed);
  const size_t insertion_shard = InsertionShardIndex(entry->key);
  const size_t previous_usage =
      resident_shard_usage_[insertion_shard].fetch_sub(
          entry->charge, std::memory_order_relaxed);
  assert(previous_usage >= entry->charge);
  if (entry->materialize_on_delete.load(std::memory_order_acquire) &&
      !shutting_down_.load(std::memory_order_acquire)) {
    {
      bool already_reserved = false;
      {
        std::lock_guard<SpinMutex> entry_lock(entry->mutex);
        entry->state = Entry::State::kEvicting;
        if (entry->pending_memory_reserved) {
          const size_t previous = pending_reserved_memory_.fetch_sub(
              entry->charge, std::memory_order_relaxed);
          assert(previous >= entry->charge);
          entry->pending_memory_reserved = false;
          already_reserved = true;
        }
      }
      if (!already_reserved) {
        ConvertPendingReservation(entry->charge);
      }
      const uint32_t pending_hash =
          Hash(entry->key.data(), entry->key.size(), 0xd1b54a35U);
      PendingShard& shard =
          pending_shards_[pending_hash & (kPendingShardCount - 1)];
      MutexLock lock(&shard.mutex);
      std::shared_ptr<Entry> pending(entry);
      const size_t pending_charge = entry->charge;
      assert(pending_memory_usage_.load(std::memory_order_relaxed) +
                 pending_reserved_memory_.load(std::memory_order_relaxed) +
                 pending_charge <=
             max_pending_memory_);
      pending_memory_usage_.fetch_add(pending_charge,
                                      std::memory_order_relaxed);
      pending_entry_count_.fetch_add(1, std::memory_order_relaxed);
      const auto range = shard.entries.equal_range(pending_hash);
      assert(std::none_of(
          range.first, range.second,
          [entry](const decltype(shard.entries)::value_type& item) {
            return item.second->key == entry->key;
          }));
      (void)range;
      shard.entries.emplace(pending_hash, std::move(pending));
    }
    return;
  }
  MarkKeyAbsent(entry->key);
  delete entry;
}

void HotKeyWriteBuffer::AppendSnapshot(Entry* entry) {
  assert(active_snapshot_ != nullptr);
  active_snapshot_->emplace_back();
  entry->CopyTo(&active_snapshot_->back());
}

void HotKeyWriteBuffer::MarkKeyPresent(const Slice& key) {
  const size_t first =
      MembershipIndex(MembershipHash(key, 0x9e3779b9U));
  const size_t second =
      MembershipIndex(MembershipHash(key, 0x85ebca6bU));
  IncrementMembershipCounter(first);
  if (second != first) {
    IncrementMembershipCounter(second);
  }
}

bool HotKeyWriteBuffer::IsKeyMaybePresent(const Slice& key) const {
  const size_t first =
      MembershipIndex(MembershipHash(key, 0x9e3779b9U));
  if (membership_counters_[first].load(std::memory_order_acquire) == 0) {
    return false;
  }
  const size_t second =
      MembershipIndex(MembershipHash(key, 0x85ebca6bU));
  return second == first ||
         membership_counters_[second].load(std::memory_order_acquire) != 0;
}

}  // namespace TERARKDB_NAMESPACE
