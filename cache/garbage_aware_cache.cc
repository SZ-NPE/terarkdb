//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cache/garbage_aware_cache.h"

#include <algorithm>
#include <inttypes.h>
#include <stdio.h>

#include "monitoring/statistics.h"
#include "rocksdb/env.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace TERARKDB_NAMESPACE {

GarbageAwareCacheShard::GarbageAwareCacheShard(size_t capacity,
                                               bool strict_capacity_limit,
                                               uint64_t log_interval)
    : capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      log_interval_(log_interval) {}

GarbageAwareCacheShard::~GarbageAwareCacheShard() { EraseUnRefEntries(); }

Status GarbageAwareCacheShard::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value), Cache::Handle** handle,
    Cache::Priority priority) {
  return InsertImpl(key, value, charge, deleter, nullptr, hash, handle,
                    priority);
}

Status GarbageAwareCacheShard::InsertWithMetadata(
    const Slice& key, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value),
    const BlockCacheMetadata* metadata, uint32_t hash, Cache::Handle** handle,
    Cache::Priority priority) {
  return InsertImpl(key, value, charge, deleter, metadata, hash, handle,
                    priority);
}

Status GarbageAwareCacheShard::InsertImpl(
    const Slice& key, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value),
    const BlockCacheMetadata* metadata, uint32_t hash, Cache::Handle** handle,
    Cache::Priority priority) {
  if (handle != nullptr) {
    *handle = nullptr;
  }
  std::vector<GAHandle*> deleted;
  GAHandle* h = new GAHandle();
  bool inserted = true;
  Status s;
  GAHandle* replaced = nullptr;
  bool replaced_last_reference = false;
  h->key.assign(key.data(), key.size());
  h->value = value;
  h->deleter = deleter;
  h->charge = charge;
  h->hash = hash;
  h->refs = handle == nullptr ? 1 : 2;
  h->in_cache = true;
  h->priority = priority;
  if (metadata != nullptr) {
    h->file_number = metadata->file_number;
    h->is_data_block = metadata->is_data_block;
    h->garbage_aware = metadata->is_blob_file && metadata->is_data_block;
    h->garbage_ratio = 0.0;
  }

  {
    MutexLock l(&mutex_);
    if (metadata != nullptr && metadata->statistics != nullptr) {
      statistics_ = metadata->statistics;
    }
    if (h->garbage_aware && IsObsoleteFile(h->file_number)) {
      h->garbage_ratio = 1.0;
    }
    replaced = TableInsert(h);
    if (replaced != nullptr) {
      // TableInsert already unlinked `replaced` from the hash table and made
      // `h` the live node for this key.  Use RemoveReplacedFromCache so we do
      // not TableRemove(key, hash) and accidentally drop the new node `h`.
      RemoveReplacedFromCache(replaced);
      replaced_last_reference = Unref(replaced);
    }
    AddToFileIndex(h);
    usage_ += h->charge;
    if (handle == nullptr) {
      AddToQueue(h, h->garbage_ratio < 1.0);
    }
    if (h->garbage_aware) {
      RecordTick(metadata != nullptr ? metadata->statistics : nullptr,
                 GC_AWARE_CACHE_VSST_DATA_INSERT);
    }
    EvictIfNeeded(&deleted);
    if (strict_capacity_limit_ && handle != nullptr && usage_ > capacity_) {
      RemoveFromCache(h);
      Unref(h);  // Drop the cache reference; caller keeps value ownership.
      inserted = false;
      s = Status::Incomplete("Insert failed due to strict capacity limit");
      if (replaced != nullptr) {
        // The new entry was rejected, so restore the entry it displaced and
        // keep it queryable.  `replaced` was unlinked by TableInsert and
        // detached via RemoveReplacedFromCache, so re-link it here.  Restore it
        // toward the LRU head: it is again the only live copy for this key and
        // was just touched, so it should not be penalized to the tail.
        if (replaced->refs == 0) {
          replaced->refs = 1;
        } else {
          ++replaced->refs;
        }
        replaced->in_cache = true;
        TableInsert(replaced);
        AddToFileIndex(replaced);
        usage_ += replaced->charge;
        if (replaced->refs == 1) {
          AddToQueue(replaced, true /* promote */);
        }
        replaced = nullptr;
        replaced_last_reference = false;
      }
    }
    if (replaced != nullptr && replaced_last_reference) {
      deleted.push_back(replaced);
    }
    MaybeLogLocked(metadata);
  }

  for (auto* e : deleted) {
    FreeEntry(e);
  }

  if (!inserted) {
    DeleteHandleOnly(h);
    return s;
  }
  if (handle != nullptr) {
    *handle = h;
  }
  return Status::OK();
}

Cache::Handle* GarbageAwareCacheShard::Lookup(const Slice& key,
                                              uint32_t hash, bool record_hit) {
  return Lookup(key, hash, record_hit, nullptr);
}

Cache::Handle* GarbageAwareCacheShard::Lookup(const Slice& key,
                                              uint32_t hash,
                                              bool record_hit,
                                              Statistics* stats) {
  MutexLock l(&mutex_);
  GAHandle* h = TableLookup(key, hash);
  if (h == nullptr) {
    return nullptr;
  }
  if (h->refs == 1 && h->in_queue) {
    RemoveFromQueue(h);
  }
  h->refs++;
  if (!record_hit) {
    // The returned handle is tagged instead of using entry-level state so a
    // no-hit lookup keeps its release semantics even when hit/no-hit lookups
    // overlap and release in arbitrary order.
  }
  return EncodeHandle(h, !record_hit);
}

bool GarbageAwareCacheShard::Ref(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  GAHandle* h = UnwrapHandle(handle);
  if (!h->in_cache) {
    return false;
  }
  if (h->refs == 1 && h->in_queue) {
    RemoveFromQueue(h);
  }
  h->refs++;
  return true;
}

bool GarbageAwareCacheShard::Release(Cache::Handle* handle, bool force_erase) {
  std::vector<GAHandle*> deleted;
  bool erased = false;
  {
    MutexLock l(&mutex_);
    GAHandle* h = UnwrapHandle(handle);
    const bool no_hit_release = IsNoHitHandle(handle);
    if (force_erase && h->in_cache) {
      RemoveFromCache(h);
      erased = true;
    }
    if (Unref(h)) {
      deleted.push_back(h);
    } else if (h->refs == 1 && h->in_cache) {
      if (h->in_queue) {
        // No-hit lookups should not promote or demote by themselves. If the
        // entry remained queued, leave its previous queue position intact.
        assert(no_hit_release);
      }
      if (!h->in_queue) {
        const bool promote = !no_hit_release && h->garbage_ratio < 1.0;
        AddToQueue(h, promote);
      }
    }
    if (usage_ > capacity_) {
      EvictIfNeeded(&deleted);
    }
  }
  for (auto* e : deleted) {
    FreeEntry(e);
  }
  return erased;
}

void* GarbageAwareCacheShard::Value(Cache::Handle* handle) {
  return UnwrapHandle(handle)->value;
}

void GarbageAwareCacheShard::Erase(const Slice& key, uint32_t hash) {
  std::vector<GAHandle*> deleted;
  {
    MutexLock l(&mutex_);
    GAHandle* h = TableRemove(key, hash);
    if (h == nullptr) {
      return;
    }
    RemoveFromQueue(h);
    RemoveFromFileIndex(h);
    h->in_cache = false;
    usage_ -= h->charge;
    if (Unref(h)) {
      deleted.push_back(h);
    }
  }
  for (auto* e : deleted) {
    FreeEntry(e);
  }
}

void GarbageAwareCacheShard::SetCapacity(size_t capacity) {
  std::vector<GAHandle*> deleted;
  {
    MutexLock l(&mutex_);
    capacity_ = capacity;
    EvictIfNeeded(&deleted);
  }
  for (auto* e : deleted) {
    FreeEntry(e);
  }
}

void GarbageAwareCacheShard::SetStrictCapacityLimit(
    bool strict_capacity_limit) {
  MutexLock l(&mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

size_t GarbageAwareCacheShard::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t GarbageAwareCacheShard::GetCharge(Cache::Handle* handle) const {
  return UnwrapHandle(handle)->charge;
}

uint32_t GarbageAwareCacheShard::GetHash(Cache::Handle* handle) const {
  return UnwrapHandle(handle)->hash;
}

size_t GarbageAwareCacheShard::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  size_t pinned = 0;
  for (GAHandle* bucket : table_) {
    for (GAHandle* h = bucket; h != nullptr; h = h->next_hash) {
      if (h->refs > 1) {
        pinned += h->charge;
      }
    }
  }
  return pinned;
}

void GarbageAwareCacheShard::ApplyToAllCacheEntries(
    void (*callback)(void*, size_t), bool thread_safe) {
  if (thread_safe) {
    mutex_.Lock();
  }
  for (GAHandle* bucket : table_) {
    for (GAHandle* h = bucket; h != nullptr; h = h->next_hash) {
      callback(h->value, h->charge);
    }
  }
  if (thread_safe) {
    mutex_.Unlock();
  }
}

void GarbageAwareCacheShard::EraseUnRefEntries() {
  std::vector<GAHandle*> deleted;
  {
    MutexLock l(&mutex_);
    std::vector<GAHandle*> handles;
    handles.reserve(table_elems_);
    for (GAHandle* bucket : table_) {
      for (GAHandle* h = bucket; h != nullptr; h = h->next_hash) {
        handles.push_back(h);
      }
    }
    for (GAHandle* h : handles) {
      if (h->refs == 1) {
        RemoveFromCache(h);
        if (Unref(h)) {
          deleted.push_back(h);
        }
      }
    }
  }
  for (auto* e : deleted) {
    FreeEntry(e);
  }
}

std::string GarbageAwareCacheShard::GetPrintableOptions() const {
  char buffer[512];
  MutexLock l(&mutex_);
  snprintf(buffer, sizeof(buffer),
           "    capacity : %" ROCKSDB_PRIszt "\n"
           "    strict_capacity_limit : %d\n"
           "    log_interval : %" PRIu64 "\n",
           capacity_, strict_capacity_limit_, log_interval_);
  return std::string(buffer);
}

size_t GarbageAwareCacheShard::TEST_GetLRUSize() const {
  MutexLock l(&mutex_);
  return lru_.size();
}

void GarbageAwareCacheShard::MarkBlockCacheFilesObsolete(
    const std::vector<uint64_t>& file_numbers,
    const std::vector<uint64_t>& /*output_file_numbers*/, const char* reason,
    uint64_t job_id, Logger* info_log) {
  std::vector<GAHandle*> deleted;
  uint64_t marked_blocks = 0;
  uint64_t marked_bytes = 0;
  bool should_log = false;
  {
    MutexLock l(&mutex_);
    for (uint64_t file_number : file_numbers) {
      if (file_number != 0) {
        obsolete_files_.insert(file_number);
      }
      auto it = file_index_.find(file_number);
      if (it == file_index_.end()) {
        continue;
      }
      std::vector<GAHandle*> handles(it->second.begin(), it->second.end());
      for (GAHandle* h : handles) {
        if (!h->in_cache || h->file_number != file_number ||
            !h->garbage_aware) {
          continue;
        }
        const bool newly_marked = h->garbage_ratio < 1.0;
        h->garbage_ratio = 1.0;
        if (newly_marked) {
          ++marked_blocks;
          marked_bytes += h->charge;
        }

        if (newly_marked && h->refs == 1) {
          MoveToLRUTail(h);
        }
      }
    }
    const uint64_t old_marked_blocks = obsolete_marked_blocks_;
    obsolete_marked_blocks_ += marked_blocks;
    should_log = info_log != nullptr && log_interval_ > 0 && marked_blocks > 0 &&
                 old_marked_blocks / log_interval_ !=
                     obsolete_marked_blocks_ / log_interval_;
    EvictIfNeeded(&deleted);
  }
  for (auto* e : deleted) {
    FreeEntry(e);
  }
  if (should_log) {
    ROCKS_LOG_INFO(info_log,
                   "[GC_AWARE_BLOCK_CACHE_OBSOLETE] reason=%s job=%" PRIu64
                   " marked_blocks=%" PRIu64 " marked_bytes=%" PRIu64,
                   reason != nullptr ? reason : "unknown", job_id,
                   marked_blocks, marked_bytes);
  }
}

GarbageAwareCacheShard::ObsoleteSample
GarbageAwareCacheShard::GetObsoleteSample() const {
  ObsoleteSample sample;
  MutexLock l(&mutex_);
  for (GAHandle* head : table_) {
    for (GAHandle* h = head; h != nullptr; h = h->next_hash) {
      if (!h->in_cache || !h->is_data_block) {
        continue;
      }
      ++sample.tracked_blocks;
      sample.tracked_bytes += h->charge;
      if (h->garbage_aware && h->garbage_ratio >= 1.0) {
        ++sample.obsolete_blocks;
        sample.obsolete_bytes += h->charge;
        if (h->file_number != 0) {
          sample.obsolete_files.insert(h->file_number);
        }
      }
    }
  }
  sample.obsolete_file_count = sample.obsolete_files.size();
  return sample;
}

void GarbageAwareCacheShard::FreeEntry(GAHandle* h) {
  if (h->deleter != nullptr) {
    h->deleter(h->key_slice(), h->value);
  }
  delete h;
}

void GarbageAwareCacheShard::DeleteHandleOnly(GAHandle* h) { delete h; }

bool GarbageAwareCacheShard::IsNoHitHandle(Cache::Handle* handle) {
  return (reinterpret_cast<uintptr_t>(handle) & uintptr_t{1}) != 0;
}

GarbageAwareCacheShard::GAHandle* GarbageAwareCacheShard::UnwrapHandle(
    Cache::Handle* handle) {
  return reinterpret_cast<GAHandle*>(reinterpret_cast<uintptr_t>(handle) &
                                     ~uintptr_t{1});
}

Cache::Handle* GarbageAwareCacheShard::EncodeHandle(GAHandle* handle,
                                                    bool no_hit) {
  uintptr_t raw = reinterpret_cast<uintptr_t>(handle);
  assert((raw & uintptr_t{1}) == 0);
  if (no_hit) {
    raw |= uintptr_t{1};
  }
  return reinterpret_cast<Cache::Handle*>(raw);
}

bool GarbageAwareCacheShard::Unref(GAHandle* h) {
  assert(h->refs > 0);
  h->refs--;
  return h->refs == 0;
}

GarbageAwareCacheShard::GAHandle** GarbageAwareCacheShard::FindTablePointer(
    const Slice& key, uint32_t hash) {
  if (table_.empty()) {
    return nullptr;
  }
  GAHandle** ptr = &table_[hash & (table_.size() - 1)];
  while (*ptr != nullptr &&
         ((*ptr)->hash != hash || key != (*ptr)->key_slice())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

GarbageAwareCacheShard::GAHandle* GarbageAwareCacheShard::TableLookup(
    const Slice& key, uint32_t hash) {
  GAHandle** ptr = FindTablePointer(key, hash);
  return ptr == nullptr ? nullptr : *ptr;
}

GarbageAwareCacheShard::GAHandle* GarbageAwareCacheShard::TableInsert(
    GAHandle* h) {
  if (table_.empty()) {
    table_.assign(16, nullptr);
  }
  GAHandle** ptr = FindTablePointer(h->key_slice(), h->hash);
  GAHandle* old = *ptr;
  h->next_hash = old == nullptr ? nullptr : old->next_hash;
  *ptr = h;
  if (old == nullptr) {
    ++table_elems_;
    if (table_elems_ > table_.size()) {
      TableResize();
    }
  } else {
    old->next_hash = nullptr;
  }
  return old;
}

GarbageAwareCacheShard::GAHandle* GarbageAwareCacheShard::TableRemove(
    const Slice& key, uint32_t hash) {
  GAHandle** ptr = FindTablePointer(key, hash);
  if (ptr == nullptr || *ptr == nullptr) {
    return nullptr;
  }
  GAHandle* old = *ptr;
  *ptr = old->next_hash;
  old->next_hash = nullptr;
  --table_elems_;
  return old;
}

void GarbageAwareCacheShard::TableResize() {
  const size_t new_size = table_.empty() ? 16 : table_.size() * 2;
  std::vector<GAHandle*> old_table;
  old_table.swap(table_);
  table_.assign(new_size, nullptr);
  for (GAHandle* bucket : old_table) {
    while (bucket != nullptr) {
      GAHandle* h = bucket;
      bucket = bucket->next_hash;
      const size_t idx = h->hash & (table_.size() - 1);
      h->next_hash = table_[idx];
      table_[idx] = h;
    }
  }
}

void GarbageAwareCacheShard::RemoveFromQueue(GAHandle* h) {
  if (!h->in_queue) {
    return;
  }
  lru_.erase(h->lru_it);
  h->in_queue = false;
}

void GarbageAwareCacheShard::AddToQueue(GAHandle* h, bool promote) {
  assert(h->refs == 1);
  if (h->in_queue) {
    return;
  }
  if (promote) {
    lru_.push_front(h);
    h->lru_it = lru_.begin();
  } else {
    lru_.push_back(h);
    h->lru_it = --lru_.end();
  }
  h->in_queue = true;
}

void GarbageAwareCacheShard::RemoveFromCache(GAHandle* h) {
  if (!h->in_cache) {
    return;
  }
  RemoveFromQueue(h);
  RemoveFromFileIndex(h);
  TableRemove(h->key_slice(), h->hash);
  h->in_cache = false;
  usage_ -= h->charge;
}

// Detach an entry that TableInsert has already unlinked from the hash table
// (the entry it replaced during a same-key overwrite).  Calling the regular
// RemoveFromCache here would issue TableRemove(key, hash), which matches by
// key+hash and would wrongly delete the freshly inserted replacement entry
// that now occupies that key.  So skip TableRemove and only undo the rest of
// the cache bookkeeping.
void GarbageAwareCacheShard::RemoveReplacedFromCache(GAHandle* h) {
  if (!h->in_cache) {
    return;
  }
  RemoveFromQueue(h);
  RemoveFromFileIndex(h);
  h->in_cache = false;
  usage_ -= h->charge;
}

void GarbageAwareCacheShard::MoveToLRUTail(GAHandle* h) {
  if (!h->in_cache || h->refs > 1) {
    return;
  }
  RemoveFromQueue(h);
  AddToQueue(h, false /* promote */);
  demotions_++;
  RecordTick(statistics_, GC_AWARE_CACHE_DEMOTE);
}

bool GarbageAwareCacheShard::IsObsoleteFile(uint64_t file_number) const {
  return file_number != 0 &&
         obsolete_files_.find(file_number) != obsolete_files_.end();
}

void GarbageAwareCacheShard::AddToFileIndex(GAHandle* h) {
  if (h->file_number == 0 || !h->is_data_block || !h->garbage_aware) {
    return;
  }
  file_index_[h->file_number].insert(h);
}

void GarbageAwareCacheShard::RemoveFromFileIndex(GAHandle* h) {
  if (h->file_number == 0 || !h->is_data_block || !h->garbage_aware) {
    return;
  }
  auto it = file_index_.find(h->file_number);
  if (it == file_index_.end()) {
    return;
  }
  it->second.erase(h);
  if (it->second.empty()) {
    file_index_.erase(it);
  }
}

GarbageAwareCacheShard::GAHandle* GarbageAwareCacheShard::FindLRUVictim() {
  if (lru_.empty()) {
    return nullptr;
  }
  GAHandle* h = nullptr;
  for (auto it = lru_.rbegin(); it != lru_.rend(); ++it) {
    if ((*it)->priority != Cache::Priority::HIGH) {
      h = *it;
      break;
    }
  }
  if (h == nullptr) {
    h = lru_.back();
  }
  assert(h->refs == 1);
  return h;
}

void GarbageAwareCacheShard::EvictIfNeeded(std::vector<GAHandle*>* deleted) {
  if (usage_ <= capacity_) {
    return;
  }

  while (usage_ > capacity_) {
    GAHandle* h = FindLRUVictim();
    if (h == nullptr) {
      break;
    }
    if (h->garbage_aware && h->garbage_ratio >= 1.0) {
      low_score_evictions_++;
      RecordTick(statistics_, GC_AWARE_CACHE_EVICT_LOW_SCORE);
    }
    RemoveFromCache(h);
    if (Unref(h)) {
      deleted->push_back(h);
    }
  }
}

void GarbageAwareCacheShard::MaybeLogLocked(
    const BlockCacheMetadata* metadata) {
  if (metadata == nullptr || metadata->info_log == nullptr ||
      log_interval_ == 0) {
    return;
  }
  const uint64_t events = demotions_ + low_score_evictions_;
  if (events == 0 || events % log_interval_ != 0) {
    return;
  }
  ROCKS_LOG_INFO(
      metadata->info_log,
      "[GC_AWARE_BLOCK_CACHE] usage=%" ROCKSDB_PRIszt
      " lru_size=%" ROCKSDB_PRIszt
      " move_obsolete_tail=%" PRIu64 " evict_obsolete=%" PRIu64,
      usage_, lru_.size(), demotions_, low_score_evictions_);
}

GarbageAwareCache::GarbageAwareCache(const GarbageAwareCacheOptions& options,
                                     int num_shard_bits)
    : ShardedCache(options.capacity, num_shard_bits,
                   options.strict_capacity_limit, options.memory_allocator) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<GarbageAwareCacheShard*>(
      port::cacheline_aligned_alloc(sizeof(GarbageAwareCacheShard) *
                                    num_shards_));
  const size_t per_shard =
      (options.capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; ++i) {
    new (&shards_[i]) GarbageAwareCacheShard(
        per_shard, options.strict_capacity_limit, options.log_interval);
  }
}

GarbageAwareCache::~GarbageAwareCache() {
  if (shards_ != nullptr) {
    for (int i = 0; i < num_shards_; ++i) {
      shards_[i].~GarbageAwareCacheShard();
    }
    port::cacheline_aligned_free(shards_);
  }
}

CacheShard* GarbageAwareCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* GarbageAwareCache::GetShard(int shard) const {
  return reinterpret_cast<const CacheShard*>(&shards_[shard]);
}

void* GarbageAwareCache::Value(Handle* handle) {
  return reinterpret_cast<GarbageAwareCacheShard*>(GetShard(ShardForHash(
             GetHash(handle))))
      ->Value(handle);
}

size_t GarbageAwareCache::GetCharge(Handle* handle) const {
  return reinterpret_cast<const GarbageAwareCacheShard*>(
             GetShard(ShardForHash(GetHash(handle))))
      ->GetCharge(handle);
}

uint32_t GarbageAwareCache::GetHash(Handle* handle) const {
  return reinterpret_cast<const GarbageAwareCacheShard*>(GetShard(0))
      ->GetHash(handle);
}

void GarbageAwareCache::DisownData() { shards_ = nullptr; }

Cache::Handle* GarbageAwareCache::Lookup(const Slice& key, Statistics* stats) {
  uint32_t hash = HashSlice(key);
  return Lookup(key, hash, true, stats);
}

Cache::Handle* GarbageAwareCache::Lookup(const Slice& key, uint32_t hash,
                                         bool record_hit, Statistics* stats) {
  return reinterpret_cast<GarbageAwareCacheShard*>(GetShard(ShardForHash(hash)))
      ->Lookup(key, hash, record_hit, stats);
}

Status GarbageAwareCache::InsertWithMetadata(
    const Slice& key, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value),
    const BlockCacheMetadata* metadata, Handle** handle, Priority priority) {
  uint32_t hash = HashSlice(key);
  return reinterpret_cast<GarbageAwareCacheShard*>(GetShard(ShardForHash(hash)))
      ->InsertWithMetadata(key, value, charge, deleter, metadata, hash, handle,
                           priority);
}

void GarbageAwareCache::MarkBlockCacheFilesObsolete(
    const std::vector<uint64_t>& file_numbers,
    const std::vector<uint64_t>& output_file_numbers, const char* reason,
    uint64_t job_id, Logger* info_log) {
  // Optimization only: relocate obsolete blocks toward the LRU tail.  The
  // diagnostic residency sample is emitted separately through
  // LogBlockCacheObsoleteSample so that it stays off the GC hot path unless
  // explicitly enabled.
  for (int i = 0; i < num_shards_; ++i) {
    shards_[i].MarkBlockCacheFilesObsolete(file_numbers, output_file_numbers,
                                           reason, job_id, info_log);
  }
}

void GarbageAwareCache::LogBlockCacheObsoleteSample(const char* reason,
                                                    uint64_t job_id,
                                                    Logger* info_log) {
  if (info_log == nullptr) {
    return;
  }
  GarbageAwareCacheShard::ObsoleteSample sample;
  std::unordered_set<uint64_t> obsolete_files;
  for (int i = 0; i < num_shards_; ++i) {
    auto shard_sample = shards_[i].GetObsoleteSample();
    sample.tracked_blocks += shard_sample.tracked_blocks;
    sample.tracked_bytes += shard_sample.tracked_bytes;
    sample.obsolete_blocks += shard_sample.obsolete_blocks;
    sample.obsolete_bytes += shard_sample.obsolete_bytes;
    obsolete_files.insert(shard_sample.obsolete_files.begin(),
                          shard_sample.obsolete_files.end());
  }
  sample.obsolete_file_count = obsolete_files.size();
  const double obsolete_block_ratio =
      sample.tracked_blocks == 0
          ? 0.0
          : static_cast<double>(sample.obsolete_blocks) /
                static_cast<double>(sample.tracked_blocks);
  const double obsolete_byte_ratio =
      sample.tracked_bytes == 0
          ? 0.0
          : static_cast<double>(sample.obsolete_bytes) /
                static_cast<double>(sample.tracked_bytes);
  const uint64_t sample_id = obsolete_sample_id_.fetch_add(1) + 1;
  // Only emit fields that downstream tooling actually consumes.  The log
  // timestamp is recovered from the line prefix by the parser, so we do not
  // fabricate ts_us/mean/peak/top-file values here.
  ROCKS_LOG_INFO(info_log,
                 "[BLOCK_CACHE_OBSOLETE_SAMPLE] sample_id=%" PRIu64
                 " job=%" PRIu64 " reason=%s tracked_blocks=%" PRIu64
                 " tracked_bytes=%" PRIu64 " obsolete_blocks=%" PRIu64
                 " obsolete_bytes=%" PRIu64 " obsolete_block_ratio=%.6f"
                 " obsolete_byte_ratio=%.6f obsolete_file_count=%" PRIu64,
                 sample_id, job_id, reason == nullptr ? "unknown" : reason,
                 sample.tracked_blocks, sample.tracked_bytes,
                 sample.obsolete_blocks, sample.obsolete_bytes,
                 obsolete_block_ratio, obsolete_byte_ratio,
                 sample.obsolete_file_count);
}

size_t GarbageAwareCache::TEST_GetLRUSize() const {
  size_t total = 0;
  for (int i = 0; i < num_shards_; ++i) {
    total += shards_[i].TEST_GetLRUSize();
  }
  return total;
}

uint32_t GarbageAwareCache::ShardForHash(uint32_t hash) const {
  int num_shard_bits = GetNumShardBits();
  return (num_shard_bits > 0) ? (hash >> (32 - num_shard_bits)) : 0;
}

std::shared_ptr<Cache> NewGarbageAwareCache(
    const GarbageAwareCacheOptions& cache_opts) {
  return NewGarbageAwareCache(cache_opts.capacity, cache_opts.num_shard_bits,
                              cache_opts.strict_capacity_limit,
                              cache_opts.log_interval,
                              cache_opts.memory_allocator);
}

std::shared_ptr<Cache> NewGarbageAwareCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    uint64_t log_interval,
    std::shared_ptr<MemoryAllocator> memory_allocator) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  GarbageAwareCacheOptions options;
  options.capacity = capacity;
  options.num_shard_bits = num_shard_bits;
  options.strict_capacity_limit = strict_capacity_limit;
  options.log_interval = log_interval;
  options.memory_allocator = std::move(memory_allocator);
  return std::make_shared<GarbageAwareCache>(options, num_shard_bits);
}

}  // namespace TERARKDB_NAMESPACE
