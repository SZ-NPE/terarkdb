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

namespace {

double ClampRatio(double ratio) {
  if (ratio < 0.0) {
    return 0.0;
  }
  if (ratio > 1.0) {
    return 1.0;
  }
  return ratio;
}

}  // namespace

GarbageAwareCacheShard::GarbageAwareCacheShard(
    size_t capacity, bool strict_capacity_limit, double admission_ratio,
    double demote_score_threshold, uint64_t log_interval, bool enable_aging,
    uint64_t aging_interval)
    : capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      admission_ratio_(ClampRatio(admission_ratio)),
      demote_score_threshold_(std::max(0.0, demote_score_threshold)),
      log_interval_(log_interval),
      enable_aging_(enable_aging && aging_interval > 0),
      aging_interval_(aging_interval) {}

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
    h->is_blob_file = metadata->is_blob_file;
    h->garbage_aware = metadata->is_blob_file && metadata->is_data_block;
    h->garbage_ratio = ClampRatio(metadata->garbage_ratio);
    h->score = h->access_freq * (1.0 - h->garbage_ratio);
  }

  {
    MutexLock l(&mutex_);
    AdvanceAgingEpoch();
    h->last_epoch = current_epoch_;
    if (metadata != nullptr && metadata->statistics != nullptr) {
      statistics_ = metadata->statistics;
    }
    auto old = table_.find(h->key);
    if (old != table_.end()) {
      replaced = old->second;
      RemoveFromCache(replaced);
      replaced_last_reference = Unref(replaced);
    }
    table_[h->key] = h;
    AddToFileIndex(h);
    usage_ += h->charge;
    admission_usage_ += h->charge;
    if (handle == nullptr) {
      AddToQueue(h);
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
        if (replaced->refs == 0) {
          replaced->refs = 1;
        } else {
          ++replaced->refs;
        }
        replaced->in_cache = true;
        table_[replaced->key] = replaced;
        AddToFileIndex(replaced);
        usage_ += replaced->charge;
        if (replaced->in_admission) {
          admission_usage_ += replaced->charge;
        } else {
          probation_usage_ += replaced->charge;
        }
        if (replaced->refs == 1) {
          AddToQueue(replaced, false /* promote */);
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
                                              uint32_t /*hash*/,
                                              bool record_hit,
                                              Statistics* stats) {
  MutexLock l(&mutex_);
  auto it = table_.find(key.ToString());
  if (it == table_.end()) {
    return nullptr;
  }
  GAHandle* h = it->second;
  if (record_hit) {
    AdvanceAgingEpoch();
  }
  if (h->refs == 1 && h->in_queue) {
    RemoveFromQueue(h);
  }
  h->refs++;
  if (record_hit) {
    ApplyAccessFreqAging(h);
    h->access_freq++;
    UpdateScore(h);
    if (h->in_admission) {
      admission_hits_++;
      RecordTick(stats, GC_AWARE_CACHE_ADMISSION_HIT);
    } else {
      probation_hits_++;
      RecordTick(stats, GC_AWARE_CACHE_PROBATION_HIT);
    }
  } else {
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
      if (!no_hit_release && h->garbage_aware && h->in_admission &&
          h->priority != Cache::Priority::HIGH &&
          (h->garbage_ratio >= 1.0 || h->score <= demote_score_threshold_)) {
        MoveToProbation(h);
      } else if (h->in_queue) {
        // No-hit lookups should not promote or demote by themselves. If the
        // entry remained queued, leave its previous queue position intact.
        assert(no_hit_release);
      }
      if (!h->in_queue) {
        AddToQueue(h, !no_hit_release /* promote */);
      }
    }
    EvictIfNeeded(&deleted);
  }
  for (auto* e : deleted) {
    FreeEntry(e);
  }
  return erased;
}

void* GarbageAwareCacheShard::Value(Cache::Handle* handle) {
  return UnwrapHandle(handle)->value;
}

void GarbageAwareCacheShard::Erase(const Slice& key, uint32_t /*hash*/) {
  std::vector<GAHandle*> deleted;
  {
    MutexLock l(&mutex_);
    auto it = table_.find(key.ToString());
    if (it == table_.end()) {
      return;
    }
    GAHandle* h = it->second;
    RemoveFromCache(h);
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
  for (const auto& item : table_) {
    if (item.second->refs > 1) {
      pinned += item.second->charge;
    }
  }
  return pinned;
}

void GarbageAwareCacheShard::ApplyToAllCacheEntries(
    void (*callback)(void*, size_t), bool thread_safe) {
  if (thread_safe) {
    mutex_.Lock();
  }
  for (const auto& item : table_) {
    callback(item.second->value, item.second->charge);
  }
  if (thread_safe) {
    mutex_.Unlock();
  }
}

void GarbageAwareCacheShard::EraseUnRefEntries() {
  std::vector<GAHandle*> deleted;
  {
    MutexLock l(&mutex_);
    for (auto it = table_.begin(); it != table_.end();) {
      GAHandle* h = it->second;
      ++it;
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
           "    admission_ratio : %.2f\n"
           "    demote_score_threshold : %.2f\n"
           "    log_interval : %" PRIu64 "\n"
           "    enable_aging : %d\n"
           "    aging_interval : %" PRIu64 "\n",
           capacity_, strict_capacity_limit_, admission_ratio_,
           demote_score_threshold_, log_interval_, enable_aging_,
           aging_interval_);
  return std::string(buffer);
}

size_t GarbageAwareCacheShard::TEST_GetAdmissionSize() const {
  MutexLock l(&mutex_);
  return admission_lru_.size();
}

size_t GarbageAwareCacheShard::TEST_GetProbationSize() const {
  MutexLock l(&mutex_);
  return probation_scores_.size();
}

void GarbageAwareCacheShard::MarkBlockCacheFilesObsolete(
    const std::vector<uint64_t>& file_numbers,
    const std::vector<uint64_t>& /*output_file_numbers*/, const char* reason,
    uint64_t job_id, Logger* info_log) {
  std::vector<GAHandle*> deleted;
  uint64_t marked_blocks = 0;
  uint64_t marked_bytes = 0;
  {
    MutexLock l(&mutex_);
    for (uint64_t file_number : file_numbers) {
      auto it = file_index_.find(file_number);
      if (it == file_index_.end()) {
        continue;
      }
      std::vector<GAHandle*> handles(it->second.begin(), it->second.end());
      for (GAHandle* h : handles) {
        if (!h->in_cache || h->file_number != file_number ||
            !h->is_data_block) {
          continue;
        }
        h->garbage_aware = true;
        h->garbage_ratio = 1.0;
        UpdateScore(h);
        ++marked_blocks;
        marked_bytes += h->charge;

        // Pinned handles (refs > 1) are left resident and will be queued or
        // demoted safely when the caller releases them. Unpinned non-HIGH data
        // blocks can be moved to probation immediately so future cache pressure
        // evicts them ahead of useful blocks.
        if (h->refs == 1 && h->in_admission &&
            h->priority != Cache::Priority::HIGH) {
          MoveToProbation(h);
        }
      }
    }
    obsolete_marked_blocks_ += marked_blocks;
    obsolete_marked_bytes_ += marked_bytes;
    EvictIfNeeded(&deleted);
  }
  for (auto* e : deleted) {
    FreeEntry(e);
  }
  if (info_log != nullptr && marked_blocks > 0) {
    ROCKS_LOG_INFO(info_log,
                   "[GC_AWARE_BLOCK_CACHE_OBSOLETE] reason=%s job=%" PRIu64
                   " marked_blocks=%" PRIu64 " marked_bytes=%" PRIu64,
                   reason != nullptr ? reason : "unknown", job_id,
                   marked_blocks, marked_bytes);
  }
}

void GarbageAwareCacheShard::LogBlockCacheObsoleteSample(const char* reason,
                                                         uint64_t job_id,
                                                         Logger* info_log) {
  if (info_log == nullptr) {
    return;
  }
  MutexLock l(&mutex_);
  ROCKS_LOG_INFO(info_log,
                 "[GC_AWARE_BLOCK_CACHE_OBSOLETE_SAMPLE] reason=%s job=%" PRIu64
                 " total_marked_blocks=%" PRIu64
                 " total_marked_bytes=%" PRIu64,
                 reason != nullptr ? reason : "unknown", job_id,
                 obsolete_marked_blocks_, obsolete_marked_bytes_);
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

void GarbageAwareCacheShard::RemoveFromQueue(GAHandle* h) {
  if (!h->in_queue) {
    return;
  }
  if (h->in_admission) {
    admission_lru_.erase(h->lru_it);
  } else {
    probation_scores_.erase(h->score_it);
  }
  h->in_queue = false;
}

void GarbageAwareCacheShard::AddToQueue(GAHandle* h, bool promote) {
  assert(h->refs == 1);
  if (h->in_queue) {
    return;
  }
  if (h->in_admission) {
    if (promote) {
      admission_lru_.push_front(h);
      h->lru_it = admission_lru_.begin();
    } else {
      admission_lru_.push_back(h);
      h->lru_it = --admission_lru_.end();
    }
  } else {
    h->score_seq = next_score_seq_++;
    h->score_it = probation_scores_.insert({h->score, h->score_seq, h}).first;
  }
  h->in_queue = true;
}

void GarbageAwareCacheShard::RemoveFromCache(GAHandle* h) {
  if (!h->in_cache) {
    return;
  }
  RemoveFromQueue(h);
  RemoveFromFileIndex(h);
  table_.erase(h->key);
  h->in_cache = false;
  usage_ -= h->charge;
  if (h->in_admission) {
    admission_usage_ -= h->charge;
  } else {
    probation_usage_ -= h->charge;
  }
}

void GarbageAwareCacheShard::MoveToProbation(GAHandle* h) {
  if (!h->in_admission || !h->garbage_aware ||
      h->priority == Cache::Priority::HIGH) {
    return;
  }
  UpdateScore(h);
  RemoveFromQueue(h);
  h->in_admission = false;
  admission_usage_ -= h->charge;
  probation_usage_ += h->charge;
  AddToQueue(h);
  demotions_++;
  RecordTick(statistics_, GC_AWARE_CACHE_DEMOTE);
}

void GarbageAwareCacheShard::AdvanceAgingEpoch() {
  if (!enable_aging_) {
    return;
  }
  ++operation_count_;
  current_epoch_ = operation_count_ / aging_interval_;
}

void GarbageAwareCacheShard::ApplyAccessFreqAging(GAHandle* h) {
  if (!enable_aging_ || h->last_epoch >= current_epoch_) {
    return;
  }
  const uint64_t steps = current_epoch_ - h->last_epoch;
  if (steps >= 63) {
    h->access_freq = 1;
  } else {
    h->access_freq = std::max<uint64_t>(1, h->access_freq >> steps);
  }
  h->last_epoch = current_epoch_;
}

void GarbageAwareCacheShard::UpdateScore(GAHandle* h) {
  const bool reinsert_score = h->in_queue && !h->in_admission;
  if (reinsert_score) {
    probation_scores_.erase(h->score_it);
  }
  ApplyAccessFreqAging(h);
  h->score = h->access_freq * (1.0 - h->garbage_ratio);
  if (reinsert_score) {
    h->score_seq = next_score_seq_++;
    h->score_it = probation_scores_.insert({h->score, h->score_seq, h}).first;
  }
}

void GarbageAwareCacheShard::RefreshProbationScoresForAging() {
  if (!enable_aging_ || probation_scores_.empty()) {
    return;
  }
  std::vector<GAHandle*> handles;
  handles.reserve(probation_scores_.size());
  for (const auto& score : probation_scores_) {
    handles.push_back(score.handle);
  }
  probation_scores_.clear();
  for (auto* h : handles) {
    ApplyAccessFreqAging(h);
    h->score = h->access_freq * (1.0 - h->garbage_ratio);
    h->score_seq = next_score_seq_++;
    h->score_it = probation_scores_.insert({h->score, h->score_seq, h}).first;
  }
}

void GarbageAwareCacheShard::AddToFileIndex(GAHandle* h) {
  if (h->file_number == 0 || !h->is_data_block) {
    return;
  }
  file_index_[h->file_number].insert(h);
}

void GarbageAwareCacheShard::RemoveFromFileIndex(GAHandle* h) {
  if (h->file_number == 0 || !h->is_data_block) {
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

GarbageAwareCacheShard::GAHandle* GarbageAwareCacheShard::FindAdmissionVictim(
    bool require_garbage_aware, bool require_low_priority) {
  for (auto it = admission_lru_.rbegin(); it != admission_lru_.rend(); ++it) {
    GAHandle* h = *it;
    if (h->refs > 1) {
      continue;
    }
    if (require_low_priority && h->priority == Cache::Priority::HIGH) {
      continue;
    }
    if (require_garbage_aware != h->garbage_aware) {
      continue;
    }
    return h;
  }
  return nullptr;
}

void GarbageAwareCacheShard::EvictIfNeeded(std::vector<GAHandle*>* deleted) {
  const size_t admission_capacity =
      static_cast<size_t>(capacity_ * admission_ratio_);
  while (admission_usage_ > admission_capacity && !admission_lru_.empty()) {
    // Admission quota pressure is a soft partitioning signal. Prefer demoting
    // low-priority garbage-aware blocks, then evict low-priority ordinary
    // blocks. High-priority index/filter blocks are not admission-quota victims;
    // they are only considered later if the hard total capacity is exceeded.
    GAHandle* h = FindAdmissionVictim(true /* require_garbage_aware */,
                                      true /* require_low_priority */);
    if (h != nullptr) {
      MoveToProbation(h);
      continue;
    }
    h = FindAdmissionVictim(false /* require_garbage_aware */,
                            true /* require_low_priority */);
    if (h != nullptr) {
      RemoveFromCache(h);
      if (Unref(h)) {
        deleted->push_back(h);
      }
      continue;
    }
    break;
  }

  const size_t probation_capacity = capacity_ - admission_capacity;
  if ((probation_usage_ > probation_capacity && usage_ > capacity_) ||
      usage_ > capacity_) {
    RefreshProbationScoresForAging();
  }
  while (probation_usage_ > probation_capacity && usage_ > capacity_ &&
         !probation_scores_.empty()) {
    auto victim_it = probation_scores_.begin();
    while (victim_it != probation_scores_.end() && victim_it->handle->refs > 1) {
      ++victim_it;
    }
    if (victim_it == probation_scores_.end()) {
      break;
    }
    GAHandle* h = victim_it->handle;
    RemoveFromCache(h);
    if (Unref(h)) {
      deleted->push_back(h);
    }
    low_score_evictions_++;
    RecordTick(statistics_, GC_AWARE_CACHE_EVICT_LOW_SCORE);
  }

  while (usage_ > capacity_) {
    GAHandle* h = nullptr;
    if (!probation_scores_.empty()) {
      auto victim_it = probation_scores_.begin();
      while (victim_it != probation_scores_.end() &&
             victim_it->handle->refs > 1) {
        ++victim_it;
      }
      h = victim_it == probation_scores_.end() ? nullptr : victim_it->handle;
    }
    if (h != nullptr) {
      low_score_evictions_++;
      RecordTick(statistics_, GC_AWARE_CACHE_EVICT_LOW_SCORE);
    } else {
      h = FindAdmissionVictim(true /* require_garbage_aware */,
                              true /* require_low_priority */);
      if (h != nullptr) {
        MoveToProbation(h);
        continue;
      }
      h = FindAdmissionVictim(false /* require_garbage_aware */,
                              true /* require_low_priority */);
      if (h == nullptr) {
        // Hard capacity fallback: high-priority blocks can still be evicted if
        // no low-priority victim exists and the cache is over capacity.
        h = FindAdmissionVictim(true /* require_garbage_aware */,
                                false /* require_low_priority */);
      }
      if (h == nullptr) {
        h = FindAdmissionVictim(false /* require_garbage_aware */,
                                false /* require_low_priority */);
      }
      if (h == nullptr) {
        break;
      }
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
      " admission_usage=%" ROCKSDB_PRIszt
      " probation_usage=%" ROCKSDB_PRIszt
      " admission_hits=%" PRIu64 " probation_hits=%" PRIu64
      " demote=%" PRIu64 " evict_low_score=%" PRIu64,
      usage_, admission_usage_, probation_usage_, admission_hits_,
      probation_hits_, demotions_, low_score_evictions_);
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
        per_shard, options.strict_capacity_limit, options.admission_ratio,
        options.demote_score_threshold, options.log_interval,
        options.enable_aging, options.aging_interval);
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
  for (int i = 0; i < num_shards_; ++i) {
    shards_[i].MarkBlockCacheFilesObsolete(file_numbers, output_file_numbers,
                                           reason, job_id, info_log);
  }
}

void GarbageAwareCache::LogBlockCacheObsoleteSample(const char* reason,
                                                    uint64_t job_id,
                                                    Logger* info_log) {
  for (int i = 0; i < num_shards_; ++i) {
    shards_[i].LogBlockCacheObsoleteSample(reason, job_id, info_log);
  }
}

size_t GarbageAwareCache::TEST_GetAdmissionSize() const {
  size_t total = 0;
  for (int i = 0; i < num_shards_; ++i) {
    total += shards_[i].TEST_GetAdmissionSize();
  }
  return total;
}

size_t GarbageAwareCache::TEST_GetProbationSize() const {
  size_t total = 0;
  for (int i = 0; i < num_shards_; ++i) {
    total += shards_[i].TEST_GetProbationSize();
  }
  return total;
}

uint32_t GarbageAwareCache::ShardForHash(uint32_t hash) const {
  int num_shard_bits = GetNumShardBits();
  return (num_shard_bits > 0) ? (hash >> (32 - num_shard_bits)) : 0;
}

std::shared_ptr<Cache> NewGarbageAwareCache(
    const GarbageAwareCacheOptions& cache_opts) {
  return NewGarbageAwareCache(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.admission_ratio,
      cache_opts.demote_score_threshold, cache_opts.log_interval,
      cache_opts.memory_allocator, cache_opts.enable_aging,
      cache_opts.aging_interval);
}

std::shared_ptr<Cache> NewGarbageAwareCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double admission_ratio, double demote_score_threshold, uint64_t log_interval,
    std::shared_ptr<MemoryAllocator> memory_allocator) {
  return NewGarbageAwareCache(capacity, num_shard_bits, strict_capacity_limit,
                              admission_ratio, demote_score_threshold,
                              log_interval, std::move(memory_allocator),
                              false /* enable_aging */,
                              10000 /* aging_interval */);
}

std::shared_ptr<Cache> NewGarbageAwareCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double admission_ratio, double demote_score_threshold, uint64_t log_interval,
    std::shared_ptr<MemoryAllocator> memory_allocator, bool enable_aging,
    uint64_t aging_interval) {
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
  options.admission_ratio = admission_ratio;
  options.demote_score_threshold = demote_score_threshold;
  options.log_interval = log_interval;
  options.enable_aging = enable_aging;
  options.aging_interval = aging_interval;
  options.memory_allocator = std::move(memory_allocator);
  return std::make_shared<GarbageAwareCache>(options, num_shard_bits);
}

}  // namespace TERARKDB_NAMESPACE
