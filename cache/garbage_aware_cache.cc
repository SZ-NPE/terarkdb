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

GarbageAwareCache::GarbageAwareCache(const GarbageAwareCacheOptions& options)
    : Cache(options.memory_allocator),
      capacity_(options.capacity),
      strict_capacity_limit_(options.strict_capacity_limit),
      admission_ratio_(ClampRatio(options.admission_ratio)),
      demote_score_threshold_(std::max(0.0, options.demote_score_threshold)),
      log_interval_(options.log_interval),
      last_id_(1) {}

GarbageAwareCache::~GarbageAwareCache() { EraseUnRefEntries(); }

Status GarbageAwareCache::Insert(
    const Slice& key, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value), Handle** handle,
    Priority priority) {
  return InsertImpl(key, value, charge, deleter, nullptr, handle, priority);
}

Status GarbageAwareCache::InsertWithMetadata(
    const Slice& key, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value),
    const BlockCacheMetadata* metadata, Handle** handle, Priority priority) {
  return InsertImpl(key, value, charge, deleter, metadata, handle, priority);
}

Status GarbageAwareCache::InsertImpl(
    const Slice& key, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value),
    const BlockCacheMetadata* metadata, Handle** handle, Priority priority) {
  if (handle != nullptr) {
    *handle = nullptr;
  }
  if (strict_capacity_limit_ && charge > capacity_) {
    return Status::Incomplete("Insert failed due to strict capacity limit");
  }

  std::vector<GAHandle*> deleted;
  GAHandle* h = new GAHandle();
  h->key.assign(key.data(), key.size());
  h->value = value;
  h->deleter = deleter;
  h->charge = charge;
  h->refs = handle == nullptr ? 1 : 2;
  h->in_cache = true;
  h->priority = priority;
  if (metadata != nullptr) {
    h->garbage_aware = metadata->is_blob_file && metadata->is_data_block;
    h->garbage_ratio = ClampRatio(metadata->garbage_ratio);
    h->score = h->access_freq * (1.0 - h->garbage_ratio);
  }

  {
    MutexLock l(&mutex_);
    if (metadata != nullptr && metadata->statistics != nullptr) {
      statistics_ = metadata->statistics;
    }
    auto old = table_.find(h->key);
    if (old != table_.end()) {
      RemoveFromCache(old->second);
      if (Unref(old->second)) {
        deleted.push_back(old->second);
      }
    }
    table_[h->key] = h;
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
    MaybeLogLocked(metadata);
  }

  for (auto* e : deleted) {
    FreeEntry(e);
  }

  if (handle != nullptr) {
    *handle = h;
  }
  return Status::OK();
}

Cache::Handle* GarbageAwareCache::Lookup(const Slice& key, Statistics* stats) {
  return Lookup(key, 0, true, stats);
}

Cache::Handle* GarbageAwareCache::Lookup(const Slice& key, uint32_t /*hash*/,
                                         bool record_hit, Statistics* stats) {
  MutexLock l(&mutex_);
  auto it = table_.find(key.ToString());
  if (it == table_.end()) {
    return nullptr;
  }
  GAHandle* h = it->second;
  if (h->refs == 1 && h->in_queue) {
    RemoveFromQueue(h);
  }
  h->refs++;
  if (record_hit) {
    h->access_freq++;
    UpdateScore(h);
    if (h->in_admission) {
      admission_hits_++;
      RecordTick(stats, GC_AWARE_CACHE_ADMISSION_HIT);
    } else {
      probation_hits_++;
      RecordTick(stats, GC_AWARE_CACHE_PROBATION_HIT);
    }
  }
  return h;
}

bool GarbageAwareCache::Ref(Handle* handle) {
  MutexLock l(&mutex_);
  GAHandle* h = reinterpret_cast<GAHandle*>(handle);
  if (!h->in_cache) {
    return false;
  }
  if (h->refs == 1 && h->in_queue) {
    RemoveFromQueue(h);
  }
  h->refs++;
  return true;
}

bool GarbageAwareCache::Release(Handle* handle, bool force_erase) {
  std::vector<GAHandle*> deleted;
  bool erased = false;
  {
    MutexLock l(&mutex_);
    GAHandle* h = reinterpret_cast<GAHandle*>(handle);
    if (force_erase && h->in_cache) {
      RemoveFromCache(h);
      erased = true;
    }
    if (Unref(h)) {
      deleted.push_back(h);
    } else if (h->refs == 1 && h->in_cache && !h->in_queue) {
      if (h->garbage_aware && h->in_admission &&
          h->score < demote_score_threshold_) {
        MoveToProbation(h);
      }
      AddToQueue(h);
    }
    EvictIfNeeded(&deleted);
  }
  for (auto* e : deleted) {
    FreeEntry(e);
  }
  return erased;
}

void* GarbageAwareCache::Value(Handle* handle) {
  return reinterpret_cast<GAHandle*>(handle)->value;
}

void GarbageAwareCache::Erase(const Slice& key) { Erase(key, 0); }

void GarbageAwareCache::Erase(const Slice& key, uint32_t /*hash*/) {
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

uint64_t GarbageAwareCache::NewId() {
  return last_id_.fetch_add(1, std::memory_order_relaxed);
}

void GarbageAwareCache::SetCapacity(size_t capacity) {
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

void GarbageAwareCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  MutexLock l(&mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

bool GarbageAwareCache::HasStrictCapacityLimit() const {
  MutexLock l(&mutex_);
  return strict_capacity_limit_;
}

size_t GarbageAwareCache::GetCapacity() const {
  MutexLock l(&mutex_);
  return capacity_;
}

size_t GarbageAwareCache::GetUsage() const {
  MutexLock l(&mutex_);
  return usage_;
}

size_t GarbageAwareCache::GetUsage(Handle* handle) const {
  return reinterpret_cast<GAHandle*>(handle)->charge;
}

size_t GarbageAwareCache::GetPinnedUsage() const {
  MutexLock l(&mutex_);
  size_t pinned = 0;
  for (const auto& item : table_) {
    if (item.second->refs > 1) {
      pinned += item.second->charge;
    }
  }
  return pinned;
}

void GarbageAwareCache::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                               bool thread_safe) {
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

void GarbageAwareCache::EraseUnRefEntries() {
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

std::string GarbageAwareCache::GetPrintableOptions() const {
  char buffer[512];
  MutexLock l(&mutex_);
  snprintf(buffer, sizeof(buffer),
           "    capacity : %" ROCKSDB_PRIszt "\n"
           "    strict_capacity_limit : %d\n"
           "    admission_ratio : %.2f\n"
           "    demote_score_threshold : %.2f\n"
           "    log_interval : %" PRIu64 "\n",
           capacity_, strict_capacity_limit_, admission_ratio_,
           demote_score_threshold_, log_interval_);
  return std::string(buffer);
}

size_t GarbageAwareCache::TEST_GetAdmissionSize() const {
  MutexLock l(&mutex_);
  return admission_lru_.size();
}

size_t GarbageAwareCache::TEST_GetProbationSize() const {
  MutexLock l(&mutex_);
  return probation_scores_.size();
}

void GarbageAwareCache::FreeEntry(GAHandle* h) {
  if (h->deleter != nullptr) {
    h->deleter(h->key_slice(), h->value);
  }
  delete h;
}

bool GarbageAwareCache::Unref(GAHandle* h) {
  assert(h->refs > 0);
  h->refs--;
  return h->refs == 0;
}

void GarbageAwareCache::RemoveFromQueue(GAHandle* h) {
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

void GarbageAwareCache::AddToQueue(GAHandle* h) {
  assert(h->refs == 1);
  if (h->in_queue) {
    return;
  }
  if (h->in_admission) {
    admission_lru_.push_front(h);
    h->lru_it = admission_lru_.begin();
  } else {
    h->score_seq = next_score_seq_++;
    h->score_it = probation_scores_.insert({h->score, h->score_seq, h}).first;
  }
  h->in_queue = true;
}

void GarbageAwareCache::RemoveFromCache(GAHandle* h) {
  if (!h->in_cache) {
    return;
  }
  RemoveFromQueue(h);
  table_.erase(h->key);
  h->in_cache = false;
  usage_ -= h->charge;
  if (h->in_admission) {
    admission_usage_ -= h->charge;
  } else {
    probation_usage_ -= h->charge;
  }
}

void GarbageAwareCache::MoveToProbation(GAHandle* h) {
  if (!h->in_admission || !h->garbage_aware || h->priority == Priority::HIGH) {
    return;
  }
  RemoveFromQueue(h);
  h->in_admission = false;
  admission_usage_ -= h->charge;
  probation_usage_ += h->charge;
  AddToQueue(h);
  demotions_++;
  RecordTick(statistics_, GC_AWARE_CACHE_DEMOTE);
}

void GarbageAwareCache::UpdateScore(GAHandle* h) {
  h->score = h->access_freq * (1.0 - h->garbage_ratio);
}

void GarbageAwareCache::EvictIfNeeded(std::vector<GAHandle*>* deleted) {
  const size_t admission_capacity =
      static_cast<size_t>(capacity_ * admission_ratio_);
  while (admission_usage_ > admission_capacity && !admission_lru_.empty()) {
    GAHandle* h = admission_lru_.back();
    if (h->garbage_aware && h->priority != Priority::HIGH) {
      MoveToProbation(h);
    } else {
      RemoveFromCache(h);
      if (Unref(h)) {
        deleted->push_back(h);
      }
    }
  }

  const size_t probation_capacity = capacity_ - admission_capacity;
  while (probation_usage_ > probation_capacity && !probation_scores_.empty()) {
    GAHandle* h = probation_scores_.begin()->handle;
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
      h = probation_scores_.begin()->handle;
      low_score_evictions_++;
      RecordTick(statistics_, GC_AWARE_CACHE_EVICT_LOW_SCORE);
    } else if (!admission_lru_.empty()) {
      h = admission_lru_.back();
    } else {
      break;
    }
    RemoveFromCache(h);
    if (Unref(h)) {
      deleted->push_back(h);
    }
  }
}

void GarbageAwareCache::MaybeLogLocked(const BlockCacheMetadata* metadata) {
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

std::shared_ptr<Cache> NewGarbageAwareCache(
    const GarbageAwareCacheOptions& cache_opts) {
  return NewGarbageAwareCache(
      cache_opts.capacity, cache_opts.num_shard_bits,
      cache_opts.strict_capacity_limit, cache_opts.admission_ratio,
      cache_opts.demote_score_threshold, cache_opts.log_interval,
      cache_opts.memory_allocator);
}

std::shared_ptr<Cache> NewGarbageAwareCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double admission_ratio, double demote_score_threshold, uint64_t log_interval,
    std::shared_ptr<MemoryAllocator> memory_allocator) {
  (void)num_shard_bits;
  GarbageAwareCacheOptions options;
  options.capacity = capacity;
  options.strict_capacity_limit = strict_capacity_limit;
  options.admission_ratio = admission_ratio;
  options.demote_score_threshold = demote_score_threshold;
  options.log_interval = log_interval;
  options.memory_allocator = std::move(memory_allocator);
  return std::make_shared<GarbageAwareCache>(options);
}

}  // namespace TERARKDB_NAMESPACE
