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
    double demote_score_threshold, uint64_t log_interval)
    : capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      admission_ratio_(ClampRatio(admission_ratio)),
      demote_score_threshold_(std::max(0.0, demote_score_threshold)),
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
  h->key.assign(key.data(), key.size());
  h->value = value;
  h->deleter = deleter;
  h->charge = charge;
  h->hash = hash;
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
    if (strict_capacity_limit_ && handle != nullptr && usage_ > capacity_) {
      RemoveFromCache(h);
      Unref(h);  // Drop the cache reference; caller keeps value ownership.
      inserted = false;
      s = Status::Incomplete("Insert failed due to strict capacity limit");
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

bool GarbageAwareCacheShard::Ref(Cache::Handle* handle) {
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

bool GarbageAwareCacheShard::Release(Cache::Handle* handle, bool force_erase) {
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

void* GarbageAwareCacheShard::Value(Cache::Handle* handle) {
  return reinterpret_cast<GAHandle*>(handle)->value;
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
  return reinterpret_cast<GAHandle*>(handle)->charge;
}

uint32_t GarbageAwareCacheShard::GetHash(Cache::Handle* handle) const {
  return reinterpret_cast<GAHandle*>(handle)->hash;
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
           "    log_interval : %" PRIu64 "\n",
           capacity_, strict_capacity_limit_, admission_ratio_,
           demote_score_threshold_, log_interval_);
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

void GarbageAwareCacheShard::FreeEntry(GAHandle* h) {
  if (h->deleter != nullptr) {
    h->deleter(h->key_slice(), h->value);
  }
  delete h;
}

void GarbageAwareCacheShard::DeleteHandleOnly(GAHandle* h) { delete h; }

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

void GarbageAwareCacheShard::AddToQueue(GAHandle* h) {
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

void GarbageAwareCacheShard::RemoveFromCache(GAHandle* h) {
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

void GarbageAwareCacheShard::MoveToProbation(GAHandle* h) {
  if (!h->in_admission || !h->garbage_aware ||
      h->priority == Cache::Priority::HIGH) {
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

void GarbageAwareCacheShard::UpdateScore(GAHandle* h) {
  h->score = h->access_freq * (1.0 - h->garbage_ratio);
}

void GarbageAwareCacheShard::EvictIfNeeded(std::vector<GAHandle*>* deleted) {
  const size_t admission_capacity =
      static_cast<size_t>(capacity_ * admission_ratio_);
  while (admission_usage_ > admission_capacity && !admission_lru_.empty()) {
    GAHandle* h = admission_lru_.back();
    if (h->garbage_aware && h->priority != Cache::Priority::HIGH) {
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
        options.demote_score_threshold, options.log_interval);
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
      cache_opts.memory_allocator);
}

std::shared_ptr<Cache> NewGarbageAwareCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double admission_ratio, double demote_score_threshold, uint64_t log_interval,
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
  options.admission_ratio = admission_ratio;
  options.demote_score_threshold = demote_score_threshold;
  options.log_interval = log_interval;
  options.memory_allocator = std::move(memory_allocator);
  return std::make_shared<GarbageAwareCache>(options, num_shard_bits);
}

}  // namespace TERARKDB_NAMESPACE
