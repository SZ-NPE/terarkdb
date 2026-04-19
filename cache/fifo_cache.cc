//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cache/fifo_cache.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

#include <string>

#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

FIFOHandleTable::FIFOHandleTable() : list_(nullptr), length_(0), elems_(0) {
  Resize();
}

FIFOHandleTable::~FIFOHandleTable() {
  ApplyToAllCacheEntries([](FIFOHandle* h) {
    if (h->refs == 1) {
      h->Free();
    }
  });
  delete[] list_;
}

FIFOHandle* FIFOHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

FIFOHandle* FIFOHandleTable::Insert(FIFOHandle* h) {
  FIFOHandle** ptr = FindPointer(h->key(), h->hash);
  FIFOHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if (elems_ > length_) {
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      Resize();
    }
  }
  return old;
}

FIFOHandle* FIFOHandleTable::Remove(const Slice& key, uint32_t hash) {
  FIFOHandle** ptr = FindPointer(key, hash);
  FIFOHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

FIFOHandle** FIFOHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  FIFOHandle** ptr = &list_[hash & (length_ - 1)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void FIFOHandleTable::Resize() {
  uint32_t new_length = 16;
  while (new_length < elems_ * 1.5) {
    new_length *= 2;
  }
  FIFOHandle** new_list = new FIFOHandle*[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);
  uint32_t count = 0;
  for (uint32_t i = 0; i < length_; i++) {
    FIFOHandle* h = list_[i];
    while (h != nullptr) {
      FIFOHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      FIFOHandle** ptr = &new_list[hash & (new_length - 1)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  assert(elems_ == count);
  delete[] list_;
  list_ = new_list;
  length_ = new_length;
}

template <class CacheMonitor>
FIFOCacheShardTemplate<CacheMonitor>::FIFOCacheShardTemplate(
    size_t capacity, bool strict_capacity_limit, double high_pri_pool_ratio,
    const typename CacheMonitor::Options& options)
    : CacheMonitor(options),
      capacity_(0),
      strict_capacity_limit_(strict_capacity_limit),
      high_pri_pool_ratio_(high_pri_pool_ratio),
      high_pri_pool_capacity_(0) {
  // Make empty circular linked list
  fifo_.next = &fifo_;
  fifo_.prev = &fifo_;
  fifo_low_pri_ = &fifo_;
  SetCapacity(capacity);
}

template <class CacheMonitor>
FIFOCacheShardTemplate<CacheMonitor>::~FIFOCacheShardTemplate() {}

template <class CacheMonitor>
bool FIFOCacheShardTemplate<CacheMonitor>::Unref(FIFOHandle* e) {
  assert(e->refs > 0);
  return e->refs.fetch_sub(1, std::memory_order_relaxed) == 1;
}

// Call deleter and free

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::EraseUnRefEntries() {
  autovector<FIFOHandle*> last_reference_list;
  {
    WriteLock l(&mutex_);
    while (fifo_.next != &fifo_) {
      FIFOHandle* old = fifo_.next;
      assert(old->InCache());
      assert(old->refs ==
             1);  // FIFO list contains elements which may be evicted
      FIFO_Remove(old);
      table_.Remove(old->key(), old->hash);
      old->SetInCache(false);
      Unref(old);
      UsageSub(old);
      last_reference_list.push_back(old);
    }
  }

  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::ApplyToAllCacheEntries(
    void (*callback)(void*, size_t), bool thread_safe) {
  if (thread_safe) {
    mutex_.ReadLock();
  }
  table_.ApplyToAllCacheEntries(
      [callback](FIFOHandle* h) { callback(h->value, h->charge); });
  if (thread_safe) {
    mutex_.ReadUnlock();
  }
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::TEST_GetFIFOList(
    FIFOHandle** fifo, FIFOHandle** fifo_low_pri) {
  *fifo = &fifo_;
  *fifo_low_pri = fifo_low_pri_;
}

template <class CacheMonitor>
size_t FIFOCacheShardTemplate<CacheMonitor>::TEST_GetFIFOSize() {
  FIFOHandle* fifo_handle = fifo_.next;
  size_t fifo_size = 0;
  while (fifo_handle != &fifo_) {
    fifo_size++;
    fifo_handle = fifo_handle->next;
  }
  return fifo_size;
}

template <class CacheMonitor>
double FIFOCacheShardTemplate<CacheMonitor>::GetHighPriPoolRatio() {
  ReadLock l(&mutex_);
  return high_pri_pool_ratio_;
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::FIFO_Remove(FIFOHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  if (fifo_low_pri_ == e) {
    fifo_low_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  FIFOUsageSub(e);
  if (e->InHighPriPool()) {
    assert(high_pri_pool_usage_ >= e->charge);
    HighPriPoolUsageSub(e);
  }
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::FIFO_Insert(FIFOHandle* e) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->HasHit())) {
    // Inset "e" to head of FIFO list.
    e->next = &fifo_;
    e->prev = fifo_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    HighPriPoolUsageAdd(e);
    MaintainPoolSize();
  } else {
    // Insert "e" to the head of low-pri pool. Note that when
    // high_pri_pool_ratio is 0, head of low-pri pool is also head of FIFO list.
    e->next = fifo_low_pri_->next;
    e->prev = fifo_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    fifo_low_pri_ = e;
  }
  FIFOUsageAdd(e);
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    fifo_low_pri_ = fifo_low_pri_->next;
    assert(fifo_low_pri_ != &fifo_);
    fifo_low_pri_->SetInHighPriPool(false);
    HighPriPoolUsageSub(fifo_low_pri_);
  }
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::EvictFromFIFO(
    size_t charge, autovector<FIFOHandle*>* deleted) {
  while (usage_ + charge > capacity_ && fifo_.next != &fifo_) {
    FIFOHandle* old = fifo_.next;
    assert(old->InCache());
    FIFO_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    if (Unref(old)) {
      UsageSub(old);
      deleted->push_back(old);
    }
  }
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::SetCapacity(size_t capacity) {
  autovector<FIFOHandle*> last_reference_list;
  {
    WriteLock l(&mutex_);
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    EvictFromFIFO(0, &last_reference_list);
  }
  // we free the entries here outside of mutex for
  // performance reasons
  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::SetStrictCapacityLimit(
    bool strict_capacity_limit) {
  WriteLock l(&mutex_);
  strict_capacity_limit_ = strict_capacity_limit;
}

template <class CacheMonitor>
Cache::Handle* FIFOCacheShardTemplate<CacheMonitor>::Lookup(const Slice& key,
                                                           uint32_t hash,
                                                           bool record_hit) {
  ReadLock l(&mutex_);
  FIFOHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    assert(e->InCache());
    e->refs.fetch_add(1, std::memory_order_relaxed);
    if (record_hit) {
      e->SetHit();
    }
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

template <class CacheMonitor>
bool FIFOCacheShardTemplate<CacheMonitor>::Ref(Cache::Handle* h) {
  FIFOHandle* handle = reinterpret_cast<FIFOHandle*>(h);
  handle->refs.fetch_add(1, std::memory_order_relaxed);
  return true;
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::SetHighPriorityPoolRatio(
    double high_pri_pool_ratio) {
  WriteLock l(&mutex_);
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

template <class CacheMonitor>
bool FIFOCacheShardTemplate<CacheMonitor>::Release(Cache::Handle* handle,
                                                  bool force_erase) {
  if (handle == nullptr) {
    return false;
  }
  FIFOHandle* e = reinterpret_cast<FIFOHandle*>(handle);
  bool last_reference = false;
  {
    WriteLock l(&mutex_);
    last_reference = Unref(e);
    if (last_reference) {
      UsageSub(e);
    }
    if (e->refs == 1 && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it
      if (usage_ > capacity_ || force_erase) {
        // the cache is full
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
        Unref(e);
        UsageSub(e);
        FIFO_Remove(e);
        last_reference = true;
      }
    }
  }

  // free outside of mutex
  if (last_reference) {
    e->Free();
  }
  return last_reference;
}

template <class CacheMonitor>
Status FIFOCacheShardTemplate<CacheMonitor>::Insert(
    const Slice& key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const Slice& key, void* value), Cache::Handle** handle,
    Cache::Priority priority) {
  // Allocate the memory here outside of the mutex
  // If the cache is full, we'll have to release it
  // It shouldn't happen very often though.
  FIFOHandle* e = reinterpret_cast<FIFOHandle*>(
      new char[sizeof(FIFOHandle) - 1 + key.size()]);
  Status s;
  autovector<FIFOHandle*> last_reference_list;

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->flags = 0;
  e->hash = hash;
  e->refs = (handle == nullptr
                 ? 1
                 : 2);  // One from FIFOCache, one for the returned handle
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->SetPriority(priority);
  memcpy(e->key_data, key.data(), key.size());

  {
    WriteLock l(&mutex_);

    // Free the space following strict FIFO policy until enough space
    // is freed or the fifo list is empty
    EvictFromFIFO(charge, &last_reference_list);

    if (usage_ - fifo_usage_ + charge > capacity_ &&
        (strict_capacity_limit_ || handle == nullptr)) {
      if (handle == nullptr) {
        // Don't insert the entry but still return ok, as if the entry inserted
        // into cache and get evicted immediately.
        last_reference_list.push_back(e);
      } else {
        delete[] reinterpret_cast<char*>(e);
        *handle = nullptr;
        s = Status::Incomplete("Insert failed due to FIFO cache being full.");
      }
    } else {
      // insert into the cache
      // note that the cache might get larger than its capacity if not enough
      // space was freed
      FIFOHandle* old = table_.Insert(e);
      UsageAdd(e);
      if (old != nullptr) {
        old->SetInCache(false);
        FIFO_Remove(old);
        if (Unref(old)) {
          UsageSub(old);
          last_reference_list.push_back(old);
        }
      }
      FIFO_Insert(e);
      if (handle != nullptr) {
        *handle = reinterpret_cast<Cache::Handle*>(e);
      }
      s = Status::OK();
    }
  }

  // we free the entries here outside of mutex for
  // performance reasons
  for (auto entry : last_reference_list) {
    entry->Free();
  }

  return s;
}

template <class CacheMonitor>
void FIFOCacheShardTemplate<CacheMonitor>::Erase(const Slice& key,
                                                uint32_t hash) {
  FIFOHandle* e;
  bool last_reference = false;
  {
    WriteLock l(&mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      e->SetInCache(false);
      FIFO_Remove(e);
      last_reference = Unref(e);
      if (last_reference) {
        UsageSub(e);
      }
    }
  }

  // mutex not held here
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    e->Free();
  }
}

template <class CacheMonitor>
size_t FIFOCacheShardTemplate<CacheMonitor>::GetUsage() const {
  ReadLock l(&mutex_);
  return usage_;
}

template <class CacheMonitor>
size_t FIFOCacheShardTemplate<CacheMonitor>::GetPinnedUsage() const {
  ReadLock l(&mutex_);
  assert(usage_ >= fifo_usage_);
  return usage_ - fifo_usage_;
}

template <class CacheMonitor>
std::string FIFOCacheShardTemplate<CacheMonitor>::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    ReadLock l(&mutex_);
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
  }
  return std::string(buffer);
}

template <>
FIFOCacheBase<FIFOCacheDiagnosableShard>::FIFOCacheBase(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    const typename FIFOCacheDiagnosableShard::MonitorOptions& options,
    std::shared_ptr<MemoryAllocator> allocator)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ =
      reinterpret_cast<FIFOCacheDiagnosableShard*>(port::cacheline_aligned_alloc(
          sizeof(FIFOCacheDiagnosableShard) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i]) FIFOCacheDiagnosableShard(per_shard, strict_capacity_limit,
                                               high_pri_pool_ratio, options);
  }
}

template <class FIFOCacheShardType>
FIFOCacheBase<FIFOCacheShardType>::FIFOCacheBase(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    const typename FIFOCacheShardType::MonitorOptions& options,
    std::shared_ptr<MemoryAllocator> allocator)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit,
                   std::move(allocator)) {
  num_shards_ = 1 << num_shard_bits;
  shards_ = reinterpret_cast<FIFOCacheShardType*>(
      port::cacheline_aligned_alloc(sizeof(FIFOCacheShardType) * num_shards_));
  size_t per_shard = (capacity + (num_shards_ - 1)) / num_shards_;
  for (int i = 0; i < num_shards_; i++) {
    new (&shards_[i]) FIFOCacheShardType(per_shard, strict_capacity_limit,
                                        high_pri_pool_ratio, options);
  }
}

template <class FIFOCacheShardType>
std::string FIFOCacheBase<FIFOCacheShardType>::DumpFIFOCacheStatistics() {
  std::string res;
  res.append("Cache Summary: \n");
  res.append("usage: " + std::to_string(GetUsage()) +
             ", pinned_usage: " + std::to_string(GetPinnedUsage()) + "\n");

  for (int i = 0; i < num_shards_; i++) {
    res.append("shard_" + std::to_string(i) + " : \n");
    res.append(shards_[i].DumpDiagnoseInfo());
  }
  return res;
}

#ifdef WITH_DIAGNOSE_CACHE
template <>
const char* FIFOCacheBase<FIFOCacheDiagnosableShard>::Name() const {
  return "DiagnosableFIFOCache";
}
#endif

template <class FIFOCacheShardType>
const char* FIFOCacheBase<FIFOCacheShardType>::Name() const {
  return "FIFOCache";
}

template <class FIFOCacheShardType>
FIFOCacheBase<FIFOCacheShardType>::~FIFOCacheBase() {
  if (shards_ != nullptr) {
    assert(num_shards_ > 0);
    for (int i = 0; i < num_shards_; i++) {
      shards_[i].~FIFOCacheShardType();
    }
    port::cacheline_aligned_free(shards_);
  }
}

template <class FIFOCacheShardType>
CacheShard* FIFOCacheBase<FIFOCacheShardType>::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

template <class FIFOCacheShardType>
const CacheShard* FIFOCacheBase<FIFOCacheShardType>::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

template <class FIFOCacheShardType>
void* FIFOCacheBase<FIFOCacheShardType>::Value(Handle* handle) {
  return reinterpret_cast<const FIFOHandle*>(handle)->value;
}

template <class FIFOCacheShardType>
size_t FIFOCacheBase<FIFOCacheShardType>::GetCharge(Handle* handle) const {
  return reinterpret_cast<const FIFOHandle*>(handle)->charge;
}

template <class FIFOCacheShardType>
uint32_t FIFOCacheBase<FIFOCacheShardType>::GetHash(Handle* handle) const {
  return reinterpret_cast<const FIFOHandle*>(handle)->hash;
}

template <class FIFOCacheShardType>
void FIFOCacheBase<FIFOCacheShardType>::DisownData() {
// Do not drop data if compile with ASAN to suppress leak warning.
#if defined(__clang__)
#if !defined(__has_feature) || !__has_feature(address_sanitizer)
  shards_ = nullptr;
  num_shards_ = 0;
#endif
#else  // __clang__
#ifndef __SANITIZE_ADDRESS__
  shards_ = nullptr;
  num_shards_ = 0;
#endif  // !__SANITIZE_ADDRESS__
#endif  // __clang__
}

template <class FIFOCacheShardType>
size_t FIFOCacheBase<FIFOCacheShardType>::TEST_GetFIFOSize() {
  size_t fifo_size_of_all_shards = 0;
  for (int i = 0; i < num_shards_; i++) {
    fifo_size_of_all_shards += shards_[i].TEST_GetFIFOSize();
  }
  return fifo_size_of_all_shards;
}

// template <class FIFOCacheShardType>
// double FIFOCacheBase<FIFOCacheShardType>::GetHighPriPoolRatio()

std::shared_ptr<Cache> NewFIFOCache(const FIFOCacheOptions& cache_opts) {
  return NewFIFOCache(cache_opts.capacity, cache_opts.num_shard_bits,
                     cache_opts.strict_capacity_limit,
                     cache_opts.high_pri_pool_ratio,
                     cache_opts.memory_allocator);
}

std::shared_ptr<Cache> NewFIFOCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<FIFOCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      FIFOCacheShard::MonitorOptions{}, std::move(memory_allocator));
}

#ifdef WITH_DIAGNOSE_CACHE
std::shared_ptr<Cache> NewDiagnosableFIFOCache(
    const FIFOCacheOptions& cache_opts) {
  assert(cache_opts.is_diagnose);
  return NewDiagnosableFIFOCache(cache_opts.capacity, cache_opts.num_shard_bits,
                                cache_opts.strict_capacity_limit,
                                cache_opts.high_pri_pool_ratio,
                                cache_opts.memory_allocator, cache_opts.topk);
}

std::shared_ptr<Cache> NewDiagnosableFIFOCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, size_t topk) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<DiagnosableFIFOCache>(
      capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio,
      FIFOCacheDiagnosableShard::MonitorOptions{topk},
      std::move(memory_allocator));
}

template class FIFOCacheShardTemplate<FIFOCacheDiagnosableMonitor>;
template class FIFOCacheBase<FIFOCacheDiagnosableShard>;
#else
std::shared_ptr<Cache> NewDiagnosableFIFOCache(
    const FIFOCacheOptions& cache_opts) {
  return NewFIFOCache(cache_opts.capacity, cache_opts.num_shard_bits,
                     cache_opts.strict_capacity_limit,
                     cache_opts.high_pri_pool_ratio,
                     cache_opts.memory_allocator);
}

std::shared_ptr<Cache> NewDiagnosableFIFOCache(
    size_t capacity, int num_shard_bits, bool strict_capacity_limit,
    double high_pri_pool_ratio,
    std::shared_ptr<MemoryAllocator> memory_allocator, size_t /* topk */) {
  return NewFIFOCache(capacity, num_shard_bits, strict_capacity_limit,
                     high_pri_pool_ratio, memory_allocator);
}
#endif

template class FIFOCacheShardTemplate<FIFOCacheNoMonitor>;
template class FIFOCacheBase<FIFOCacheShard>;

}  // namespace TERARKDB_NAMESPACE
