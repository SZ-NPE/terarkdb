//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <list>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class GarbageAwareCache : public Cache {
 public:
  explicit GarbageAwareCache(const GarbageAwareCacheOptions& options);
  ~GarbageAwareCache() override;

  const char* Name() const override { return "GarbageAwareCache"; }

  Status Insert(const Slice& key, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value),
                Handle** handle = nullptr,
                Priority priority = Priority::LOW) override;
  using Cache::Insert;

  Status InsertWithMetadata(
      const Slice& key, void* value, size_t charge,
      void (*deleter)(const Slice& key, void* value),
      const BlockCacheMetadata* metadata, Handle** handle = nullptr,
      Priority priority = Priority::LOW) override;

  Handle* Lookup(const Slice& key, Statistics* stats = nullptr) override;
  Handle* Lookup(const Slice& key, uint32_t hash, bool record_hit = true,
                 Statistics* stats = nullptr) override;
  bool Ref(Handle* handle) override;
  bool Release(Handle* handle, bool force_erase = false) override;
  void* Value(Handle* handle) override;
  void Erase(const Slice& key) override;
  void Erase(const Slice& key, uint32_t hash) override;
  uint64_t NewId() override;
  void SetCapacity(size_t capacity) override;
  void SetStrictCapacityLimit(bool strict_capacity_limit) override;
  bool HasStrictCapacityLimit() const override;
  size_t GetCapacity() const override;
  size_t GetUsage() const override;
  size_t GetUsage(Handle* handle) const override;
  size_t GetPinnedUsage() const override;
  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override;
  void EraseUnRefEntries() override;
  std::string GetPrintableOptions() const override;

  size_t TEST_GetAdmissionSize() const;
  size_t TEST_GetProbationSize() const;

 private:
  struct GAHandle;
  struct ScoreKey {
    double score;
    uint64_t seq;
    GAHandle* handle;
  };
  struct ScoreCmp {
    bool operator()(const ScoreKey& a, const ScoreKey& b) const {
      if (a.score != b.score) {
        return a.score < b.score;
      }
      return a.seq < b.seq;
    }
  };

  struct GAHandle : public Cache::Handle {
    std::string key;
    void* value = nullptr;
    void (*deleter)(const Slice&, void*) = nullptr;
    size_t charge = 0;
    uint32_t refs = 0;
    bool in_cache = false;
    bool in_admission = true;
    bool in_queue = false;
    bool garbage_aware = false;
    Priority priority = Priority::LOW;
    uint64_t access_freq = 1;
    double garbage_ratio = 0.0;
    double score = 1.0;
    std::list<GAHandle*>::iterator lru_it;
    std::set<ScoreKey, ScoreCmp>::iterator score_it;
    uint64_t score_seq = 0;

    Slice key_slice() const { return Slice(key); }
  };

  Status InsertImpl(const Slice& key, void* value, size_t charge,
                    void (*deleter)(const Slice& key, void* value),
                    const BlockCacheMetadata* metadata, Handle** handle,
                    Priority priority);
  void FreeEntry(GAHandle* h);
  bool Unref(GAHandle* h);
  void RemoveFromQueue(GAHandle* h);
  void AddToQueue(GAHandle* h);
  void RemoveFromCache(GAHandle* h);
  void MoveToProbation(GAHandle* h);
  void UpdateScore(GAHandle* h);
  void EvictIfNeeded(std::vector<GAHandle*>* deleted);
  void MaybeLogLocked(const BlockCacheMetadata* metadata);

  mutable port::Mutex mutex_;
  std::unordered_map<std::string, GAHandle*> table_;
  std::list<GAHandle*> admission_lru_;
  std::set<ScoreKey, ScoreCmp> probation_scores_;
  size_t capacity_;
  size_t usage_ = 0;
  size_t admission_usage_ = 0;
  size_t probation_usage_ = 0;
  bool strict_capacity_limit_;
  double admission_ratio_;
  double demote_score_threshold_;
  uint64_t log_interval_;
  std::atomic<uint64_t> last_id_;
  uint64_t next_score_seq_ = 1;
  uint64_t demotions_ = 0;
  uint64_t low_score_evictions_ = 0;
  uint64_t admission_hits_ = 0;
  uint64_t probation_hits_ = 0;
  Statistics* statistics_ = nullptr;
};

}  // namespace TERARKDB_NAMESPACE
