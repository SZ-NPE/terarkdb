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
#include <unordered_set>
#include <vector>

#include "cache/sharded_cache.h"
#include "port/port.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class GarbageAwareCacheShard : public CacheShard {
 public:
  struct ObsoleteSample {
    uint64_t tracked_blocks = 0;
    uint64_t tracked_bytes = 0;
    uint64_t obsolete_blocks = 0;
    uint64_t obsolete_bytes = 0;
    uint64_t obsolete_file_count = 0;
  };

  GarbageAwareCacheShard(size_t capacity, bool strict_capacity_limit,
                         double admission_ratio, double demote_score_threshold,
                         uint64_t log_interval, bool enable_aging,
                         uint64_t aging_interval);
  ~GarbageAwareCacheShard() override;

  Status Insert(const Slice& key, uint32_t hash, void* value, size_t charge,
                void (*deleter)(const Slice& key, void* value),
                Cache::Handle** handle,
                Cache::Priority priority) override;

  Status InsertWithMetadata(
      const Slice& key, void* value, size_t charge,
      void (*deleter)(const Slice& key, void* value),
      const BlockCacheMetadata* metadata, uint32_t hash,
      Cache::Handle** handle, Cache::Priority priority);

  Cache::Handle* Lookup(const Slice& key, uint32_t hash,
                        bool record_hit = true) override;
  Cache::Handle* Lookup(const Slice& key, uint32_t hash, bool record_hit,
                        Statistics* stats);
  bool Ref(Cache::Handle* handle) override;
  bool Release(Cache::Handle* handle, bool force_erase = false) override;
  void Erase(const Slice& key, uint32_t hash) override;
  void SetCapacity(size_t capacity) override;
  void SetStrictCapacityLimit(bool strict_capacity_limit) override;
  size_t GetUsage() const override;
  size_t GetPinnedUsage() const override;
  void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                              bool thread_safe) override;
  void EraseUnRefEntries() override;
  std::string GetPrintableOptions() const override;
  void MarkBlockCacheFilesObsolete(
      const std::vector<uint64_t>& file_numbers,
      const std::vector<uint64_t>& output_file_numbers, const char* reason,
      uint64_t job_id, Logger* info_log = nullptr);
  void LogBlockCacheObsoleteSample(const char* reason, uint64_t job_id,
                                   Logger* info_log = nullptr);
  ObsoleteSample GetObsoleteSample() const;

  void* Value(Cache::Handle* handle);
  size_t GetCharge(Cache::Handle* handle) const;
  uint32_t GetHash(Cache::Handle* handle) const;

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
    uint32_t hash = 0;
    GAHandle* next_hash = nullptr;
    uint32_t refs = 0;
    bool in_cache = false;
    bool in_admission = true;
    bool in_queue = false;
    bool garbage_aware = false;
    uint64_t file_number = 0;
    bool is_data_block = false;
    bool is_blob_file = false;
    Cache::Priority priority = Cache::Priority::LOW;
    uint64_t access_freq = 1;
    uint64_t last_epoch = 0;
    double garbage_ratio = 0.0;
    double score = 1.0;
    std::list<GAHandle*>::iterator lru_it;
    std::set<ScoreKey, ScoreCmp>::iterator score_it;
    uint64_t score_seq = 0;

    Slice key_slice() const { return Slice(key); }
  };

  Status InsertImpl(const Slice& key, void* value, size_t charge,
                    void (*deleter)(const Slice& key, void* value),
                    const BlockCacheMetadata* metadata, uint32_t hash,
                    Cache::Handle** handle, Cache::Priority priority);
  static bool IsNoHitHandle(Cache::Handle* handle);
  static GAHandle* UnwrapHandle(Cache::Handle* handle);
  static Cache::Handle* EncodeHandle(GAHandle* handle, bool no_hit);
  void FreeEntry(GAHandle* h);
  void DeleteHandleOnly(GAHandle* h);
  bool Unref(GAHandle* h);
  GAHandle** FindTablePointer(const Slice& key, uint32_t hash);
  GAHandle* TableLookup(const Slice& key, uint32_t hash);
  GAHandle* TableInsert(GAHandle* h);
  GAHandle* TableRemove(const Slice& key, uint32_t hash);
  void TableResize();
  void RemoveFromQueue(GAHandle* h);
  void AddToQueue(GAHandle* h, bool promote = true);
  void RemoveFromCache(GAHandle* h);
  void AddToFileIndex(GAHandle* h);
  void RemoveFromFileIndex(GAHandle* h);
  bool IsDemotable(GAHandle* h);
  void MoveToProbation(GAHandle* h);
  void AdvanceAgingEpoch();
  void ApplyAccessFreqAging(GAHandle* h);
  void UpdateScore(GAHandle* h);
  void RefreshProbationScoresForAging();
  GAHandle* FindAdmissionVictim(bool require_garbage_aware,
                                bool require_low_priority);
  void EvictIfNeeded(std::vector<GAHandle*>* deleted);
  void MaybeLogLocked(const BlockCacheMetadata* metadata);

  mutable port::Mutex mutex_;
  std::vector<GAHandle*> table_;
  uint32_t table_elems_ = 0;
  std::unordered_map<uint64_t, std::unordered_set<GAHandle*>> file_index_;
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
  uint64_t next_score_seq_ = 1;
  uint64_t demotions_ = 0;
  uint64_t low_score_evictions_ = 0;
  uint64_t admission_hits_ = 0;
  uint64_t probation_hits_ = 0;
  uint64_t obsolete_marked_blocks_ = 0;
  uint64_t obsolete_marked_bytes_ = 0;
  bool enable_aging_ = false;
  uint64_t aging_interval_ = 0;
  uint64_t operation_count_ = 0;
  uint64_t current_epoch_ = 0;
  Statistics* statistics_ = nullptr;
};

class GarbageAwareCache : public ShardedCache {
 public:
  GarbageAwareCache(const GarbageAwareCacheOptions& options,
                    int num_shard_bits);
  ~GarbageAwareCache() override;

  const char* Name() const override { return "GarbageAwareCache"; }
  CacheShard* GetShard(int shard) override;
  const CacheShard* GetShard(int shard) const override;
  void* Value(Handle* handle) override;
  size_t GetCharge(Handle* handle) const override;
  uint32_t GetHash(Handle* handle) const override;
  void DisownData() override;

  using Cache::Insert;
  using Cache::Lookup;
  Handle* Lookup(const Slice& key, Statistics* stats = nullptr) override;
  Handle* Lookup(const Slice& key, uint32_t hash, bool record_hit = true,
                 Statistics* stats = nullptr) override;
  Status InsertWithMetadata(
      const Slice& key, void* value, size_t charge,
      void (*deleter)(const Slice& key, void* value),
      const BlockCacheMetadata* metadata, Handle** handle = nullptr,
      Priority priority = Priority::LOW) override;
  void MarkBlockCacheFilesObsolete(
      const std::vector<uint64_t>& file_numbers,
      const std::vector<uint64_t>& output_file_numbers, const char* reason,
      uint64_t job_id, Logger* info_log = nullptr) override;
  void LogBlockCacheObsoleteSample(const char* reason, uint64_t job_id,
                                   Logger* info_log = nullptr) override;

  size_t TEST_GetAdmissionSize() const;
  size_t TEST_GetProbationSize() const;

 private:
  uint32_t ShardForHash(uint32_t hash) const;

  GarbageAwareCacheShard* shards_ = nullptr;
  int num_shards_ = 0;
  std::atomic<uint64_t> obsolete_sample_id_{0};
};

}  // namespace TERARKDB_NAMESPACE
