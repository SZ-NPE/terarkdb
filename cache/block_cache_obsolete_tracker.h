#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "port/port.h"
#include "rocksdb/cache.h"
#include "rocksdb/env.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class BlockCacheObsoleteTracker {
 public:
  explicit BlockCacheObsoleteTracker(
      const BlockCacheObsoleteTrackingOptions& options);
  ~BlockCacheObsoleteTracker();

  void RecordInsert(const void* handle, const BlockCacheMetadata* metadata,
                    size_t charge);
  void RecordErase(const void* handle);
  void MarkFilesObsolete(const std::vector<uint64_t>& file_numbers,
                         const std::vector<uint64_t>& output_file_numbers,
                         const char* reason, uint64_t job_id,
                         Logger* info_log);
  void LogSample(const char* reason, uint64_t job_id, Logger* info_log);

 private:
  struct Entry {
    uint64_t file_number = 0;
    size_t charge = 0;
  };

  struct FileResidency {
    uint64_t blocks = 0;
    uint64_t bytes = 0;
    bool obsolete = false;
    uint64_t obsolete_since_us = 0;
  };

  struct Sample {
    uint64_t ts_us = 0;
    uint64_t sample_id = 0;
    uint64_t tracked_blocks = 0;
    uint64_t tracked_bytes = 0;
    uint64_t obsolete_blocks = 0;
    uint64_t obsolete_bytes = 0;
    double obsolete_block_ratio = 0.0;
    double obsolete_byte_ratio = 0.0;
    double mean_obsolete_byte_ratio = 0.0;
    double peak_obsolete_byte_ratio = 0.0;
    uint64_t obsolete_file_count = 0;
    std::string top_obsolete_files;
  };

  void SampleLoop();
  Sample BuildSampleLocked();
  void EmitSample(const Sample& sample, const char* reason, uint64_t job_id,
                  Logger* info_log);
  void MaybeEmitDrainLocked(uint64_t file_number, FileResidency* residency,
                            uint64_t now_us);
  std::string FilesToString(const std::vector<uint64_t>& files) const;
  std::string BuildTopObsoleteFilesLocked() const;

  BlockCacheObsoleteTrackingOptions options_;
  Env* env_;
  std::atomic<bool> stop_;
  std::unique_ptr<port::Thread> sample_thread_;

  mutable port::Mutex mutex_;
  std::unordered_map<const void*, Entry> entries_;
  std::unordered_map<uint64_t, FileResidency> files_;
  Logger* info_log_ = nullptr;

  uint64_t tracked_blocks_ = 0;
  uint64_t tracked_bytes_ = 0;
  uint64_t obsolete_blocks_ = 0;
  uint64_t obsolete_bytes_ = 0;
  uint64_t sample_id_ = 0;
  uint64_t ratio_sample_count_ = 0;
  double ratio_sample_sum_ = 0.0;
  double peak_obsolete_byte_ratio_ = 0.0;
};

}  // namespace TERARKDB_NAMESPACE
