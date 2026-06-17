#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cache/block_cache_obsolete_tracker.h"

#include <inttypes.h>

#include <algorithm>
#include <sstream>
#include <utility>

#include "util/logging.h"
#include "util/mutexlock.h"

namespace TERARKDB_NAMESPACE {

BlockCacheObsoleteTracker::BlockCacheObsoleteTracker(
    const BlockCacheObsoleteTrackingOptions& options)
    : options_(options), env_(Env::Default()), stop_(false) {
  if (options_.enabled && options_.sample_interval_sec > 0) {
    sample_thread_.reset(
        new port::Thread(&BlockCacheObsoleteTracker::SampleLoop, this));
  }
}

BlockCacheObsoleteTracker::~BlockCacheObsoleteTracker() {
  stop_.store(true, std::memory_order_release);
  if (sample_thread_ != nullptr && sample_thread_->joinable()) {
    sample_thread_->join();
  }
}

void BlockCacheObsoleteTracker::RecordInsert(
    const void* handle, const BlockCacheMetadata* metadata, size_t charge) {
  if (handle == nullptr || metadata == nullptr || !metadata->is_data_block ||
      metadata->file_number == 0) {
    return;
  }

  MutexLock l(&mutex_);
  if (metadata->info_log != nullptr) {
    info_log_ = metadata->info_log;
  }
  auto inserted =
      entries_.emplace(handle, Entry{metadata->file_number, charge});
  if (!inserted.second) {
    return;
  }

  FileResidency& file = files_[metadata->file_number];
  ++file.blocks;
  file.bytes += charge;
  ++tracked_blocks_;
  tracked_bytes_ += charge;
  if (file.obsolete) {
    ++obsolete_blocks_;
    obsolete_bytes_ += charge;
  }
}

void BlockCacheObsoleteTracker::RecordErase(const void* handle) {
  if (handle == nullptr) {
    return;
  }

  MutexLock l(&mutex_);
  auto it = entries_.find(handle);
  if (it == entries_.end()) {
    return;
  }

  const uint64_t file_number = it->second.file_number;
  const size_t charge = it->second.charge;
  entries_.erase(it);

  auto file_it = files_.find(file_number);
  if (file_it == files_.end()) {
    return;
  }
  FileResidency& file = file_it->second;
  assert(file.blocks > 0);
  assert(file.bytes >= charge);
  --file.blocks;
  file.bytes -= charge;
  assert(tracked_blocks_ > 0);
  assert(tracked_bytes_ >= charge);
  --tracked_blocks_;
  tracked_bytes_ -= charge;
  if (file.obsolete) {
    assert(obsolete_blocks_ > 0);
    assert(obsolete_bytes_ >= charge);
    --obsolete_blocks_;
    obsolete_bytes_ -= charge;
    MaybeEmitDrainLocked(file_number, &file, env_->NowMicros());
  }
}

void BlockCacheObsoleteTracker::MarkFilesObsolete(
    const std::vector<uint64_t>& file_numbers,
    const std::vector<uint64_t>& output_file_numbers, const char* reason,
    uint64_t job_id, Logger* info_log) {
  if (file_numbers.empty()) {
    return;
  }

  uint64_t resident_bytes_added = 0;
  uint64_t resident_blocks_added = 0;
  uint64_t newly_marked_files = 0;
  Sample sample;
  {
    MutexLock l(&mutex_);
    if (info_log != nullptr) {
      info_log_ = info_log;
    }
    const uint64_t now_us = env_->NowMicros();
    for (uint64_t file_number : file_numbers) {
      FileResidency& file = files_[file_number];
      if (file.obsolete) {
        continue;
      }
      file.obsolete = true;
      file.obsolete_since_us = now_us;
      ++newly_marked_files;
      resident_bytes_added += file.bytes;
      resident_blocks_added += file.blocks;
      obsolete_bytes_ += file.bytes;
      obsolete_blocks_ += file.blocks;
    }
    sample = BuildSampleLocked();
  }

  Logger* log = info_log != nullptr ? info_log : info_log_;
  if (log != nullptr) {
    ROCKS_LOG_INFO(
        log,
        "[BLOCK_CACHE_OBSOLETE_EVENT] ts_us=%" PRIu64 " job=%" PRIu64
        " reason=%s obsolete_files=%s output_files=%s obsolete_file_count=%zu"
        " newly_marked_files=%" PRIu64
        " resident_obsolete_bytes_added=%" PRIu64
        " resident_obsolete_blocks_added=%" PRIu64,
        env_->NowMicros(), job_id, reason == nullptr ? "unknown" : reason,
        FilesToString(file_numbers).c_str(),
        FilesToString(output_file_numbers).c_str(), file_numbers.size(),
        newly_marked_files, resident_bytes_added, resident_blocks_added);
  }
  EmitSample(sample, "obsolete_event", job_id, log);
}

void BlockCacheObsoleteTracker::LogSample(const char* reason, uint64_t job_id,
                                          Logger* info_log) {
  Sample sample;
  {
    MutexLock l(&mutex_);
    if (info_log != nullptr) {
      info_log_ = info_log;
    }
    sample = BuildSampleLocked();
  }
  EmitSample(sample, reason, job_id, info_log != nullptr ? info_log : info_log_);
}

void BlockCacheObsoleteTracker::SampleLoop() {
  const uint64_t sleep_us = 1000000;
  uint64_t elapsed_us = 0;
  const uint64_t interval_us = options_.sample_interval_sec * 1000000;
  while (!stop_.load(std::memory_order_acquire)) {
    env_->SleepForMicroseconds(static_cast<int>(sleep_us));
    elapsed_us += sleep_us;
    if (elapsed_us >= interval_us) {
      elapsed_us = 0;
      LogSample("periodic", 0, nullptr);
    }
  }
}

BlockCacheObsoleteTracker::Sample
BlockCacheObsoleteTracker::BuildSampleLocked() {
  Sample sample;
  sample.ts_us = env_->NowMicros();
  sample.sample_id = ++sample_id_;
  sample.tracked_blocks = tracked_blocks_;
  sample.tracked_bytes = tracked_bytes_;
  sample.obsolete_blocks = obsolete_blocks_;
  sample.obsolete_bytes = obsolete_bytes_;
  sample.obsolete_block_ratio =
      tracked_blocks_ == 0
          ? 0.0
          : static_cast<double>(obsolete_blocks_) / tracked_blocks_;
  sample.obsolete_byte_ratio =
      tracked_bytes_ == 0 ? 0.0
                          : static_cast<double>(obsolete_bytes_) / tracked_bytes_;
  ++ratio_sample_count_;
  ratio_sample_sum_ += sample.obsolete_byte_ratio;
  if (sample.obsolete_byte_ratio > peak_obsolete_byte_ratio_) {
    peak_obsolete_byte_ratio_ = sample.obsolete_byte_ratio;
  }
  sample.mean_obsolete_byte_ratio = ratio_sample_sum_ / ratio_sample_count_;
  sample.peak_obsolete_byte_ratio = peak_obsolete_byte_ratio_;
  for (const auto& kv : files_) {
    if (kv.second.obsolete) {
      ++sample.obsolete_file_count;
    }
  }
  sample.top_obsolete_files = BuildTopObsoleteFilesLocked();
  return sample;
}

void BlockCacheObsoleteTracker::EmitSample(const Sample& sample,
                                           const char* reason, uint64_t job_id,
                                           Logger* info_log) {
  if (info_log == nullptr) {
    return;
  }
  ROCKS_LOG_INFO(
      info_log,
      "[BLOCK_CACHE_OBSOLETE_SAMPLE] ts_us=%" PRIu64 " sample_id=%" PRIu64
      " job=%" PRIu64 " reason=%s tracked_blocks=%" PRIu64
      " tracked_bytes=%" PRIu64 " obsolete_blocks=%" PRIu64
      " obsolete_bytes=%" PRIu64 " obsolete_block_ratio=%.6f"
      " obsolete_byte_ratio=%.6f mean_obsolete_byte_ratio=%.6f"
      " peak_obsolete_byte_ratio=%.6f obsolete_file_count=%" PRIu64
      " top_obsolete_files=%s",
      sample.ts_us, sample.sample_id, job_id,
      reason == nullptr ? "unknown" : reason, sample.tracked_blocks,
      sample.tracked_bytes, sample.obsolete_blocks, sample.obsolete_bytes,
      sample.obsolete_block_ratio, sample.obsolete_byte_ratio,
      sample.mean_obsolete_byte_ratio, sample.peak_obsolete_byte_ratio,
      sample.obsolete_file_count, sample.top_obsolete_files.c_str());
}

void BlockCacheObsoleteTracker::MaybeEmitDrainLocked(
    uint64_t file_number, FileResidency* residency, uint64_t now_us) {
  if (residency->obsolete && residency->bytes == 0 &&
      residency->obsolete_since_us != 0 && info_log_ != nullptr) {
    ROCKS_LOG_INFO(
        info_log_,
        "[BLOCK_CACHE_OBSOLETE_DRAIN] ts_us=%" PRIu64 " file=%" PRIu64
        " residency_us=%" PRIu64,
        now_us, file_number, now_us - residency->obsolete_since_us);
    residency->obsolete_since_us = 0;
  }
}

std::string BlockCacheObsoleteTracker::FilesToString(
    const std::vector<uint64_t>& files) const {
  std::ostringstream oss;
  oss << "[";
  for (size_t i = 0; i < files.size(); ++i) {
    if (i != 0) {
      oss << ",";
    }
    oss << files[i];
  }
  oss << "]";
  return oss.str();
}

std::string BlockCacheObsoleteTracker::BuildTopObsoleteFilesLocked() const {
  std::vector<std::pair<uint64_t, FileResidency>> obsolete_files;
  obsolete_files.reserve(files_.size());
  for (const auto& kv : files_) {
    if (kv.second.obsolete && kv.second.bytes > 0) {
      obsolete_files.emplace_back(kv.first, kv.second);
    }
  }
  std::sort(obsolete_files.begin(), obsolete_files.end(),
            [](const std::pair<uint64_t, FileResidency>& a,
               const std::pair<uint64_t, FileResidency>& b) {
              if (a.second.bytes != b.second.bytes) {
                return a.second.bytes > b.second.bytes;
              }
              return a.first < b.first;
            });

  std::ostringstream oss;
  oss << "[";
  const size_t limit = std::min<size_t>(options_.topk_files,
                                        obsolete_files.size());
  for (size_t i = 0; i < limit; ++i) {
    if (i != 0) {
      oss << ",";
    }
    oss << obsolete_files[i].first << ":" << obsolete_files[i].second.bytes
        << ":" << obsolete_files[i].second.blocks;
  }
  oss << "]";
  return oss.str();
}

}  // namespace TERARKDB_NAMESPACE
