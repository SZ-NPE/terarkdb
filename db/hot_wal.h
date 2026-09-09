//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

class WriteBatch;
enum class WALRecoveryMode : char;
namespace log {
class Writer;
}

class HotWal {
 public:
  struct RecoveredRecord {
    uint64_t segment_number = 0;
    std::string contents;
  };

  HotWal(Env* env, const EnvOptions& env_options, std::string directory,
         uint64_t segment_size, bool manual_flush, bool use_fsync);
  ~HotWal();

  HotWal(const HotWal&) = delete;
  HotWal& operator=(const HotWal&) = delete;

  Status Open(bool writable);
  Status Append(WriteBatch* batch, uint64_t sequence, size_t reference_count,
                bool sync, uint64_t* segment_number);
  void Release(uint64_t segment_number, size_t reference_count = 1);
  Status Flush(bool sync);
  Status ReadAll(std::vector<RecoveredRecord>* records,
                 WALRecoveryMode recovery_mode, bool paranoid_checks) const;
  Status PurgeObsolete(const std::function<uint64_t()>& oldest_segment_to_keep);
  Status Close();

  size_t segment_count() const;
  bool TakePurgeRequest();

 private:
  struct Segment {
    uint64_t number = 0;
    uint64_t size = 0;
    size_t pending_writes = 0;
    std::unique_ptr<log::Writer> writer;
  };

  Status CreateSegmentLocked();
  Status SealActiveSegmentLocked();
  Status SyncActiveSegmentLocked();

  Env* const env_;
  const EnvOptions env_options_;
  const std::string directory_;
  const uint64_t segment_size_;
  const bool manual_flush_;
  const bool use_fsync_;
  mutable std::mutex mutex_;
  std::unique_ptr<Directory> directory_handle_;
  std::deque<Segment> segments_;
  uint64_t next_segment_number_ = 1;
  size_t releases_since_purge_ = 0;
  bool purge_requested_ = false;
  bool directory_synced_ = false;
  bool writable_ = false;
};

std::string HotWalDirectory(const std::string& wal_directory);
Status DestroyHotWal(Env* env, const std::string& wal_directory);

}  // namespace TERARKDB_NAMESPACE
