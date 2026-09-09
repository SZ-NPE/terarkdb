//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/hot_wal.h"

#include <algorithm>
#include <cassert>
#include <utility>

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/write_batch_internal.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"

namespace TERARKDB_NAMESPACE {

namespace {

class HotWalReporter : public log::Reader::Reporter {
 public:
  explicit HotWalReporter(bool report_errors) : report_errors_(report_errors) {}

  void Corruption(size_t, const Status& status) override {
    if (report_errors_ && status_.ok()) {
      status_ = status;
    }
  }

  const bool report_errors_;
  Status status_;
};

}  // namespace

std::string HotWalDirectory(const std::string& wal_directory) {
  return wal_directory + "/hot-wal";
}

HotWal::HotWal(Env* env, const EnvOptions& env_options, std::string directory,
               uint64_t segment_size, bool manual_flush, bool use_fsync)
    : env_(env),
      env_options_(env_options),
      directory_(std::move(directory)),
      segment_size_(std::max<uint64_t>(1U << 20, segment_size)),
      manual_flush_(manual_flush),
      use_fsync_(use_fsync) {}

HotWal::~HotWal() { Close(); }

Status HotWal::Open(bool writable) {
  std::lock_guard<std::mutex> lock(mutex_);
  writable_ = writable;
  Status status = env_->FileExists(directory_);
  if (status.IsNotFound()) {
    if (!writable_) {
      return Status::OK();
    }
    status = env_->CreateDirIfMissing(directory_);
  }
  if (!status.ok()) {
    return status;
  }

  std::vector<std::string> children;
  status = env_->GetChildren(directory_, &children);
  if (!status.ok()) {
    return status;
  }
  std::vector<uint64_t> numbers;
  for (const auto& child : children) {
    uint64_t number = 0;
    FileType type;
    if (ParseFileName(child, &number, &type) && type == kLogFile) {
      numbers.push_back(number);
    }
  }
  std::sort(numbers.begin(), numbers.end());
  for (uint64_t number : numbers) {
    Segment segment;
    segment.number = number;
    Status size_status =
        env_->GetFileSize(LogFileName(directory_, number), &segment.size);
    if (!size_status.ok()) {
      return size_status;
    }
    segments_.push_back(std::move(segment));
    next_segment_number_ = std::max(next_segment_number_, number + 1);
  }

  if (writable_) {
    status = env_->NewDirectory(directory_, &directory_handle_);
  }
  return status;
}

Status HotWal::CreateSegmentLocked() {
  std::unique_ptr<WritableFile> file;
  const uint64_t number = next_segment_number_++;
  const std::string filename = LogFileName(directory_, number);
  Status status = NewWritableFile(env_, filename, &file, env_options_);
  if (!status.ok()) {
    return status;
  }
  file->SetPreallocationBlockSize(static_cast<size_t>(segment_size_));
  std::unique_ptr<WritableFileWriter> file_writer(
      new WritableFileWriter(std::move(file), filename, env_options_));
  Segment segment;
  segment.number = number;
  segment.writer.reset(
      new log::Writer(std::move(file_writer), number, false, manual_flush_));
  segments_.push_back(std::move(segment));
  purge_requested_ = purge_requested_ || segments_.size() > 1;
  releases_since_purge_ = 0;
  directory_synced_ = false;
  return Status::OK();
}

Status HotWal::SealActiveSegmentLocked() {
  if (segments_.empty() || segments_.back().writer == nullptr) {
    return Status::OK();
  }
  Status status = segments_.back().writer->WriteBuffer();
  if (status.ok()) {
    status = segments_.back().writer->Frozen();
  }
  if (status.ok()) {
    segments_.back().writer.reset();
  }
  return status;
}

Status HotWal::SyncActiveSegmentLocked() {
  if (segments_.empty() || segments_.back().writer == nullptr) {
    return Status::OK();
  }
  Status status = segments_.back().writer->WriteBuffer();
  if (status.ok()) {
    status = segments_.back().writer->file()->Sync(use_fsync_);
  }
  if (status.ok() && !directory_synced_ && directory_handle_ != nullptr) {
    status = directory_handle_->Fsync();
    if (status.ok()) {
      directory_synced_ = true;
    }
  }
  return status;
}

Status HotWal::Append(WriteBatch* batch, uint64_t sequence,
                      size_t reference_count, bool sync,
                      uint64_t* segment_number) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!writable_) {
    return Status::InvalidArgument("Hot WAL is not writable");
  }
  const uint64_t record_size = WriteBatchInternal::ByteSize(batch);
  if (segments_.empty() || segments_.back().writer == nullptr ||
      (segments_.back().size != 0 &&
       segments_.back().size + record_size > segment_size_)) {
    Status status = SealActiveSegmentLocked();
    if (!status.ok()) {
      return status;
    }
    status = CreateSegmentLocked();
    if (!status.ok()) {
      return status;
    }
  }

  WriteBatchInternal::SetSequence(batch, sequence);
  Segment& segment = segments_.back();
  Status status =
      segment.writer->AddRecord(WriteBatchInternal::Contents(batch));
  if (!status.ok()) {
    return status;
  }
  segment.size += record_size;
  segment.pending_writes += reference_count;
  *segment_number = segment.number;
  return sync ? SyncActiveSegmentLocked() : Status::OK();
}

void HotWal::Release(uint64_t segment_number, size_t reference_count) {
  if (segment_number == 0 || reference_count == 0) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  auto segment = std::find_if(segments_.begin(), segments_.end(),
                              [segment_number](const Segment& candidate) {
                                return candidate.number == segment_number;
                              });
  assert(segment != segments_.end());
  assert(segment->pending_writes >= reference_count);
  segment->pending_writes -= reference_count;
  if (segments_.size() > 1 && ++releases_since_purge_ >= 256) {
    purge_requested_ = true;
    releases_since_purge_ = 0;
  }
}

Status HotWal::Flush(bool sync) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (segments_.empty() || segments_.back().writer == nullptr) {
    return Status::OK();
  }
  return sync ? SyncActiveSegmentLocked()
              : segments_.back().writer->WriteBuffer();
}

Status HotWal::ReadAll(std::vector<RecoveredRecord>* records,
                       WALRecoveryMode recovery_mode,
                       bool paranoid_checks) const {
  records->clear();
  std::lock_guard<std::mutex> lock(mutex_);
  for (const Segment& segment : segments_) {
    if (segment.writer != nullptr) {
      return Status::Busy("cannot recover an active Hot WAL segment");
    }
    const std::string filename = LogFileName(directory_, segment.number);
    std::unique_ptr<SequentialFile> file;
    Status status = env_->NewSequentialFile(filename, &file, env_options_);
    if (!status.ok()) {
      return status;
    }
    std::unique_ptr<SequentialFileReader> reader_file(
        new SequentialFileReader(std::move(file), filename));
    HotWalReporter reporter(paranoid_checks &&
                            recovery_mode !=
                                WALRecoveryMode::kSkipAnyCorruptedRecords);
    log::Reader reader(nullptr, std::move(reader_file), &reporter,
                       true /* checksum */, segment.number,
                       false /* retry_after_eof */);
    std::string scratch;
    Slice record;
    while (reader.ReadRecord(&record, &scratch, recovery_mode) &&
           reporter.status_.ok()) {
      if (record.size() < WriteBatchInternal::kHeader) {
        return Status::Corruption("Hot WAL record is too small");
      }
      records->push_back({segment.number, record.ToString()});
    }
    if (!reporter.status_.ok()) {
      if (recovery_mode == WALRecoveryMode::kPointInTimeRecovery) {
        return Status::OK();
      }
      return reporter.status_;
    }
  }
  std::stable_sort(
      records->begin(), records->end(),
      [](const RecoveredRecord& left, const RecoveredRecord& right) {
        WriteBatch left_batch(left.contents);
        WriteBatch right_batch(right.contents);
        return WriteBatchInternal::Sequence(&left_batch) <
               WriteBatchInternal::Sequence(&right_batch);
      });
  return Status::OK();
}

Status HotWal::PurgeObsolete(
    const std::function<uint64_t()>& oldest_segment_to_keep) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t oldest = oldest_segment_to_keep();
  if (oldest == 0 && !segments_.empty() &&
      segments_.back().pending_writes == 0) {
    Status status = SealActiveSegmentLocked();
    if (!status.ok()) {
      return status;
    }
  }
  while (!segments_.empty() && segments_.front().writer == nullptr &&
         segments_.front().pending_writes == 0 &&
         (oldest == 0 || segments_.front().number < oldest)) {
    Status status =
        env_->DeleteFile(LogFileName(directory_, segments_.front().number));
    if (!status.ok() && !status.IsNotFound()) {
      return status;
    }
    segments_.pop_front();
  }
  return Status::OK();
}

Status HotWal::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  Status status = SealActiveSegmentLocked();
  directory_handle_.reset();
  writable_ = false;
  return status;
}

size_t HotWal::segment_count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return segments_.size();
}

bool HotWal::TakePurgeRequest() {
  std::lock_guard<std::mutex> lock(mutex_);
  const bool requested = purge_requested_;
  purge_requested_ = false;
  return requested;
}

Status DestroyHotWal(Env* env, const std::string& wal_directory) {
  const std::string directory = HotWalDirectory(wal_directory);
  std::vector<std::string> children;
  Status status = env->GetChildren(directory, &children);
  if (status.IsNotFound()) {
    return Status::OK();
  }
  if (!status.ok()) {
    return status;
  }
  for (const auto& child : children) {
    if (child == "." || child == "..") {
      continue;
    }
    Status delete_status = env->DeleteFile(directory + "/" + child);
    if (!delete_status.ok() && status.ok()) {
      status = delete_status;
    }
  }
  Status delete_status = env->DeleteDir(directory);
  if (!delete_status.ok() && status.ok()) {
    status = delete_status;
  }
  return status;
}

}  // namespace TERARKDB_NAMESPACE
