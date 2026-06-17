#include "table/delta_builder.h"

#include "util/coding.h"

namespace TERARKDB_NAMESPACE {

DeltaBuilder::DeltaBuilder(int data_restart_interval,
                           int index_restart_interval, int version,
                           uint64_t flush_size)
    : delta_index_builder_(1),
      version_(version),
      counter_(0),
      flush_size_(flush_size),
      total_size_(0) {
  uint32_t internal_meta_size = 0;
  switch (version) {
    case 1:
      internal_meta_size = sizeof(uint32_t);
      break;
    default:
      assert(false);
  }
  PutVarint32(&meta_block_, index_restart_interval);
  PutVarint32(&meta_block_, data_restart_interval);
  PutVarint32(&meta_block_, internal_meta_size);
  static_cast<void>(version_);
}

void DeltaBuilder::Reset() {
  buffer_.clear();
  bitmap_.clear();
  delta_block_.clear();
  delta_blocks_.clear();
  meta_block_.clear();
  finishing_delta_ = false;
  counter_ = 0;
  total_size_ = 0;
  num_entries_.clear();
}

void DeltaBuilder::Add(bool is_separated, uint32_t separated_value_size,
                       const std::string* value_meta) {
  if (!is_separated) {
    assert(separated_value_size == 0);
    bitmap_.push_back(false);
    return;
  }
  PutVarint32(&buffer_, separated_value_size);
  if (value_meta != nullptr && !value_meta->empty()) {
    PutVarint32(&buffer_, static_cast<uint32_t>(value_meta->size()));
    buffer_.append(*value_meta);
  } else {
    PutVarint32(&buffer_, 0);
  }
  bitmap_.push_back(true);
  ++counter_;
}

void DeltaBuilder::AddIndexEntry(uint32_t block_number, uint32_t num_entry,
                                 bool force) {
  // The builder may receive repeated finalization calls in some empty/flush
  // boundary cases. Once delta finalization has started, no new data-block
  // boundary should be appended.
  if (finishing_delta_) {
    return;
  }
  num_entries_.push_back(num_entry);
  if (!buffer_.empty()) {
    PutVarint32(&delta_block_, block_number);
    PutVarint32(&delta_block_, static_cast<uint32_t>(counter_));
    delta_block_.append(buffer_);
  }
  const uint32_t current_size = static_cast<uint32_t>(delta_block_.size());
  if ((!delta_block_.empty() && force) || current_size > flush_size_) {
    std::string key;
    PutFixed32(&key, block_number);
    delta_blocks_.emplace_back(key, delta_block_);
    total_size_ += delta_block_.size();
    delta_block_.clear();
  }
  buffer_.clear();
  counter_ = 0;
}

Status DeltaBuilder::Finish(LazyBuffer* block_content) {
  BlockHandle last_delta_block_handle;
  return Finish(block_content, last_delta_block_handle);
}

Status DeltaBuilder::Finish(LazyBuffer* block_content,
                            const BlockHandle& last_delta_block_handle) {
  assert(buffer_.empty() && delta_block_.empty());
  if (finishing_delta_) {
    Entry& last_block = delta_blocks_.front();
    std::string handle_encoding;
    last_delta_block_handle.EncodeTo(&handle_encoding);
    delta_index_builder_.Add(last_block.key, handle_encoding);
    delta_blocks_.pop_front();
  }
  if (delta_blocks_.empty()) {
    block_content->reset(delta_index_builder_.Finish(), true, uint64_t(-1));
    return Status::OK();
  }
  if (!finishing_delta_) {
    FinishMetaBlock();
  }
  Entry& last_block = delta_blocks_.front();
  block_content->reset(last_block.buffer, true, uint64_t(-1));
  finishing_delta_ = true;
  return Status::Incomplete();
}

void DeltaBuilder::FinishMetaBlock() {
  PutVarint32(&meta_block_, static_cast<uint32_t>(num_entries_.size()));
  for (uint32_t entries : num_entries_) {
    PutVarint32(&meta_block_, entries);
  }
  bitmap_.build_cache(false, false);
  PutVarint32(&meta_block_, static_cast<uint32_t>(bitmap_.mem_size()));
  meta_block_.append(reinterpret_cast<char*>(bitmap_.data()),
                     bitmap_.mem_size());
  std::string key;
  PutFixed32(&key, 0);
  total_size_ += meta_block_.size();
  delta_blocks_.emplace_front(key, meta_block_);
}

}  // namespace TERARKDB_NAMESPACE
