#include "table/delta_reader.h"

#include <cstdlib>

#include "db/dbformat.h"
#include "table/block_based_table_reader.h"
#include "table/block_fetcher.h"
#include "util/arena.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/sync_point.h"

namespace TERARKDB_NAMESPACE {

namespace {

bool GetVarint32Checked(const char** p, const char* limit, uint32_t* value) {
  const char* next = GetVarint32Ptr(*p, limit, value);
  if (next == nullptr) {
    return false;
  }
  *p = next;
  return true;
}

}  // namespace

DeltaBlockReader::DeltaBlockReader(
    BlockBasedTable* table, InternalIteratorBase<BlockHandle>* delta_index_iter)
    : table_(table),
      delta_index_iter_(delta_index_iter),
      read_delta_block_(Status::OK()) {
  InitDeltaMetaBlock();
}

DeltaBlockReader::~DeltaBlockReader() { delete delta_index_iter_; }

void DeltaBlockReader::Reset() {
  entry_index_within_block_ = 0;
  sep_entry_index_within_block_ = 0;
}

void DeltaBlockReader::SeekForBlockIndex(const BlockEntryIndex& index_block) {
  if (!read_delta_block_.ok() || delta_index_iter_ == nullptr) {
    return;
  }
  prev_delta_index_ = delta_index_iter_->value();
  block_number_ =
      index_block.first * index_block_restart_interval_ + index_block.second;
  std::string block_index;
  PutFixed32(&block_index, block_number_);
  delta_index_iter_->Seek(block_index);
  if (delta_index_iter_->Valid()) {
    InitDeltaBlock();
  }
}

void DeltaBlockReader::SeekForEntryIndexWithinBlock(
    const BlockEntryIndex& data_block) {
  if (!read_delta_block_.ok()) {
    return;
  }
  entry_index_within_block_ =
      data_block.first * data_block_restart_interval_ + data_block.second - 1;
  bool is_separated = IsSeparated(block_number_, entry_index_within_block_);
  auto rank1 =
      static_cast<int>(BitmapRank1(block_number_, entry_index_within_block_));
  sep_entry_index_within_block_ = is_separated ? rank1 + 1 : -rank1;
}

void DeltaBlockReader::SeekToFirst() {
  if (!read_delta_block_.ok() || delta_index_iter_ == nullptr) {
    return;
  }
  prev_delta_index_ = delta_index_iter_->value();
  delta_index_iter_->SeekToFirst();
  // Entry 0 in the delta index points to the delta meta block.
  delta_index_iter_->Next();
  if (delta_index_iter_->Valid()) {
    InitDeltaBlock();
  }
  block_number_ = 1;
  entry_index_within_block_ = 0;
  bool is_separated = IsSeparated(block_number_, entry_index_within_block_);
  sep_entry_index_within_block_ = is_separated ? 1 : 0;
}

void DeltaBlockReader::Next() {
  if (!read_delta_block_.ok()) {
    return;
  }
  entry_index_within_block_++;
  bool is_separated = IsSeparated(block_number_, entry_index_within_block_);
  if (is_separated) {
    sep_entry_index_within_block_ = abs(sep_entry_index_within_block_) + 1;
  } else {
    sep_entry_index_within_block_ = -abs(sep_entry_index_within_block_);
  }
}

void DeltaBlockReader::FindKeyForward() {
  if (!read_delta_block_.ok() || delta_index_iter_ == nullptr) {
    return;
  }
  block_number_++;
  entry_index_within_block_ = 0;
  bool is_separated = IsSeparated(block_number_, entry_index_within_block_);
  sep_entry_index_within_block_ = is_separated ? 1 : 0;
  if (delta_index_iter_->Valid()) {
    Slice key = delta_index_iter_->key();
    uint32_t max_block_number = 0;
    if (!GetFixed32(&key, &max_block_number)) {
      read_delta_block_ = Status::Corruption("bad delta index key");
      return;
    }
    if (max_block_number < block_number_) {
      delta_index_iter_->Next();
      if (delta_index_iter_->Valid()) {
        InitDeltaBlock();
      }
    }
  }
}

uint32_t DeltaBlockReader::CurrentValueSize() const {
  const DeltaBlockInfo* info = CurrentDeltaBlockInfo();
  if (info == nullptr || sep_entry_index_within_block_ <= 0) {
    return 0;
  }
  const size_t index = static_cast<size_t>(sep_entry_index_within_block_ - 1);
  return index < info->value_size.size() ? info->value_size[index] : 0;
}

Slice DeltaBlockReader::CurrentValueMeta() const {
  const DeltaBlockInfo* info = CurrentDeltaBlockInfo();
  if (info == nullptr || sep_entry_index_within_block_ <= 0) {
    return Slice();
  }
  const size_t index = static_cast<size_t>(sep_entry_index_within_block_ - 1);
  return index < info->value_meta.size() ? info->value_meta[index] : Slice();
}

void DeltaBlockReader::InitDeltaMetaBlock() {
  if (delta_index_iter_ == nullptr) {
    read_delta_block_ = Status::Corruption("missing delta index iterator");
    return;
  }
  auto* rep = table_->get_rep();
  delta_index_iter_->SeekToFirst();
  if (!delta_index_iter_->Valid()) {
    read_delta_block_ = delta_index_iter_->status().ok()
                            ? Status::Corruption("empty delta index block")
                            : delta_index_iter_->status();
    return;
  }
  BlockHandle delta_block_handle = delta_index_iter_->value();
  auto delta_cont = std::unique_ptr<BlockContents>(new BlockContents());
  PersistentCacheOptions cache_options;
  ReadOptions read_options;
  BlockFetcher delta_block_fetcher(
      rep->file.get(), nullptr, rep->footer, read_options, delta_block_handle,
      delta_cont.get(), rep->ioptions, false /* decompress */,
      false /* maybe_compressed */, Slice() /* compression dict */,
      cache_options);
  Status s = delta_block_fetcher.ReadBlockContents();
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTable::InitDeltaMetaBlock::ReadDeltaMetaBlockFailed", &s);

  if (!s.ok()) {
    ROCKS_LOG_WARN(rep->ioptions.info_log,
                   "Encountered error while reading delta meta block: %s",
                   s.ToString().c_str());
    read_delta_block_ = s;
    return;
  }

  Slice data = delta_cont->data;
  const char* p = data.data();
  const char* limit = p + data.size();
  uint32_t entry_size = 0;
  if (!GetVarint32Checked(&p, limit, &index_block_restart_interval_) ||
      !GetVarint32Checked(&p, limit, &data_block_restart_interval_) ||
      !GetVarint32Checked(&p, limit, &internal_size_) ||
      !GetVarint32Checked(&p, limit, &entry_size)) {
    read_delta_block_ = Status::Corruption("bad delta meta block");
    return;
  }
  block_num_entry_.clear();
  block_num_entry_.reserve(entry_size);
  for (uint32_t i = 0; i < entry_size; ++i) {
    uint32_t entry_num = 0;
    if (!GetVarint32Checked(&p, limit, &entry_num)) {
      read_delta_block_ = Status::Corruption("bad delta meta entries");
      return;
    }
    block_num_entry_.push_back(entry_num);
  }
  uint32_t bitmap_len = 0;
  if (!GetVarint32Checked(&p, limit, &bitmap_len) || p + bitmap_len > limit) {
    read_delta_block_ = Status::Corruption("bad delta bitmap");
    return;
  }
  terark::rank_select_il bitmap;
  bitmap.risk_mmap_from(reinterpret_cast<unsigned char*>(const_cast<char*>(p)),
                        bitmap_len);
  separated_bit_map_ = bitmap;
  bitmap.risk_release_ownership();
}

void DeltaBlockReader::InitDeltaBlock() {
  auto* rep = table_->get_rep();
  BlockHandle delta_block_handle = delta_index_iter_->value();
  if (delta_block_handle.offset() == prev_delta_index_.offset()) {
    return;
  }
  delta_blocks_.clear();
  auto delta_cont = std::unique_ptr<BlockContents>(new BlockContents());
  PersistentCacheOptions cache_options;
  ReadOptions read_options;
  BlockFetcher delta_block_fetcher(
      rep->file.get(), nullptr, rep->footer, read_options, delta_block_handle,
      delta_cont.get(), rep->ioptions, false /* decompress */,
      false /* maybe_compressed */, Slice() /* compression dict */,
      cache_options);
  Status s = delta_block_fetcher.ReadBlockContents();
  TEST_SYNC_POINT_CALLBACK(
      "BlockBasedTable::InitDeltaBlock::ReadDeltaBlockFailed", &s);

  if (!s.ok()) {
    ROCKS_LOG_WARN(rep->ioptions.info_log,
                   "Encountered error while reading delta block: %s",
                   s.ToString().c_str());
    read_delta_block_ = s;
    return;
  }

  Slice data = delta_cont->data;
  const char* p = data.data();
  const char* limit = p + data.size();
  while (p < limit) {
    uint32_t block_number = 0;
    uint32_t value_size_length = 0;
    if (!GetVarint32Checked(&p, limit, &block_number) ||
        !GetVarint32Checked(&p, limit, &value_size_length)) {
      read_delta_block_ = Status::Corruption("bad delta block header");
      return;
    }

    DeltaBlockInfo delta_block_info;
    delta_block_info.holder.reset(new Arena());
    for (uint32_t i = 0; i < value_size_length; ++i) {
      uint32_t value_size = 0;
      uint32_t meta_size = 0;
      if (!GetVarint32Checked(&p, limit, &value_size) ||
          !GetVarint32Checked(&p, limit, &meta_size) || p + meta_size > limit) {
        read_delta_block_ = Status::Corruption("bad delta value metadata");
        return;
      }
      delta_block_info.value_size.emplace_back(value_size);
      if (meta_size != 0) {
        delta_block_info.value_meta.emplace_back(
            ArenaPinSlice(Slice(p, meta_size), delta_block_info.holder.get()));
        p += meta_size;
      } else {
        delta_block_info.value_meta.emplace_back();
      }
    }
    delta_blocks_.emplace(block_number, std::move(delta_block_info));
  }
}

bool DeltaBlockReader::IsSeparated(uint32_t block_number,
                                   uint32_t entry_index) const {
  if (block_number == 0 || block_num_entry_.empty()) {
    return false;
  }
  uint64_t index = entry_index;
  if (block_number > 1) {
    if (block_number - 2 >= block_num_entry_.size()) {
      return false;
    }
    index += block_num_entry_[block_number - 2];
  }
  return separated_bit_map_.is1(index);
}

uint32_t DeltaBlockReader::BitmapRank1(uint32_t block_number,
                                       uint32_t entry_index) const {
  if (block_number == 0 || block_num_entry_.empty()) {
    return 0;
  }
  if (block_number == 1) {
    return static_cast<uint32_t>(separated_bit_map_.rank1(entry_index));
  }
  if (block_number - 2 >= block_num_entry_.size()) {
    return 0;
  }
  uint64_t prev_entries = block_num_entry_[block_number - 2];
  uint32_t prev_rank = static_cast<uint32_t>(separated_bit_map_.rank1(prev_entries));
  uint64_t index = prev_entries + entry_index;
  uint32_t rank1 = static_cast<uint32_t>(separated_bit_map_.rank1(index));
  return rank1 - prev_rank;
}

const DeltaBlockReader::DeltaBlockInfo*
DeltaBlockReader::CurrentDeltaBlockInfo() const {
  auto iter = delta_blocks_.find(block_number_);
  if (iter == delta_blocks_.end()) {
    return nullptr;
  }
  return &iter->second;
}

}  // namespace TERARKDB_NAMESPACE
