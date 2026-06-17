#pragma once

#include <stdint.h>

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <terark/rank_select.hpp>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"
#include "table/format.h"
#include "table/internal_iterator.h"

namespace TERARKDB_NAMESPACE {

class Arena;
class BlockBasedTable;

// Reader for the SST delta metadata block.
//
// The delta index block maps data-block numbers to physical delta blocks. The
// first delta entry is the meta block; the remaining entries contain separated
// value metadata grouped by data block.
class DeltaBlockReader {
 public:
  using BlockEntryIndex = std::pair<uint32_t, uint32_t>;

  struct DeltaBlockInfo {
    std::vector<uint32_t> value_size;
    std::vector<Slice> value_meta;
    std::unique_ptr<Arena> holder;
  };

  DeltaBlockReader(BlockBasedTable* table,
                   InternalIteratorBase<BlockHandle>* delta_index_iter);
  ~DeltaBlockReader();

  Status status() const { return read_delta_block_; }

  void SeekForBlockIndex(const BlockEntryIndex& index_block);
  void SeekForEntryIndexWithinBlock(const BlockEntryIndex& data_block);
  void SeekToFirst();
  void Next();
  void FindKeyForward();
  void Reset();

  bool CurrentEntryIsSeparated() const {
    return sep_entry_index_within_block_ > 0;
  }
  uint32_t CurrentValueSize() const;
  Slice CurrentValueMeta() const;

 private:
  void InitDeltaMetaBlock();
  void InitDeltaBlock();

  bool IsSeparated(uint32_t block_number, uint32_t entry_index) const;
  uint32_t BitmapRank1(uint32_t block_number, uint32_t entry_index) const;
  const DeltaBlockInfo* CurrentDeltaBlockInfo() const;

  BlockBasedTable* table_;
  InternalIteratorBase<BlockHandle>* delta_index_iter_;
  std::unordered_map<uint32_t, DeltaBlockInfo> delta_blocks_;
  uint32_t data_block_restart_interval_ = 0;
  uint32_t index_block_restart_interval_ = 0;
  uint32_t internal_size_ = 0;
  uint32_t block_number_ = 0;
  uint32_t entry_index_within_block_ = 0;
  int32_t sep_entry_index_within_block_ = 0;
  BlockHandle prev_delta_index_;
  std::vector<uint32_t> block_num_entry_;
  terark::rank_select_il separated_bit_map_;
  Status read_delta_block_;
};

}  // namespace TERARKDB_NAMESPACE
