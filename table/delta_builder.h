#pragma once

#include <stdint.h>

#include <list>
#include <string>
#include <vector>

#include <terark/rank_select.hpp>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"
#include "table/block_builder.h"
#include "table/format.h"
#include "table/internal_iterator.h"

namespace TERARKDB_NAMESPACE {

// Build the SST-side delta metadata block.
//
// The delta block contains no keys. Its entries are ordered exactly as the
// corresponding data block entries and record per-entry metadata for separated
// values, currently value size and optional value meta.
class DeltaBuilder {
 public:
  DeltaBuilder(const DeltaBuilder&) = delete;
  void operator=(const DeltaBuilder&) = delete;

  explicit DeltaBuilder(int data_restart_interval, int index_restart_interval,
                        int version, uint64_t flush_size = 16 * 1024);

  void Reset();

  void Add(bool is_separated, uint32_t separated_value_size,
           const std::string* value_meta = nullptr);

  Status Finish(LazyBuffer* block_content);
  Status Finish(LazyBuffer* block_content,
                const BlockHandle& last_partition_block_handle);

  bool empty() const { return delta_blocks_.empty() && delta_block_.empty(); }
  uint64_t size() const { return total_size_; }

  // Records the data-block boundary so the reader can align data block and
  // delta block positions during compaction.
  void AddIndexEntry(uint32_t block_number, uint32_t num_entry,
                     bool force = false);

  void FinishMetaBlock();

 private:
  struct Entry {
    std::string key;
    std::string buffer;
    Entry(const std::string& k, const std::string& b) : key(k), buffer(b) {}
  };

  std::list<Entry> delta_blocks_;
  BlockBuilder delta_index_builder_;
  std::string buffer_;
  std::string meta_block_;
  terark::rank_select_il bitmap_;
  std::string delta_block_;
  bool finishing_delta_ = false;
  uint32_t version_;
  uint64_t counter_;
  uint64_t flush_size_;
  std::vector<uint32_t> num_entries_;
  uint64_t total_size_;
};

}  // namespace TERARKDB_NAMESPACE
