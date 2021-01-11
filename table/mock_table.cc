//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "table/mock_table.h"

#include "db/dbformat.h"
#include "port/port.h"
#include "rocksdb/table_properties.h"
#include "table/get_context.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"

namespace TERARKDB_NAMESPACE {
namespace mock {

namespace {

const InternalKeyComparator icmp_(BytewiseComparator());

}  // namespace

stl_wrappers::KVMap MakeMockFile(
    std::initializer_list<std::pair<const std::string, std::string>> l) {
  return stl_wrappers::KVMap(l, stl_wrappers::LessOfComparator(&icmp_));
}

InternalIterator* MockTableReader::NewIterator(
    const ReadOptions&, const SliceTransform* /* prefix_extractor */,
    Arena* arena, bool /*skip_filters*/, bool /*for_compaction*/) {
  using IterType = MockTableIterator<LazyBuffer>;
  if (arena == nullptr) {
    return new IterType(file_data_.table);
  } else {
    return new (arena->AllocateAligned(sizeof(IterType)))
        IterType(file_data_.table);
  }
}

Status MockTableReader::Get(const ReadOptions&, const Slice& key,
                            GetContext* get_context,
                            const SliceTransform* /*prefix_extractor*/,
                            bool /*skip_filters*/) {
  MockTableIterator<LazyBuffer> iter(file_data_.table);
  for (iter.Seek(key); iter.Valid(); iter.Next()) {
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(iter.key(), &parsed_key)) {
      return Status::Corruption(Slice());
    }

    bool dont_care __attribute__((__unused__));
    if (!get_context->SaveValue(parsed_key, iter.value(), &dont_care)) {
      break;
    }
  }
  return Status::OK();
}

std::shared_ptr<const TableProperties> MockTableReader::GetTableProperties()
    const {
  return file_data_.prop;
}

FragmentedRangeTombstoneIterator* MockTableReader::NewRangeTombstoneIterator(
    const ReadOptions& /*read_options*/) {
  std::unique_ptr<InternalIteratorBase<Slice>> unfragmented_range_del_iter(
      new MockTableIterator<Slice>(file_data_.tombstone));
  auto tombstone_list = std::make_shared<FragmentedRangeTombstoneList>(
      std::move(unfragmented_range_del_iter), icmp_);
  FragmentedRangeTombstoneIterator* range_del_iter =
      new FragmentedRangeTombstoneIterator(tombstone_list, icmp_,
                                           kMaxSequenceNumber);
  return range_del_iter;
}

MockTableFactory::MockTableFactory() : next_id_(1) {}

Status MockTableFactory::NewTableReader(
    const TableReaderOptions& /*table_reader_options*/,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t /*file_size*/,
    std::unique_ptr<TableReader>* table_reader,
    bool /*prefetch_index_and_filter_in_cache*/) const {
  uint32_t id = GetIDFromFile(file.get());

  MutexLock lock_guard(&file_system_.mutex);

  auto it = file_system_.files.find(id);
  if (it == file_system_.files.end()) {
    return Status::IOError("Mock file not found");
  }

  table_reader->reset(new MockTableReader(it->second));

  return Status::OK();
}

TableBuilder* MockTableFactory::NewTableBuilder(
    const TableBuilderOptions& /*table_builder_options*/,
    uint32_t /*column_family_id*/, WritableFileWriter* file) const {
  uint32_t id = GetAndWriteNextID(file);

  return new MockTableBuilder(id, &file_system_);
}

Status MockTableFactory::CreateMockTable(Env* env, const std::string& fname,
                                         stl_wrappers::KVMap file_contents) {
  std::unique_ptr<WritableFile> file;
  auto s = env->NewWritableFile(fname, &file, EnvOptions());
  if (!s.ok()) {
    return s;
  }

  WritableFileWriter file_writer(std::move(file), fname, EnvOptions());

  uint32_t id = GetAndWriteNextID(&file_writer);

  MockTableFileSystem::FileData file_data;
  TableProperties prop;

  file_data.table = std::move(file_contents);

  prop.num_entries = file_data.table.size();
  prop.raw_key_size = file_data.table.size();
  prop.raw_value_size = file_data.table.size();
  file_data.prop = std::make_shared<const TableProperties>(std::move(prop));

  file_system_.files.emplace(id, std::move(file_data));
  return Status::OK();
}

uint32_t MockTableFactory::GetAndWriteNextID(WritableFileWriter* file) const {
  uint32_t next_id = next_id_.fetch_add(1);
  char buf[4];
  EncodeFixed32(buf, next_id);
  file->Append(Slice(buf, 4));
  return next_id;
}

uint32_t MockTableFactory::GetIDFromFile(RandomAccessFileReader* file) const {
  char buf[4];
  Slice result;
  file->Read(0, 4, &result, buf);
  return DecodeFixed32(result.data());
}

void MockTableFactory::AssertSingleFile(
    const stl_wrappers::KVMap& file_contents,
    const stl_wrappers::KVMap& range_deletions) {
  ASSERT_EQ(file_system_.files.size(), 1U);
  ASSERT_EQ(file_contents, file_system_.files.begin()->second.table);
  ASSERT_EQ(range_deletions, file_system_.files.begin()->second.tombstone);
}

void MockTableFactory::AssertLatestFile(
    const stl_wrappers::KVMap& file_contents) {
  ASSERT_GE(file_system_.files.size(), 1U);
  auto latest = file_system_.files.end();
  --latest;

  if (file_contents != latest->second.table) {
    std::cout << "Wrong content! Content of latest file:" << std::endl;
    for (const auto& kv : latest->second.table) {
      ParsedInternalKey ikey;
      std::string key, value;
      std::tie(key, value) = kv;
      ParseInternalKey(Slice(key), &ikey);
      std::cout << ikey.DebugString(false) << " -> " << value << std::endl;
    }
    FAIL();
  }
}

}  // namespace mock
}  // namespace TERARKDB_NAMESPACE
