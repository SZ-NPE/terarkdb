#include "util/static_index_map.h"

#include <string>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/slice.h"
#include "table/table_reader.h"

namespace TERARKDB_NAMESPACE {

StaticMapIndex::StaticMapIndex(const InternalKeyComparator *c)
    : c_(c),
      key_buff_(nullptr),
      value_buff_(nullptr),
      key_offset_(nullptr),
      value_offset_(nullptr) {}

StaticMapIndex::~StaticMapIndex() {
  if (key_buff_ != nullptr) {
    delete[] key_buff_;
    delete[] value_buff_;
    delete[] key_offset_;
    delete[] value_offset_;
  }
}

char *StaticMapIndex::GetKeyOffset(uint32_t id) const {
  assert(id < key_nums_);
  return key_buff_ + key_offset_[id];
}

uint32_t StaticMapIndex::GetKeyLen(uint32_t id) const {
  assert(id < key_nums_);
  return key_offset_[id + 1] - key_offset_[id];
}

char *StaticMapIndex::GetValueOffset(uint32_t id) const {
  assert(id < key_nums_);
  return value_buff_ + value_offset_[id];
}

uint32_t StaticMapIndex::GetValueLen(uint32_t id) const {
  assert(id < key_nums_);
  return value_offset_[id + 1] - value_offset_[id];
}

Slice StaticMapIndex::GetKey(uint32_t id) const {
  assert(key_buff_ != nullptr);
  return Slice(GetKeyOffset(id), GetKeyLen(id));
}

Slice StaticMapIndex::GetValue(uint32_t id) const {
  assert(value_buff_ != nullptr);
  return Slice(GetValueOffset(id), GetValueLen(id));
}

uint32_t StaticMapIndex::Size() {
  return key_len_ + value_len_ + key_nums_ * 16 + 16;
}

uint32_t StaticMapIndex::GetIndex(const Slice &key) {
  uint32_t l = 0;
  uint32_t r = key_nums_ - 1;
  while (l < r) {
    uint32_t mid = (l + r) >> 1;
    if (c_->Compare(key, GetKey(mid)) == 0) {
      l = mid;
      break;
    } else if (c_->Compare(key, GetKey(mid)) < 0) {
      r = mid - 1;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

bool StaticMapIndex::FindKey(const Slice &key) {
  uint32_t index = GetIndex(key);
  if (c_->Compare(key, GetKey(index)) == 0) {
    return true;
  } else {
    return false;
  }
}

bool StaticMapIndex::DecodeFrom(Slice &map_input) {
  Slice smallest_key;
  uint64_t link_count;
  std::vector<int> dependence;
  uint64_t flags;
  if (!GetVarint64(&map_input, &flags) ||
      !GetVarint64(&map_input, &link_count) ||
      !GetLengthPrefixedSlice(&map_input, &smallest_key)) {
    std::cout << "parse error" << std::endl;
    return false;
  }
  uint64_t file_number;
  for (uint64_t i = 0; i < link_count; ++i) {
    if (!GetVarint64(&map_input, &file_number)) {
      std::cout << "parse error" << std::endl;
    }
    dependence.push_back(file_number);
  }
  InternalKey ikey;
  ikey.DecodeFrom(smallest_key);
  std::cout << "link_count:" << link_count
            << " smallest_key:" << ikey.DebugString() << std::endl;
}

void StaticMapIndex::DebugString() {
  InternalKey ikey;
  for (uint32_t i = 0; i < key_nums_; i++) {
    ikey.DecodeFrom(GetKey(i));
    std::cout << ikey.DebugString() << std::endl;
    Slice v = GetValue(i);
    DecodeFrom(v);
  }
}

Status StaticMapIndex::BuildStaticMapIndex(
    std::unique_ptr<InternalIteratorBase<Slice>> iter) {
  Status status = iter->status();
  if (!status.ok()) {
    return status;
  }
  uint32_t key_nums = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    key_nums++;
  }
  uint32_t key_lens = 0;
  uint32_t value_lens = 0;
  uint32_t *key_offset = new uint32_t[key_nums + 1];
  uint32_t *value_offset = new uint32_t[key_nums + 1];
  uint32_t i = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    key_offset[i] = key_lens;
    value_offset[i] = value_lens;
    key_lens += iter->key().size();
    value_lens += iter->value().size();
    i++;
  }
  key_offset[key_nums] = key_lens;
  value_offset[key_nums] = value_lens;
  char *key_buffer = (char *)malloc(key_lens);
  char *value_buffer = (char *)malloc(value_lens);
  i = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    Slice curr_key = iter->key();
    IterKey iter_key;
    ParsedInternalKey ikey;
    if (!ParseInternalKey(curr_key, &ikey)) {
      status =
          Status::Corruption("ProcessGarbageCollection invalid InternalKey");
      break;
    }
    iter_key.SetInternalKey(ikey.user_key, ikey.sequence, kValueTypeForSeek);
    // Used for GC GetKey: Insert key, value to key_buffer and value_buffer
    // The format adapts to which passed in by the GetKey operation
    // key: user_key + Pack(sequence, kValueTypeForSeek)  value:
    // DataBlockIter->value()
    memcpy(key_buffer + key_offset[i], iter_key.GetInternalKey().data(),
           iter_key.Size());
    memcpy(value_buffer + value_offset[i], iter->value().data(),
           iter->value().size());
    i++;
  }
  key_buff_ = key_buffer;
  value_buff_ = value_buffer;
  key_offset_ = key_offset;
  value_offset_ = value_offset;
  key_nums_ = key_nums;
  key_len_ = key_lens;
  value_len_ = value_lens;
  return status;
}

}  // namespace TERARKDB_NAMESPACE
