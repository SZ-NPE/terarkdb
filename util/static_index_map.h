#pragma once
#include <assert.h>
#include <stdio.h>

#include <iostream>
#include <vector>

#include "db/dbformat.h"
#include "db/map_builder.h"
#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "util/coding.h"

namespace TERARKDB_NAMESPACE {

class StaticMapIndex {
 public:
  StaticMapIndex(const InternalKeyComparator* c);

  ~StaticMapIndex();

  char* GetKeyOffset(uint32_t id) const;

  uint32_t GetKeyLen(uint32_t id) const;

  char* GetValueOffset(uint32_t id) const;

  uint32_t GetValueLen(uint32_t id) const;

  Slice GetKey(uint32_t id) const;

  Slice GetValue(uint32_t id) const;

  uint32_t Size();

  uint32_t GetIndex(const Slice& key);

  bool FindKey(const Slice& key);

  bool DecodeFrom(Slice& map_input);

  void DebugString();

  Status BuildStaticMapIndex(std::unique_ptr<InternalIteratorBase<Slice>> iter);

 private:
  const InternalKeyComparator* c_;
  char* key_buff_;
  char* value_buff_;
  uint32_t* key_offset_;
  uint32_t* value_offset_;
  uint32_t key_nums_;
  uint32_t key_len_;
  uint32_t value_len_;
};

}  // namespace TERARKDB_NAMESPACE