// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <stdint.h>

#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

// Define all public custom types here.

// SST role used by middle-value delta-separate. Normal key SSTs contain inline
// values plus value indexes. Large blob SSTs contain values whose size is at or
// above middle_blob_size. Hot/cold middle blob SSTs contain values in
// [blob_size, middle_blob_size), split by whether they were produced by flush
// or compaction/GC.
//
// kWarmLargeBlob/kColdLargeBlob are access-hotness variants of kLargeBlob, used
// only when enable_hotness_tracker is on. Flush emits warm; GC keeps inputs
// grouped by hotness so a job only mixes large blobs of the same variant and
// the inheritance owner stays unique (one-to-one). Reclassification after GC:
// warm survivors stay warm, cold survivors stay cold.
enum SstType : uint8_t {
  kNormal = 0,
  kLargeBlob = 1,
  kHotMidBlob = 2,
  kColdMidBlob = 3,
  kWarmLargeBlob = 4,
  kColdLargeBlob = 5,
  kMaxSstType = 64,
};

// Represents a sequence number in a WAL file.
typedef uint64_t SequenceNumber;

// Dependence pair
struct Dependence {
  uint64_t file_number;
  uint64_t entry_count;
  union {
    uint64_t byte_count;
    uint64_t separated_total_size;
  };

  Dependence() : file_number(0), entry_count(0), byte_count(0) {}
  Dependence(uint64_t file, uint64_t entries, uint64_t separated_size)
      : file_number(file),
        entry_count(entries),
        separated_total_size(separated_size) {}
};

// User-oriented representation of internal key types.
enum EntryType {
  kEntryPut,
  kEntryDelete,
  kEntrySingleDelete,
  kEntryMerge,
  kEntryRangeDeletion,
  kEntryValueIndex,
  kEntryMergeIndex,
  kEntryOther,
};

// <user key, sequence number, and entry type> tuple.
struct FullKey {
  Slice user_key;
  SequenceNumber sequence;
  EntryType type;

  FullKey() : sequence(0) {}  // Intentionally left uninitialized (for speed)
  FullKey(const Slice& u, const SequenceNumber& seq, EntryType t)
      : user_key(u), sequence(seq), type(t) {}
  std::string DebugString(bool hex = false) const;

  void clear() {
    user_key.clear();
    sequence = 0;
    type = EntryType::kEntryPut;
  }
};

// Parse slice representing internal key to FullKey
// Parsed FullKey is valid for as long as the memory pointed to by
// internal_key is alive.
bool ParseFullKey(const Slice& internal_key, FullKey* result);

}  //  namespace TERARKDB_NAMESPACE
