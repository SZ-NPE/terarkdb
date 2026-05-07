//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/dbformat.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdio.h>

#include "monitoring/perf_context_imp.h"
#include "port/port.h"
#include "rocksdb/terark_namespace.h"
#include "util/coding.h"
#include "util/string_util.h"

namespace TERARKDB_NAMESPACE {

// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
const ValueType kValueTypeForSeek = kTypeMergeIndex;
const ValueType kValueTypeForSeekForPrev = kTypeDeletion;

uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(IsExtendedValueType(t));
  return (seq << 8) | t;
}

EntryType GetEntryType(ValueType value_type) {
  switch (value_type) {
    case kTypeValue:
      return kEntryPut;
    case kTypeDeletion:
      return kEntryDelete;
    case kTypeSingleDeletion:
      return kEntrySingleDelete;
    case kTypeMerge:
      return kEntryMerge;
    case kTypeRangeDeletion:
      return kEntryRangeDeletion;
    case kTypeValueIndex:
      return kEntryValueIndex;
    case kTypeMergeIndex:
      return kEntryMergeIndex;
    default:
      return kEntryOther;
  }
}

bool ParseFullKey(const Slice& internal_key, FullKey* fkey) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }
  fkey->user_key = ikey.user_key;
  fkey->sequence = ikey.sequence;
  fkey->type = GetEntryType(ikey.type);
  return true;
}

void UnPackSequenceAndType(uint64_t packed, uint64_t* seq, ValueType* t) {
  *seq = packed >> 8;
  *t = static_cast<ValueType>(packed & 0xff);

  assert(*seq <= kMaxSequenceNumber);
  assert(IsExtendedValueType(*t));
}

void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));
}

void AppendInternalKeyFooter(std::string* result, SequenceNumber s,
                             ValueType t) {
  PutFixed64(result, PackSequenceAndType(s, t));
}

std::string ParsedInternalKey::DebugString(bool hex) const {
  char buf[50];
  snprintf(buf, sizeof(buf), "' seq:%" PRIu64 ", type:%d", sequence,
           static_cast<int>(type));
  std::string result = "'";
  result += user_key.ToString(hex);
  result += buf;
  return result;
}

std::string InternalKey::DebugString(bool hex) const {
  std::string result;
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    result = parsed.DebugString(hex);
  } else {
    result = "(bad)";
    result.append(EscapeString(rep_));
  }
  return result;
}

const char* InternalKeyComparator::Name() const { return name_.c_str(); }

int InternalKeyComparator::Compare(const ParsedInternalKey& a,
                                   const ParsedInternalKey& b) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)
  int r = user_comparator_->Compare(a.user_key, b.user_key);
  PERF_COUNTER_ADD(user_key_comparison_count, 1);
  if (r == 0) {
    if (a.sequence > b.sequence) {
      r = -1;
    } else if (a.sequence < b.sequence) {
      r = +1;
    } else if (a.type > b.type) {
      r = -1;
    } else if (a.type < b.type) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestSeparator(std::string* start,
                                                  const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);
  Slice user_limit = ExtractUserKey(limit);
  std::string tmp(user_start.data(), user_start.size());
  user_comparator_->FindShortestSeparator(&tmp, user_limit);
  if (tmp.size() <= user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() <= user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

LookupKey::LookupKey(const Slice& _user_key, SequenceNumber s) {
  size_t usize = _user_key.size();
  size_t needed = usize + 13;  // A conservative estimate
  char* dst;
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }
  start_ = dst;
  // NOTE: We don't support users keys of more than 2GB :)
  dst = EncodeVarint32(dst, static_cast<uint32_t>(usize + 8));
  kstart_ = dst;
  memcpy(dst, _user_key.data(), usize);
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek));
  dst += 8;
  end_ = dst;
}

void IterKey::EnlargeBuffer(size_t key_size) {
  // If size is smaller than buffer size, continue using current buffer,
  // or the static allocated one, as default
  assert(key_size > buf_size_);
  // Need to enlarge the buffer.
  ResetBuffer();
  buf_ = new char[key_size];
  buf_size_ = key_size;
}

Status SeparateHelper::TransToSeparate(
    const Slice& internal_key, LazyBuffer& value, uint64_t file_number,
    const Slice& meta, bool is_merge, bool is_index,
    const ValueExtractor* value_meta_extractor) {
  assert(file_number != uint64_t(-1));
  if (value_meta_extractor == nullptr || is_merge) {
    value.reset(EncodeFileNumber(file_number), true, file_number);
    return Status::OK();
  }
  if (is_index) {
    Slice parts[] = {EncodeFileNumber(file_number), meta};
    value.reset(SliceParts(parts, 2), file_number);
    return Status::OK();
  } else {
    auto s = value.fetch();
    if (!s.ok()) {
      return s;
    }
    std::string value_meta;
    s = value_meta_extractor->Extract(ExtractUserKey(internal_key),
                                      value.slice(), &value_meta);
    if (s.ok()) {
      Slice parts[] = {EncodeFileNumber(file_number), value_meta};
      value.reset(SliceParts(parts, 2), file_number);
    }
    return s;
  }
}

// Out-of-class definitions for the static constexpr members declared in
// dbformat.h. Required under C++14 for ODR-use (e.g. passing them by
// reference to ASSERT_EQ in tests). Harmless under C++17+ where they
// are implicitly inline.
constexpr uint64_t SeparateHelper::kNoChunkId;
constexpr uint8_t SeparateHelper::kChunkIdTrailerMagic;

void SeparateHelper::EncodeChunkIdTrailer(std::string* dst, uint64_t chunk_id) {
  assert(dst != nullptr);
  assert(dst->size() >= sizeof(uint64_t));
  PutVarint64(dst, chunk_id);
  dst->push_back(static_cast<char>(kChunkIdTrailerMagic));
}

// Attempt to locate a chunk-id trailer at the tail of slice. On
// success, returns true and writes the decoded chunk_id to *chunk_id
// and the varint-encoding length (in bytes, excluding the magic) to
// *varint_len. On failure (no trailer / malformed / would eat into
// the file_number head), returns false and leaves outputs unspecified.
static bool ParseChunkIdTrailer(const Slice& slice, uint64_t* chunk_id,
                                size_t* varint_len) {
  // Minimum payload: 8B file_number + 1B varint + 1B magic.
  if (slice.size() < sizeof(uint64_t) + 2) {
    return false;
  }
  const uint8_t last =
      static_cast<uint8_t>(slice.data()[slice.size() - 1]);
  if (last != SeparateHelper::kChunkIdTrailerMagic) {
    return false;
  }

  // Scan backwards from the byte just before magic to find the start
  // of the varint64. A varint64 terminator is a byte whose top bit is
  // 0, and any "continuation" byte has top bit 1. The start of the
  // varint is the byte immediately after the previous terminator
  // (or the first non-head byte, whichever is earlier).
  //
  // Upper bound on varint64 length is 10 bytes.
  const char* data = slice.data();
  const size_t n = slice.size();
  const size_t head = sizeof(uint64_t);  // inclusive lower bound
  const size_t max_varint_bytes = 10;
  // last - 1 is the final varint byte; its top bit must be 0.
  if (n < head + 2) {
    return false;
  }
  const size_t last_varint_idx = n - 2;
  if ((static_cast<uint8_t>(data[last_varint_idx]) & 0x80) != 0) {
    // Last varint byte must be a terminator (top bit 0).
    return false;
  }

  // Scan backwards to find either (a) another terminator (meaning the
  // varint starts just after it), or (b) hit the file_number boundary.
  size_t start = last_varint_idx;
  while (start > head) {
    size_t prev = start - 1;
    if ((static_cast<uint8_t>(data[prev]) & 0x80) == 0) {
      // data[prev] is a terminator of something *before* our varint.
      break;
    }
    // data[prev] is a continuation byte of our varint.
    --start;
    if (last_varint_idx - start + 1 > max_varint_bytes) {
      // Would exceed max varint64 length.
      return false;
    }
  }
  // Do not eat into the file_number head.
  if (start < head) {
    return false;
  }

  const size_t varint_bytes = last_varint_idx - start + 1;
  uint64_t decoded = 0;
  const char* ptr = GetVarint64Ptr(data + start, data + last_varint_idx + 1,
                                   &decoded);
  if (ptr == nullptr || ptr != data + last_varint_idx + 1) {
    return false;
  }

  // Reject sentinel as a chunk id to keep kNoChunkId unambiguous.
  if (decoded == SeparateHelper::kNoChunkId) {
    return false;
  }

  *chunk_id = decoded;
  *varint_len = varint_bytes;
  return true;
}

bool SeparateHelper::HasChunkId(const Slice& slice) {
  uint64_t unused_chunk_id = 0;
  size_t unused_len = 0;
  return ParseChunkIdTrailer(slice, &unused_chunk_id, &unused_len);
}

uint64_t SeparateHelper::DecodeChunkId(const Slice& slice) {
  uint64_t chunk_id = 0;
  size_t unused_len = 0;
  if (!ParseChunkIdTrailer(slice, &chunk_id, &unused_len)) {
    return kNoChunkId;
  }
  return chunk_id;
}

Slice SeparateHelper::DecodeValueMetaStripChunk(const Slice& slice) {
  assert(slice.size() >= sizeof(uint64_t));
  uint64_t unused_chunk_id = 0;
  size_t varint_len = 0;
  if (ParseChunkIdTrailer(slice, &unused_chunk_id, &varint_len)) {
    const size_t trailer_bytes = varint_len + 1;  // varint + magic
    return Slice(slice.data() + sizeof(uint64_t),
                 slice.size() - sizeof(uint64_t) - trailer_bytes);
  }
  return Slice(slice.data() + sizeof(uint64_t),
               slice.size() - sizeof(uint64_t));
}

Slice ArenaPinSlice(const Slice& slice, Arena* arena) {
  char* buf = static_cast<char*>(arena->Allocate(slice.size() + 1));
  memcpy(buf, slice.data(), slice.size());
  buf[slice.size()] = '\0';  // better for debug read
  return Slice(buf, slice.size());
}

Slice ArenaPinInternalKey(const Slice& user_key, SequenceNumber seq,
                          ValueType type, Arena* arena) {
  size_t key_size = user_key.size() + 8;
  char* buf = static_cast<char*>(arena->Allocate(key_size + 1));
  memcpy(buf, user_key.data(), user_key.size());
  EncodeFixed64(buf + user_key.size(), PackSequenceAndType(seq, type));
  buf[key_size] = '\0';  // better for debug read
  return Slice(buf, key_size);
}

}  // namespace TERARKDB_NAMESPACE
