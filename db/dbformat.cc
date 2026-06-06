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

Status SeparateHelper::TransToSeparate(
    const Slice& internal_key, LazyBuffer& value, uint64_t file_number,
    const Slice& meta, bool is_merge, bool is_index,
    const ValueExtractor* value_meta_extractor, uint64_t block_id,
    uint64_t layout_id) {
  // Fast path: caller has no usable block_id/layout_id to stamp.
  // Delegate to the legacy overload so behavior is bit-for-bit
  // identical to pre-block-aware code. This is the path exercised
  // whenever the CF option `enable_blob_block_bitmap` is off, or when
  // the builder does not support data block ids.
  if (block_id == kNoBlockId || layout_id == kNoBlockLayoutId) {
    return TransToSeparate(internal_key, value, file_number, meta, is_merge,
                           is_index, value_meta_extractor);
  }

  assert(file_number != uint64_t(-1));
  // Build the encoded value-index into a temporary buffer so that we
  // can append the fixed-length block-id trailer before handing the
  // final bytes back to LazyBuffer::reset(). The buffer layout is:
  //   [ file_number(8B, LE) ]
  //   [ meta(M bytes, optional) ]
  //   [ block_id(fixed64) ]
  //   [ layout_id(fixed64) ]
  //   [ version(1B) ]
  //   [ kBlockIdTrailerMagic(1B) ]
  //
  // `is_merge` callers never carry meta (see the legacy overload),
  // so we mirror the exact meta-presence policy used there.
  std::string buf;
  buf.reserve(sizeof(uint64_t) + meta.size() + kBlockIdTrailerLength);

  // file_number head.
  uint64_t fn = file_number;
  Slice file_number_slice = EncodeFileNumber(fn);
  buf.append(file_number_slice.data(), file_number_slice.size());

  if (value_meta_extractor == nullptr || is_merge) {
    // No meta region. Append trailer directly after file_number.
    EncodeBlockIdTrailer(&buf, block_id, layout_id);
    value.reset(Slice(buf), true, file_number);
    // reset() with copy=true internally copies the bytes, so `buf`
    // can safely go out of scope after this call.
    return Status::OK();
  }

  if (is_index) {
    // Caller already supplies meta as a raw Slice.
    buf.append(meta.data(), meta.size());
    EncodeBlockIdTrailer(&buf, block_id, layout_id);
    value.reset(Slice(buf), true, file_number);
    return Status::OK();
  }

  // Need to materialize meta via the extractor.
  auto s = value.fetch();
  if (!s.ok()) {
    return s;
  }
  std::string value_meta;
  s = value_meta_extractor->Extract(ExtractUserKey(internal_key),
                                    value.slice(), &value_meta);
  if (!s.ok()) {
    return s;
  }
  buf.append(value_meta.data(), value_meta.size());
  EncodeBlockIdTrailer(&buf, block_id, layout_id);
  value.reset(Slice(buf), true, file_number);
  return Status::OK();
}

// Out-of-class definitions for the static constexpr members declared in
// dbformat.h. Required under C++14 for ODR-use (e.g. passing them by
// reference to ASSERT_EQ in tests). Harmless under C++17+ where they
// are implicitly inline.
constexpr uint64_t SeparateHelper::kNoBlockId;
constexpr uint64_t SeparateHelper::kNoBlockLayoutId;
constexpr uint8_t SeparateHelper::kBlockIdTrailerMagic;
constexpr uint8_t SeparateHelper::kBlockIndexVersion;
constexpr size_t SeparateHelper::kBlockIdTrailerLength;

void SeparateHelper::EncodeBlockIdTrailer(std::string* dst, uint64_t block_id,
                                          uint64_t layout_id) {
  assert(dst != nullptr);
  assert(dst->size() >= sizeof(uint64_t));
  // Fixed-length trailer: fixed64(block_id) + fixed64(layout_id) +
  // version(1B) + magic(1B). No varint, no reverse scan.
  PutFixed64(dst, block_id);
  PutFixed64(dst, layout_id);
  dst->push_back(static_cast<char>(kBlockIndexVersion));
  dst->push_back(static_cast<char>(kBlockIdTrailerMagic));
}

// Attempt to validate and decode a fixed-length block-id trailer at the
// tail of slice. On success, returns true and writes the decoded
// block_id / layout_id to the provided out-params. On failure (slice
// too short / wrong magic / wrong version / sentinel values), returns
// false and leaves outputs unspecified.
//
// Validity is decided purely by the fixed size + magic + version +
// non-sentinel checks. There is NO backwards varint scan: this is the
// whole point of the new format.
static bool ParseBlockIdTrailer(const Slice& slice, uint64_t* block_id,
                                uint64_t* layout_id) {
  // Minimum payload: 8B file_number head + fixed trailer.
  if (slice.size() < sizeof(uint64_t) + SeparateHelper::kBlockIdTrailerLength) {
    return false;
  }
  const char* data = slice.data();
  const size_t n = slice.size();

  // magic is the very last byte.
  const uint8_t magic = static_cast<uint8_t>(data[n - 1]);
  if (magic != SeparateHelper::kBlockIdTrailerMagic) {
    return false;
  }
  // version is the byte before magic.
  const uint8_t version = static_cast<uint8_t>(data[n - 2]);
  if (version != SeparateHelper::kBlockIndexVersion) {
    // Unknown trailer version: treat as legacy and fall back.
    return false;
  }

  // Decode the two fixed64 fields located right before version+magic.
  const size_t trailer_start = n - SeparateHelper::kBlockIdTrailerLength;
  const uint64_t decoded_block_id = DecodeFixed64(data + trailer_start);
  const uint64_t decoded_layout_id =
      DecodeFixed64(data + trailer_start + sizeof(uint64_t));

  // Reject sentinels so kNoBlockId / kNoBlockLayoutId stay unambiguous.
  if (decoded_block_id == SeparateHelper::kNoBlockId ||
      decoded_layout_id == SeparateHelper::kNoBlockLayoutId) {
    return false;
  }

  *block_id = decoded_block_id;
  *layout_id = decoded_layout_id;
  return true;
}

bool SeparateHelper::HasBlockId(const Slice& slice) {
  uint64_t unused_block_id = 0;
  uint64_t unused_layout_id = 0;
  return ParseBlockIdTrailer(slice, &unused_block_id, &unused_layout_id);
}

uint64_t SeparateHelper::DecodeBlockId(const Slice& slice) {
  uint64_t block_id = 0;
  uint64_t unused_layout_id = 0;
  if (!ParseBlockIdTrailer(slice, &block_id, &unused_layout_id)) {
    return kNoBlockId;
  }
  return block_id;
}

uint64_t SeparateHelper::DecodeBlockLayoutId(const Slice& slice) {
  uint64_t unused_block_id = 0;
  uint64_t layout_id = 0;
  if (!ParseBlockIdTrailer(slice, &unused_block_id, &layout_id)) {
    return kNoBlockLayoutId;
  }
  return layout_id;
}

Slice SeparateHelper::DecodeValueMetaStripBlockId(const Slice& slice) {
  assert(slice.size() >= sizeof(uint64_t));
  uint64_t unused_block_id = 0;
  uint64_t unused_layout_id = 0;
  if (ParseBlockIdTrailer(slice, &unused_block_id, &unused_layout_id)) {
    // Strip exactly the fixed trailer bytes.
    return Slice(slice.data() + sizeof(uint64_t),
                 slice.size() - sizeof(uint64_t) - kBlockIdTrailerLength);
  }
  // Legacy meta view (no trailer / malformed): never strip.
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
