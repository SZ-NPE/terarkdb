//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include "rocksdb/terark_namespace.h"
#include "util/logging.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

static std::string IKey(const std::string& user_key, uint64_t seq,
                        ValueType vt) {
  std::string encoded;
  AppendInternalKey(&encoded, ParsedInternalKey(user_key, seq, vt));
  return encoded;
}

static std::string Shorten(const std::string& s, const std::string& l) {
  std::string result = s;
  InternalKeyComparator(BytewiseComparator()).FindShortestSeparator(&result, l);
  return result;
}

static std::string ShortSuccessor(const std::string& s) {
  std::string result = s;
  InternalKeyComparator(BytewiseComparator()).FindShortSuccessor(&result);
  return result;
}

static void TestKey(const std::string& key, uint64_t seq, ValueType vt) {
  std::string encoded = IKey(key, seq, vt);

  Slice in(encoded);
  ParsedInternalKey decoded("", 0, kTypeValue);

  ASSERT_TRUE(ParseInternalKey(in, &decoded));
  ASSERT_EQ(key, decoded.user_key.ToString());
  ASSERT_EQ(seq, decoded.sequence);
  ASSERT_EQ(vt, decoded.type);

  ASSERT_TRUE(!ParseInternalKey(Slice("bar"), &decoded));
}

class FormatTest : public testing::Test {};

TEST_F(FormatTest, InternalKey_EncodeDecode) {
  const char* keys[] = {"", "k", "hello", "longggggggggggggggggggggg"};
  const uint64_t seq[] = {1,
                          2,
                          3,
                          (1ull << 8) - 1,
                          1ull << 8,
                          (1ull << 8) + 1,
                          (1ull << 16) - 1,
                          1ull << 16,
                          (1ull << 16) + 1,
                          (1ull << 32) - 1,
                          1ull << 32,
                          (1ull << 32) + 1};
  for (unsigned int k = 0; k < sizeof(keys) / sizeof(keys[0]); k++) {
    for (unsigned int s = 0; s < sizeof(seq) / sizeof(seq[0]); s++) {
      TestKey(keys[k], seq[s], kTypeValue);
      TestKey("hello", 1, kTypeDeletion);
    }
  }
}

TEST_F(FormatTest, InternalKeyShortSeparator) {
  // When user keys are same
  ASSERT_EQ(IKey("foo", 100, kTypeValue),
            Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 99, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 101, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 100, kTypeValue)));
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foo", 100, kTypeDeletion)));

  // When user keys are misordered
  ASSERT_EQ(IKey("foo", 100, kTypeValue),
            Shorten(IKey("foo", 100, kTypeValue), IKey("bar", 99, kTypeValue)));

  // When user keys are different, but correctly ordered
  ASSERT_EQ(
      IKey("g", kMaxSequenceNumber, kValueTypeForSeek),
      Shorten(IKey("foo", 100, kTypeValue), IKey("hello", 200, kTypeValue)));

  ASSERT_EQ(IKey("ABC2", kMaxSequenceNumber, kValueTypeForSeek),
            Shorten(IKey("ABC1AAAAA", 100, kTypeValue),
                    IKey("ABC2ABB", 200, kTypeValue)));

  ASSERT_EQ(IKey("AAA2", kMaxSequenceNumber, kValueTypeForSeek),
            Shorten(IKey("AAA1AAA", 100, kTypeValue),
                    IKey("AAA2AA", 200, kTypeValue)));

  ASSERT_EQ(
      IKey("AAA2", kMaxSequenceNumber, kValueTypeForSeek),
      Shorten(IKey("AAA1AAA", 100, kTypeValue), IKey("AAA4", 200, kTypeValue)));

  ASSERT_EQ(
      IKey("AAA1B", kMaxSequenceNumber, kValueTypeForSeek),
      Shorten(IKey("AAA1AAA", 100, kTypeValue), IKey("AAA2", 200, kTypeValue)));

  ASSERT_EQ(IKey("AAA2", kMaxSequenceNumber, kValueTypeForSeek),
            Shorten(IKey("AAA1AAA", 100, kTypeValue),
                    IKey("AAA2A", 200, kTypeValue)));

  ASSERT_EQ(
      IKey("AAA1", 100, kTypeValue),
      Shorten(IKey("AAA1", 100, kTypeValue), IKey("AAA2", 200, kTypeValue)));

  // When start user key is prefix of limit user key
  ASSERT_EQ(
      IKey("foo", 100, kTypeValue),
      Shorten(IKey("foo", 100, kTypeValue), IKey("foobar", 200, kTypeValue)));

  // When limit user key is prefix of start user key
  ASSERT_EQ(
      IKey("foobar", 100, kTypeValue),
      Shorten(IKey("foobar", 100, kTypeValue), IKey("foo", 200, kTypeValue)));
}

TEST_F(FormatTest, InternalKeyShortestSuccessor) {
  ASSERT_EQ(IKey("g", kMaxSequenceNumber, kValueTypeForSeek),
            ShortSuccessor(IKey("foo", 100, kTypeValue)));
  ASSERT_EQ(IKey("\xff\xff", 100, kTypeValue),
            ShortSuccessor(IKey("\xff\xff", 100, kTypeValue)));
}

TEST_F(FormatTest, IterKeyOperation) {
  IterKey k;
  const char p[] = "abcdefghijklmnopqrstuvwxyz";
  const char q[] = "0123456789";

  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string(""));

  k.TrimAppend(0, p, 3);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abc"));

  k.TrimAppend(1, p, 3);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("aabc"));

  k.TrimAppend(0, p, 26);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz"));

  k.TrimAppend(26, q, 10);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz0123456789"));

  k.TrimAppend(36, q, 1);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz01234567890"));

  k.TrimAppend(26, q, 1);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz0"));

  // Size going up, memory allocation is triggered
  k.TrimAppend(27, p, 26);
  ASSERT_EQ(std::string(k.GetUserKey().data(), k.GetUserKey().size()),
            std::string("abcdefghijklmnopqrstuvwxyz0"
                        "abcdefghijklmnopqrstuvwxyz"));
}

TEST_F(FormatTest, UpdateInternalKey) {
  std::string user_key("abcdefghijklmnopqrstuvwxyz");
  uint64_t new_seq = 0x123456;
  ValueType new_val_type = kTypeDeletion;

  std::string ikey;
  AppendInternalKey(&ikey, ParsedInternalKey(user_key, 100U, kTypeValue));
  size_t ikey_size = ikey.size();
  UpdateInternalKey(&ikey, new_seq, new_val_type);
  ASSERT_EQ(ikey_size, ikey.size());

  Slice in(ikey);
  ParsedInternalKey decoded;
  ASSERT_TRUE(ParseInternalKey(in, &decoded));
  ASSERT_EQ(user_key, decoded.user_key.ToString());
  ASSERT_EQ(new_seq, decoded.sequence);
  ASSERT_EQ(new_val_type, decoded.type);
}

TEST_F(FormatTest, RangeTombstoneSerializeEndKey) {
  RangeTombstone t("a", "b", 2);
  InternalKey k("b", 3, kTypeValue);
  const InternalKeyComparator cmp(BytewiseComparator());
  ASSERT_LT(cmp.Compare(t.SerializeEndKey(), k), 0);
}

// ---------------------------------------------------------------
// chunk-aware value-index encoding tests.
//
// Layout:
//   Legacy:     [ file_number(8B, LE) ] [ meta(M bytes) ]
//   Chunk-aware:[ file_number(8B, LE) ] [ meta(M bytes) ]
//               [ varint64(chunk_id) ] [ magic 0xC1 ]
// ---------------------------------------------------------------

namespace {

// Build a legacy (pre-Phase 2) value-index slice: file_number + meta.
std::string BuildLegacyIndex(uint64_t file_number, const Slice& meta) {
  std::string out;
  uint64_t fn = file_number;
  out.append(SeparateHelper::EncodeFileNumber(fn).data(), sizeof(uint64_t));
  out.append(meta.data(), meta.size());
  return out;
}

// Build a chunk-aware value-index slice: legacy head + chunk-id trailer.
std::string BuildChunkAwareIndex(uint64_t file_number, const Slice& meta,
                                 uint64_t chunk_id) {
  std::string out = BuildLegacyIndex(file_number, meta);
  SeparateHelper::EncodeChunkIdTrailer(&out, chunk_id);
  return out;
}

}  // namespace

// Test 1: Legacy format (no trailer) encodes and decodes correctly, and
// HasChunkId() == false, DecodeChunkId() == kNoChunkId.
TEST_F(FormatTest, ValueIndexEncodeDecode_LegacyFormat) {
  const uint64_t file_number = 0x0123456789abcdefULL;
  const std::string meta_str = "user-meta-bytes";
  const Slice meta(meta_str);

  std::string encoded = BuildLegacyIndex(file_number, meta);
  Slice slice(encoded);

  // Head: file_number roundtrips.
  ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice));

  // Legacy DecodeValueMeta returns everything after the head.
  Slice decoded_meta = SeparateHelper::DecodeValueMeta(slice);
  ASSERT_EQ(meta_str, decoded_meta.ToString());

  // Tail: no trailer -> HasChunkId false, DecodeChunkId == kNoChunkId.
  ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
  ASSERT_EQ(SeparateHelper::kNoChunkId,
            SeparateHelper::DecodeChunkId(slice));

  // DecodeValueMetaStripChunk on a legacy slice falls back to the
  // legacy meta view (no bytes stripped).
  Slice stripped = SeparateHelper::DecodeValueMetaStripChunk(slice);
  ASSERT_EQ(meta_str, stripped.ToString());

  // Edge case: legacy slice with empty meta.
  std::string no_meta = BuildLegacyIndex(file_number, Slice());
  Slice no_meta_slice(no_meta);
  ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(no_meta_slice));
  ASSERT_FALSE(SeparateHelper::HasChunkId(no_meta_slice));
  ASSERT_EQ(SeparateHelper::kNoChunkId,
            SeparateHelper::DecodeChunkId(no_meta_slice));
  ASSERT_EQ(0u,
            SeparateHelper::DecodeValueMetaStripChunk(no_meta_slice).size());
}

// Test 2: Chunk-aware format encodes and decodes across a variety of
// chunk_id values (including varint boundary cases).
TEST_F(FormatTest, ValueIndexEncodeDecode_NewFormatWithChunkId) {
  const uint64_t file_number = 42ULL;
  const std::string meta_str = "meta";
  const Slice meta(meta_str);

  const uint64_t chunk_ids[] = {
      0ULL,
      1ULL,
      127ULL,                       // 1-byte varint boundary
      128ULL,                       // 2-byte varint
      16383ULL,                     // 2-byte varint max
      16384ULL,                     // 3-byte varint
      (1ULL << 35) - 1,             // 5-byte varint
      (1ULL << 56),                 // 9-byte varint
      static_cast<uint64_t>(-1) - 1 // 10-byte varint, not sentinel
  };

  for (uint64_t cid : chunk_ids) {
    std::string encoded = BuildChunkAwareIndex(file_number, meta, cid);
    Slice slice(encoded);

    // Head unchanged.
    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice))
        << "chunk_id=" << cid;

    // Trailer present.
    ASSERT_TRUE(SeparateHelper::HasChunkId(slice))
        << "chunk_id=" << cid;
    ASSERT_EQ(cid, SeparateHelper::DecodeChunkId(slice))
        << "chunk_id=" << cid;

    // Stripped meta equals the original meta.
    Slice stripped = SeparateHelper::DecodeValueMetaStripChunk(slice);
    ASSERT_EQ(meta_str, stripped.ToString())
        << "chunk_id=" << cid;
  }

  // chunk-aware slice with empty meta is also valid.
  {
    std::string encoded = BuildChunkAwareIndex(file_number, Slice(), 7ULL);
    Slice slice(encoded);
    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice));
    ASSERT_TRUE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(7ULL, SeparateHelper::DecodeChunkId(slice));
    ASSERT_EQ(0u,
              SeparateHelper::DecodeValueMetaStripChunk(slice).size());
  }
}

// Test 3: Mixed readers must treat non-trailer slices as legacy even when
// the meta tail accidentally contains bytes that look similar to the
// trailer encoding. Readers unaware of the trailer must continue to see a
// usable meta view.
TEST_F(FormatTest, ValueIndexDecode_BackwardCompatible) {
  const uint64_t file_number = 7ULL;

  // (a) Meta whose last byte is exactly the magic byte, but the byte
  //     before the magic has its top bit set (so it cannot be a varint
  //     terminator). Must be classified as legacy.
  {
    std::string meta_bytes;
    meta_bytes.push_back(static_cast<char>(0xFF));  // top bit 1
    meta_bytes.push_back(
        static_cast<char>(SeparateHelper::kChunkIdTrailerMagic));
    std::string encoded = BuildLegacyIndex(file_number, Slice(meta_bytes));
    Slice slice(encoded);

    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice));
    ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(slice));
    ASSERT_EQ(meta_bytes,
              SeparateHelper::DecodeValueMeta(slice).ToString());
  }

  // (b) Meta that contains the magic byte in the interior but does not
  //     end with it. Must be classified as legacy.
  {
    std::string meta_bytes = "abc";
    meta_bytes.push_back(
        static_cast<char>(SeparateHelper::kChunkIdTrailerMagic));
    meta_bytes += "xyz";
    std::string encoded = BuildLegacyIndex(file_number, Slice(meta_bytes));
    Slice slice(encoded);

    ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(slice));
    ASSERT_EQ(meta_bytes,
              SeparateHelper::DecodeValueMeta(slice).ToString());
  }

  // (c) A chunk-aware slice, when read by a legacy-only reader through
  //     DecodeValueMeta, still produces a slice that includes the
  //     trailer bytes (which is fine: old extractor-aware callers parse
  //     meta by a user-owned length). But HasChunkId is still true.
  {
    const uint64_t cid = 123456ULL;
    std::string encoded =
        BuildChunkAwareIndex(file_number, Slice("meta"), cid);
    Slice slice(encoded);

    // New reader sees the chunk id.
    ASSERT_TRUE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(cid, SeparateHelper::DecodeChunkId(slice));

    // Legacy reader: head parses; meta view is larger than "meta" because
    // the trailer bytes follow. The important invariant is that the
    // *file_number* decodes correctly.
    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice));
    Slice legacy_meta_view = SeparateHelper::DecodeValueMeta(slice);
    ASSERT_GE(legacy_meta_view.size(), std::string("meta").size());

    // New reader: stripped meta equals the original.
    ASSERT_EQ(std::string("meta"),
              SeparateHelper::DecodeValueMetaStripChunk(slice).ToString());
  }
}

// Test 4: Malformed or pathological encodings must be rejected rather
// than misinterpreted as valid chunk trailers.
TEST_F(FormatTest, ValueIndexDecode_RejectMalformedChunkEncoding) {
  const uint64_t file_number = 99ULL;

  // (a) Slice too short: only the 8B file_number.
  {
    std::string encoded = BuildLegacyIndex(file_number, Slice());
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(slice));
  }

  // (b) Slice of length 9: file_number + a stray magic byte, no varint
  //     before it. Must be rejected (no room for varint).
  {
    std::string encoded = BuildLegacyIndex(file_number, Slice());
    encoded.push_back(
        static_cast<char>(SeparateHelper::kChunkIdTrailerMagic));
    Slice slice(encoded);
    // size == 9, < 8 + 2, so rejected by size check.
    ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(slice));
  }

  // (c) Last byte is magic, but the byte before it is a continuation
  //     byte (top bit 1). There is no preceding terminator, so forward
  //     varint parse over the would-be region must fail. Must be
  //     rejected.
  {
    std::string encoded = BuildLegacyIndex(file_number, Slice());
    // Append a single continuation byte then the magic.
    encoded.push_back(static_cast<char>(0x80));
    encoded.push_back(
        static_cast<char>(SeparateHelper::kChunkIdTrailerMagic));
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(slice));
  }

  // (d) A full run of 10 continuation bytes followed by magic would
  //     eat into or past the file_number head. Must be rejected.
  {
    std::string encoded = BuildLegacyIndex(file_number, Slice());
    for (int i = 0; i < 10; ++i) {
      encoded.push_back(static_cast<char>(0x80));
    }
    encoded.push_back(
        static_cast<char>(SeparateHelper::kChunkIdTrailerMagic));
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(slice));
  }

  // (e) Encoding that would decode to the sentinel kNoChunkId. Must be
  //     rejected so the sentinel remains unambiguous.
  {
    std::string encoded = BuildLegacyIndex(file_number, Slice());
    PutVarint64(&encoded, SeparateHelper::kNoChunkId);
    encoded.push_back(
        static_cast<char>(SeparateHelper::kChunkIdTrailerMagic));
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(slice));
  }

  // (f) Last byte is NOT the magic. Trivially rejected.
  {
    std::string encoded = BuildChunkAwareIndex(file_number, Slice("m"), 5ULL);
    encoded.back() = static_cast<char>(0x00);  // clobber magic
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasChunkId(slice));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(slice));
  }
}

// Test 5: The chunk-aware TransToSeparate() overload actually emits a
// well-formed chunk-id trailer on the outgoing value-index, while
// staying bit-for-bit identical to the legacy overload when the
// caller passes kNoChunkId. This directly covers the writer side of
// Issue 1 (flush/compaction failing to stamp chunk_id onto the index
// value).
TEST_F(FormatTest, TransToSeparate_StampsChunkIdTrailer) {
  const uint64_t file_number = 123ULL;

  // Legacy behavior: chunk_id == kNoChunkId must produce a slice
  // indistinguishable from the legacy overload's output (no trailer).
  {
    LazyBuffer value;
    Status s = SeparateHelper::TransToSeparate(
        "ik", value, file_number, Slice(), /*is_merge=*/true,
        /*is_index=*/false, /*value_meta_extractor=*/nullptr,
        SeparateHelper::kNoChunkId);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(value.fetch().ok());
    ASSERT_EQ(file_number,
              SeparateHelper::DecodeFileNumber(value.slice()));
    ASSERT_FALSE(SeparateHelper::HasChunkId(value.slice()));
    ASSERT_EQ(SeparateHelper::kNoChunkId,
              SeparateHelper::DecodeChunkId(value.slice()));
  }

  // Chunk-aware behavior on the "merge / no meta" path.
  {
    LazyBuffer value;
    Status s = SeparateHelper::TransToSeparate(
        "ik", value, file_number, Slice(), /*is_merge=*/true,
        /*is_index=*/false, /*value_meta_extractor=*/nullptr,
        /*chunk_id=*/7ULL);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(value.fetch().ok());
    ASSERT_EQ(file_number,
              SeparateHelper::DecodeFileNumber(value.slice()));
    ASSERT_TRUE(SeparateHelper::HasChunkId(value.slice()));
    ASSERT_EQ(7ULL, SeparateHelper::DecodeChunkId(value.slice()));
    // Meta is empty and the trailer is stripped cleanly.
    ASSERT_EQ(0u,
              SeparateHelper::DecodeValueMetaStripChunk(value.slice()).size());
  }

  // Chunk-aware behavior on the "caller-supplied meta" path. The
  // stripped meta view must exactly equal the caller-supplied meta.
  // We pass a non-null sentinel for value_meta_extractor so that the
  // implementation skips the (value_meta_extractor == nullptr)
  // shortcut and enters the is_index branch, which copies the
  // caller-supplied meta verbatim without ever dereferencing the
  // extractor pointer.
  {
    LazyBuffer value;
    const std::string meta_str = "ext-meta";
    const ValueExtractor* sentinel =
        reinterpret_cast<const ValueExtractor*>(uintptr_t{0x1});
    Status s = SeparateHelper::TransToSeparate(
        "ik", value, file_number, Slice(meta_str),
        /*is_merge=*/false, /*is_index=*/true,
        /*value_meta_extractor=*/sentinel,
        /*chunk_id=*/128ULL /* 2-byte varint boundary */);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(value.fetch().ok());
    ASSERT_EQ(file_number,
              SeparateHelper::DecodeFileNumber(value.slice()));
    ASSERT_TRUE(SeparateHelper::HasChunkId(value.slice()));
    ASSERT_EQ(128ULL, SeparateHelper::DecodeChunkId(value.slice()));
    ASSERT_EQ(meta_str,
              SeparateHelper::DecodeValueMetaStripChunk(value.slice())
                  .ToString());
  }
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
