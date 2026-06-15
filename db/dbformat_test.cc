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
// block-aware value-index encoding tests.
//
// Layout (fixed-length trailer, NO reverse varint scan):
//   Legacy:     [ file_number(8B, LE) ] [ meta(M bytes) ]
//   Block-aware:[ file_number(8B, LE) ] [ meta(M bytes) ]
//               [ block_id(fixed64) ] [ layout_id(fixed64) ]
//               [ version(1B) ] [ magic 0xC1 ]
//
// The trailer is exactly kBlockIdTrailerLength (18) bytes. Validity is
// decided purely by fixed size + magic + version + non-sentinel checks.
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

// Build a block-aware value-index slice: legacy head + fixed block-id
// trailer carrying block_id and layout_id.
std::string BuildBlockAwareIndex(uint64_t file_number, const Slice& meta,
                                 uint64_t block_id, uint64_t layout_id) {
  std::string out = BuildLegacyIndex(file_number, meta);
  SeparateHelper::EncodeBlockIdTrailer(&out, block_id, layout_id);
  return out;
}

// Build a v2 block-aware value-index slice that additionally carries the
// in-block slot ordinal.
std::string BuildBlockAwareIndexV2(uint64_t file_number, const Slice& meta,
                                   uint64_t block_id, uint64_t layout_id,
                                   uint64_t slot_id) {
  std::string out = BuildLegacyIndex(file_number, meta);
  SeparateHelper::EncodeBlockIdTrailer(&out, block_id, layout_id, slot_id);
  return out;
}

}  // namespace

// Test 1: Legacy format (no trailer) encodes and decodes correctly, and
// HasBlockId() == false, DecodeBlockId() == kNoBlockId.
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

  // Tail: no trailer -> HasBlockId false, DecodeBlockId == kNoBlockId,
  // DecodeBlockLayoutId == kNoBlockLayoutId.
  ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
  ASSERT_EQ(SeparateHelper::kNoBlockId,
            SeparateHelper::DecodeBlockId(slice));
  ASSERT_EQ(SeparateHelper::kNoBlockLayoutId,
            SeparateHelper::DecodeBlockLayoutId(slice));

  // DecodeValueMetaStripBlockId on a legacy slice falls back to the
  // legacy meta view (no bytes stripped).
  Slice stripped = SeparateHelper::DecodeValueMetaStripBlockId(slice);
  ASSERT_EQ(meta_str, stripped.ToString());

  // Edge case: legacy slice with empty meta.
  std::string no_meta = BuildLegacyIndex(file_number, Slice());
  Slice no_meta_slice(no_meta);
  ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(no_meta_slice));
  ASSERT_FALSE(SeparateHelper::HasBlockId(no_meta_slice));
  ASSERT_EQ(SeparateHelper::kNoBlockId,
            SeparateHelper::DecodeBlockId(no_meta_slice));
  ASSERT_EQ(
      0u,
      SeparateHelper::DecodeValueMetaStripBlockId(no_meta_slice).size());
}

// Test 2: Block-aware format encodes and decodes across a variety of
// block_id / layout_id values, including large fixed64 values. The fixed
// trailer has no varint boundaries, so we exercise a representative set
// of values and verify both block_id and layout_id round-trip.
TEST_F(FormatTest, ValueIndexEncodeDecode_NewFormatWithBlockId) {
  const uint64_t file_number = 42ULL;
  const std::string meta_str = "meta";
  const Slice meta(meta_str);

  struct Case {
    uint64_t block_id;
    uint64_t layout_id;
  };
  const Case cases[] = {
      {0ULL, 0ULL},
      {1ULL, 2ULL},
      {127ULL, 128ULL},
      {16383ULL, 16384ULL},
      {(1ULL << 35) - 1, (1ULL << 40) + 3},
      {(1ULL << 56), (1ULL << 48)},
      {static_cast<uint64_t>(-1) - 1, static_cast<uint64_t>(-1) - 2},
  };

  for (const Case& c : cases) {
    std::string encoded =
        BuildBlockAwareIndex(file_number, meta, c.block_id, c.layout_id);
    Slice slice(encoded);

    // Head unchanged.
    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice))
        << "block_id=" << c.block_id;

    // Trailer present; both block_id and layout_id round-trip.
    ASSERT_TRUE(SeparateHelper::HasBlockId(slice))
        << "block_id=" << c.block_id;
    ASSERT_EQ(c.block_id, SeparateHelper::DecodeBlockId(slice))
        << "block_id=" << c.block_id;
    ASSERT_EQ(c.layout_id, SeparateHelper::DecodeBlockLayoutId(slice))
        << "layout_id=" << c.layout_id;

    // Stripped meta equals the original meta.
    Slice stripped = SeparateHelper::DecodeValueMetaStripBlockId(slice);
    ASSERT_EQ(meta_str, stripped.ToString())
        << "block_id=" << c.block_id;
  }

  // block-aware slice with empty meta is also valid.
  {
    std::string encoded =
        BuildBlockAwareIndex(file_number, Slice(), 7ULL, 9ULL);
    Slice slice(encoded);
    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice));
    ASSERT_TRUE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(7ULL, SeparateHelper::DecodeBlockId(slice));
    ASSERT_EQ(9ULL, SeparateHelper::DecodeBlockLayoutId(slice));
    ASSERT_EQ(0u,
              SeparateHelper::DecodeValueMetaStripBlockId(slice).size());
  }
}

// Test 2b: v2 trailer round-trips block_id + layout_id + slot_id, strips
// the (longer) v2 trailer correctly, and DecodeValueLocation reports a
// fully valid ValueLocation only for v2.
TEST_F(FormatTest, ValueIndexEncodeDecode_NewFormatWithSlot) {
  const uint64_t file_number = 42ULL;
  const std::string meta_str = "meta";
  const Slice meta(meta_str);

  struct Case {
    uint64_t block_id, layout_id, slot_id;
  };
  const Case cases[] = {
      {0ULL, 1ULL, 0ULL},
      {3ULL, 100ULL, 17ULL},
      {(1ULL << 35), (1ULL << 40) + 3, 4095ULL},
  };
  for (const Case& c : cases) {
    std::string encoded = BuildBlockAwareIndexV2(file_number, meta, c.block_id,
                                                 c.layout_id, c.slot_id);
    Slice slice(encoded);
    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice));
    ASSERT_TRUE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(c.block_id, SeparateHelper::DecodeBlockId(slice));
    ASSERT_EQ(c.layout_id, SeparateHelper::DecodeBlockLayoutId(slice));
    ASSERT_EQ(c.slot_id, SeparateHelper::DecodeSlotId(slice));
    // Meta strips the longer v2 trailer exactly.
    ASSERT_EQ(meta_str,
              SeparateHelper::DecodeValueMetaStripBlockId(slice).ToString());
    // Full location decodes and is valid.
    ValueLocation loc;
    ASSERT_TRUE(SeparateHelper::DecodeValueLocation(slice, &loc));
    ASSERT_TRUE(loc.valid());
    ASSERT_EQ(file_number, loc.file_number);
    ASSERT_EQ(c.block_id, loc.block_id);
    ASSERT_EQ(c.layout_id, loc.layout_id);
    ASSERT_EQ(c.slot_id, loc.slot_id);
  }

  // A v1 trailer (no slot) yields kNoSlotId and an INVALID ValueLocation,
  // so the death log conservatively falls back for it.
  {
    std::string encoded = BuildBlockAwareIndex(file_number, meta, 5ULL, 6ULL);
    Slice slice(encoded);
    ASSERT_EQ(SeparateHelper::kNoSlotId, SeparateHelper::DecodeSlotId(slice));
    ValueLocation loc;
    ASSERT_FALSE(SeparateHelper::DecodeValueLocation(slice, &loc));
    ASSERT_FALSE(loc.valid());
  }

  // A legacy index (no trailer) yields an INVALID ValueLocation.
  {
    std::string encoded = BuildLegacyIndex(file_number, meta);
    Slice slice(encoded);
    ValueLocation loc;
    ASSERT_FALSE(SeparateHelper::DecodeValueLocation(slice, &loc));
    ASSERT_FALSE(loc.valid());
  }
}

TEST_F(FormatTest, ValueIndexDecode_BackwardCompatible) {
  const uint64_t file_number = 7ULL;

  // (a) Meta whose last byte is exactly the magic byte, but the byte
  //     before the magic is not a valid version byte. Must be classified
  //     as legacy.
  {
    std::string meta_bytes;
    meta_bytes.push_back(static_cast<char>(0xFF));  // not the version byte
    meta_bytes.push_back(
        static_cast<char>(SeparateHelper::kBlockIdTrailerMagic));
    std::string encoded = BuildLegacyIndex(file_number, Slice(meta_bytes));
    Slice slice(encoded);

    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice));
    ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockId,
              SeparateHelper::DecodeBlockId(slice));
    ASSERT_EQ(meta_bytes,
              SeparateHelper::DecodeValueMeta(slice).ToString());
  }

  // (b) Meta that contains the magic byte in the interior but does not
  //     end with it. Must be classified as legacy.
  {
    std::string meta_bytes = "abc";
    meta_bytes.push_back(
        static_cast<char>(SeparateHelper::kBlockIdTrailerMagic));
    meta_bytes += "xyz";
    std::string encoded = BuildLegacyIndex(file_number, Slice(meta_bytes));
    Slice slice(encoded);

    ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockId,
              SeparateHelper::DecodeBlockId(slice));
    ASSERT_EQ(meta_bytes,
              SeparateHelper::DecodeValueMeta(slice).ToString());
  }

  // (c) A block-aware slice, when read by a legacy-only reader through
  //     DecodeValueMeta, still produces a slice that includes the
  //     trailer bytes (which is fine: old extractor-aware callers parse
  //     meta by a user-owned length). But HasBlockId is still true.
  {
    const uint64_t bid = 123456ULL;
    const uint64_t lid = 654321ULL;
    std::string encoded =
        BuildBlockAwareIndex(file_number, Slice("meta"), bid, lid);
    Slice slice(encoded);

    // New reader sees the block id and layout id.
    ASSERT_TRUE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(bid, SeparateHelper::DecodeBlockId(slice));
    ASSERT_EQ(lid, SeparateHelper::DecodeBlockLayoutId(slice));

    // Legacy reader: head parses; meta view is larger than "meta" because
    // the trailer bytes follow. The important invariant is that the
    // *file_number* decodes correctly.
    ASSERT_EQ(file_number, SeparateHelper::DecodeFileNumber(slice));
    Slice legacy_meta_view = SeparateHelper::DecodeValueMeta(slice);
    ASSERT_GE(legacy_meta_view.size(), std::string("meta").size());

    // New reader: stripped meta equals the original.
    ASSERT_EQ(
        std::string("meta"),
        SeparateHelper::DecodeValueMetaStripBlockId(slice).ToString());
  }
}

// Test 4: Malformed or pathological encodings must be rejected rather
// than misinterpreted as valid block trailers. With the fixed-length
// trailer, validity is decided purely by size + magic + version +
// non-sentinel. Any deviation must fall back to legacy and never strip.
TEST_F(FormatTest, ValueIndexDecode_RejectMalformedBlockEncoding) {
  const uint64_t file_number = 99ULL;

  // (a) Slice too short: only the 8B file_number. size < 8 + 18.
  {
    std::string encoded = BuildLegacyIndex(file_number, Slice());
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockId,
              SeparateHelper::DecodeBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockLayoutId,
              SeparateHelper::DecodeBlockLayoutId(slice));
  }

  // (b) Slice one byte shorter than the minimum block-aware length.
  //     A trailer-sized tail minus one byte must be rejected by the
  //     size check and treated as legacy meta (never stripped).
  {
    std::string encoded =
        BuildBlockAwareIndex(file_number, Slice(), 5ULL, 6ULL);
    encoded.pop_back();  // now size == 8 + 17 < 8 + 18
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockId,
              SeparateHelper::DecodeBlockId(slice));
    // Malformed trailer must NOT strip: legacy meta view returned.
    ASSERT_EQ(SeparateHelper::DecodeValueMeta(slice).ToString(),
              SeparateHelper::DecodeValueMetaStripBlockId(slice).ToString());
  }

  // (c) Version byte is wrong. Even with a valid magic and a full-size
  //     trailer, an unknown trailer version must be treated as legacy.
  {
    std::string encoded =
        BuildBlockAwareIndex(file_number, Slice(), 5ULL, 6ULL);
    // version byte is the second-to-last byte.
    encoded[encoded.size() - 2] =
        static_cast<char>(SeparateHelper::kBlockIndexVersion + 1);
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockId,
              SeparateHelper::DecodeBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockLayoutId,
              SeparateHelper::DecodeBlockLayoutId(slice));
  }

  // (d) block_id field equals the sentinel kNoBlockId. Must be rejected
  //     so the sentinel stays unambiguous.
  {
    std::string encoded = BuildBlockAwareIndex(
        file_number, Slice(), SeparateHelper::kNoBlockId, 6ULL);
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockId,
              SeparateHelper::DecodeBlockId(slice));
  }

  // (e) layout_id field equals the sentinel kNoBlockLayoutId. Must be
  //     rejected so the sentinel stays unambiguous.
  {
    std::string encoded = BuildBlockAwareIndex(
        file_number, Slice(), 5ULL, SeparateHelper::kNoBlockLayoutId);
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockLayoutId,
              SeparateHelper::DecodeBlockLayoutId(slice));
  }

  // (f) Last byte is NOT the magic. Trivially rejected.
  {
    std::string encoded =
        BuildBlockAwareIndex(file_number, Slice("m"), 5ULL, 6ULL);
    encoded.back() = static_cast<char>(0x00);  // clobber magic
    Slice slice(encoded);
    ASSERT_FALSE(SeparateHelper::HasBlockId(slice));
    ASSERT_EQ(SeparateHelper::kNoBlockId,
              SeparateHelper::DecodeBlockId(slice));
    // Malformed trailer must NOT strip the meta region.
    ASSERT_EQ(SeparateHelper::DecodeValueMeta(slice).ToString(),
              SeparateHelper::DecodeValueMetaStripBlockId(slice).ToString());
  }
}

// Test 5: The block-aware TransToSeparate() overload actually emits a
// well-formed block-id trailer on the outgoing value-index, while
// staying bit-for-bit identical to the legacy overload when the caller
// passes a sentinel block_id OR layout_id. This directly covers the
// writer side of stamping the data-block id onto the index value during
// flush/compaction.
TEST_F(FormatTest, TransToSeparate_StampsBlockIdTrailer) {
  const uint64_t file_number = 123ULL;

  // Legacy behavior: block_id == kNoBlockId must produce a slice
  // indistinguishable from the legacy overload's output (no trailer).
  {
    LazyBuffer value;
    Status s = SeparateHelper::TransToSeparate(
        "ik", value, file_number, Slice(), /*is_merge=*/true,
        /*is_index=*/false, /*value_meta_extractor=*/nullptr,
        SeparateHelper::kNoBlockId, /*layout_id=*/9ULL);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(value.fetch().ok());
    ASSERT_EQ(file_number,
              SeparateHelper::DecodeFileNumber(value.slice()));
    ASSERT_FALSE(SeparateHelper::HasBlockId(value.slice()));
    ASSERT_EQ(SeparateHelper::kNoBlockId,
              SeparateHelper::DecodeBlockId(value.slice()));
  }

  // Legacy behavior: layout_id == kNoBlockLayoutId also degrades to the
  // legacy overload even with a valid block_id.
  {
    LazyBuffer value;
    Status s = SeparateHelper::TransToSeparate(
        "ik", value, file_number, Slice(), /*is_merge=*/true,
        /*is_index=*/false, /*value_meta_extractor=*/nullptr,
        /*block_id=*/7ULL, SeparateHelper::kNoBlockLayoutId);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(value.fetch().ok());
    ASSERT_EQ(file_number,
              SeparateHelper::DecodeFileNumber(value.slice()));
    ASSERT_FALSE(SeparateHelper::HasBlockId(value.slice()));
    ASSERT_EQ(SeparateHelper::kNoBlockLayoutId,
              SeparateHelper::DecodeBlockLayoutId(value.slice()));
  }

  // Block-aware behavior on the "merge / no meta" path.
  {
    LazyBuffer value;
    Status s = SeparateHelper::TransToSeparate(
        "ik", value, file_number, Slice(), /*is_merge=*/true,
        /*is_index=*/false, /*value_meta_extractor=*/nullptr,
        /*block_id=*/7ULL, /*layout_id=*/11ULL);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(value.fetch().ok());
    ASSERT_EQ(file_number,
              SeparateHelper::DecodeFileNumber(value.slice()));
    ASSERT_TRUE(SeparateHelper::HasBlockId(value.slice()));
    ASSERT_EQ(7ULL, SeparateHelper::DecodeBlockId(value.slice()));
    ASSERT_EQ(11ULL, SeparateHelper::DecodeBlockLayoutId(value.slice()));
    // Meta is empty and the trailer is stripped cleanly.
    ASSERT_EQ(
        0u,
        SeparateHelper::DecodeValueMetaStripBlockId(value.slice()).size());
  }

  // Block-aware behavior on the "caller-supplied meta" path. The
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
        /*block_id=*/128ULL, /*layout_id=*/256ULL);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(value.fetch().ok());
    ASSERT_EQ(file_number,
              SeparateHelper::DecodeFileNumber(value.slice()));
    ASSERT_TRUE(SeparateHelper::HasBlockId(value.slice()));
    ASSERT_EQ(128ULL, SeparateHelper::DecodeBlockId(value.slice()));
    ASSERT_EQ(256ULL, SeparateHelper::DecodeBlockLayoutId(value.slice()));
    ASSERT_EQ(meta_str,
              SeparateHelper::DecodeValueMetaStripBlockId(value.slice())
                  .ToString());
  }
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
