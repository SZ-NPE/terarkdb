//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "rocksdb/terark_namespace.h"
#include "util/blob_block_bitmap.h"
#include "util/sync_point.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

namespace {
TablePropertyCache GetPropCache(
    uint8_t purpose, std::initializer_list<uint64_t> dependence = {},
    std::initializer_list<uint64_t> inheritance = {}) {
  std::vector<Dependence> dep;
  for (auto& d : dependence) dep.emplace_back(Dependence{d, 1, 0});
  return TablePropertyCache{0, 0, 1, 1, 0, purpose, 0, 0, dep, inheritance};
}

// precise_gc: build a TablePropertyCache with explicit per-dependence
// (entry_count, byte_count) pairs so tests can cover the new field.
TablePropertyCache GetPropCacheWithBytes(
    uint8_t purpose,
    std::initializer_list<std::tuple<uint64_t, uint64_t, uint64_t>> dep_list) {
  std::vector<Dependence> dep;
  for (auto& t : dep_list) {
    dep.emplace_back(Dependence{std::get<0>(t), std::get<1>(t), std::get<2>(t)});
  }
  return TablePropertyCache{0, 0, 1, 1, 0, purpose, 0, 0, dep, {}};
}
}  // namespace

static void TestEncodeDecode(const VersionEdit& edit) {
  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  parsed.EncodeTo(&encoded2);
  ASSERT_EQ(encoded, encoded2);
}

class VersionEditTest : public testing::Test {};

TEST_F(VersionEditTest, EncodeDecode) {
  static const uint64_t kBig = 1ull << 50;
  static const uint32_t kBig32Bit = 1ull << 30;

  VersionEdit edit;
  for (int i = 0; i < 4; i++) {
    TestEncodeDecode(edit);
    edit.AddFile(3, kBig + 300 + i, kBig32Bit + 400 + i, 0,
                 InternalKey("foo", kBig + 500 + i, kTypeValue),
                 InternalKey("zoo", kBig + 600 + i, kTypeDeletion),
                 kBig + 500 + i, kBig + 600 + i, false,
                 GetPropCache(1, {2U, 3U}, {}));
    edit.DeleteFile(4, kBig + 700 + i);
  }

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, EncodeDecodeNewFile4) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, GetPropCache(0, {}, {}));
  edit.AddFile(4, 301, 3, 100, InternalKey("foo", kBig + 501, kTypeValue),
               InternalKey("zoo", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false, GetPropCache(1, {1U}, {}));
  edit.AddFile(5, 302, 0, 100, InternalKey("foo", kBig + 502, kTypeValue),
               InternalKey("zoo", kBig + 602, kTypeDeletion), kBig + 502,
               kBig + 602, true, GetPropCache(2, {2U, 3U}, {}));

  edit.DeleteFile(4, 700);

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);
  TestEncodeDecode(edit);

  std::string encoded, encoded2;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files[0].second.marked_for_compaction);
  ASSERT_TRUE(!new_files[1].second.marked_for_compaction);
  ASSERT_TRUE(new_files[2].second.marked_for_compaction);
  ASSERT_EQ(3, new_files[0].second.fd.GetPathId());
  ASSERT_EQ(3, new_files[1].second.fd.GetPathId());
  ASSERT_EQ(0, new_files[2].second.fd.GetPathId());
  ASSERT_EQ(0, new_files[0].second.prop.purpose);
  ASSERT_EQ(1, new_files[1].second.prop.purpose);
  ASSERT_EQ(2, new_files[2].second.prop.purpose);
  ASSERT_EQ(0, new_files[0].second.prop.dependence.size());
  ASSERT_EQ(1U, new_files[1].second.prop.dependence[0].file_number);
  ASSERT_EQ(2U, new_files[2].second.prop.dependence[0].file_number);
  ASSERT_EQ(3U, new_files[2].second.prop.dependence[1].file_number);
}

TEST_F(VersionEditTest, ForwardCompatibleNewFile4) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, GetPropCache(0, {}, {}));
  edit.AddFile(4, 301, 3, 100, InternalKey("foo", kBig + 501, kTypeValue),
               InternalKey("zoo", kBig + 601, kTypeDeletion), kBig + 501,
               kBig + 601, false, GetPropCache(0, {}, {}));
  edit.DeleteFile(4, 700);

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  std::string encoded;

  // Call back function to add extra customized builds.
  bool first = true;
  TERARKDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:NewFile4:CustomizeFields", [&](void* arg) {
        std::string* str = reinterpret_cast<std::string*>(arg);
        PutVarint32(str, 33);
        const std::string str1 = "random_string";
        PutLengthPrefixedSlice(str, str1);
        if (first) {
          first = false;
          PutVarint32(str, 22);
          const std::string str2 = "s";
          PutLengthPrefixedSlice(str, str2);
        }
      });
  TERARKDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  edit.EncodeTo(&encoded);
  TERARKDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(!first);
  auto& new_files = parsed.GetNewFiles();
  ASSERT_TRUE(new_files[0].second.marked_for_compaction);
  ASSERT_TRUE(!new_files[1].second.marked_for_compaction);
  ASSERT_EQ(3, new_files[0].second.fd.GetPathId());
  ASSERT_EQ(3, new_files[1].second.fd.GetPathId());
  ASSERT_EQ(1u, parsed.GetDeletedFiles().size());
}

TEST_F(VersionEditTest, NewFile4NotSupportedField) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 3, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, true, GetPropCache(0, {}, {}));

  edit.SetComparatorName("foo");
  edit.SetLogNumber(kBig + 100);
  edit.SetNextFile(kBig + 200);
  edit.SetLastSequence(kBig + 1000);

  std::string encoded;

  // Call back function to add extra customized builds.
  TERARKDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "VersionEdit::EncodeTo:NewFile4:CustomizeFields", [&](void* arg) {
        std::string* str = reinterpret_cast<std::string*>(arg);
        const std::string str1 = "s";
        PutLengthPrefixedSlice(str, str1);
      });
  TERARKDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();
  edit.EncodeTo(&encoded);
  TERARKDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();

  VersionEdit parsed;
  Status s = parsed.DecodeFrom(encoded);
  ASSERT_NOK(s);
}

TEST_F(VersionEditTest, EncodeEmptyFile) {
  VersionEdit edit;
  edit.AddFile(0, 0, 0, 0, InternalKey(), InternalKey(), 0, 0, false,
               GetPropCache(0, {}, {}));
  std::string buffer;
  ASSERT_TRUE(!edit.EncodeTo(&buffer));
}

TEST_F(VersionEditTest, ColumnFamilyTest) {
  VersionEdit edit;
  edit.SetColumnFamily(2);
  edit.AddColumnFamily("column_family");
  edit.SetMaxColumnFamily(5);
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetColumnFamily(3);
  edit.DropColumnFamily();
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, MinLogNumberToKeep) {
  VersionEdit edit;
  edit.SetMinLogNumberToKeep(13);
  TestEncodeDecode(edit);

  edit.Clear();
  edit.SetMinLogNumberToKeep(23);
  TestEncodeDecode(edit);
}

TEST_F(VersionEditTest, AtomicGroupTest) {
  VersionEdit edit;
  edit.MarkAtomicGroup(1);
  TestEncodeDecode(edit);
}

// precise_gc: verify per-dependence byte_count roundtrips through manifest
// encode/decode and is preserved per-file.
TEST_F(VersionEditTest, DependenceByteCountRoundTrip) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(
      3, 300, 0, 100, InternalKey("foo", kBig + 500, kTypeValue),
      InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500, kBig + 600,
      false,
      GetPropCacheWithBytes(
          1, {std::make_tuple(10U, 5U, 4096U),
              std::make_tuple(11U, 3U, 2048U)}));
  // A second file referencing the same blob (10U) with different byte_count
  // ensures per-file independence after decode.
  edit.AddFile(
      3, 301, 0, 100, InternalKey("bar", kBig + 501, kTypeValue),
      InternalKey("baz", kBig + 601, kTypeDeletion), kBig + 501, kBig + 601,
      false,
      GetPropCacheWithBytes(1, {std::make_tuple(10U, 1U, 1024U)}));

  std::string encoded;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  ASSERT_OK(parsed.DecodeFrom(encoded));

  auto& new_files = parsed.GetNewFiles();
  ASSERT_EQ(2U, new_files.size());
  auto& dep0 = new_files[0].second.prop.dependence;
  ASSERT_EQ(2U, dep0.size());
  ASSERT_EQ(10U, dep0[0].file_number);
  ASSERT_EQ(5U, dep0[0].entry_count);
  ASSERT_EQ(4096U, dep0[0].byte_count);
  ASSERT_EQ(11U, dep0[1].file_number);
  ASSERT_EQ(3U, dep0[1].entry_count);
  ASSERT_EQ(2048U, dep0[1].byte_count);

  auto& dep1 = new_files[1].second.prop.dependence;
  ASSERT_EQ(1U, dep1.size());
  ASSERT_EQ(10U, dep1[0].file_number);
  ASSERT_EQ(1U, dep1[0].entry_count);
  ASSERT_EQ(1024U, dep1[0].byte_count);
}

// precise_gc: verify that a pre-byte_count manifest (byte_count==0) decodes
// cleanly without errors; VersionBuilder will fall back to averaged estimate
// for such files.
TEST_F(VersionEditTest, DependenceByteCountDefaultsToZero) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 0, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, false, GetPropCache(1, {7U, 8U}, {}));
  std::string encoded;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  ASSERT_OK(parsed.DecodeFrom(encoded));
  auto& dep = parsed.GetNewFiles()[0].second.prop.dependence;
  ASSERT_EQ(2U, dep.size());
  ASSERT_EQ(0U, dep[0].byte_count);
  ASSERT_EQ(0U, dep[1].byte_count);
}

// dependence_block_bitmaps exactly: size, per-row available/layout_id,
// per-bitmap bit contents. The test also covers an explicitly
// unavailable row (which GC reads as "fall back for this blob") versus
// an available-but-empty row.
TEST_F(VersionEditTest, EncodeDecodeWithBlockBitmap) {
  static const uint64_t kBig = 1ull << 50;

  TablePropertyCache prop = GetPropCache(1, {10U, 11U, 12U}, {});
  prop.dependence_block_bitmaps.resize(prop.dependence.size());
  prop.dependence_block_bitmaps[0].available = true;
  prop.dependence_block_bitmaps[0].layout_id = 1000U;
  prop.dependence_block_bitmaps[0].bitmap.Set(0);
  prop.dependence_block_bitmaps[0].bitmap.Set(5);
  prop.dependence_block_bitmaps[0].bitmap.Set(17);
  // dep[1] intentionally unavailable: per-row "bitmap unavailable".
  prop.dependence_block_bitmaps[1].available = false;
  prop.dependence_block_bitmaps[2].available = true;
  prop.dependence_block_bitmaps[2].layout_id = 1002U;
  prop.dependence_block_bitmaps[2].bitmap.Set(3);
  prop.dependence_block_bitmaps[2].bitmap.Set(4);

  VersionEdit edit;
  edit.AddFile(3, 300, 0, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, false, prop);

  std::string encoded;
  ASSERT_TRUE(edit.EncodeTo(&encoded));
  VersionEdit parsed;
  ASSERT_OK(parsed.DecodeFrom(encoded));

  auto& files = parsed.GetNewFiles();
  ASSERT_EQ(1U, files.size());
  auto& rp = files[0].second.prop;
  ASSERT_EQ(3U, rp.dependence.size());
  ASSERT_EQ(3U, rp.dependence_block_bitmaps.size());

  EXPECT_TRUE(rp.dependence_block_bitmaps[0].available);
  EXPECT_EQ(1000U, rp.dependence_block_bitmaps[0].layout_id);
  EXPECT_EQ(uint64_t(3), rp.dependence_block_bitmaps[0].bitmap.CountSetBits());
  EXPECT_TRUE(rp.dependence_block_bitmaps[0].bitmap.Test(0));
  EXPECT_TRUE(rp.dependence_block_bitmaps[0].bitmap.Test(5));
  EXPECT_TRUE(rp.dependence_block_bitmaps[0].bitmap.Test(17));
  EXPECT_FALSE(rp.dependence_block_bitmaps[0].bitmap.Test(1));

  EXPECT_FALSE(rp.dependence_block_bitmaps[1].available);

  EXPECT_TRUE(rp.dependence_block_bitmaps[2].available);
  EXPECT_EQ(1002U, rp.dependence_block_bitmaps[2].layout_id);
  EXPECT_EQ(uint64_t(2), rp.dependence_block_bitmaps[2].bitmap.CountSetBits());
  EXPECT_TRUE(rp.dependence_block_bitmaps[2].bitmap.Test(3));
  EXPECT_TRUE(rp.dependence_block_bitmaps[2].bitmap.Test(4));

  // Re-encoding the decoded edit should be byte-equal to the original
  // encoding. This pins down the stability of the persisted format.
  std::string re_encoded;
  ASSERT_TRUE(parsed.EncodeTo(&re_encoded));
  ASSERT_EQ(encoded, re_encoded);
}

// A VersionEdit produced before the block bitmap feature (no block
// bitmap at all, i.e. vector is empty and its payload is omitted)
// must decode cleanly, with `dependence_block_bitmaps` staying empty.
// This is the explicit "bitmap unavailable" sentinel that the GC
// side uses to pick the legacy lookup path.
TEST_F(VersionEditTest, DecodeBackwardCompatibleWithoutBlockBitmap) {
  static const uint64_t kBig = 1ull << 50;
  VersionEdit edit;
  edit.AddFile(3, 300, 0, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, false, GetPropCache(1, {21U, 22U}, {}));
  std::string encoded;
  ASSERT_TRUE(edit.EncodeTo(&encoded));
  VersionEdit parsed;
  ASSERT_OK(parsed.DecodeFrom(encoded));
  auto& p = parsed.GetNewFiles()[0].second.prop;
  ASSERT_EQ(2U, p.dependence.size());
  ASSERT_TRUE(p.dependence_block_bitmaps.empty());
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
