//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include <limits>

#include "db/gc_liveness_bloom.h"
#include "db/separated_value_reference.h"
#include "rocksdb/terark_namespace.h"
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
  TablePropertyCache properties = GetPropCache(1, {7U, 8U}, {});
  properties.num_entries = 123;
  properties.num_deletions = 7;
  properties.raw_key_size = 4567;
  properties.raw_value_size = 8910;
  properties.flags = TablePropertyCache::kNoRangeDeletions;
  properties.max_read_amp = 9;
  properties.read_amp = 2.5F;
  properties.earliest_time_begin_compact = 111;
  properties.latest_time_end_compact = 222;
  edit.AddFile(3, 300, 0, 100, InternalKey("foo", kBig + 500, kTypeValue),
               InternalKey("zoo", kBig + 600, kTypeDeletion), kBig + 500,
               kBig + 600, false, properties);
  std::string encoded;
  edit.EncodeTo(&encoded);
  VersionEdit parsed;
  ASSERT_OK(parsed.DecodeFrom(encoded));
  const auto& parsed_properties = parsed.GetNewFiles()[0].second.prop;
  ASSERT_EQ(123U, parsed_properties.num_entries);
  ASSERT_EQ(7U, parsed_properties.num_deletions);
  ASSERT_EQ(4567U, parsed_properties.raw_key_size);
  ASSERT_EQ(8910U, parsed_properties.raw_value_size);
  ASSERT_EQ(TablePropertyCache::kNoRangeDeletions, parsed_properties.flags);
  ASSERT_EQ(9U, parsed_properties.max_read_amp);
  ASSERT_FLOAT_EQ(2.5F, parsed_properties.read_amp);
  ASSERT_EQ(111U, parsed_properties.earliest_time_begin_compact);
  ASSERT_EQ(222U, parsed_properties.latest_time_end_compact);
  const auto& dep = parsed_properties.dependence;
  ASSERT_EQ(2U, dep.size());
  ASSERT_EQ(0U, dep[0].byte_count);
  ASSERT_EQ(0U, dep[1].byte_count);
}

TEST_F(VersionEditTest, SeparatedValueReferenceMetadataRoundTrip) {
  SeparatedValueReferenceMetadata metadata;
  metadata.record_count = 5;
  metadata.AddReference(Slice("a"), 11);
  metadata.AddReference(Slice("b"), 11);
  metadata.AddReference(Slice("c"), 22);
  metadata.AddReference(Slice("d"), 11);
  metadata.AddReference(Slice("e"), 11);

  const std::string encoded =
      EncodeSeparatedValueReferenceMetadata(metadata);
  SeparatedValueReferenceMetadata parsed;
  ASSERT_OK(DecodeSeparatedValueReferenceMetadata(Slice(encoded), &parsed));

  ASSERT_EQ(5U, parsed.record_count);
  ASSERT_EQ(5U, parsed.separated_record_count);
  ASSERT_EQ((std::vector<uint64_t>{11, 22}), parsed.target_file_numbers);
}

TEST_F(VersionEditTest, SeparatedValueReferenceMetadataPropertyFallback) {
  SeparatedValueReferenceMetadata metadata;
  metadata.record_count = 3;
  metadata.range_deletion_count = 1;
  metadata.complete = false;
  metadata.AddReference(Slice("a"), 11);
  metadata.AddReference(Slice("b"), 11);

  UserCollectedProperties properties;
  properties[kSeparatedValueReferenceProperty] =
      EncodeSeparatedValueReferenceMetadata(metadata);

  SeparatedValueReferenceMetadata parsed;
  ASSERT_OK(ParseSeparatedValueReferenceProperty(properties, &parsed));
  ASSERT_EQ(3U, parsed.record_count);
  ASSERT_EQ(2U, parsed.separated_record_count);
  ASSERT_EQ(1U, parsed.range_deletion_count);
  ASSERT_FALSE(parsed.complete);
  ASSERT_EQ((std::vector<uint64_t>{11}), parsed.target_file_numbers);

  SeparatedValueReferenceMetadata stale;
  stale.record_count = 99;
  stale.separated_record_count = 98;
  stale.range_deletion_count = 97;
  stale.complete = true;
  stale.AddReference(Slice("old"), 99);
  UserCollectedProperties missing;
  Status status = ParseSeparatedValueReferenceProperty(missing, &stale);
  ASSERT_TRUE(status.IsNotFound());
  ASSERT_EQ(0U, stale.record_count);
  ASSERT_EQ(0U, stale.separated_record_count);
  ASSERT_EQ(0U, stale.range_deletion_count);
  ASSERT_FALSE(stale.complete);
  ASSERT_TRUE(stale.target_file_numbers.empty());
}

TEST_F(VersionEditTest, CompleteSeparatedValueReferenceMetadataMatchesManifest) {
  TableProperties table_properties;
  table_properties.num_entries = 3;
  table_properties.dependence.emplace_back(Dependence{11, 2, 0});

  SeparatedValueReferenceMetadata metadata;
  metadata.record_count = 3;
  metadata.AddReference(Slice("a"), 11);
  metadata.AddReference(Slice("b"), 11);
  table_properties.user_collected_properties
      [kSeparatedValueReferenceProperty] =
      EncodeSeparatedValueReferenceMetadata(metadata);

  ASSERT_TRUE(
      HasCompleteSeparatedValueReferenceMetadata(table_properties));

  table_properties.dependence.emplace_back(Dependence{22, 1, 0});
  ASSERT_FALSE(
      HasCompleteSeparatedValueReferenceMetadata(table_properties));

  table_properties.dependence.pop_back();
  table_properties.num_entries = 4;
  ASSERT_FALSE(
      HasCompleteSeparatedValueReferenceMetadata(table_properties));
}

TEST_F(VersionEditTest, SeparatedValueReferenceMetadataRejectsCorruption) {
  SeparatedValueReferenceMetadata metadata;
  metadata.record_count = 2;
  metadata.AddReference(Slice("a"), 11);
  metadata.AddReference(Slice("b"), 11);
  const std::string encoded =
      EncodeSeparatedValueReferenceMetadata(metadata);

  SeparatedValueReferenceMetadata parsed;
  parsed.record_count = 77;
  parsed.complete = false;
  parsed.AddReference(Slice("sentinel"), 55);
  parsed.separated_record_count = 66;

  std::string truncated = encoded;
  truncated.pop_back();
  ASSERT_TRUE(DecodeSeparatedValueReferenceMetadata(Slice(truncated), &parsed)
                  .IsCorruption());
  ASSERT_EQ(77U, parsed.record_count);
  ASSERT_EQ(66U, parsed.separated_record_count);
  ASSERT_FALSE(parsed.complete);
  ASSERT_EQ((std::vector<uint64_t>{55}), parsed.target_file_numbers);

  std::string trailing = encoded;
  trailing.push_back('\0');
  ASSERT_TRUE(DecodeSeparatedValueReferenceMetadata(Slice(trailing), &parsed)
                  .IsCorruption());

  SeparatedValueReferenceMetadata inconsistent = metadata;
  inconsistent.separated_record_count = 3;
  const std::string inconsistent_encoding =
      EncodeSeparatedValueReferenceMetadata(inconsistent);
  ASSERT_TRUE(
      DecodeSeparatedValueReferenceMetadata(Slice(inconsistent_encoding),
                                            &parsed)
          .IsCorruption());
}

TEST_F(VersionEditTest, GarbageCollectionLivenessBloomRoundTrip) {
  GarbageCollectionLivenessBloomBuilder builder;
  const ParsedInternalKey live_a("a", 7, kTypeValueIndex);
  const ParsedInternalKey live_b("b", 9, kTypeMergeIndex);
  builder.Add(11, live_a);
  builder.Add(22, live_b);

  GarbageCollectionLivenessBloomIndex bloom;
  ASSERT_OK(bloom.Decode(Slice(builder.Finish())));
  ASSERT_TRUE(bloom.MatchesDependence(
      {Dependence{11, 1, 0}, Dependence{22, 1, 0}}));
  ASSERT_TRUE(bloom.MayContain({11}, live_a));
  ASSERT_TRUE(bloom.MayContain({22}, live_b));
  ASSERT_FALSE(
      bloom.MayContain({11}, ParsedInternalKey("missing", 7, kTypeValueIndex)));
}

TEST_F(VersionEditTest, GarbageCollectionLivenessBloomRejectsMismatch) {
  GarbageCollectionLivenessBloomBuilder builder;
  builder.Add(11, ParsedInternalKey("a", 7, kTypeValueIndex));
  const std::string encoded = builder.Finish();

  GarbageCollectionLivenessBloomIndex bloom;
  ASSERT_OK(bloom.Decode(Slice(encoded)));
  ASSERT_FALSE(
      bloom.MatchesDependence({Dependence{11, 2, 0}}));
  ASSERT_FALSE(
      bloom.MatchesDependence({Dependence{22, 1, 0}}));

  std::string truncated = encoded;
  truncated.pop_back();
  GarbageCollectionLivenessBloomIndex corrupt;
  ASSERT_TRUE(corrupt.Decode(Slice(truncated)).IsCorruption());

  std::string oversized;
  PutVarint64(&oversized, GarbageCollectionLivenessBloomBuilder::kVersion);
  PutVarint32(&oversized,
              GarbageCollectionLivenessBloomBuilder::kBitsPerReference);
  PutVarint32(&oversized,
              GarbageCollectionLivenessBloomBuilder::kNumProbes);
  PutVarint64(&oversized, 1);
  PutVarint64(&oversized, 11);
  PutVarint64(&oversized, 1);
  PutVarint64(&oversized, std::numeric_limits<uint64_t>::max());
  GarbageCollectionLivenessBloomIndex oversized_bloom;
  ASSERT_TRUE(oversized_bloom.Decode(Slice(oversized)).IsCorruption());
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
