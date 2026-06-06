//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <inttypes.h>

#include <string>

#include "db/version_edit.h"
#include "db/version_set.h"
#include "rocksdb/terark_namespace.h"
#include "util/blob_block_bitmap.h"
#include "util/logging.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace TERARKDB_NAMESPACE {

class VersionBuilderTest : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableCFOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  VersionStorageInfo vstorage_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::vector<uint64_t> size_being_compacted_;

  VersionBuilderTest()
      : ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        ioptions_(options_),
        mutable_cf_options_(options_),
        vstorage_(&icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
                  false),
        file_num_(1) {
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    size_being_compacted_.resize(options_.num_levels);
  }

  ~VersionBuilderTest() {
    for (int i = -1; i < vstorage_.num_levels(); i++) {
      for (auto* f : vstorage_.LevelFiles(i)) {
        if (--f->refs == 0) {
          delete f;
        }
      }
    }
  }

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           uint64_t num_entries = 0, uint64_t num_deletions = 0,
           SequenceNumber smallest_seqno = 0, SequenceNumber largest_seqno = 0,
           const TablePropertyCache& prop = TablePropertyCache{}) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, path_id, file_size);
    f->smallest = GetInternalKey(smallest, smallest_seq);
    f->largest = GetInternalKey(largest, largest_seq);
    f->fd.smallest_seqno = smallest_seqno;
    f->fd.largest_seqno = largest_seqno;
    f->compensated_file_size = file_size;
    f->refs = 0;
    f->prop = prop;
    f->prop.num_entries = num_entries;
    f->prop.num_deletions = num_deletions;
    f->gc_status = FileMetaData::kGarbageCollectionForbidden;
    vstorage_.AddFile(level, f);
    vstorage_.UpdateAccumulatedStats(f);
  }

  void UpdateVersionStorageInfo() {
    vstorage_.UpdateFilesByCompactionPri(ioptions_.compaction_pri);
    vstorage_.UpdateNumNonEmptyLevels();
    vstorage_.GenerateFileIndexer();
    vstorage_.GenerateLevelFilesBrief();
    vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
    vstorage_.GenerateLevel0NonOverlapping();
    vstorage_.SetFinalized();
  }
};

void UnrefFilesInVersion(VersionStorageInfo* new_vstorage) {
  for (int i = -1; i < new_vstorage->num_levels(); i++) {
    for (auto* f : new_vstorage->LevelFiles(i)) {
      if (--f->refs == 0) {
        delete f;
      }
    }
  }
}

bool VerifyDependFiles(VersionStorageInfo* new_vstorage,
                       const std::vector<uint64_t>& dependence) {
  auto& vstorage_dependence = new_vstorage->dependence_map();
  if (vstorage_dependence.size() != dependence.size()) {
    return false;
  }
  for (auto depend : dependence) {
    if (vstorage_dependence.count(depend) == 0) {
      return false;
    }
  }
  return true;
}

TablePropertyCache GetPropCache(
    uint8_t purpose, std::initializer_list<uint64_t> dependence = {},
    std::initializer_list<uint64_t> inheritance = {}) {
  std::vector<Dependence> dep;
  for (auto& d : dependence) dep.emplace_back(Dependence{d, 1, 0});
  TablePropertyCache ret;
  ret.purpose = purpose;
  ret.dependence = dep;
  ret.inheritance = inheritance;
  return ret;
}

// precise_gc: build a TablePropertyCache with explicit per-dependence
// (file_number, entry_count, byte_count) tuples so tests can drive the new
// byte-based path through VersionBuilder.
TablePropertyCache GetPropCacheWithBytes(
    uint8_t purpose,
    std::initializer_list<std::tuple<uint64_t, uint64_t, uint64_t>> dep_list) {
  std::vector<Dependence> dep;
  for (auto& t : dep_list) {
    dep.emplace_back(
        Dependence{std::get<0>(t), std::get<1>(t), std::get<2>(t)});
  }
  TablePropertyCache ret;
  ret.purpose = purpose;
  ret.dependence = dep;
  return ret;
}

// build a TablePropertyCache that has both `dependence` and an
// aligned `dependence_block_bitmaps` vector. Each entry is
// (blob_file_number, set_block_ids). Every row is marked available with
// its layout_id stamped to the referenced blob file number, which is
// what the aggregator requires in order to trust the row (the blob's
// current physical file number equals its own file number at level -1).
// Tests wanting the legacy / unavailable regime should use
// GetPropCacheBlockBitmapLegacy() or
// GetPropCacheBlockBitmapWithUnavailableRow() instead.
TablePropertyCache GetPropCacheWithBlockBitmaps(
    uint8_t purpose,
    std::initializer_list<
        std::pair<uint64_t, std::vector<uint64_t>>> dep_list) {
  TablePropertyCache ret;
  ret.purpose = purpose;
  for (auto& kv : dep_list) {
    ret.dependence.emplace_back(Dependence{kv.first, 1, 0});
    DependenceBlockBitmap row;
    row.available = true;
    row.layout_id = kv.first;
    for (uint64_t bid : kv.second) row.bitmap.Set(bid);
    ret.dependence_block_bitmaps.emplace_back(std::move(row));
  }
  return ret;
}

// build a TablePropertyCache that references blobs with a block bitmap,
// but stamps a caller-chosen layout_id on every row. This is used to
// exercise the aggregator's layout_id guard: when the stamped layout_id
// does not match the blob's current physical file number, the row must
// be rejected and the blob's aggregated view becomes unavailable.
TablePropertyCache GetPropCacheWithBlockBitmapsLayoutId(
    uint8_t purpose, uint64_t layout_id,
    std::initializer_list<
        std::pair<uint64_t, std::vector<uint64_t>>> dep_list) {
  TablePropertyCache ret;
  ret.purpose = purpose;
  for (auto& kv : dep_list) {
    ret.dependence.emplace_back(Dependence{kv.first, 1, 0});
    DependenceBlockBitmap row;
    row.available = true;
    row.layout_id = layout_id;
    for (uint64_t bid : kv.second) row.bitmap.Set(bid);
    ret.dependence_block_bitmaps.emplace_back(std::move(row));
  }
  return ret;
}

// build a TablePropertyCache with `dependence` populated but
// `dependence_block_bitmaps` omitted. This is the legacy regime and
// must cause every referenced blob to be marked bitmap_available=false.
TablePropertyCache GetPropCacheBlockBitmapLegacy(
    uint8_t purpose, std::initializer_list<uint64_t> dep_list) {
  TablePropertyCache ret;
  ret.purpose = purpose;
  for (auto fn : dep_list) {
    ret.dependence.emplace_back(Dependence{fn, 1, 0});
  }
  // Intentionally leave dependence_block_bitmaps empty -> legacy.
  return ret;
}

// build a TablePropertyCache that has `dependence` AND a
// per-row vector where one specific row is the "bitmap unavailable"
// row (available=false). All other rows carry the supplied
// set_block_ids and a matching layout_id.
TablePropertyCache GetPropCacheBlockBitmapWithUnavailableRow(
    uint8_t purpose,
    std::initializer_list<
        std::pair<uint64_t, std::vector<uint64_t>>> dep_list,
    uint64_t unavailable_file_number) {
  TablePropertyCache ret;
  ret.purpose = purpose;
  for (auto& kv : dep_list) {
    ret.dependence.emplace_back(Dependence{kv.first, 1, 0});
    DependenceBlockBitmap row;
    if (kv.first != unavailable_file_number) {
      row.available = true;
      row.layout_id = kv.first;
      for (uint64_t bid : kv.second) row.bitmap.Set(bid);
    } else {
      // Unavailable row: GC must fall back for this blob.
      row.available = false;
      row.layout_id = kNoBlockLayoutId;
    }
    ret.dependence_block_bitmaps.emplace_back(std::move(row));
  }
  return ret;
}

TEST_F(VersionBuilderTest, ApplyAndSaveTo) {
  Add(0, 1U, "150", "200", 100U);

  Add(1, 66U, "150", "200", 100U);
  Add(1, 88U, "201", "300", 100U);

  Add(2, 6U, "150", "179", 100U);
  Add(2, 7U, "180", "220", 100U);
  Add(2, 8U, "221", "300", 100U);

  Add(3, 26U, "150", "170", 100U);
  Add(3, 27U, "171", "179", 100U);
  Add(3, 28U, "191", "220", 100U);
  Add(3, 29U, "221", "300", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false,
                       GetPropCache(1, {27U}));
  version_edit.DeleteFile(3, 27U);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  ASSERT_EQ(400U, new_vstorage.NumLevelBytes(2));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(3));
  ASSERT_TRUE(VerifyDependFiles(
      &new_vstorage, {1U, 66U, 88U, 6U, 7U, 8U, 26U, 27U, 28U, 29U, 666}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, 200U);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, 100U);

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(3, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false,
                       GetPropCache(1, {1U}));
  version_edit.DeleteFile(0, 1U);
  version_edit.DeleteFile(0, 88U);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(0));
  ASSERT_EQ(100U, new_vstorage.NumLevelBytes(3));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));
  ASSERT_TRUE(
      VerifyDependFiles(&new_vstorage, {1U, 6U, 7U, 8U, 26U, 27U, 666}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, 200U);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, 100U);

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);

  Add(-1, 4U, "90", "119", 100U);
  Add(-1, 5U, "120", "149", 100U);

  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(4, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false,
                       GetPropCache(1, {1U, 4U}));
  version_edit.AddFile(4, 5, 0, 100U, GetInternalKey("120"),
                       GetInternalKey("149"), 200, 200, false,
                       GetPropCache(0, {}));
  version_edit.DeleteFile(0, 1U);
  version_edit.DeleteFile(0, 88U);
  version_edit.DeleteFile(4, 6U);
  version_edit.DeleteFile(4, 7U);
  version_edit.DeleteFile(4, 8U);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(0));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {1U, 4U, 26U, 27U, 4U, 5U}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic3) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  Add(-1, 31U, "115", "119", 50U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(1, 11U, 0, 100U, GetInternalKey("100"),
                       GetInternalKey("119"), 2, 2, false,
                       GetPropCache(1, {4U, 5U}));
  version_edit.AddFile(1, 12U, 0, 100U, GetInternalKey("120"),
                       GetInternalKey("129"), 2, 2, false, {});
  version_edit.AddFile(1, 13U, 0, 100U, GetInternalKey("130"),
                       GetInternalKey("139"), 2, 2, false, {});
  version_edit.AddFile(1, 14U, 0, 100U, GetInternalKey("140"),
                       GetInternalKey("149"), 2, 2, false, {});
  version_edit.AddFile(1, 15U, 0, 100U, GetInternalKey("150"),
                       GetInternalKey("149"), 2, 2, false, {});

  version_edit.AddFile(-1, 2U, 0, 100U, GetInternalKey("100"),
                       GetInternalKey("109"), 2, 2, false, {});
  version_edit.AddFile(-1, 4U, 0, 50U, GetInternalKey("100"),
                       GetInternalKey("114"), 2, 2, false,
                       GetPropCache(1, {2U}));
  version_edit.AddFile(-1, 5U, 0, 50U, GetInternalKey("115"),
                       GetInternalKey("119"), 2, 2, false, {});
  version_builder.Apply(&version_edit);

  VersionEdit version_edit2;
  version_edit2.AddFile(2, 21U, 0, 100U, GetInternalKey("110"),
                        GetInternalKey("159"), 2, 2, false,
                        GetPropCache(1, {11U, 12U, 13U, 14U, 15U}));
  version_edit2.DeleteFile(1, 11U);
  version_edit2.DeleteFile(1, 12U);
  version_edit2.DeleteFile(1, 13U);
  version_edit2.DeleteFile(1, 14U);
  version_edit2.DeleteFile(1, 15U);
  version_builder.Apply(&version_edit2);

  VersionEdit version_edit3;
  version_edit3.AddFile(2, 22U, 0, 100U, GetInternalKey("100"),
                        GetInternalKey("159"), 2, 2, false,
                        GetPropCache(1, {4U, 5U, 12U, 13U, 14U, 15U}));
  version_edit3.DeleteFile(2, 21U);
  version_builder.Apply(&version_edit3);

  VersionEdit version_edit4;
  version_edit4.AddFile(2, 23U, 0, 100U, GetInternalKey("140"),
                        GetInternalKey("159"), 2, 2, false,
                        GetPropCache(1, {4U, 12U, 13U, 14U, 15U}));
  version_edit4.AddFile(2, 5U, 0, 50U, GetInternalKey("115"),
                        GetInternalKey("119"), 2, 2, false, {});
  version_edit4.DeleteFile(2, 22U);
  version_builder.Apply(&version_edit4);

  VersionEdit version_edit5;
  version_edit5.AddFile(2, 24U, 0, 100U, GetInternalKey("140"),
                        GetInternalKey("159"), 2, 2, false,
                        GetPropCache(1, {14U, 15U}));
  version_edit5.DeleteFile(2, 23U);
  version_builder.Apply(&version_edit5);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.SaveTo(&new_vstorage, 0);

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(1));
  ASSERT_EQ(150U, new_vstorage.NumLevelBytes(2));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {5U, 15U, 14U, 24U}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyMultipleAndSaveTo) {
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false, {});
  version_edit.AddFile(2, 676, 0, 100U, GetInternalKey("401"),
                       GetInternalKey("450"), 200, 200, false, {});
  version_edit.AddFile(2, 636, 0, 100U, GetInternalKey("601"),
                       GetInternalKey("650"), 200, 200, false, {});
  version_edit.AddFile(2, 616, 0, 100U, GetInternalKey("501"),
                       GetInternalKey("550"), 200, 200, false, {});
  version_edit.AddFile(2, 606, 0, 100U, GetInternalKey("701"),
                       GetInternalKey("750"), 200, 200, false, {});

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  ASSERT_EQ(500U, new_vstorage.NumLevelBytes(2));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {666, 676, 636, 616, 606}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyDeleteAndSaveTo) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false, {});
  version_edit.AddFile(2, 676, 0, 100U, GetInternalKey("401"),
                       GetInternalKey("450"), 200, 200, false, {});
  version_edit.AddFile(2, 636, 0, 100U, GetInternalKey("601"),
                       GetInternalKey("650"), 200, 200, false, {});
  version_edit.AddFile(2, 616, 0, 100U, GetInternalKey("501"),
                       GetInternalKey("550"), 200, 200, false, {});
  version_edit.AddFile(2, 606, 0, 100U, GetInternalKey("701"),
                       GetInternalKey("750"), 200, 200, false, {});
  version_builder.Apply(&version_edit);

  VersionEdit version_edit2;
  version_edit.AddFile(2, 808, 0, 100U, GetInternalKey("901"),
                       GetInternalKey("950"), 200, 200, false, {});
  version_edit2.DeleteFile(2, 616);
  version_edit2.DeleteFile(2, 636);
  version_edit.AddFile(2, 806, 0, 100U, GetInternalKey("801"),
                       GetInternalKey("850"), 200, 200, false, {});
  version_builder.Apply(&version_edit2);

  version_builder.SaveTo(&new_vstorage, 0);

  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(2));
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {666, 676, 606}));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, EstimatedActiveKeys) {
  // const uint32_t kTotalSamples = 20;
  const uint32_t kNumLevels = 5;
  const uint32_t kFilesPerLevel = 8;
  const uint32_t kNumFiles = kNumLevels * kFilesPerLevel;
  const uint32_t kEntriesPerFile = 1000;
  const uint32_t kDeletionsPerFile = 100;
  for (uint32_t i = 0; i < kNumFiles; ++i) {
    Add(static_cast<int>(i / kFilesPerLevel), i + 1,
        ToString((i + 100) * 1000).c_str(),
        ToString((i + 100) * 1000 + 999).c_str(), 100U, 0, 100, 100,
        kEntriesPerFile, kDeletionsPerFile, kNumFiles - i, kNumFiles - i);
  }
  UpdateVersionStorageInfo();

  // minus 2X for the number of deletion entries because:
  // 1x for deletion entry does not count as a data entry.
  // 1x for each deletion entry will actually remove one data entry.
  ASSERT_EQ(vstorage_.GetEstimatedActiveKeys(),
            (kEntriesPerFile - kDeletionsPerFile) * kNumFiles);
}

TEST_F(VersionBuilderTest, HugeLSM) {
  const uint32_t kNumLevels = 7;
  const uint32_t kFilesPerLevel = 64;
  const uint32_t kFilesPerLevelMultiplier = 4;
  const uint32_t kFilesBlobDependence = 512;
  const uint32_t kFilesBlobInheritance = 32;
  const uint32_t kFilesBlobCount = 32768;

  Random64 _rand(301);
  uint64_t fn = kFilesBlobCount * kFilesBlobInheritance + 1;
  uint64_t level_file_count = kFilesPerLevel;

  for (uint32_t i = 0; i < kFilesBlobCount; ++i) {
    TablePropertyCache prop;
    for (uint32_t j = i * 32 + 1, je = j + kFilesBlobInheritance - 1; j < je;
         ++j) {
      prop.inheritance.emplace_back(j);
    }
    Add(-1, (i + 1) * 32, "0", "1", 100, 0, 0, 100, 100000, 0, 0, 100, prop);
  }

  auto make_prop = [&] {
    TablePropertyCache prop;
    for (uint32_t j = 0; j < kFilesBlobDependence; ++j) {
      prop.dependence.emplace_back(Dependence{
          (_rand.Uniform(kFilesBlobCount) * kFilesBlobInheritance) + 1, 1});
    }
    std::sort(prop.dependence.begin(), prop.dependence.end(),
              [](const Dependence& l, const Dependence& r) {
                return l.file_number < r.file_number;
              });
    prop.dependence.erase(
        std::unique(prop.dependence.begin(), prop.dependence.end(),
                    [](const Dependence& l, const Dependence& r) {
                      return l.file_number == r.file_number;
                    }),
        prop.dependence.end());
    return prop;
  };
  auto to_fix_string = [](uint64_t n) {
    char buffer[32];
    snprintf(buffer, sizeof buffer, "%012" PRIu64, n);
    return std::string(buffer);
  };

  for (uint32_t level = 1; level < kNumLevels; ++level) {
    for (uint32_t i = 0; i < level_file_count; ++i) {
      Add(level, fn++, to_fix_string(i * 2).c_str(),
          to_fix_string(i * 2 + 1).c_str(), 100, 0, 0, 100,
          kFilesBlobDependence, 0, 0, 100, make_prop());
    }
    level_file_count *= kFilesPerLevelMultiplier;
  }
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);

  VersionEdit version_edit;
  version_edit.AddFile(
      1, fn, 0, 100,
      GetInternalKey(to_fix_string(kFilesPerLevel * 2).c_str(), 0),
      GetInternalKey(to_fix_string(kFilesPerLevel * 2 + 1).c_str(), 100), 0,
      100, false, make_prop());
  version_edit.DeleteFile(1, kFilesBlobCount * kFilesBlobInheritance + 1);

  version_builder.Apply(&version_edit);

  version_builder.SaveTo(&new_vstorage, 0);

  UnrefFilesInVersion(&new_vstorage);
}

// precise_gc: when a referencing SST carries an explicit per-dependence
// byte_count, VersionBuilder should propagate it to the blob file's
// num_antiquation_bytes field instead of falling back to the averaged
// estimate. This test also verifies that the entry-based
// num_antiquation still works in parallel with the byte-based accounting.
TEST_F(VersionBuilderTest, PreciseGcByteCountFromDependence) {
  // Blob B (file_number=100) at level -1:
  //   num_entries=100, file_size=10000.
  // Referencing SST S (file_number=200) at level 2 declares that it points
  // at 70 entries / 6000 bytes from B.
  // Expected after SaveTo:
  //   B.num_antiquation       = 100 - 70   = 30
  //   B.num_antiquation_bytes = 10000 - 6000 = 4000
  Add(-1, 100U, "100", "199", 10000U /*file_size*/, 0 /*path_id*/,
      100 /*smallest_seq*/, 100 /*largest_seq*/, 100 /*num_entries*/,
      0 /*num_deletions*/, 100 /*smallest_seqno*/, 100 /*largest_seqno*/);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2 /*level*/, 200U /*file_number*/, 0 /*path_id*/, 500U /*file_size*/,
      GetInternalKey("100"), GetInternalKey("199"), 200, 200, false,
      GetPropCacheWithBytes(0, {std::make_tuple(100U, 70U, 6000U)}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  auto& dep_map = new_vstorage.dependence_map();
  auto it = dep_map.find(100U);
  ASSERT_TRUE(it != dep_map.end());
  FileMetaData* b = it->second;
  ASSERT_EQ(30U, b->num_antiquation);
  ASSERT_EQ(4000U, b->num_antiquation_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

// precise_gc: when byte_count is 0 in every referencing Dependence (legacy
// manifests / MapSst / remote-compaction paths), VersionBuilder must fall
// back to the averaged estimate (entry_count * file_size / num_entries)
// so that num_antiquation_bytes is still a meaningful approximation.
TEST_F(VersionBuilderTest, PreciseGcByteCountFallbackWhenZero) {
  // Blob B: 100 entries, 10000 bytes. Average entry size = 100 bytes.
  // S references 70 entries with byte_count=0 -> fallback estimate:
  //   bytes_depended ~= 70 * 10000 / 100 = 7000
  //   num_antiquation_bytes ~= 10000 - 7000 = 3000
  Add(-1, 101U, "100", "199", 10000U, 0, 100, 100, 100, 0, 100, 100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 201U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200, 200,
      false, GetPropCacheWithBytes(0, {std::make_tuple(101U, 70U, 0U)}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  auto& dep_map = new_vstorage.dependence_map();
  auto it = dep_map.find(101U);
  ASSERT_TRUE(it != dep_map.end());
  FileMetaData* b = it->second;
  ASSERT_EQ(30U, b->num_antiquation);
  ASSERT_EQ(3000U, b->num_antiquation_bytes);

  UnrefFilesInVersion(&new_vstorage);
}
// a single SST referencing a single blob with a non-empty
// block bitmap must produce a BlobLiveBlockInfo whose
// live_block_bitmap exactly matches the SST's per-row bitmap, with
// bitmap_available == true and live_block_count equal to the number
// of set bits.
TEST_F(VersionBuilderTest, VersionBuilderAggregatesLiveBlockBitmap) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1000U;

  // Blob B at hidden level -1 with file_size = 4 block units worth of bytes.
  Add(-1, kBlobFn, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // SST S references blob B and reports blocks {0, 2} live.
  version_edit.AddFile(
      2, 2000U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(0, {{kBlobFn, std::vector<uint64_t>{0, 2}}}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  new_vstorage.AggregateBlobLiveBlockBitmaps();

  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  ASSERT_TRUE(info->bitmap_available);
  EXPECT_TRUE(info->live_block_bitmap.Test(0));
  EXPECT_FALSE(info->live_block_bitmap.Test(1));
  EXPECT_TRUE(info->live_block_bitmap.Test(2));
  EXPECT_EQ(2U, info->live_block_count);
  // total_block_count derives from the observed bitmap width (num_bits):
  // highest set block id is 2 -> 3 tracked blocks.
  EXPECT_EQ(3U, info->total_block_count);

  UnrefFilesInVersion(&new_vstorage);
}

// When a row's layout_id matches the blob's current physical file
// number, the aggregated view is available. This is the positive
// counterpart to the layout-mismatch test below; it pins down that a
// freshly stamped (layout_id == fd.GetNumber()) row is trusted.
TEST_F(VersionBuilderTest, VersionBuilderTrustsMatchingLayoutId) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1050U;

  Add(-1, kBlobFn, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // SST references blob with blocks {0, 2}, layout_id stamped to the
  // blob's own (current) physical file number.
  version_edit.AddFile(
      2, 2050U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmapsLayoutId(
          0, kBlobFn, {{kBlobFn, std::vector<uint64_t>{0, 2}}}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  new_vstorage.AggregateBlobLiveBlockBitmaps();

  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  EXPECT_TRUE(info->bitmap_available);
  EXPECT_TRUE(info->live_block_bitmap.Test(0));
  EXPECT_TRUE(info->live_block_bitmap.Test(2));
  EXPECT_EQ(2U, info->live_block_count);

  UnrefFilesInVersion(&new_vstorage);
}

// When a row's layout_id does NOT match the blob's current physical
// file number (e.g. a stale value-index that survived a vSST GC
// rewrite), the aggregator must reject the row and the blob's
// aggregated view must be unavailable. This is the core safety
// guarantee that prevents stale block ids from being misused after a
// layout change.
TEST_F(VersionBuilderTest, VersionBuilderRejectsStaleLayoutId) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1060U;
  constexpr uint64_t kStaleLayoutId = 99999U;

  Add(-1, kBlobFn, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // SST references blob with blocks {0, 2} but stamped with a layout_id
  // that does not match the blob's current physical file number.
  version_edit.AddFile(
      2, 2060U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmapsLayoutId(
          0, kStaleLayoutId, {{kBlobFn, std::vector<uint64_t>{0, 2}}}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  new_vstorage.AggregateBlobLiveBlockBitmaps();

  using L = VersionStorageInfo::BlobBlockLiveness;
  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  EXPECT_FALSE(info->bitmap_available)
      << "a row with a stale layout_id must not be trusted";
  EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobFn, 0));
  EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobFn, 2));
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn));

  UnrefFilesInVersion(&new_vstorage);
}

// A single legacy/unavailable row must make the blob sticky-unavailable
// even when another SST in the same version contributes a perfectly
// valid layout-matched bitmap for the same blob. Availability is
// monotone: once cleared it can never be resurrected by a later OR.
TEST_F(VersionBuilderTest, VersionBuilderStickyUnavailableAcrossSsts) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1070U;

  Add(-1, kBlobFn, "100", "199", 8 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // SST S1 contributes a valid layout-matched bitmap {0, 3}.
  version_edit.AddFile(
      1, 2070U, 0, 500U, GetInternalKey("100"), GetInternalKey("149"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(0, {{kBlobFn, std::vector<uint64_t>{0, 3}}}));
  // SST S2 is legacy for the same blob -> must sticky-clear the blob.
  version_edit.AddFile(2, 2071U, 0, 500U, GetInternalKey("150"),
                       GetInternalKey("199"), 201, 201, false,
                       GetPropCacheBlockBitmapLegacy(0, {kBlobFn}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  new_vstorage.AggregateBlobLiveBlockBitmaps();

  using L = VersionStorageInfo::BlobBlockLiveness;
  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  EXPECT_FALSE(info->bitmap_available)
      << "a single legacy row must make the whole blob unavailable";
  EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobFn, 0));
  EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobFn, 3));
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn));

  UnrefFilesInVersion(&new_vstorage);
}

// when multiple SSTs in the same version reference the same
// blob with different block bitmaps, the aggregated live bitmap must
// be the union (OR) of all per-SST bitmaps.
TEST_F(VersionBuilderTest, VersionBuilderOrsBitmapsFromMultipleSsts) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1100U;

  Add(-1, kBlobFn, "100", "199", 8 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // SST S1 (level 1) references blob with blocks {0, 3}.
  version_edit.AddFile(
      1, 2100U, 0, 500U, GetInternalKey("100"), GetInternalKey("149"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(0, {{kBlobFn, std::vector<uint64_t>{0, 3}}}));
  // SST S2 (level 2) references blob with blocks {3, 5, 7} (overlap on 3).
  version_edit.AddFile(
      2, 2101U, 0, 500U, GetInternalKey("150"), GetInternalKey("199"), 201,
      201, false,
      GetPropCacheWithBlockBitmaps(
          0, {{kBlobFn, std::vector<uint64_t>{3, 5, 7}}}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  new_vstorage.AggregateBlobLiveBlockBitmaps();

  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  ASSERT_TRUE(info->bitmap_available);
  // Expected union: {0, 3, 5, 7}.
  for (uint64_t bid : {0U, 3U, 5U, 7U}) {
    EXPECT_TRUE(info->live_block_bitmap.Test(bid))
        << "expected block " << bid << " to be live";
  }
  for (uint64_t bid : {1U, 2U, 4U, 6U}) {
    EXPECT_FALSE(info->live_block_bitmap.Test(bid))
        << "expected block " << bid << " to be dead";
  }
  EXPECT_EQ(4U, info->live_block_count);
  // num_bits = highest set block (7) + 1 = 8.
  EXPECT_EQ(8U, info->total_block_count);

  UnrefFilesInVersion(&new_vstorage);
}

// dead_block_ratio / live_block_bytes / dead_block_bytes must be
// derived from the blob file size split proportionally by the
// live/total block ratio (total comes from the observed bitmap width).
TEST_F(VersionBuilderTest, VersionBuilderComputesDeadBlockRatio) {
  constexpr uint64_t kBlobFn = 1200U;
  // Blob file size = 3500 bytes. Live blocks {0, 2} -> num_bits = 3.
  // live_bytes = 3500 * 2 / 3 = 2333; dead_bytes = 3500 - 2333 = 1167.
  constexpr uint64_t kBlobFileSize = 3500;

  Add(-1, kBlobFn, "100", "199", kBlobFileSize, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 2200U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(0, {{kBlobFn, std::vector<uint64_t>{0, 2}}}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  new_vstorage.AggregateBlobLiveBlockBitmaps();

  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  ASSERT_TRUE(info->bitmap_available);
  EXPECT_EQ(2U, info->live_block_count);
  EXPECT_EQ(3U, info->total_block_count);
  const uint64_t expect_live_bytes = kBlobFileSize * 2 / 3;  // 2333
  const uint64_t expect_dead_bytes = kBlobFileSize - expect_live_bytes;
  EXPECT_EQ(expect_live_bytes, info->live_block_bytes);
  EXPECT_EQ(expect_dead_bytes, info->dead_block_bytes);
  EXPECT_NEAR(static_cast<double>(expect_dead_bytes) / kBlobFileSize,
              info->dead_block_ratio, 1e-9);

  UnrefFilesInVersion(&new_vstorage);
}

// if ANY referencing SST is in legacy regime (no per-row
// bitmap vector at all), the blob's bitmap_available flag must be
// sticky-cleared even if other SSTs supplied valid bitmaps. The
// derived stats must then be zeroed out so callers cannot mistakenly
// use a partial bitmap.
TEST_F(VersionBuilderTest, VersionBuilderFallbackWhenAnyReferenceIsLegacy) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1300U;

  Add(-1, kBlobFn, "100", "199", 8 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // SST S1 supplies a good block bitmap on blocks {0, 1}.
  version_edit.AddFile(
      1, 2300U, 0, 500U, GetInternalKey("100"), GetInternalKey("149"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(0, {{kBlobFn, std::vector<uint64_t>{0, 1}}}));
  // SST S2 references the same blob but in the legacy regime
  // (dependence_block_bitmaps omitted).
  version_edit.AddFile(
      2, 2301U, 0, 500U, GetInternalKey("150"), GetInternalKey("199"), 201,
      201, false, GetPropCacheBlockBitmapLegacy(0, {kBlobFn}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  new_vstorage.AggregateBlobLiveBlockBitmaps();

  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  EXPECT_FALSE(info->bitmap_available);
  EXPECT_EQ(0U, info->live_block_count);
  EXPECT_EQ(0U, info->total_block_count);
  EXPECT_EQ(0U, info->live_block_bytes);
  EXPECT_EQ(0U, info->dead_block_bytes);
  EXPECT_DOUBLE_EQ(0.0, info->dead_block_ratio);
  // Sticky-clear implies the bitmap is empty (no partial OR survives).
  EXPECT_TRUE(info->live_block_bitmap.empty());

  UnrefFilesInVersion(&new_vstorage);
}

// after a recovery-style rebuild of VersionStorageInfo (which
// is what happens when VersionSet replays the manifest into a brand
// new VersionStorageInfo), AggregateBlobLiveBlockBitmaps() must
// reproduce the same aggregated state as during a fresh apply, and
// the per-row "bitmap unavailable" sentinel must continue to mark the
// affected blob as legacy without poisoning unrelated blobs.
TEST_F(VersionBuilderTest, VersionBuilderRecoveryPathRestoresAggregatedState) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobA = 1400U;
  constexpr uint64_t kBlobB = 1401U;

  // Two blobs at hidden level -1.
  Add(-1, kBlobA, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100, 100);
  Add(-1, kBlobB, "200", "299", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100, 100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // SST references both blobs. For blob A it supplies blocks {1}; for blob
  // B it supplies the empty bitmap sentinel -> blob B becomes unavailable
  // while blob A stays available.
  version_edit.AddFile(
      2, 2400U, 0, 500U, GetInternalKey("100"), GetInternalKey("299"), 200,
      200, false,
      GetPropCacheBlockBitmapWithUnavailableRow(
          0,
          {{kBlobA, std::vector<uint64_t>{1}}, {kBlobB, std::vector<uint64_t>{}}},
          kBlobB));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  new_vstorage.AggregateBlobLiveBlockBitmaps();

  // First aggregation: blob A available with block 1 live; blob B legacy.
  {
    const auto* info_a = new_vstorage.GetBlobLiveBlockInfo(kBlobA);
    ASSERT_NE(nullptr, info_a);
    EXPECT_TRUE(info_a->bitmap_available);
    EXPECT_TRUE(info_a->live_block_bitmap.Test(1));
    EXPECT_EQ(1U, info_a->live_block_count);

    const auto* info_b = new_vstorage.GetBlobLiveBlockInfo(kBlobB);
    ASSERT_NE(nullptr, info_b);
    EXPECT_FALSE(info_b->bitmap_available);
    EXPECT_EQ(0U, info_b->live_block_count);
  }

  // Recovery-style replay: invoke aggregation a second time. It must
  // be idempotent (wipe + recompute) and converge to identical state.
  new_vstorage.AggregateBlobLiveBlockBitmaps();
  {
    const auto* info_a = new_vstorage.GetBlobLiveBlockInfo(kBlobA);
    ASSERT_NE(nullptr, info_a);
    EXPECT_TRUE(info_a->bitmap_available);
    EXPECT_TRUE(info_a->live_block_bitmap.Test(1));
    EXPECT_EQ(1U, info_a->live_block_count);

    const auto* info_b = new_vstorage.GetBlobLiveBlockInfo(kBlobB);
    ASSERT_NE(nullptr, info_b);
    EXPECT_FALSE(info_b->bitmap_available);
    EXPECT_EQ(0U, info_b->live_block_count);
  }

  // Feature disabled (the CF toggle is off): Version::PrepareApply calls
  // ClearBlobLiveBlockInfo() so GC consumers consistently observe "bitmap
  // unavailable" instead of a stale view. After clearing, no blob has an
  // aggregated entry at all.
  new_vstorage.ClearBlobLiveBlockInfo();
  {
    EXPECT_EQ(nullptr, new_vstorage.GetBlobLiveBlockInfo(kBlobA));
    EXPECT_EQ(nullptr, new_vstorage.GetBlobLiveBlockInfo(kBlobB));
  }

  UnrefFilesInVersion(&new_vstorage);
}

// The production GC loop in ProcessGarbageCollection() uses the
// aggregated live-block view to decide whether the per-record
// GetKey() point lookup can be skipped. The decision logic is
// exposed on VersionStorageInfo as two APIs:
//   - IsBlockLive(blob_fn, block_id): three-state (kLive / kDead /
//     kUnknown).
//   - IsBlobEntirelyDead(blob_fn): blob-level gate used by the GC
//     loop to short-circuit every record in a blob that has zero
//     live blocks under the aggregated view.
// These UTs validate the decision contract so downstream GC code
// can rely on it.

// when the aggregated bitmap is available and the queried
// block has at least one SST reference, IsBlockLive must return kLive;
// IsBlobEntirelyDead must be false because the blob has live blocks.
TEST_F(VersionBuilderTest, GcUsesBitmapFastPathWhenAvailable) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1500U;

  Add(-1, kBlobFn, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 2500U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(0, {{kBlobFn, std::vector<uint64_t>{1, 3}}}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);
  new_vstorage.AggregateBlobLiveBlockBitmaps();

  using L = VersionStorageInfo::BlobBlockLiveness;
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobFn, 1));
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobFn, 3));
  EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobFn, 0));
  EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobFn, 2));
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn));

  UnrefFilesInVersion(&new_vstorage);
}

// when the aggregated bitmap is sticky-cleared because some
// referencing SST is in legacy regime, IsBlockLive must return
// kUnknown for every block (signalling "fall back to GetKey()") and
// IsBlobEntirelyDead must return false (GC must not skip records).
TEST_F(VersionBuilderTest, GcFallsBackToLookupWhenBitmapUnavailable) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1501U;

  Add(-1, kBlobFn, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // Legacy regime: dependence_block_bitmaps vector omitted.
  version_edit.AddFile(2, 2501U, 0, 500U, GetInternalKey("100"),
                       GetInternalKey("199"), 200, 200, false,
                       GetPropCacheBlockBitmapLegacy(0, {kBlobFn}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);
  new_vstorage.AggregateBlobLiveBlockBitmaps();

  using L = VersionStorageInfo::BlobBlockLiveness;
  for (uint64_t bid : {0U, 1U, 2U, 3U}) {
    EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobFn, bid))
        << "legacy blob must report kUnknown for block " << bid;
  }
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn))
      << "IsBlobEntirelyDead must be false when bitmap is unavailable, so GC "
         "falls back to GetKey() instead of silently dropping records";

  // And for an entirely unknown blob (never aggregated / not present
  // in the version), IsBlockLive must also be kUnknown.
  EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(999999U, 0));
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(999999U));

  UnrefFilesInVersion(&new_vstorage);
}

// GC must be able to skip dead blocks wholesale. This test
// locks the decision contract: for a blob whose aggregated bitmap is
// available, every block id that was never OR-ed in by any
// referencing SST must be reported as kDead (i.e. the GC loop can
// skip records belonging to that block without a GetKey() lookup),
// while any block id that appears in the aggregated bitmap must be
// reported as kLive (GC must still validate those). A second blob in
// the same version whose blocks are all live must not be affected.
TEST_F(VersionBuilderTest, GcSkipsDeadBlocks) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1502U;
  constexpr uint64_t kOtherBlob = 1503U;

  Add(-1, kBlobFn, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  Add(-1, kOtherBlob, "200", "299", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // S1 references kBlobFn and marks only block {0} live. Blocks
  // {1,2,3} become dead -> GC may skip them wholesale.
  version_edit.AddFile(
      2, 2502U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(
          0, {{kBlobFn, std::vector<uint64_t>{0}}}));
  // S2 references kOtherBlob and marks every block live so it must
  // NOT be collaterally treated as dead by the fast path.
  version_edit.AddFile(
      2, 2503U, 0, 500U, GetInternalKey("200"), GetInternalKey("299"), 201,
      201, false,
      GetPropCacheWithBlockBitmaps(
          0, {{kOtherBlob, std::vector<uint64_t>{0, 1, 2, 3}}}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);
  new_vstorage.AggregateBlobLiveBlockBitmaps();

  using L = VersionStorageInfo::BlobBlockLiveness;
  // kBlobFn: block 0 is live, blocks 1..3 must be reported kDead so
  // GC can skip them without touching the LSM.
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobFn, 0));
  for (uint64_t bid : {1U, 2U, 3U}) {
    EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobFn, bid))
        << "dead block must be skippable, bid=" << bid;
  }
  // At least one block is live -> not entirely dead.
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn));

  // kOtherBlob: all blocks live, none must be reported kDead.
  for (uint64_t bid : {0U, 1U, 2U, 3U}) {
    EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kOtherBlob, bid))
        << "live block must not be mis-flagged dead, bid=" << bid;
  }
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kOtherBlob));

  UnrefFilesInVersion(&new_vstorage);
}

// when a blob mixes live and dead blocks, IsBlobEntirelyDead
// must be false (GC must not skip the blob wholesale) while
// IsBlockLive must return kLive for live blocks and kDead for dead
// ones. This is the common in-production case.
TEST_F(VersionBuilderTest, GcPreservesCorrectnessForLiveBlocks) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1504U;

  Add(-1, kBlobFn, "100", "199", 8 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 2504U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(
          0, {{kBlobFn, std::vector<uint64_t>{2, 5}}}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);
  new_vstorage.AggregateBlobLiveBlockBitmaps();

  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn));
  using L = VersionStorageInfo::BlobBlockLiveness;
  // Live blocks.
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobFn, 2));
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobFn, 5));
  // Dead blocks (GC could drop records in these without a lookup).
  // num_bits = highest set block (5) + 1 = 6, so blocks 0,1,3,4 are
  // tracked-dead; blocks >= 6 are beyond the bitmap width but a query
  // still resolves to kDead because the bitmap is available.
  for (uint64_t bid : {0U, 1U, 3U, 4U, 6U, 7U}) {
    EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobFn, bid))
        << "bid=" << bid << " must be kDead";
  }

  UnrefFilesInVersion(&new_vstorage);
}

// in a version where one blob has a valid aggregated bitmap
// and another is in legacy/fallback regime, the two must be strictly
// isolated: the bitmap-aware blob keeps kLive/kDead answers while
// the legacy blob yields kUnknown and is NOT entirely dead.
TEST_F(VersionBuilderTest, GcMixedLegacyAndBitmapBlobUsesSafeFallback) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobBitmap = 1600U;
  constexpr uint64_t kBlobLegacy = 1601U;

  Add(-1, kBlobBitmap, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0,
      100, 100);
  Add(-1, kBlobLegacy, "200", "299", 4 * kBlockUnit, 0, 100, 100, 100, 0,
      100, 100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // SST S1 -> bitmap-aware reference to kBlobBitmap blocks {0, 2}.
  version_edit.AddFile(
      1, 2600U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(
          0, {{kBlobBitmap, std::vector<uint64_t>{0, 2}}}));
  // SST S2 -> legacy reference to kBlobLegacy.
  version_edit.AddFile(2, 2601U, 0, 500U, GetInternalKey("200"),
                       GetInternalKey("299"), 201, 201, false,
                       GetPropCacheBlockBitmapLegacy(0, {kBlobLegacy}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);
  new_vstorage.AggregateBlobLiveBlockBitmaps();

  using L = VersionStorageInfo::BlobBlockLiveness;
  // Bitmap-aware blob: crisp kLive / kDead answers, NOT entirely dead.
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobBitmap));
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobBitmap, 0));
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobBitmap, 2));
  EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobBitmap, 1));
  EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobBitmap, 3));

  // Legacy blob: every block kUnknown, NOT entirely dead, GC must
  // therefore invoke the legacy GetKey() path for its records.
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobLegacy));
  for (uint64_t bid : {0U, 1U, 2U, 3U}) {
    EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobLegacy, bid))
        << "legacy blob must report kUnknown for block " << bid;
  }

  UnrefFilesInVersion(&new_vstorage);
}

// -----------------------------------------------------------------
// recovery & backward-compatibility validation.
//
// The per-SST `dependence_block_bitmaps` survive a full VersionEdit
// encode/decode round-trip through the manifest. The in-version
// aggregation step (AggregateBlobLiveBlockBitmaps) is idempotent so
// it can be re-run at recovery time without drift. IsBlockLive /
// IsBlobEntirelyDead are the GC decision contract.
//
// These UTs lock the end-to-end recovery + compatibility story:
//   1. OpenCloseRebuildBitmapState — write, simulate close/reopen
//      by round-tripping the VersionEdit through manifest bytes,
//      and prove the rebuilt VersionStorageInfo carries the same
//      bitmap-backed liveness answers as the pre-close one.
//   2. OpenLegacyManifestFallback — simulate opening a DB against
//      a manifest produced by pre-bitmap code (no block bitmaps
//      at all) and prove the referenced blob cleanly falls back
//      to kUnknown without crashing or corrupting neighbours.
//   3. MixedOldAndNewSstCompatibility — mixed legacy + bitmap-aware
//      SSTs in the same rebuilt version must still produce strictly
//      isolated liveness answers per blob.
//   4. FeatureDisabledRestoresOldGcBehavior — clearing the
//      aggregated state (feature turned off) must drop every
//      aggregated bitmap even if the on-disk manifest still
//      carries them, so GC degrades to the pre-feature behavior.
// -----------------------------------------------------------------

// Test 1: write -> close/reopen -> bitmap still usable.
// We simulate the close/reopen by running a full VersionEdit
// Encode -> Decode round-trip (what VersionSet::Recover does when
// replaying MANIFEST records) before feeding the edit into the
// VersionBuilder. The post-recovery aggregated view must be
// byte-for-byte equivalent to the pre-close view.
TEST_F(VersionBuilderTest, OpenCloseRebuildBitmapState) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1700U;

  Add(-1, kBlobFn, "100", "199", 8 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 2700U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(
          0, {{kBlobFn, std::vector<uint64_t>{1, 4, 7}}}));

  // Simulate the on-disk manifest round-trip: encode the edit to
  // bytes, then decode it into a fresh VersionEdit as VersionSet
  // would during Recover().
  std::string manifest_bytes;
  ASSERT_TRUE(version_edit.EncodeTo(&manifest_bytes));
  VersionEdit recovered_edit;
  ASSERT_OK(recovered_edit.DecodeFrom(manifest_bytes));

  // The recovered edit must carry the exact same block bitmap.
  ASSERT_EQ(1U, recovered_edit.GetNewFiles().size());
  const auto& recovered_prop =
      recovered_edit.GetNewFiles()[0].second.prop;
  ASSERT_EQ(1U, recovered_prop.dependence.size());
  ASSERT_EQ(1U, recovered_prop.dependence_block_bitmaps.size());
  EXPECT_TRUE(recovered_prop.dependence_block_bitmaps[0].available);
  EXPECT_EQ(kBlobFn, recovered_prop.dependence_block_bitmaps[0].layout_id);
  EXPECT_EQ(3U,
            recovered_prop.dependence_block_bitmaps[0].bitmap.CountSetBits());
  EXPECT_TRUE(recovered_prop.dependence_block_bitmaps[0].bitmap.Test(1));
  EXPECT_TRUE(recovered_prop.dependence_block_bitmaps[0].bitmap.Test(4));
  EXPECT_TRUE(recovered_prop.dependence_block_bitmaps[0].bitmap.Test(7));

  // Replay the recovered edit into a brand-new VersionStorageInfo
  // (this is what VersionSet::Recover does).
  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&recovered_edit);
  version_builder.SaveTo(&new_vstorage, 0);
  new_vstorage.AggregateBlobLiveBlockBitmaps();

  // Post-recovery liveness answers must match the pre-close contract.
  using L = VersionStorageInfo::BlobBlockLiveness;
  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  EXPECT_TRUE(info->bitmap_available);
  EXPECT_EQ(3U, info->live_block_count);
  // num_bits = highest set block (7) + 1 = 8 tracked blocks.
  EXPECT_EQ(8U, info->total_block_count);
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobFn, 1));
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobFn, 4));
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobFn, 7));
  for (uint64_t bid : {0U, 2U, 3U, 5U, 6U}) {
    EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobFn, bid))
        << "post-recovery dead block must remain dead, bid=" << bid;
  }
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn));

  // Re-run the aggregation (idempotence guarantee). The result must
  // be byte-for-byte identical so a subsequent Recover() call from a
  // crash mid-replay would converge to the same state.
  new_vstorage.AggregateBlobLiveBlockBitmaps();
  const auto* info2 = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info2);
  EXPECT_TRUE(info2->bitmap_available);
  EXPECT_EQ(info->live_block_count, info2->live_block_count);
  EXPECT_EQ(info->total_block_count, info2->total_block_count);
  EXPECT_EQ(info->live_block_bytes, info2->live_block_bytes);
  EXPECT_EQ(info->dead_block_bytes, info2->dead_block_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

// Test 2: opening a DB whose manifest was produced by
// pre-bitmap code (no block bitmap payload at all) must cleanly
// auto-fallback: the recovered VersionEdit decodes, the referenced
// blob ends up with bitmap_available=false, and every IsBlockLive
// query returns kUnknown so GC reverts to the legacy lookup path.
TEST_F(VersionBuilderTest, OpenLegacyManifestFallback) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1701U;

  Add(-1, kBlobFn, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // Legacy regime: dependence_block_bitmaps is omitted, mimicking
  // what a pre-bitmap manifest on disk would decode into.
  version_edit.AddFile(2, 2701U, 0, 500U, GetInternalKey("100"),
                       GetInternalKey("199"), 200, 200, false,
                       GetPropCacheBlockBitmapLegacy(0, {kBlobFn}));

  std::string manifest_bytes;
  ASSERT_TRUE(version_edit.EncodeTo(&manifest_bytes));
  VersionEdit recovered_edit;
  ASSERT_OK(recovered_edit.DecodeFrom(manifest_bytes));

  // A legacy manifest must decode with an empty bitmap vector; this
  // is the sentinel the aggregation reads as "bitmap unavailable".
  ASSERT_EQ(1U, recovered_edit.GetNewFiles().size());
  const auto& recovered_prop =
      recovered_edit.GetNewFiles()[0].second.prop;
  ASSERT_EQ(1U, recovered_prop.dependence.size());
  EXPECT_TRUE(recovered_prop.dependence_block_bitmaps.empty());

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&recovered_edit);
  version_builder.SaveTo(&new_vstorage, 0);
  new_vstorage.AggregateBlobLiveBlockBitmaps();

  const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  EXPECT_FALSE(info->bitmap_available);
  EXPECT_EQ(0U, info->live_block_count);
  EXPECT_EQ(0U, info->total_block_count);
  EXPECT_TRUE(info->live_block_bitmap.empty());

  using L = VersionStorageInfo::BlobBlockLiveness;
  for (uint64_t bid : {0U, 1U, 2U, 3U}) {
    EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobFn, bid))
        << "legacy-manifest blob must report kUnknown for block " << bid;
  }
  // IsBlobEntirelyDead must be false so GC does NOT silently drop
  // records belonging to a legacy blob.
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn));

  UnrefFilesInVersion(&new_vstorage);
}

// Test 3: a recovered version in which one SST is legacy
// (pre-bitmap) and another SST is bitmap-aware (post-bitmap),
// referencing distinct blobs, must preserve strict isolation across
// the two blobs. The bitmap-aware blob keeps kLive/kDead answers;
// the legacy blob cleanly falls back to kUnknown. This mirrors the
// real-world rollout case where a previously opened DB has some
// SSTs flushed before the upgrade and some after.
TEST_F(VersionBuilderTest, MixedOldAndNewSstCompatibility) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobNew = 1702U;
  constexpr uint64_t kBlobOld = 1703U;

  Add(-1, kBlobNew, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  Add(-1, kBlobOld, "200", "299", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  // New SST (post-upgrade flush/compaction): bitmap-aware.
  version_edit.AddFile(
      2, 2702U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(
          0, {{kBlobNew, std::vector<uint64_t>{0, 2}}}));
  // Old SST (pre-upgrade, still on disk): legacy regime.
  version_edit.AddFile(2, 2703U, 0, 500U, GetInternalKey("200"),
                       GetInternalKey("299"), 201, 201, false,
                       GetPropCacheBlockBitmapLegacy(0, {kBlobOld}));

  // Manifest round-trip both edits together: this is what VersionSet
  // sees when replaying a manifest written partially by old code
  // and partially by new code.
  std::string manifest_bytes;
  ASSERT_TRUE(version_edit.EncodeTo(&manifest_bytes));
  VersionEdit recovered_edit;
  ASSERT_OK(recovered_edit.DecodeFrom(manifest_bytes));
  ASSERT_EQ(2U, recovered_edit.GetNewFiles().size());

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&recovered_edit);
  version_builder.SaveTo(&new_vstorage, 0);
  new_vstorage.AggregateBlobLiveBlockBitmaps();

  using L = VersionStorageInfo::BlobBlockLiveness;

  // New blob: aggregated bitmap intact, precise answers.
  const auto* info_new = new_vstorage.GetBlobLiveBlockInfo(kBlobNew);
  ASSERT_NE(nullptr, info_new);
  EXPECT_TRUE(info_new->bitmap_available);
  EXPECT_EQ(2U, info_new->live_block_count);
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobNew, 0));
  EXPECT_EQ(L::kLive, new_vstorage.IsBlockLive(kBlobNew, 2));
  EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobNew, 1));
  EXPECT_EQ(L::kDead, new_vstorage.IsBlockLive(kBlobNew, 3));
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobNew));

  // Old blob: sticky-cleared, legacy fallback.
  const auto* info_old = new_vstorage.GetBlobLiveBlockInfo(kBlobOld);
  ASSERT_NE(nullptr, info_old);
  EXPECT_FALSE(info_old->bitmap_available);
  for (uint64_t bid : {0U, 1U, 2U, 3U}) {
    EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobOld, bid))
        << "old/legacy blob must report kUnknown for block " << bid;
  }
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobOld));

  // Neither blob may leak into the other: a block live on kBlobNew
  // must not be reflected on kBlobOld, and vice-versa.
  EXPECT_NE(L::kLive, new_vstorage.IsBlockLive(kBlobOld, 0));
  EXPECT_NE(L::kUnknown, new_vstorage.IsBlockLive(kBlobNew, 0));

  UnrefFilesInVersion(&new_vstorage);
}

// Test 4: when the feature is turned off (Version::PrepareApply
// calls ClearBlobLiveBlockInfo() because the CF toggle is off), the
// recovered version must behave exactly like pre-feature code: no
// blob has an aggregated entry, every IsBlockLive answer is
// kUnknown, and GC therefore falls back to GetKey() on every record.
// Crucially, this must hold even if the on-disk manifest still
// carries per-SST block bitmaps from a previous run when the feature
// WAS enabled -- i.e. clearing alone is a hard kill switch.
TEST_F(VersionBuilderTest, FeatureDisabledRestoresOldGcBehavior) {
  constexpr uint64_t kBlockUnit = 4096;
  constexpr uint64_t kBlobFn = 1704U;

  Add(-1, kBlobFn, "100", "199", 4 * kBlockUnit, 0, 100, 100, 100, 0, 100,
      100);
  UpdateVersionStorageInfo();

  // The on-disk manifest still has a full block bitmap for this blob.
  VersionEdit version_edit;
  version_edit.AddFile(
      2, 2704U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200,
      200, false,
      GetPropCacheWithBlockBitmaps(
          0, {{kBlobFn, std::vector<uint64_t>{0, 1, 2}}}));

  std::string manifest_bytes;
  ASSERT_TRUE(version_edit.EncodeTo(&manifest_bytes));
  VersionEdit recovered_edit;
  ASSERT_OK(recovered_edit.DecodeFrom(manifest_bytes));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&recovered_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  // First aggregate with the feature enabled to prove the persisted
  // bitmap is intact and would otherwise drive the fast path.
  new_vstorage.AggregateBlobLiveBlockBitmaps();
  {
    const auto* info = new_vstorage.GetBlobLiveBlockInfo(kBlobFn);
    ASSERT_NE(nullptr, info);
    EXPECT_TRUE(info->bitmap_available);
    EXPECT_EQ(3U, info->live_block_count);
  }

  // Now simulate the user turning the feature off at runtime (the
  // hard kill switch). ClearBlobLiveBlockInfo() must drop every
  // aggregated entry regardless of what the manifest carries.
  new_vstorage.ClearBlobLiveBlockInfo();

  // After clearing, no blob has an aggregated entry at all.
  EXPECT_EQ(nullptr, new_vstorage.GetBlobLiveBlockInfo(kBlobFn));

  using L = VersionStorageInfo::BlobBlockLiveness;
  // Every block query must report kUnknown -> GC degrades to GetKey().
  for (uint64_t bid : {0U, 1U, 2U, 3U}) {
    EXPECT_EQ(L::kUnknown, new_vstorage.IsBlockLive(kBlobFn, bid))
        << "feature-disabled mode must report kUnknown for block " << bid;
  }
  // Crucially NOT entirely-dead: GC must still walk the blob's
  // records through the legacy path rather than dropping them.
  EXPECT_FALSE(new_vstorage.IsBlobEntirelyDead(kBlobFn));

  UnrefFilesInVersion(&new_vstorage);
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
