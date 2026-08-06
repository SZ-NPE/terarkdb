//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <inttypes.h>

#include <string>

#include "db/version_edit.h"
#include "db/version_set.h"
#include "rocksdb/terark_namespace.h"
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

TEST_F(VersionBuilderTest, ApplyHiddenDeleteAndSaveTo) {
  Add(-1, 700U, "100", "199", 100U, 0, 100U, 100U, 10U);
  Add(1, 800U, "100", "199", 100U, 0, 100U, 100U, 10U, 0, 0, 0,
      GetPropCache(0, {700U}));
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.DeleteFile(-1, 700U);
  version_edit.AddFile(-1, 701U, 0, 100U, GetInternalKey("200"),
                       GetInternalKey("299"), 100U, 100U, false,
                       GetPropCache(0, {}, {700U}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  auto old_alias = new_vstorage.dependence_map().find(700U);
  ASSERT_NE(old_alias, new_vstorage.dependence_map().end());
  ASSERT_EQ(701U, old_alias->second->fd.GetNumber());
  ASSERT_EQ(new_vstorage.dependence_map().count(701U), 1U);
  ASSERT_EQ(1U, new_vstorage.LevelFiles(-1).size());
  ASSERT_TRUE(VerifyDependFiles(&new_vstorage, {700U, 701U, 800U}));

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
  //   num_entries=100, raw_value_size=8000, file_size=10000.
  // Referencing SST S (file_number=200) at level 2 declares that it points
  // at 70 entries / 6000 bytes from B.
  // Expected after SaveTo:
  //   B.num_antiquation       = 100 - 70   = 30
  //   B.num_antiquation_bytes = 8000 - 6000 = 2000
  TablePropertyCache blob_prop;
  blob_prop.raw_value_size = 8000;
  Add(-1, 100U, "100", "199", 10000U /*file_size*/, 0 /*path_id*/,
      100 /*smallest_seq*/, 100 /*largest_seq*/, 100 /*num_entries*/,
      0 /*num_deletions*/, 100 /*smallest_seqno*/, 100 /*largest_seqno*/,
      blob_prop);
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
  ASSERT_EQ(2000U, b->num_antiquation_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

// precise_gc: when byte_count is 0 in every referencing Dependence (legacy
// manifests / MapSst / remote-compaction paths), VersionBuilder must fall
// back to the averaged estimate (entry_count * raw_value_size / num_entries)
// so that num_antiquation_bytes is still a meaningful approximation.
TEST_F(VersionBuilderTest, PreciseGcByteCountFallbackWhenZero) {
  // Blob B: 100 entries, 4000 raw value bytes and 10000 physical bytes.
  // Average logical value size = 40 bytes.
  // S references 70 entries with byte_count=0 -> fallback estimate:
  //   bytes_depended ~= 70 * 4000 / 100 = 2800
  //   num_antiquation_bytes ~= 4000 - 2800 = 1200
  TablePropertyCache blob_prop;
  blob_prop.raw_value_size = 4000;
  Add(-1, 101U, "100", "199", 10000U, 0, 100, 100, 100, 0, 100, 100,
      blob_prop);
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
  ASSERT_EQ(1200U, b->num_antiquation_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, PreciseGcAggregatesBytesAcrossOutputs) {
  TablePropertyCache blob_prop;
  blob_prop.raw_value_size = 8000;
  Add(-1, 104U, "100", "199", 10000U, 0, 100, 100, 100, 0, 100, 100,
      blob_prop);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 204U, 0, 500U, GetInternalKey("100"), GetInternalKey("149"), 200, 200,
      false, GetPropCacheWithBytes(0, {std::make_tuple(104U, 40U, 3000U)}));
  version_edit.AddFile(
      2, 205U, 0, 500U, GetInternalKey("150"), GetInternalKey("199"), 201, 201,
      false, GetPropCacheWithBytes(0, {std::make_tuple(104U, 60U, 5000U)}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  auto it = new_vstorage.dependence_map().find(104U);
  ASSERT_TRUE(it != new_vstorage.dependence_map().end());
  ASSERT_EQ(0U, it->second->num_antiquation_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, PreciseGcInvalidByteCountFallsBackToEstimate) {
  TablePropertyCache blob_prop;
  blob_prop.raw_value_size = 4000;
  Add(-1, 102U, "100", "199", 10000U, 0, 100, 100, 100, 0, 100, 100,
      blob_prop);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(
      2, 202U, 0, 500U, GetInternalKey("100"), GetInternalKey("199"), 200, 200,
      false, GetPropCacheWithBytes(0, {std::make_tuple(102U, 70U, 5000U)}));

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage, 0);

  auto it = new_vstorage.dependence_map().find(102U);
  ASSERT_TRUE(it != new_vstorage.dependence_map().end());
  ASSERT_EQ(1200U, it->second->num_antiquation_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, PreciseGcAllowsFullyDeadBlob) {
  TablePropertyCache blob_prop;
  blob_prop.raw_value_size = 4000;
  Add(-1, 103U, "100", "199", 10000U, 0, 100, 100, 100, 0, 100, 100,
      blob_prop);
  TablePropertyCache map_prop;
  map_prop.purpose = kMapSst;
  map_prop.dependence.emplace_back(Dependence{103U, 0U, 0U});
  Add(2, 203U, "100", "199", 500U, 0, 200, 200, 0, 0, 200, 200,
      map_prop);
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, false);
  version_builder.SaveTo(&new_vstorage, 0);

  auto it = new_vstorage.dependence_map().find(103U);
  ASSERT_TRUE(it != new_vstorage.dependence_map().end());
  ASSERT_EQ(4000U, it->second->num_antiquation_bytes);

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CanPurgeBlobFileRequiresNoReferenceCertificate) {
  TablePropertyCache blob_prop;
  blob_prop.flags |= TablePropertyCache::kCompleteDependence;
  Add(-1, 301U, "100", "199", 10000U, 0, 100, 100, 100, 0, 100, 100,
      blob_prop);
  FileMetaData* blob = vstorage_.LevelFiles(-1).back();

  ASSERT_TRUE(blob->Unref());
  ASSERT_FALSE(vstorage_.CanPurgeBlobFile(blob));
  blob->Ref();
  ASSERT_TRUE(vstorage_.CanPurgeBlobFile(blob));
  blob->Ref();
  ASSERT_TRUE(vstorage_.CanPurgeBlobFile(blob));
  blob->Ref();
  ASSERT_FALSE(vstorage_.CanPurgeBlobFile(blob));
  ASSERT_FALSE(blob->Unref());
  ASSERT_FALSE(blob->Unref());

  TablePropertyCache key_prop;
  key_prop.flags |= TablePropertyCache::kCompleteDependence;
  key_prop.dependence.emplace_back(Dependence{301U, 100U, 0});
  Add(1, 401U, "100", "199", 500U, 0, 100, 100, 100, 0, 100, 100,
      key_prop);
  ASSERT_FALSE(vstorage_.CanPurgeBlobFile(blob));
}

TEST_F(VersionBuilderTest, CanPurgeBlobFileRejectsIndirectReference) {
  TablePropertyCache blob_prop;
  blob_prop.flags |= TablePropertyCache::kCompleteDependence;
  Add(-1, 302U, "100", "199", 10000U, 0, 100, 100, 100, 0, 100, 100,
      blob_prop);
  FileMetaData* blob = vstorage_.LevelFiles(-1).back();

  TablePropertyCache map_prop;
  map_prop.purpose = kMapSst;
  map_prop.flags |= TablePropertyCache::kCompleteDependence;
  map_prop.dependence.emplace_back(Dependence{302U, 100U, 0});
  Add(1, 402U, "100", "199", 500U, 0, 100, 100, 1, 0, 100, 100,
      map_prop);

  TablePropertyCache key_prop;
  key_prop.flags |= TablePropertyCache::kCompleteDependence;
  key_prop.dependence.emplace_back(Dependence{402U, 1U, 0});
  Add(2, 403U, "100", "199", 500U, 0, 100, 100, 1, 0, 100, 100,
      key_prop);

  ASSERT_FALSE(vstorage_.CanPurgeBlobFile(blob));
}

TEST_F(VersionBuilderTest, CanPurgeBlobFileRejectsUnknownMetadataAndSnapshots) {
  Add(-1, 304U, "100", "199", 10000U, 0, 100, 100, 100, 0, 100, 100);
  FileMetaData* blob = vstorage_.LevelFiles(-1).back();

  TablePropertyCache incomplete_key_prop;
  incomplete_key_prop.dependence.emplace_back(Dependence{304U, 100U, 0});
  Add(1, 404U, "100", "199", 500U, 0, 100, 100, 100, 0, 100, 100,
      incomplete_key_prop);
  ASSERT_FALSE(vstorage_.CanPurgeBlobFile(blob));

  blob->prop.flags |= TablePropertyCache::kHasSnapshots;
  ASSERT_FALSE(vstorage_.CanPurgeBlobFile(blob));
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
