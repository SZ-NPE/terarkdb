//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Integration-style tests for the vSST data-block-level live bitmap
// pipeline. These stitch together every stage of the feature:
//
//   FlushBlockBitmapCollector
//     -> records which vSST data blocks each kSST references at flush
//        time (via the real BlockBasedTable data block ordinal).
//   TablePropertyCache::dependence_block_bitmaps
//     -> the bitmap travels inside the SST's table property cache.
//   VersionEdit manifest encode/decode
//     -> the bitmap survives a full manifest round-trip.
//   VersionStorageInfo::AggregateBlobLiveBlockBitmaps
//     -> aggregated per-blob live-block view, sticky-clear on legacy.
//   VersionStorageInfo::IsBlockLive / IsBlobEntirelyDead
//     -> fast-path decision contract used by ProcessGarbageCollection.
//
// These tests do not spin up a DB; they exercise the same aggregation
// logic that db/builder.cc, db/version_edit.cc, db/version_set.cc and
// db/compaction_job.cc drive, which keeps them fast and stable
// regardless of table-format details.

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "monitoring/statistics.h"
#include "rocksdb/advanced_options.h"
#include "rocksdb/comparator.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/types.h"
#include "util/blob_block_bitmap.h"
#include "util/coding.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class BlobValidityBitmapFlushTest : public testing::Test {
 protected:
  // Drive a flush-like sequence on a FlushBlockBitmapCollector and then
  // materialize the result into a TablePropertyCache. Mirrors exactly
  // what db/builder.cc does on the real flush path.
  void DriveFlush(
      const std::vector<std::pair<uint64_t, uint64_t>>& observed_blocks,
      const std::vector<Dependence>& dependence,
      FlushBlockBitmapCollector* collector, TablePropertyCache* prop) {
    for (auto& kv : observed_blocks) {
      // On the flush path the value is written into the blob file being
      // created, so the physical layout id equals the blob file number.
      collector->ObserveBlockId(kv.first /* blob_file_number */,
                                kv.first /* layout_id */,
                                kv.second /* data_block_id */);
    }
    prop->dependence = dependence;
    collector->Materialize(prop->dependence, &prop->dependence_block_bitmaps);
  }
};

// Test 1: Flush produces a correctly populated block bitmap when the
// feature is enabled and we actually observe separated values.
TEST_F(BlobValidityBitmapFlushTest, FlushBuildsBlockBitmapForSeparatedValues) {
  FlushBlockBitmapCollector collector(/*enabled=*/true);
  TablePropertyCache prop;

  // Two blob files. Into blob#42 we put 3 values landing in data blocks
  // 0, 1 and 2. Into blob#43 we put a single value landing in block 5.
  std::vector<std::pair<uint64_t, uint64_t>> observed = {
      {42, 0},
      {42, 1},
      {42, 2},
      {43, 5},
  };
  std::vector<Dependence> dep = {
      {42, /*entry_count=*/3, /*byte_count=*/0},
      {43, /*entry_count=*/1, /*byte_count=*/0},
  };

  DriveFlush(observed, dep, &collector, &prop);

  ASSERT_EQ(prop.dependence.size(), prop.dependence_block_bitmaps.size());
  ASSERT_EQ(size_t(2), prop.dependence_block_bitmaps.size());

  const BlobBlockBitmap& b42 = prop.dependence_block_bitmaps[0].bitmap;
  EXPECT_TRUE(prop.dependence_block_bitmaps[0].available);
  EXPECT_EQ(42U, prop.dependence_block_bitmaps[0].layout_id);
  EXPECT_EQ(uint64_t(3), b42.CountSetBits());
  EXPECT_TRUE(b42.Test(0));
  EXPECT_TRUE(b42.Test(1));
  EXPECT_TRUE(b42.Test(2));
  EXPECT_FALSE(b42.Test(3));

  const BlobBlockBitmap& b43 = prop.dependence_block_bitmaps[1].bitmap;
  EXPECT_TRUE(prop.dependence_block_bitmaps[1].available);
  EXPECT_EQ(43U, prop.dependence_block_bitmaps[1].layout_id);
  EXPECT_EQ(uint64_t(1), b43.CountSetBits());
  EXPECT_TRUE(b43.Test(5));
  EXPECT_FALSE(b43.Test(0));
  EXPECT_FALSE(b43.Test(4));
}

// Test 2: When flush produces no separated values (e.g. a purely
// in-place SST, or a map SST, or a non-KV-separation workload), the
// per-dependence bitmap vector must come out empty (or all-empty)
// rather than noisily reporting spurious live blocks.
TEST_F(BlobValidityBitmapFlushTest, FlushWithoutSeparationDoesNotEmitBitmap) {
  // (a) Feature on, but no ObserveBlockId() call. Dependence may still
  //     list blobs inherited from compaction context. Output is resized
  //     to dep.size() with each entry being an empty bitmap.
  {
    FlushBlockBitmapCollector collector(/*enabled=*/true);
    TablePropertyCache prop;
    std::vector<Dependence> dep = {
        {100, 0, 0},
        {101, 0, 0},
    };
    DriveFlush(/*observed_blocks=*/{}, dep, &collector, &prop);

    ASSERT_EQ(dep.size(), prop.dependence_block_bitmaps.size());
    for (const auto& row : prop.dependence_block_bitmaps) {
      // Available but with an empty bitmap and no usable layout id:
      // "observed nothing", which the aggregator treats conservatively.
      EXPECT_TRUE(row.available);
      EXPECT_EQ(kNoBlockLayoutId, row.layout_id);
      EXPECT_TRUE(row.bitmap.empty());
      EXPECT_EQ(uint64_t(0), row.bitmap.CountSetBits());
    }
    EXPECT_TRUE(collector.empty());
  }

  // (b) Feature on, empty dependence. Output must also be empty.
  {
    FlushBlockBitmapCollector collector(/*enabled=*/true);
    TablePropertyCache prop;
    DriveFlush(/*observed_blocks=*/{}, /*dep=*/{}, &collector, &prop);
    EXPECT_TRUE(prop.dependence_block_bitmaps.empty());
  }
}

// Test 3: When the feature switch is disabled, the collector must
// ignore every ObserveBlockId() call and Materialize() must clear the
// output vector to empty. This is the explicit "bitmap unavailable"
// contract consumed by later GC phases.
TEST_F(BlobValidityBitmapFlushTest, FlushLegacyPathFallbackWhenBitmapDisabled) {
  std::vector<std::pair<uint64_t, uint64_t>> observed = {
      {9, 0},
      {9, 1},
      {9, 3},
  };
  std::vector<Dependence> dep = {
      {9, /*entry_count=*/3, /*byte_count=*/0},
  };

  FlushBlockBitmapCollector collector(/*enabled=*/false);
  TablePropertyCache prop;

  // Pre-fill with garbage to prove Materialize clears it.
  prop.dependence_block_bitmaps.resize(7);
  DriveFlush(observed, dep, &collector, &prop);

  EXPECT_FALSE(collector.enabled());
  EXPECT_TRUE(collector.empty());
  EXPECT_TRUE(prop.dependence_block_bitmaps.empty())
      << "disabled collector must produce an empty block bitmap vector";
}

// Test 4: Many values funnelled into the same blob must set multiple
// distinct block bits (and idempotently collapse repeated hits on the
// same block), matching the flush pattern where a burst of inserts
// lands in the same blob file across its data blocks.
TEST_F(BlobValidityBitmapFlushTest, FlushRecordsMultipleBlocksForSameBlob) {
  const uint64_t kBlob = 77;

  FlushBlockBitmapCollector collector(/*enabled=*/true);
  TablePropertyCache prop;

  std::vector<std::pair<uint64_t, uint64_t>> observed;

  // 10 values all in data block 0. Must only set bit 0 once.
  for (uint64_t i = 0; i < 10; ++i) {
    observed.emplace_back(kBlob, 0);
  }
  observed.emplace_back(kBlob, 1);
  observed.emplace_back(kBlob, 1);  // dup
  observed.emplace_back(kBlob, 2);
  observed.emplace_back(kBlob, 3);
  observed.emplace_back(kBlob, 3);  // dup
  // A far-away block id to prove the bitmap grows on demand.
  observed.emplace_back(kBlob, 1000);

  std::vector<Dependence> dep = {{kBlob, 0, 0}};
  DriveFlush(observed, dep, &collector, &prop);

  ASSERT_EQ(size_t(1), prop.dependence_block_bitmaps.size());
  EXPECT_TRUE(prop.dependence_block_bitmaps[0].available);
  EXPECT_EQ(kBlob, prop.dependence_block_bitmaps[0].layout_id);
  const BlobBlockBitmap& bm = prop.dependence_block_bitmaps[0].bitmap;

  // Expected block ids: 0, 1, 2, 3, 1000. Total = 5 unique bits.
  EXPECT_EQ(uint64_t(5), bm.CountSetBits());
  EXPECT_TRUE(bm.Test(0));
  EXPECT_TRUE(bm.Test(1));
  EXPECT_TRUE(bm.Test(2));
  EXPECT_TRUE(bm.Test(3));
  EXPECT_TRUE(bm.Test(1000));

  // Absent ids must not leak set bits even across the sparse gap.
  EXPECT_FALSE(bm.Test(4));
  EXPECT_FALSE(bm.Test(500));
  EXPECT_FALSE(bm.Test(999));
  EXPECT_FALSE(bm.Test(1001));

  // The bitmap must be large enough to cover the highest observed id.
  EXPECT_GT(bm.num_bits(), uint64_t(1000));
}

// Compaction Test 1: The compaction collector must translate
// block-aware value-index entries into a per-dependence block bitmap
// and surface them on prop.dependence_block_bitmaps aligned to
// prop.dependence.
TEST(BlobValidityBitmapCompactionTest,
     CompactionPropagatesBitmapForLiveBlobReferences) {
  CompactionBlockBitmapCollector collector(/*enabled=*/true);

  // Simulate compaction reading value-index entries referencing
  // blob#42 blocks {0, 1, 3} and blob#43 block {7}. The layout id of
  // each blob is its own physical file number on this path.
  collector.ObserveBlockId(42, 42, 0);
  collector.ObserveBlockId(42, 42, 1);
  collector.ObserveBlockId(42, 42, 3);
  collector.ObserveBlockId(42, 42, 1);  // duplicate must collapse
  collector.ObserveBlockId(43, 43, 7);

  TablePropertyCache prop;
  prop.dependence = {
      {42, /*entry_count=*/3, /*byte_count=*/0},
      {43, /*entry_count=*/1, /*byte_count=*/0},
  };
  collector.Materialize(prop.dependence, &prop.dependence_block_bitmaps);

  ASSERT_EQ(size_t(2), prop.dependence_block_bitmaps.size());
  EXPECT_TRUE(prop.dependence_block_bitmaps[0].available);
  EXPECT_EQ(42U, prop.dependence_block_bitmaps[0].layout_id);
  const BlobBlockBitmap& b42 = prop.dependence_block_bitmaps[0].bitmap;
  EXPECT_EQ(uint64_t(3), b42.CountSetBits());
  EXPECT_TRUE(b42.Test(0));
  EXPECT_TRUE(b42.Test(1));
  EXPECT_FALSE(b42.Test(2));
  EXPECT_TRUE(b42.Test(3));

  const BlobBlockBitmap& b43 = prop.dependence_block_bitmaps[1].bitmap;
  EXPECT_TRUE(prop.dependence_block_bitmaps[1].available);
  EXPECT_EQ(43U, prop.dependence_block_bitmaps[1].layout_id);
  EXPECT_EQ(uint64_t(1), b43.CountSetBits());
  EXPECT_TRUE(b43.Test(7));
  EXPECT_FALSE(b43.Test(0));
}

// Compaction Test 2: A legacy value index (one without a block-id
// trailer) encountered during compaction must force the corresponding
// dependence's block bitmap to the "bitmap unavailable" sentinel
// (empty), even if other block-aware observations also landed on the
// same blob. This is the sticky-unavailable contract required by the
// GC fallback path.
TEST(BlobValidityBitmapCompactionTest,
     CompactionHandlesLegacyIndexAsBitmapUnavailable) {
  CompactionBlockBitmapCollector collector(/*enabled=*/true);

  // blob#42: some block-aware refs PLUS a legacy ref -> must be empty.
  collector.ObserveBlockId(42, 42, 0);
  collector.ObserveBlockId(42, 42, 5);
  collector.MarkUnavailable(42);

  // blob#43: only legacy ref (no ObserveBlockId) -> must also be empty.
  collector.MarkUnavailable(43);

  // blob#44: only block-aware refs, must be preserved as normal.
  collector.ObserveBlockId(44, 44, 2);
  collector.ObserveBlockId(44, 44, 4);

  TablePropertyCache prop;
  prop.dependence = {
      {42, 2, 0},
      {43, 1, 0},
      {44, 2, 0},
  };
  collector.Materialize(prop.dependence, &prop.dependence_block_bitmaps);

  ASSERT_EQ(size_t(3), prop.dependence_block_bitmaps.size());
  EXPECT_TRUE(collector.IsUnavailable(42));
  EXPECT_TRUE(collector.IsUnavailable(43));
  EXPECT_FALSE(collector.IsUnavailable(44));

  // 42 and 43 must be marked unavailable; 44 must carry the
  // collected blocks.
  EXPECT_FALSE(prop.dependence_block_bitmaps[0].available);
  EXPECT_TRUE(prop.dependence_block_bitmaps[0].bitmap.empty());
  EXPECT_FALSE(prop.dependence_block_bitmaps[1].available);
  EXPECT_TRUE(prop.dependence_block_bitmaps[1].bitmap.empty());

  EXPECT_TRUE(prop.dependence_block_bitmaps[2].available);
  EXPECT_EQ(44U, prop.dependence_block_bitmaps[2].layout_id);
  const BlobBlockBitmap& b44 = prop.dependence_block_bitmaps[2].bitmap;
  EXPECT_EQ(uint64_t(2), b44.CountSetBits());
  EXPECT_TRUE(b44.Test(2));
  EXPECT_TRUE(b44.Test(4));
}

// Compaction Test 3: A single compaction output SST is normally
// produced from multiple input SSTs; the collector must OR-merge block
// observations across those inputs into a single bitmap per blob.
TEST(BlobValidityBitmapCompactionTest,
     CompactionMergesBlockRefsAcrossMultipleInputSsts) {
  CompactionBlockBitmapCollector collector(/*enabled=*/true);

  // Simulate input SST A contributes blocks {0, 2, 4} for blob#11
  // and block {0} for blob#12.
  const std::vector<std::pair<uint64_t, uint64_t>> from_a = {
      {11, 0}, {11, 2}, {11, 4}, {12, 0},
  };
  // Simulate input SST B contributes blocks {1, 4, 5} for blob#11
  // and block {3} for blob#12.
  const std::vector<std::pair<uint64_t, uint64_t>> from_b = {
      {11, 1}, {11, 4}, {11, 5}, {12, 3},
  };

  // Interleave the two streams: this mirrors the order in which the
  // compaction iterator hands entries to the output builder.
  auto ia = from_a.begin();
  auto ib = from_b.begin();
  while (ia != from_a.end() || ib != from_b.end()) {
    if (ia != from_a.end()) {
      collector.ObserveBlockId(ia->first, ia->first, ia->second);
      ++ia;
    }
    if (ib != from_b.end()) {
      collector.ObserveBlockId(ib->first, ib->first, ib->second);
      ++ib;
    }
  }

  TablePropertyCache prop;
  prop.dependence = {
      {11, /*entry_count=*/6, /*byte_count=*/0},
      {12, /*entry_count=*/2, /*byte_count=*/0},
  };
  collector.Materialize(prop.dependence, &prop.dependence_block_bitmaps);

  ASSERT_EQ(size_t(2), prop.dependence_block_bitmaps.size());

  // blob#11 union should be {0,1,2,4,5} (block 4 dedup'd across inputs).
  const BlobBlockBitmap& b11 = prop.dependence_block_bitmaps[0].bitmap;
  EXPECT_EQ(uint64_t(5), b11.CountSetBits());
  EXPECT_TRUE(b11.Test(0));
  EXPECT_TRUE(b11.Test(1));
  EXPECT_TRUE(b11.Test(2));
  EXPECT_FALSE(b11.Test(3));
  EXPECT_TRUE(b11.Test(4));
  EXPECT_TRUE(b11.Test(5));

  // blob#12 union should be {0,3}.
  const BlobBlockBitmap& b12 = prop.dependence_block_bitmaps[1].bitmap;
  EXPECT_EQ(uint64_t(2), b12.CountSetBits());
  EXPECT_TRUE(b12.Test(0));
  EXPECT_TRUE(b12.Test(3));
}

// Compaction Test 4: Running the same compaction twice on a canonical
// input stream must produce byte-identical block bitmap output. This is
// what allows downstream phases (manifest persistence and live-view
// aggregation) to reason about the bitmap as a deterministic function
// of its input.
TEST(BlobValidityBitmapCompactionTest,
     CompactionOutputBitmapStableAcrossRepeatedRuns) {
  // The canonical input stream. Intentionally includes duplicates,
  // a legacy dependence (blob#50), and out-of-order block ids to
  // shake out any hidden ordering dependence in the collector.
  struct Entry {
    enum class Kind { kBlock, kLegacy } kind;
    uint64_t blob_fn;
    uint64_t block_id;  // only for kBlock
  };
  const std::vector<Entry> stream = {
      {Entry::Kind::kBlock, 10, 7},
      {Entry::Kind::kBlock, 10, 0},
      {Entry::Kind::kBlock, 10, 3},
      {Entry::Kind::kBlock, 10, 7},  // dup
      {Entry::Kind::kLegacy, 50, 0},
      {Entry::Kind::kBlock, 10, 5},
      {Entry::Kind::kBlock, 20, 2},
      {Entry::Kind::kLegacy, 50, 0},  // dup legacy
      {Entry::Kind::kBlock, 20, 8},
      {Entry::Kind::kBlock, 20, 8},  // dup
  };
  const std::vector<Dependence> dep = {
      {10, 0, 0},
      {20, 0, 0},
      {50, 0, 0},
  };

  auto run_once = [&]() {
    CompactionBlockBitmapCollector collector(/*enabled=*/true);
    for (const auto& e : stream) {
      if (e.kind == Entry::Kind::kBlock) {
        collector.ObserveBlockId(e.blob_fn, e.blob_fn, e.block_id);
      } else {
        collector.MarkUnavailable(e.blob_fn);
      }
    }
    TablePropertyCache prop;
    prop.dependence = dep;
    collector.Materialize(prop.dependence, &prop.dependence_block_bitmaps);

    // Serialize the entire block-bitmap vector into a single byte
    // string so we can byte-compare runs.
    std::string blob;
    for (const auto& bm : prop.dependence_block_bitmaps) {
      bm.Serialize(&blob);
      blob.push_back('|');  // separator keeps entries self-delimited
    }
    return blob;
  };

  const std::string run_a = run_once();
  const std::string run_b = run_once();
  const std::string run_c = run_once();

  EXPECT_FALSE(run_a.empty());
  EXPECT_EQ(run_a, run_b);
  EXPECT_EQ(run_b, run_c);

  // Also spot-check the logical content of one run: blocks {0,3,5,7}
  // on blob 10, blocks {2,8} on blob 20, and empty/unavailable on 50.
  CompactionBlockBitmapCollector collector(/*enabled=*/true);
  for (const auto& e : stream) {
    if (e.kind == Entry::Kind::kBlock) {
      collector.ObserveBlockId(e.blob_fn, e.blob_fn, e.block_id);
    } else {
      collector.MarkUnavailable(e.blob_fn);
    }
  }
  TablePropertyCache prop;
  prop.dependence = dep;
  collector.Materialize(prop.dependence, &prop.dependence_block_bitmaps);

  ASSERT_EQ(size_t(3), prop.dependence_block_bitmaps.size());
  EXPECT_EQ(uint64_t(4),
            prop.dependence_block_bitmaps[0].bitmap.CountSetBits());
  EXPECT_EQ(uint64_t(2),
            prop.dependence_block_bitmaps[1].bitmap.CountSetBits());
  EXPECT_FALSE(prop.dependence_block_bitmaps[2].available);
  EXPECT_TRUE(prop.dependence_block_bitmaps[2].bitmap.empty());
}

// Manifest Test: TableProperties round-trip with block bitmap payload.
// We exercise the same encode/decode format that PropertyBlockBuilder
// and ReadProperties use in table/meta_blocks.cc, so this test pins
// down the wire contract independently of the SST format. A per-row
// empty payload (== empty BlobBlockBitmap) must survive the round-trip
// as an empty BlobBlockBitmap on the consumer side, which is the
// per-row "bitmap unavailable" signal.
namespace manifest_testutil {

// Encode: mirror of meta_blocks.cc PropertyBlockBuilder::AddTableProperty
// for the dependence-block-bitmaps branch.
std::string EncodeBlockBitmapPayload(
    const std::vector<std::string>& serialized_bitmaps) {
  std::string payload;
  PutVarint64(&payload, static_cast<uint64_t>(serialized_bitmaps.size()));
  for (const auto& b : serialized_bitmaps) {
    PutVarint64(&payload, static_cast<uint64_t>(b.size()));
    payload.append(b);
  }
  return payload;
}

// Decode: mirror of meta_blocks.cc ReadProperties dependence-block-bitmaps
// branch. Returns true on success.
bool DecodeBlockBitmapPayload(const std::string& payload,
                              std::vector<std::string>* out) {
  Slice raw(payload);
  uint64_t n = 0;
  if (!GetVarint64(&raw, &n)) return false;
  out->clear();
  out->reserve(n);
  for (uint64_t i = 0; i < n; ++i) {
    uint64_t blen = 0;
    if (!GetVarint64(&raw, &blen) || raw.size() < blen) {
      return false;
    }
    out->emplace_back(raw.data(), blen);
    raw.remove_prefix(blen);
  }
  return true;
}

}  // namespace manifest_testutil

TEST(BlobValidityBitmapManifestTest, TablePropertiesRoundTripWithBitmap) {
  // Build three block bitmaps: two are populated with distinct
  // patterns, one is empty (per-row "bitmap unavailable").
  BlobBlockBitmap bm0;
  bm0.Set(0);
  bm0.Set(1);
  bm0.Set(63);
  bm0.Set(64);
  bm0.Set(65);
  BlobBlockBitmap bm1;  // empty on purpose
  BlobBlockBitmap bm2;
  bm2.Set(7);
  bm2.Set(9999);

  // Writer side: simulate what TableBuilder does when turning
  // TablePropertyCache::dependence_block_bitmaps into the serializable
  // TableProperties::dependence_block_bitmaps.
  std::vector<std::string> serialized;
  {
    std::string s;
    bm0.Serialize(&s);
    serialized.emplace_back(std::move(s));
  }
  serialized.emplace_back();  // empty string <-> empty BlobBlockBitmap
  {
    std::string s;
    bm2.Serialize(&s);
    serialized.emplace_back(std::move(s));
  }

  // Writer packs it into the dependence-block-bitmaps property value.
  std::string payload =
      manifest_testutil::EncodeBlockBitmapPayload(serialized);

  // Reader unpacks it back.
  std::vector<std::string> reparsed;
  ASSERT_TRUE(
      manifest_testutil::DecodeBlockBitmapPayload(payload, &reparsed));
  ASSERT_EQ(serialized.size(), reparsed.size());
  for (size_t i = 0; i < serialized.size(); ++i) {
    EXPECT_EQ(serialized[i], reparsed[i]) << "row " << i;
  }

  // Finally, reinflating the byte strings into BlobBlockBitmap (what
  // compaction_job.cc / repair.cc do after ReadProperties) must
  // recover the original logical content.
  std::vector<BlobBlockBitmap> round_tripped(reparsed.size());
  for (size_t i = 0; i < reparsed.size(); ++i) {
    if (reparsed[i].empty()) continue;
    Slice in(reparsed[i]);
    ASSERT_TRUE(round_tripped[i].Deserialize(&in));
  }
  ASSERT_EQ(size_t(3), round_tripped.size());

  EXPECT_EQ(uint64_t(5), round_tripped[0].CountSetBits());
  EXPECT_TRUE(round_tripped[0].Test(0));
  EXPECT_TRUE(round_tripped[0].Test(1));
  EXPECT_TRUE(round_tripped[0].Test(63));
  EXPECT_TRUE(round_tripped[0].Test(64));
  EXPECT_TRUE(round_tripped[0].Test(65));
  EXPECT_FALSE(round_tripped[0].Test(2));

  EXPECT_TRUE(round_tripped[1].empty());

  EXPECT_EQ(uint64_t(2), round_tripped[2].CountSetBits());
  EXPECT_TRUE(round_tripped[2].Test(7));
  EXPECT_TRUE(round_tripped[2].Test(9999));

  // Also verify that a payload produced by an old writer (no block
  // bitmap property at all) decodes as an empty bitmap vector, which
  // is the "bitmap unavailable" sentinel that triggers GC fallback.
  std::vector<std::string> absent_reparsed;
  std::string absent_payload =
      manifest_testutil::EncodeBlockBitmapPayload({});
  ASSERT_TRUE(manifest_testutil::DecodeBlockBitmapPayload(absent_payload,
                                                          &absent_reparsed));
  EXPECT_TRUE(absent_reparsed.empty());
}

// Manifest recovery must restore dependence_block_bitmaps from the
// persisted kPropertyCache payload and do so for every file in the
// edit. Mixed-file layouts (some files have bitmap, some do not, some
// have per-row empty bitmap) must round-trip independently on each file.
TEST(BlobValidityBitmapManifestTest, ManifestRecoveryRestoresBitmapMetadata) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;

  // File A: two dependencies, both with populated block bitmaps.
  {
    TablePropertyCache prop;
    prop.purpose = 1;
    prop.num_entries = 1;
    prop.raw_key_size = 1;
    prop.raw_value_size = 1;
    prop.dependence = {Dependence{100, 5, 2048}, Dependence{101, 7, 4096}};
    prop.dependence_block_bitmaps.resize(2);
    prop.dependence_block_bitmaps[0].available = true;
    prop.dependence_block_bitmaps[0].layout_id = 100;
    prop.dependence_block_bitmaps[0].bitmap.Set(0);
    prop.dependence_block_bitmaps[0].bitmap.Set(3);
    prop.dependence_block_bitmaps[1].available = true;
    prop.dependence_block_bitmaps[1].layout_id = 101;
    prop.dependence_block_bitmaps[1].bitmap.Set(1);
    prop.dependence_block_bitmaps[1].bitmap.Set(2);
    prop.dependence_block_bitmaps[1].bitmap.Set(100);
    edit.AddFile(3, 300, 0, 100, InternalKey("k0", kBig + 1, kTypeValue),
                 InternalKey("k1", kBig + 2, kTypeDeletion), kBig + 1, kBig + 2,
                 false, prop);
  }

  // File B: three dependencies, but only the middle one has a bitmap,
  // the other two are per-row empty (i.e. blob is seen but bitmap is
  // unavailable for that blob on this SST).
  {
    TablePropertyCache prop;
    prop.purpose = 1;
    prop.num_entries = 1;
    prop.raw_key_size = 1;
    prop.raw_value_size = 1;
    prop.dependence = {Dependence{200, 1, 0}, Dependence{201, 2, 0},
                       Dependence{202, 3, 0}};
    prop.dependence_block_bitmaps.resize(3);
    prop.dependence_block_bitmaps[1].available = true;
    prop.dependence_block_bitmaps[1].layout_id = 201;
    prop.dependence_block_bitmaps[1].bitmap.Set(4);
    prop.dependence_block_bitmaps[1].bitmap.Set(5);
    edit.AddFile(3, 301, 0, 100, InternalKey("m0", kBig + 3, kTypeValue),
                 InternalKey("m1", kBig + 4, kTypeDeletion), kBig + 3, kBig + 4,
                 false, prop);
  }

  // File C: legacy-style file that does not carry any block bitmap at
  // all (empty vector). Decoder must keep it empty (== bitmap
  // unavailable for the whole SST).
  {
    TablePropertyCache prop;
    prop.purpose = 1;
    prop.num_entries = 1;
    prop.raw_key_size = 1;
    prop.raw_value_size = 1;
    prop.dependence = {Dependence{300, 1, 0}};
    // dependence_block_bitmaps intentionally left empty.
    edit.AddFile(3, 302, 0, 100, InternalKey("z0", kBig + 5, kTypeValue),
                 InternalKey("z1", kBig + 6, kTypeDeletion), kBig + 5, kBig + 6,
                 false, prop);
  }

  std::string encoded;
  ASSERT_TRUE(edit.EncodeTo(&encoded));

  VersionEdit parsed;
  ASSERT_OK(parsed.DecodeFrom(encoded));
  auto& files = parsed.GetNewFiles();
  ASSERT_EQ(size_t(3), files.size());

  // File A: both bitmaps survive.
  {
    auto& p = files[0].second.prop;
    ASSERT_EQ(2U, p.dependence.size());
    ASSERT_EQ(2U, p.dependence_block_bitmaps.size());
    EXPECT_TRUE(p.dependence_block_bitmaps[0].available);
    EXPECT_EQ(100U, p.dependence_block_bitmaps[0].layout_id);
    EXPECT_EQ(uint64_t(2),
              p.dependence_block_bitmaps[0].bitmap.CountSetBits());
    EXPECT_TRUE(p.dependence_block_bitmaps[0].bitmap.Test(0));
    EXPECT_TRUE(p.dependence_block_bitmaps[0].bitmap.Test(3));
    EXPECT_TRUE(p.dependence_block_bitmaps[1].available);
    EXPECT_EQ(101U, p.dependence_block_bitmaps[1].layout_id);
    EXPECT_EQ(uint64_t(3),
              p.dependence_block_bitmaps[1].bitmap.CountSetBits());
    EXPECT_TRUE(p.dependence_block_bitmaps[1].bitmap.Test(100));
  }
  // File B: mixed case preserved.
  {
    auto& p = files[1].second.prop;
    ASSERT_EQ(3U, p.dependence.size());
    ASSERT_EQ(3U, p.dependence_block_bitmaps.size());
    EXPECT_FALSE(p.dependence_block_bitmaps[0].available);
    EXPECT_TRUE(p.dependence_block_bitmaps[0].bitmap.empty());
    EXPECT_TRUE(p.dependence_block_bitmaps[1].available);
    EXPECT_EQ(201U, p.dependence_block_bitmaps[1].layout_id);
    EXPECT_EQ(uint64_t(2),
              p.dependence_block_bitmaps[1].bitmap.CountSetBits());
    EXPECT_TRUE(p.dependence_block_bitmaps[1].bitmap.Test(4));
    EXPECT_TRUE(p.dependence_block_bitmaps[1].bitmap.Test(5));
    EXPECT_FALSE(p.dependence_block_bitmaps[2].available);
    EXPECT_TRUE(p.dependence_block_bitmaps[2].bitmap.empty());
  }
  // File C: legacy path, full-SST bitmap unavailable.
  {
    auto& p = files[2].second.prop;
    ASSERT_EQ(1U, p.dependence.size());
    EXPECT_TRUE(p.dependence_block_bitmaps.empty());
  }

  // Stability: re-encoding the decoded edit should be byte-equal, so
  // a second-generation manifest keeps the exact same format.
  std::string re_encoded;
  ASSERT_TRUE(parsed.EncodeTo(&re_encoded));
  ASSERT_EQ(encoded, re_encoded);
}

// =================================================================
// End-to-end tests for the block-level blob GC pipeline. These tests
// stitch together every stage of the feature, driving the same
// sequence of components that db/builder.cc, db/version_edit.cc,
// db/version_set.cc and db/compaction_job.cc would drive on the
// production write->GC path. The GC fast-path gate is replayed by a
// small `SimulateGc` helper that is a line-for-line mirror of the
// hot section of ProcessGarbageCollection: for each record, consult
// `IsBlobEntirelyDead` once per blob and short-circuit, otherwise
// consult `IsBlockLive` per-record (bit==0 => skip), otherwise fall
// back to the legacy GetKey()-path.
//
// User-visible data correctness is validated through an explicit
// "ground truth" liveness map built at write time: any record the
// simulator decides to drop (fast path + dead) must also be genuinely
// dead in the ground-truth; any record kept must not be corrupted or
// reordered. This reproduces the real-world contract users rely on
// ("bitmap fast path must never silently drop a live record").
// =================================================================

namespace e2e_testutil {

// Minimal record description used by the GC simulator. Mirrors the
// fields ProcessGarbageCollection's inner loop reads off each
// value-index entry.
struct BlobRecord {
  uint64_t blob_file_number;
  uint64_t block_id;
  uint64_t byte_overhead;  // key+value payload size
  bool ground_truth_live;  // what a correct GC MUST keep
};

// Build a FileMetaData that references blob_file with the given block
// bitmap. Caller owns the returned pointer and must release it after
// the VersionStorageInfo teardown.
FileMetaData* MakeSstReferencingBlob(uint64_t sst_file_number,
                                     uint64_t blob_file_number,
                                     std::vector<uint64_t> blocks_referenced,
                                     uint64_t file_size = 1024) {
  auto* f = new FileMetaData;
  f->fd = FileDescriptor(sst_file_number, /*path_id=*/0, file_size);
  char buf[32];
  snprintf(buf, sizeof(buf), "k%08" PRIu64 "_0", sst_file_number);
  f->smallest = InternalKey(buf, /*s=*/100, kTypeValue);
  snprintf(buf, sizeof(buf), "k%08" PRIu64 "_9", sst_file_number);
  f->largest = InternalKey(buf, /*s=*/200, kTypeValue);
  f->fd.smallest_seqno = 100;
  f->fd.largest_seqno = 200;
  f->compensated_file_size = file_size;
  f->refs = 0;
  f->prop.purpose = 0;  // value SST, not a pure-blob SST
  f->prop.dependence.push_back(Dependence{blob_file_number, /*ec=*/1, /*bc=*/0});
  DependenceBlockBitmap row;
  row.available = true;
  // The aggregator only trusts a row whose layout_id matches the blob's
  // current physical file number. For a freshly written reference that
  // is the blob file number itself.
  row.layout_id = blob_file_number;
  for (uint64_t bid : blocks_referenced) row.bitmap.Set(bid);
  f->prop.dependence_block_bitmaps.emplace_back(std::move(row));
  f->prop.num_entries = static_cast<uint64_t>(blocks_referenced.size());
  f->gc_status = FileMetaData::kGarbageCollectionForbidden;
  return f;
}

// Build a FileMetaData that references blob_file in the LEGACY regime
// (no dependence_block_bitmaps). This mimics an SST flushed by a
// pre-feature binary.
FileMetaData* MakeLegacySstReferencingBlob(uint64_t sst_file_number,
                                           uint64_t blob_file_number,
                                           uint64_t file_size = 1024) {
  auto* f = new FileMetaData;
  f->fd = FileDescriptor(sst_file_number, 0, file_size);
  char buf[32];
  snprintf(buf, sizeof(buf), "k%08" PRIu64 "_0", sst_file_number);
  f->smallest = InternalKey(buf, 100, kTypeValue);
  snprintf(buf, sizeof(buf), "k%08" PRIu64 "_9", sst_file_number);
  f->largest = InternalKey(buf, 200, kTypeValue);
  f->fd.smallest_seqno = 100;
  f->fd.largest_seqno = 200;
  f->compensated_file_size = file_size;
  f->refs = 0;
  f->prop.purpose = 0;
  f->prop.dependence.push_back(Dependence{blob_file_number, 1, 0});
  // Intentionally leave dependence_block_bitmaps empty -> legacy.
  f->prop.num_entries = 1;
  f->gc_status = FileMetaData::kGarbageCollectionForbidden;
  return f;
}

// Build a blob FileMetaData at hidden level -1.
FileMetaData* MakeBlobFile(uint64_t blob_file_number, uint64_t file_size) {
  auto* f = new FileMetaData;
  f->fd = FileDescriptor(blob_file_number, 0, file_size);
  f->smallest = InternalKey("blobstart", 100, kTypeValue);
  f->largest = InternalKey("blobend", 200, kTypeValue);
  f->fd.smallest_seqno = 100;
  f->fd.largest_seqno = 200;
  f->compensated_file_size = file_size;
  f->refs = 0;
  f->prop.num_entries = 1;
  f->gc_status = FileMetaData::kGarbageCollectionForbidden;
  return f;
}

// Counters the simulator produces. These mirror 1:1 the subset of the
// `counter` struct in compaction_job.cc::ProcessGarbageCollection that
// the observability tickers surface.
struct SimulatedGcCounters {
  uint64_t input = 0;
  uint64_t bitmap_aware_blobs = 0;
  uint64_t bitmap_fallback_blobs = 0;
  uint64_t bitmap_entirely_dead_blobs = 0;
  uint64_t bitmap_fast_path_skips = 0;
  uint64_t bitmap_skipped_bytes = 0;
  uint64_t bitmap_live_bytes = 0;
  // Records actually kept after the GC sub-compaction (post-filter).
  std::vector<BlobRecord> kept_records;
};

// Replays the GC gate over a synthetic record stream. Per-blob
// IsBlobEntirelyDead is consulted exactly once (cached) -- this matches
// compaction_job.cc's blob meta cache.
SimulatedGcCounters SimulateGc(const VersionStorageInfo& vstorage,
                               const std::vector<BlobRecord>& records) {
  SimulatedGcCounters c;
  struct CacheEntry {
    uint64_t blob_fn;
    bool entirely_dead;
    bool bitmap_aware;
  };
  std::vector<CacheEntry> cache;

  for (const BlobRecord& r : records) {
    ++c.input;
    auto it = std::find_if(
        cache.begin(), cache.end(),
        [&](const CacheEntry& e) { return e.blob_fn == r.blob_file_number; });
    bool entirely_dead = false;
    if (it == cache.end()) {
      const auto* info = vstorage.GetBlobLiveBlockInfo(r.blob_file_number);
      bool aware = info != nullptr && info->bitmap_available;
      if (aware) {
        ++c.bitmap_aware_blobs;
        if (info->live_block_count == 0) {
          entirely_dead = true;
          ++c.bitmap_entirely_dead_blobs;
        }
      } else {
        ++c.bitmap_fallback_blobs;
      }
      cache.push_back(CacheEntry{r.blob_file_number, entirely_dead, aware});
    } else {
      entirely_dead = it->entirely_dead;
    }

    if (entirely_dead) {
      // Blob-level fast path: skip the GetKey() lookup altogether.
      ++c.bitmap_fast_path_skips;
      c.bitmap_skipped_bytes += r.byte_overhead;
      continue;
    }
    if (!entirely_dead && r.block_id != kNoBlockId &&
        vstorage.IsBlockLive(r.blob_file_number, r.block_id) ==
            VersionStorageInfo::BlobBlockLiveness::kDead) {
      // Record-level fast path: bit==0 => block dead => skip GetKey().
      ++c.bitmap_fast_path_skips;
      c.bitmap_skipped_bytes += r.byte_overhead;
      continue;
    }

    // Legacy path: consult ground truth (stand-in for
    // input_version->GetKey() liveness verdict) and accumulate bytes.
    c.bitmap_live_bytes += r.byte_overhead;
    if (r.ground_truth_live) {
      c.kept_records.push_back(r);
    }
  }
  return c;
}

// Ground-truth invariant: every record the simulator KEPT must have
// been genuinely live, AND every genuinely live record must have ended
// up in kept_records. This is the "user-visible data is never
// corrupted" assertion.
void AssertNoUserVisibleDataLoss(const std::vector<BlobRecord>& inputs,
                                 const SimulatedGcCounters& out) {
  for (const BlobRecord& r : out.kept_records) {
    ASSERT_TRUE(r.ground_truth_live)
        << "GC kept a record that was not ground-truth live "
        << "(blob=" << r.blob_file_number << ", block=" << r.block_id << ")";
  }
  std::vector<BlobRecord> expected_kept;
  for (const BlobRecord& r : inputs) {
    if (r.ground_truth_live) expected_kept.push_back(r);
  }
  ASSERT_EQ(expected_kept.size(), out.kept_records.size())
      << "GC dropped or duplicated records vs. ground truth";
  for (size_t i = 0; i < expected_kept.size(); ++i) {
    EXPECT_EQ(expected_kept[i].blob_file_number,
              out.kept_records[i].blob_file_number);
    EXPECT_EQ(expected_kept[i].block_id, out.kept_records[i].block_id);
  }
}

std::unique_ptr<VersionStorageInfo> MakeVStorage(
    const InternalKeyComparator* icmp, const Comparator* ucmp) {
  Options tmp_opts;
  return std::unique_ptr<VersionStorageInfo>(
      new VersionStorageInfo(icmp, ucmp, tmp_opts.num_levels,
                             kCompactionStyleLevel,
                             /*force_consistency_checks=*/false));
}

}  // namespace e2e_testutil

class BlobValidityBitmapEndToEndTest : public testing::Test {
 protected:
  BlobValidityBitmapEndToEndTest()
      : ucmp_(BytewiseComparator()), icmp_(ucmp_) {}

  void ReleaseFiles(VersionStorageInfo* vs) {
    for (int lvl = -1; lvl < vs->num_levels(); ++lvl) {
      for (auto* f : vs->LevelFiles(lvl)) {
        if (--f->refs == 0) delete f;
      }
    }
  }

  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
};

// E2E Test 1: the full flush->manifest->aggregate->GC pipeline. A flush
// writes separated values into blob B1. The FlushBlockBitmapCollector
// builds a block bitmap for B1 that the SST's TablePropertyCache holds.
// The manifest round-trips it. VersionStorageInfo aggregates it. The GC
// loop short-circuits records in dead blocks while live blocks still
// fall back to GetKey(); no live record is ever silently dropped.
TEST_F(BlobValidityBitmapEndToEndTest,
       EndToEnd_FlushBuildsBitmapThenGcUsesFastPath) {
  using namespace e2e_testutil;
  constexpr uint64_t kBlobFn = 4200U;
  constexpr uint64_t kSstFn = 5200U;
  constexpr uint64_t kNumBlocks = 8;

  // --- flush observes real data block ordinals ---
  FlushBlockBitmapCollector collector(/*enabled=*/true);
  // Simulate a flush that writes 3 separated values into blob B1,
  // landing in data blocks {0, 3, 5}.
  collector.ObserveBlockId(kBlobFn, kBlobFn, 0);
  collector.ObserveBlockId(kBlobFn, kBlobFn, 3);
  collector.ObserveBlockId(kBlobFn, kBlobFn, 5);

  // --- collector materializes into TablePropertyCache ---
  TablePropertyCache prop;
  prop.dependence = {{kBlobFn, /*ec=*/3, /*bc=*/0}};
  collector.Materialize(prop.dependence, &prop.dependence_block_bitmaps);
  ASSERT_EQ(1U, prop.dependence_block_bitmaps.size());
  EXPECT_TRUE(prop.dependence_block_bitmaps[0].available);
  EXPECT_EQ(kBlobFn, prop.dependence_block_bitmaps[0].layout_id);
  EXPECT_EQ(3U, prop.dependence_block_bitmaps[0].bitmap.CountSetBits());
  EXPECT_TRUE(prop.dependence_block_bitmaps[0].bitmap.Test(0));
  EXPECT_TRUE(prop.dependence_block_bitmaps[0].bitmap.Test(3));
  EXPECT_TRUE(prop.dependence_block_bitmaps[0].bitmap.Test(5));

  // --- VersionEdit encode/decode (manifest round-trip) ---
  VersionEdit edit;
  InternalKey sm("a", 100, kTypeValue);
  InternalKey lg("z", 200, kTypeValue);
  edit.AddFile(/*level=*/1, kSstFn, /*path_id=*/0, /*file_size=*/1024, sm, lg,
               /*smallest_seqno=*/100, /*largest_seqno=*/200,
               /*marked_for_compaction=*/false, prop);
  std::string bytes;
  ASSERT_TRUE(edit.EncodeTo(&bytes));
  VersionEdit recovered;
  ASSERT_OK(recovered.DecodeFrom(bytes));
  ASSERT_EQ(1U, recovered.GetNewFiles().size());
  const auto& rec_prop = recovered.GetNewFiles()[0].second.prop;
  ASSERT_EQ(1U, rec_prop.dependence_block_bitmaps.size());
  EXPECT_EQ(3U, rec_prop.dependence_block_bitmaps[0].bitmap.CountSetBits());

  // --- install blob + SST into VersionStorageInfo and aggregate ---
  auto vstorage = MakeVStorage(&icmp_, ucmp_);
  vstorage->AddFile(/*level=*/-1, MakeBlobFile(kBlobFn, 1024 * kNumBlocks));
  auto* sst_meta = new FileMetaData(recovered.GetNewFiles()[0].second);
  sst_meta->refs = 0;
  vstorage->AddFile(/*level=*/1, sst_meta);
  vstorage->AggregateBlobLiveBlockBitmaps();

  const auto* info = vstorage->GetBlobLiveBlockInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  EXPECT_TRUE(info->bitmap_available);
  EXPECT_EQ(3U, info->live_block_count);

  // --- per-block liveness sanity: live blocks read as kLive, dead as
  //     kDead, so the GC loop can skip-only-the-dead safely. The bitmap
  //     tracks up to block 5 (highest observed), so 6,7 are also dead.
  using L = VersionStorageInfo::BlobBlockLiveness;
  for (uint64_t bid : {0U, 3U, 5U}) {
    EXPECT_EQ(L::kLive, vstorage->IsBlockLive(kBlobFn, bid));
  }
  for (uint64_t bid : {1U, 2U, 4U, 6U, 7U}) {
    EXPECT_EQ(L::kDead, vstorage->IsBlockLive(kBlobFn, bid));
  }
  EXPECT_FALSE(vstorage->IsBlobEntirelyDead(kBlobFn));

  // --- drive a GC stream across all 8 blocks. ground-truth live iff
  //     block in {0, 3, 5}. dead blocks are short-circuited; live
  //     blocks fall back to GetKey() and our ground-truth drives the
  //     kept set.
  std::vector<BlobRecord> records;
  for (uint64_t bid = 0; bid < kNumBlocks; ++bid) {
    bool live = (bid == 0 || bid == 3 || bid == 5);
    records.push_back(BlobRecord{kBlobFn, bid, /*byte_overhead=*/128, live});
  }
  // Add a few extra records to stress per-record accounting.
  for (uint64_t bid : {0U, 3U, 5U, 1U}) {
    bool live = (bid == 0 || bid == 3 || bid == 5);
    records.push_back(BlobRecord{kBlobFn, bid, 200, live});
  }

  auto c = SimulateGc(*vstorage, records);

  // This blob has live blocks, so the whole-blob fast path does NOT
  // fire. Records in dead blocks are short-circuited by the per-record
  // block-aware branch.
  EXPECT_EQ(1U, c.bitmap_aware_blobs);
  EXPECT_EQ(0U, c.bitmap_fallback_blobs);
  EXPECT_EQ(0U, c.bitmap_entirely_dead_blobs);
  EXPECT_EQ(6U, c.bitmap_fast_path_skips);
  EXPECT_EQ(5U * 128U + 1U * 200U, c.bitmap_skipped_bytes);
  uint64_t expected_live_bytes = 3U * 128U + 3U * 200U;
  EXPECT_EQ(expected_live_bytes, c.bitmap_live_bytes);

  AssertNoUserVisibleDataLoss(records, c);

  ReleaseFiles(vstorage.get());
}

// E2E Test 2: update-heavy workload. The same user-facing keys are
// updated many times. Older blobs whose blocks are no longer referenced
// by anyone end up with ZERO live blocks -> IsBlobEntirelyDead returns
// true -> GC fast-path skips every record in those blobs without a
// GetKey() call. This is the scenario where the feature delivers its
// largest wins, and the test locks that contract.
TEST_F(BlobValidityBitmapEndToEndTest,
       EndToEnd_UpdateHeavyWorkloadAvoidsLookupOnGc) {
  using namespace e2e_testutil;
  constexpr uint64_t kBlobDead = 6000U;  // updated away, no refs
  constexpr uint64_t kBlobLive = 6001U;  // still referenced

  // Install a reference-less dead blob into the version: no SST touches
  // it. Aggregation must treat it as bitmap_available=true with zero
  // live blocks -> IsBlobEntirelyDead == true.
  auto vstorage = MakeVStorage(&icmp_, ucmp_);
  vstorage->AddFile(-1, MakeBlobFile(kBlobDead, 8 * 1024));
  vstorage->AddFile(-1, MakeBlobFile(kBlobLive, 8 * 1024));
  // Two SSTs reference the live blob: blocks {2,4} and {4,7}.
  vstorage->AddFile(1, MakeSstReferencingBlob(7100, kBlobLive, {2, 4}));
  vstorage->AddFile(1, MakeSstReferencingBlob(7101, kBlobLive, {4, 7}));

  vstorage->AggregateBlobLiveBlockBitmaps();

  // Live blob: blocks {2, 4, 7} are live (union of both SSTs).
  const auto* info_live = vstorage->GetBlobLiveBlockInfo(kBlobLive);
  ASSERT_NE(nullptr, info_live);
  EXPECT_TRUE(info_live->bitmap_available);
  EXPECT_EQ(3U, info_live->live_block_count);
  EXPECT_FALSE(vstorage->IsBlobEntirelyDead(kBlobLive));

  // Dead blob: zero SST references -> the aggregation step never
  // sticky-clears it (no legacy reference); it stays bitmap_available
  // with live_block_count=0 -> IsBlobEntirelyDead == true.
  const auto* info_dead = vstorage->GetBlobLiveBlockInfo(kBlobDead);
  ASSERT_NE(nullptr, info_dead);
  EXPECT_TRUE(info_dead->bitmap_available);
  EXPECT_EQ(0U, info_dead->live_block_count);
  EXPECT_TRUE(vstorage->IsBlobEntirelyDead(kBlobDead));

  // Drive GC over 200 records against the dead blob (all dead in ground
  // truth) plus 20 records against the live blob (live iff block in
  // {2, 4, 7}).
  std::vector<BlobRecord> records;
  for (uint64_t i = 0; i < 200; ++i) {
    records.push_back(BlobRecord{kBlobDead, i % 8,
                                 /*byte_overhead=*/64,
                                 /*ground_truth_live=*/false});
  }
  for (uint64_t i = 0; i < 20; ++i) {
    uint64_t bid = i % 8;
    bool live = (bid == 2 || bid == 4 || bid == 7);
    records.push_back(BlobRecord{kBlobLive, bid, 64, live});
  }

  auto result = SimulateGc(*vstorage, records);

  // The dead blob (200 records) plus the dead-block records in the live
  // blob must avoid GetKey(). Live blob: blocks {0,1,3,5,6} are dead in
  // the live blob's 20-record stream.
  // Of 20 live-blob records (i%8 for i in 0..19): blocks 0,1,3,5,6 dead.
  // count dead: i%8 in {0,1,3,5,6}. i in 0..19 -> per 8: indices
  // {0,1,3,5,6} = 5 dead per full period; 0..7 (5), 8..15 (5), 16..19 ->
  // i%8 = 0,1,2,3 -> dead {0,1,3} = 3. total dead = 5+5+3 = 13.
  EXPECT_EQ(200U + 13U, result.bitmap_fast_path_skips);
  EXPECT_EQ((200U + 13U) * 64U, result.bitmap_skipped_bytes);
  EXPECT_EQ(2U, result.bitmap_aware_blobs);
  EXPECT_EQ(1U, result.bitmap_entirely_dead_blobs);
  EXPECT_EQ(0U, result.bitmap_fallback_blobs);
  // 20 - 13 = 7 live-blob records reach GetKey().
  EXPECT_EQ(7U * 64U, result.bitmap_live_bytes);

  AssertNoUserVisibleDataLoss(records, result);

  ReleaseFiles(vstorage.get());
}

// E2E Test 3: recovery keeps bitmap usable end-to-end. We drive the
// full flush -> manifest -> version-aggregate pipeline, then discard
// the original version and rebuild a fresh one by re-decoding the
// manifest bytes. The rebuilt version must drive GC identically to the
// original -- same fast-path wins and no user-visible record dropped.
TEST_F(BlobValidityBitmapEndToEndTest, EndToEnd_RecoveryKeepsBitmapUsable) {
  using namespace e2e_testutil;
  constexpr uint64_t kBlobDead = 6200U;  // no refs -> entirely dead

  // Build a VersionEdit that adds an SST that does NOT reference the
  // dead blob, then do an encode/decode round. The rebuilt vstorage
  // must still find kBlobDead entirely dead.
  TablePropertyCache prop_unrelated;
  prop_unrelated.dependence = {};  // No blob deps at all.

  VersionEdit edit;
  InternalKey sm("a", 100, kTypeValue);
  InternalKey lg("z", 200, kTypeValue);
  edit.AddFile(1, 7200, 0, 1024, sm, lg, 100, 200, false, prop_unrelated);
  std::string bytes;
  ASSERT_TRUE(edit.EncodeTo(&bytes));
  VersionEdit recovered;
  ASSERT_OK(recovered.DecodeFrom(bytes));

  auto build_vstorage = [&](bool use_recovered) {
    auto vs = MakeVStorage(&icmp_, ucmp_);
    vs->AddFile(-1, MakeBlobFile(kBlobDead, 4 * 1024));
    VersionEdit& e = use_recovered ? recovered : edit;
    auto* sst_meta = new FileMetaData(e.GetNewFiles()[0].second);
    sst_meta->refs = 0;
    vs->AddFile(1, sst_meta);
    vs->AggregateBlobLiveBlockBitmaps();
    return vs;
  };

  auto vs_orig = build_vstorage(/*use_recovered=*/false);
  auto vs_rcvr = build_vstorage(/*use_recovered=*/true);

  EXPECT_TRUE(vs_orig->IsBlobEntirelyDead(kBlobDead));
  EXPECT_TRUE(vs_rcvr->IsBlobEntirelyDead(kBlobDead));

  std::vector<BlobRecord> records;
  for (uint64_t i = 0; i < 50; ++i) {
    records.push_back(BlobRecord{kBlobDead, i % 4, 200, false});
  }

  auto c_orig = SimulateGc(*vs_orig, records);
  auto c_rcvr = SimulateGc(*vs_rcvr, records);

  EXPECT_EQ(c_orig.bitmap_fast_path_skips, c_rcvr.bitmap_fast_path_skips);
  EXPECT_EQ(c_orig.bitmap_skipped_bytes, c_rcvr.bitmap_skipped_bytes);
  EXPECT_EQ(c_orig.bitmap_live_bytes, c_rcvr.bitmap_live_bytes);
  EXPECT_EQ(c_orig.bitmap_aware_blobs, c_rcvr.bitmap_aware_blobs);
  EXPECT_EQ(c_orig.bitmap_entirely_dead_blobs,
            c_rcvr.bitmap_entirely_dead_blobs);
  EXPECT_EQ(50U, c_rcvr.bitmap_fast_path_skips);

  AssertNoUserVisibleDataLoss(records, c_orig);
  AssertNoUserVisibleDataLoss(records, c_rcvr);

  ReleaseFiles(vs_orig.get());
  ReleaseFiles(vs_rcvr.get());
}

// E2E Test 4: a legacy blob (referenced by a pre-feature SST) must fall
// back SAFELY end-to-end. The GC loop must NOT fast-path it, must walk
// every record through the GetKey() path, and ZERO records may be
// dropped incorrectly: the legacy fallback is the safety net that keeps
// the feature opt-safe for mixed-version clusters.
TEST_F(BlobValidityBitmapEndToEndTest, EndToEnd_LegacyBlobFallsBackSafely) {
  using namespace e2e_testutil;
  constexpr uint64_t kBlobLegacy = 6400U;
  constexpr uint64_t kBlobNew = 6401U;

  auto vstorage = MakeVStorage(&icmp_, ucmp_);
  vstorage->AddFile(-1, MakeBlobFile(kBlobLegacy, 4 * 1024));
  vstorage->AddFile(-1, MakeBlobFile(kBlobNew, 4 * 1024));
  // Legacy SST references the legacy blob (no block bitmap).
  vstorage->AddFile(1, MakeLegacySstReferencingBlob(7300, kBlobLegacy));
  // New SST references the new blob with blocks {0, 2}.
  vstorage->AddFile(1, MakeSstReferencingBlob(7301, kBlobNew, {0, 2}));
  vstorage->AggregateBlobLiveBlockBitmaps();

  // Legacy blob: sticky-cleared -> bitmap_available=false. GC path must
  // be forced to the legacy GetKey() path.
  const auto* info_legacy = vstorage->GetBlobLiveBlockInfo(kBlobLegacy);
  ASSERT_NE(nullptr, info_legacy);
  EXPECT_FALSE(info_legacy->bitmap_available);
  using L = VersionStorageInfo::BlobBlockLiveness;
  for (uint64_t bid = 0; bid < 4; ++bid) {
    EXPECT_EQ(L::kUnknown, vstorage->IsBlockLive(kBlobLegacy, bid));
  }
  EXPECT_FALSE(vstorage->IsBlobEntirelyDead(kBlobLegacy));

  // New blob: bitmap_available=true.
  const auto* info_new = vstorage->GetBlobLiveBlockInfo(kBlobNew);
  ASSERT_NE(nullptr, info_new);
  EXPECT_TRUE(info_new->bitmap_available);
  EXPECT_EQ(2U, info_new->live_block_count);

  // Drive GC: 30 legacy-blob records (mixed live/dead per ground-truth)
  // + 10 new-blob records (live iff block in {0,2}).
  std::vector<BlobRecord> records;
  for (uint64_t i = 0; i < 30; ++i) {
    bool live = (i % 2 == 0);
    records.push_back(BlobRecord{kBlobLegacy, i % 4,
                                 /*byte_overhead=*/96, live});
  }
  for (uint64_t i = 0; i < 10; ++i) {
    uint64_t bid = i % 4;
    bool live = (bid == 0 || bid == 2);
    records.push_back(BlobRecord{kBlobNew, bid, 96, live});
  }

  auto c = SimulateGc(*vstorage, records);

  // Legacy blob NEVER enters fast path; new blob has live blocks so it
  // does not fast-path the whole blob, but records in dead blocks {1,3}
  // still bypass GetKey() via the per-record branch. Of 10 new-blob
  // records (i%4): blocks 1,3 dead. i in 0..9 -> dead at i%4 in {1,3}:
  // i=1,3,5,7,9 -> 5 records.
  EXPECT_EQ(5U, c.bitmap_fast_path_skips);
  EXPECT_EQ(5U * 96U, c.bitmap_skipped_bytes);
  EXPECT_EQ(1U, c.bitmap_aware_blobs);     // kBlobNew
  EXPECT_EQ(1U, c.bitmap_fallback_blobs);  // kBlobLegacy
  EXPECT_EQ(0U, c.bitmap_entirely_dead_blobs);
  // 30 legacy records + 5 live new-blob records reach GetKey().
  EXPECT_EQ((30U + 5U) * 96U, c.bitmap_live_bytes);

  AssertNoUserVisibleDataLoss(records, c);

  ReleaseFiles(vstorage.get());
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
