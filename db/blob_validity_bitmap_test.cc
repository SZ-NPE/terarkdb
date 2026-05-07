//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Phase 3 focused tests for the flush-path chunk reference collector
// and its interaction with TablePropertyCache::dependence_chunk_bitmaps.
//
// These tests do not spin up a DB; they exercise the same aggregation
// logic that db/builder.cc drives, which keeps them fast and stable
// regardless of table-format details.

#include <cstdint>
#include <cinttypes>
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
#include "util/blob_chunk_bitmap.h"
#include "util/coding.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class BlobValidityBitmapFlushTest : public testing::Test {
 protected:
  // Drive a flush-like sequence on a FlushChunkBitmapCollector and then
  // materialize the result into a TablePropertyCache. Mirrors exactly
  // what db/builder.cc does on the real flush path.
  void DriveFlush(
      const std::vector<std::pair<uint64_t, uint64_t>>& separated_values,
      const std::vector<Dependence>& dependence,
      FlushChunkBitmapCollector* collector, TablePropertyCache* prop) {
    for (auto& kv : separated_values) {
      collector->Observe(kv.first /* blob_file_number */,
                         kv.second /* blob_offset */);
    }
    prop->dependence = dependence;
    collector->Materialize(prop->dependence, &prop->dependence_chunk_bitmaps);
  }
};

// Test 1: Flush produces a correctly populated chunk bitmap when the
// feature is enabled and we actually observe separated values.
TEST_F(BlobValidityBitmapFlushTest,
       FlushBuildsChunkBitmapForSeparatedValues) {
  const uint64_t kChunkSize = 1024;
  FlushChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);
  TablePropertyCache prop;

  // Two blob files. Into blob#42 we put 3 values at offsets 0,
  // kChunkSize and 2*kChunkSize (so chunk ids 0, 1, 2). Into blob#43
  // we put a single value at an offset that maps to chunk 5.
  std::vector<std::pair<uint64_t, uint64_t>> separated = {
      {42, 0},
      {42, kChunkSize},
      {42, kChunkSize * 2},
      {43, kChunkSize * 5 + 37},  // mid-chunk, still chunk 5
  };
  std::vector<Dependence> dep = {
      {42, /*entry_count=*/3, /*byte_count=*/0},
      {43, /*entry_count=*/1, /*byte_count=*/0},
  };

  DriveFlush(separated, dep, &collector, &prop);

  ASSERT_EQ(prop.dependence.size(),
            prop.dependence_chunk_bitmaps.size());
  ASSERT_EQ(size_t(2), prop.dependence_chunk_bitmaps.size());

  const BlobChunkBitmap& b42 = prop.dependence_chunk_bitmaps[0];
  EXPECT_EQ(uint64_t(3), b42.CountSetBits());
  EXPECT_TRUE(b42.Test(0));
  EXPECT_TRUE(b42.Test(1));
  EXPECT_TRUE(b42.Test(2));
  EXPECT_FALSE(b42.Test(3));

  const BlobChunkBitmap& b43 = prop.dependence_chunk_bitmaps[1];
  EXPECT_EQ(uint64_t(1), b43.CountSetBits());
  EXPECT_TRUE(b43.Test(5));
  EXPECT_FALSE(b43.Test(0));
  EXPECT_FALSE(b43.Test(4));
}

// Test 2: When flush produces no separated values (e.g. a purely
// in-place SST, or a map SST, or a non-KV-separation workload), the
// per-dependence bitmap vector must come out empty (or all-empty)
// rather than noisily reporting spurious live chunks.
TEST_F(BlobValidityBitmapFlushTest,
       FlushWithoutSeparationDoesNotEmitChunkBitmap) {
  const uint64_t kChunkSize = 4096;

  // (a) Feature on, but no Observe() call. Dependence may still list
  //     blobs inherited from compaction context (empty here for
  //     simplicity). Output is resized to dep.size() with each entry
  //     being an empty bitmap.
  {
    FlushChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);
    TablePropertyCache prop;
    std::vector<Dependence> dep = {
        {100, 0, 0},
        {101, 0, 0},
    };
    DriveFlush(/*separated_values=*/{}, dep, &collector, &prop);

    ASSERT_EQ(dep.size(), prop.dependence_chunk_bitmaps.size());
    for (const auto& bm : prop.dependence_chunk_bitmaps) {
      EXPECT_TRUE(bm.empty());
      EXPECT_EQ(uint64_t(0), bm.CountSetBits());
    }
    EXPECT_TRUE(collector.empty());
  }

  // (b) Feature on, empty dependence. Output must also be empty.
  {
    FlushChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);
    TablePropertyCache prop;
    DriveFlush(/*separated_values=*/{}, /*dep=*/{}, &collector, &prop);
    EXPECT_TRUE(prop.dependence_chunk_bitmaps.empty());
  }
}

// Test 3: When the feature switch is disabled (or chunk_size is 0),
// the collector must ignore every Observe() call and Materialize()
// must clear the output vector to empty. This is the explicit
// "bitmap unavailable" contract consumed by later GC phases.
TEST_F(BlobValidityBitmapFlushTest,
       FlushLegacyPathFallbackWhenBitmapDisabled) {
  const uint64_t kChunkSize = 512;
  std::vector<std::pair<uint64_t, uint64_t>> separated = {
      {9, 0},
      {9, kChunkSize},
      {9, kChunkSize * 3},
  };
  std::vector<Dependence> dep = {
      {9, /*entry_count=*/3, /*byte_count=*/0},
  };

  // (a) Feature switch off, chunk_size non-zero.
  {
    FlushChunkBitmapCollector collector(/*enabled=*/false, kChunkSize);
    TablePropertyCache prop;

    // Pre-fill with garbage to prove Materialize clears it.
    prop.dependence_chunk_bitmaps.resize(7);
    DriveFlush(separated, dep, &collector, &prop);

    EXPECT_FALSE(collector.enabled());
    EXPECT_TRUE(collector.empty());
    EXPECT_TRUE(prop.dependence_chunk_bitmaps.empty())
        << "disabled collector must produce an empty chunk bitmap vector";
  }

  // (b) Feature switch on, but chunk_size == 0 must still degrade to
  //     disabled behaviour (division-by-zero guard + fallback).
  {
    FlushChunkBitmapCollector collector(/*enabled=*/true, /*chunk_size=*/0);
    TablePropertyCache prop;
    DriveFlush(separated, dep, &collector, &prop);

    EXPECT_FALSE(collector.enabled());
    EXPECT_TRUE(prop.dependence_chunk_bitmaps.empty());
  }
}

// Test 4: Many values funnelled into the same blob must set multiple
// distinct chunk bits (and idempotently collapse repeated hits on the
// same chunk), matching the flush pattern where a burst of inserts
// lands in the same blob file across its length.
TEST_F(BlobValidityBitmapFlushTest,
       FlushRecordsMultipleChunksForSameBlob) {
  const uint64_t kChunkSize = 256;
  const uint64_t kBlob = 77;

  FlushChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);
  TablePropertyCache prop;

  std::vector<std::pair<uint64_t, uint64_t>> separated;

  // 10 values all in chunk 0 (offsets 0..9). Must only set bit 0 once.
  for (uint64_t off = 0; off < 10; ++off) {
    separated.emplace_back(kBlob, off);
  }
  // Straddle chunks: offsets near boundaries of chunks 1, 2, 3.
  separated.emplace_back(kBlob, kChunkSize);         // chunk 1
  separated.emplace_back(kBlob, kChunkSize * 2 - 1); // chunk 1
  separated.emplace_back(kBlob, kChunkSize * 2);     // chunk 2
  separated.emplace_back(kBlob, kChunkSize * 3);     // chunk 3
  separated.emplace_back(kBlob, kChunkSize * 3 + 5); // chunk 3 (dup)
  // A far-away chunk id to prove the bitmap grows on demand.
  separated.emplace_back(kBlob, kChunkSize * 1000);  // chunk 1000

  std::vector<Dependence> dep = {{kBlob, 0, 0}};
  DriveFlush(separated, dep, &collector, &prop);

  ASSERT_EQ(size_t(1), prop.dependence_chunk_bitmaps.size());
  const BlobChunkBitmap& bm = prop.dependence_chunk_bitmaps[0];

  // Expected chunk ids: 0, 1, 2, 3, 1000. Total = 5 unique bits.
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

// Phase 4 Test 1: The compaction collector must translate chunk-aware
// value-index entries into a per-dependence chunk bitmap and surface
// them on prop.dependence_chunk_bitmaps aligned to prop.dependence.
TEST(BlobValidityBitmapCompactionTest,
     CompactionPropagatesChunkBitmapForLiveBlobReferences) {
  const uint64_t kChunkSize = 1024;
  CompactionChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);

  // Simulate compaction reading value-index entries referencing
  // blob#42 chunks {0, 1, 3} and blob#43 chunk {7}.
  collector.ObserveChunkId(42, 0);
  collector.ObserveChunkId(42, 1);
  collector.ObserveChunkId(42, 3);
  collector.ObserveChunkId(42, 1);  // duplicate must collapse
  collector.ObserveChunkId(43, 7);

  TablePropertyCache prop;
  prop.dependence = {
      {42, /*entry_count=*/3, /*byte_count=*/0},
      {43, /*entry_count=*/1, /*byte_count=*/0},
  };
  collector.Materialize(prop.dependence, &prop.dependence_chunk_bitmaps);

  ASSERT_EQ(size_t(2), prop.dependence_chunk_bitmaps.size());
  const BlobChunkBitmap& b42 = prop.dependence_chunk_bitmaps[0];
  EXPECT_EQ(uint64_t(3), b42.CountSetBits());
  EXPECT_TRUE(b42.Test(0));
  EXPECT_TRUE(b42.Test(1));
  EXPECT_FALSE(b42.Test(2));
  EXPECT_TRUE(b42.Test(3));

  const BlobChunkBitmap& b43 = prop.dependence_chunk_bitmaps[1];
  EXPECT_EQ(uint64_t(1), b43.CountSetBits());
  EXPECT_TRUE(b43.Test(7));
  EXPECT_FALSE(b43.Test(0));
}

// Phase 4 Test 2: A legacy value index (one without a chunk-id
// trailer) encountered during compaction must force the corresponding
// dependence's chunk bitmap to the "bitmap unavailable" sentinel
// (empty), even if other chunk-aware observations also landed on the
// same blob. This is the sticky-unavailable contract required by the
// GC fallback path.
TEST(BlobValidityBitmapCompactionTest,
     CompactionHandlesLegacyIndexAsBitmapUnavailable) {
  const uint64_t kChunkSize = 1024;
  CompactionChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);

  // blob#42: some chunk-aware refs PLUS a legacy ref -> must be empty.
  collector.ObserveChunkId(42, 0);
  collector.ObserveChunkId(42, 5);
  collector.MarkUnavailable(42);

  // blob#43: only legacy ref (no ObserveChunkId) -> must also be empty.
  collector.MarkUnavailable(43);

  // blob#44: only chunk-aware refs, must be preserved as normal.
  collector.ObserveChunkId(44, 2);
  collector.ObserveChunkId(44, 4);

  TablePropertyCache prop;
  prop.dependence = {
      {42, 2, 0},
      {43, 1, 0},
      {44, 2, 0},
  };
  collector.Materialize(prop.dependence, &prop.dependence_chunk_bitmaps);

  ASSERT_EQ(size_t(3), prop.dependence_chunk_bitmaps.size());
  EXPECT_TRUE(collector.IsUnavailable(42));
  EXPECT_TRUE(collector.IsUnavailable(43));
  EXPECT_FALSE(collector.IsUnavailable(44));

  // 42 and 43 must be empty (unavailable sentinel); 44 must carry the
  // collected chunks.
  EXPECT_TRUE(prop.dependence_chunk_bitmaps[0].empty());
  EXPECT_EQ(uint64_t(0), prop.dependence_chunk_bitmaps[0].CountSetBits());
  EXPECT_TRUE(prop.dependence_chunk_bitmaps[1].empty());
  EXPECT_EQ(uint64_t(0), prop.dependence_chunk_bitmaps[1].CountSetBits());

  const BlobChunkBitmap& b44 = prop.dependence_chunk_bitmaps[2];
  EXPECT_EQ(uint64_t(2), b44.CountSetBits());
  EXPECT_TRUE(b44.Test(2));
  EXPECT_TRUE(b44.Test(4));
}

// Phase 4 Test 3: A single compaction output SST is normally produced
// from multiple input SSTs; the collector must OR-merge chunk
// observations across those inputs into a single bitmap per blob.
// This test simulates the input iterator handing the collector a
// mixed interleaving of references from two upstream SSTs, and
// verifies the resulting bitmap is the union.
TEST(BlobValidityBitmapCompactionTest,
     CompactionMergesChunkRefsAcrossMultipleInputSsts) {
  const uint64_t kChunkSize = 1024;
  CompactionChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);

  // Simulate input SST A contributes chunks {0, 2, 4} for blob#11
  // and chunks {0} for blob#12.
  const std::vector<std::pair<uint64_t, uint64_t>> from_a = {
      {11, 0}, {11, 2}, {11, 4}, {12, 0},
  };
  // Simulate input SST B contributes chunks {1, 4, 5} for blob#11
  // and chunks {3} for blob#12.
  const std::vector<std::pair<uint64_t, uint64_t>> from_b = {
      {11, 1}, {11, 4}, {11, 5}, {12, 3},
  };

  // Interleave the two streams: this mirrors the order in which the
  // compaction iterator hands entries to the output builder.
  auto ia = from_a.begin();
  auto ib = from_b.begin();
  while (ia != from_a.end() || ib != from_b.end()) {
    if (ia != from_a.end()) {
      collector.ObserveChunkId(ia->first, ia->second);
      ++ia;
    }
    if (ib != from_b.end()) {
      collector.ObserveChunkId(ib->first, ib->second);
      ++ib;
    }
  }

  TablePropertyCache prop;
  prop.dependence = {
      {11, /*entry_count=*/6, /*byte_count=*/0},
      {12, /*entry_count=*/2, /*byte_count=*/0},
  };
  collector.Materialize(prop.dependence, &prop.dependence_chunk_bitmaps);

  ASSERT_EQ(size_t(2), prop.dependence_chunk_bitmaps.size());

  // blob#11 union should be {0,1,2,4,5} (chunk 4 dedup'd across inputs).
  const BlobChunkBitmap& b11 = prop.dependence_chunk_bitmaps[0];
  EXPECT_EQ(uint64_t(5), b11.CountSetBits());
  EXPECT_TRUE(b11.Test(0));
  EXPECT_TRUE(b11.Test(1));
  EXPECT_TRUE(b11.Test(2));
  EXPECT_FALSE(b11.Test(3));
  EXPECT_TRUE(b11.Test(4));
  EXPECT_TRUE(b11.Test(5));

  // blob#12 union should be {0,3}.
  const BlobChunkBitmap& b12 = prop.dependence_chunk_bitmaps[1];
  EXPECT_EQ(uint64_t(2), b12.CountSetBits());
  EXPECT_TRUE(b12.Test(0));
  EXPECT_TRUE(b12.Test(3));
}

// Phase 4 Test 4: Running the same compaction twice on a canonical
// input stream must produce byte-identical chunk bitmap output. This
// is what allows downstream phases (manifest persistence in Phase 5
// and live-view aggregation in Phase 6) to reason about the bitmap
// as a deterministic function of its input.
TEST(BlobValidityBitmapCompactionTest,
     CompactionOutputBitmapStableAcrossRepeatedRuns) {
  const uint64_t kChunkSize = 1024;

  // The canonical input stream. Intentionally includes duplicates,
  // a legacy dependence (blob#50), and out-of-order chunk ids to
  // shake out any hidden ordering dependence in the collector.
  struct Entry {
    enum class Kind { kChunk, kLegacy } kind;
    uint64_t blob_fn;
    uint64_t chunk_id;  // only for kChunk
  };
  const std::vector<Entry> stream = {
      {Entry::Kind::kChunk, 10, 7},
      {Entry::Kind::kChunk, 10, 0},
      {Entry::Kind::kChunk, 10, 3},
      {Entry::Kind::kChunk, 10, 7},  // dup
      {Entry::Kind::kLegacy, 50, 0},
      {Entry::Kind::kChunk, 10, 5},
      {Entry::Kind::kChunk, 20, 2},
      {Entry::Kind::kLegacy, 50, 0},  // dup legacy
      {Entry::Kind::kChunk, 20, 8},
      {Entry::Kind::kChunk, 20, 8},  // dup
  };
  const std::vector<Dependence> dep = {
      {10, 0, 0},
      {20, 0, 0},
      {50, 0, 0},
  };

  auto run_once = [&]() {
    CompactionChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);
    for (const auto& e : stream) {
      if (e.kind == Entry::Kind::kChunk) {
        collector.ObserveChunkId(e.blob_fn, e.chunk_id);
      } else {
        collector.MarkUnavailable(e.blob_fn);
      }
    }
    TablePropertyCache prop;
    prop.dependence = dep;
    collector.Materialize(prop.dependence, &prop.dependence_chunk_bitmaps);

    // Serialize the entire chunk-bitmap vector into a single byte
    // string so we can byte-compare runs. This mirrors what Phase 5
    // will do when persisting the bitmap into the SST property block.
    std::string blob;
    for (const auto& bm : prop.dependence_chunk_bitmaps) {
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

  // Also spot-check the logical content of one run: chunks {0,3,5,7}
  // on blob 10, chunks {2,8} on blob 20, and empty/unavailable on 50.
  CompactionChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);
  for (const auto& e : stream) {
    if (e.kind == Entry::Kind::kChunk) {
      collector.ObserveChunkId(e.blob_fn, e.chunk_id);
    } else {
      collector.MarkUnavailable(e.blob_fn);
    }
  }
  TablePropertyCache prop;
  prop.dependence = dep;
  collector.Materialize(prop.dependence, &prop.dependence_chunk_bitmaps);

  ASSERT_EQ(size_t(3), prop.dependence_chunk_bitmaps.size());
  EXPECT_EQ(uint64_t(4), prop.dependence_chunk_bitmaps[0].CountSetBits());
  EXPECT_EQ(uint64_t(2), prop.dependence_chunk_bitmaps[1].CountSetBits());
  EXPECT_TRUE(prop.dependence_chunk_bitmaps[2].empty());
}

// Phase 5 Test 3: TableProperties round-trip with chunk bitmap
// payload. We exercise the same encode/decode format that
// PropertyBlockBuilder and ReadProperties use in table/meta_blocks.cc,
// so this test pins down the wire contract independently of the SST
// format. A per-row empty payload (== empty BlobChunkBitmap) must
// survive the round-trip as an empty BlobChunkBitmap on the consumer
// side, which is the per-row "bitmap unavailable" signal.
namespace phase5_testutil {

// Encode: mirror of meta_blocks.cc PropertyBlockBuilder::AddTableProperty
// for the kDependenceChunkBitmaps branch.
std::string EncodeChunkBitmapPayload(
    const std::vector<std::string>& serialized_bitmaps) {
  std::string payload;
  PutVarint64(&payload, static_cast<uint64_t>(serialized_bitmaps.size()));
  for (const auto& b : serialized_bitmaps) {
    PutVarint64(&payload, static_cast<uint64_t>(b.size()));
    payload.append(b);
  }
  return payload;
}

// Decode: mirror of meta_blocks.cc ReadProperties kDependenceChunkBitmaps
// branch. Returns true on success.
bool DecodeChunkBitmapPayload(const std::string& payload,
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

}  // namespace phase5_testutil

TEST(BlobValidityBitmapPhase5Test, TablePropertiesRoundTripWithChunkBitmap) {
  // Build three chunk bitmaps: two are populated with distinct
  // patterns, one is empty (per-row "bitmap unavailable").
  BlobChunkBitmap bm0;
  bm0.Set(0);
  bm0.Set(1);
  bm0.Set(63);
  bm0.Set(64);
  bm0.Set(65);
  BlobChunkBitmap bm1;  // empty on purpose
  BlobChunkBitmap bm2;
  bm2.Set(7);
  bm2.Set(9999);

  // Writer side: simulate what TableBuilder does when turning
  // TablePropertyCache::dependence_chunk_bitmaps into the
  // serializable TableProperties::dependence_chunk_bitmaps.
  std::vector<std::string> serialized;
  {
    std::string s;
    bm0.Serialize(&s);
    serialized.emplace_back(std::move(s));
  }
  serialized.emplace_back();  // empty string <-> empty BlobChunkBitmap
  {
    std::string s;
    bm2.Serialize(&s);
    serialized.emplace_back(std::move(s));
  }

  // Writer packs it into the kDependenceChunkBitmaps property value.
  std::string payload = phase5_testutil::EncodeChunkBitmapPayload(serialized);

  // Reader unpacks it back.
  std::vector<std::string> reparsed;
  ASSERT_TRUE(phase5_testutil::DecodeChunkBitmapPayload(payload, &reparsed));
  ASSERT_EQ(serialized.size(), reparsed.size());
  for (size_t i = 0; i < serialized.size(); ++i) {
    EXPECT_EQ(serialized[i], reparsed[i]) << "row " << i;
  }

  // Finally, reinflating the byte strings into BlobChunkBitmap (what
  // compaction_job.cc / repair.cc do after ReadProperties) must
  // recover the original logical content.
  std::vector<BlobChunkBitmap> round_tripped(reparsed.size());
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

  // Also verify that a payload produced by an old writer (no chunk
  // bitmap property at all) decodes as an empty bitmap vector, which
  // is the "bitmap unavailable" sentinel that triggers GC fallback.
  std::vector<std::string> absent_reparsed;
  std::string absent_payload =
      phase5_testutil::EncodeChunkBitmapPayload({});
  ASSERT_TRUE(phase5_testutil::DecodeChunkBitmapPayload(absent_payload,
                                                       &absent_reparsed));
  EXPECT_TRUE(absent_reparsed.empty());
}

// Phase 5 Test 4: Manifest recovery must restore
// dependence_chunk_bitmaps from the persisted kPropertyCache payload
// and do so for every file in the edit. Mixed-file layouts (some
// files have bitmap, some do not, some have per-row empty bitmap)
// must round-trip independently on each file.
TEST(BlobValidityBitmapPhase5Test, ManifestRecoveryRestoresChunkBitmapMetadata) {
  static const uint64_t kBig = 1ull << 50;

  VersionEdit edit;

  // File A: two dependencies, both with populated chunk bitmaps.
  {
    TablePropertyCache prop;
    prop.purpose = 1;
    prop.num_entries = 1;
    prop.raw_key_size = 1;
    prop.raw_value_size = 1;
    prop.dependence = {Dependence{100, 5, 2048}, Dependence{101, 7, 4096}};
    prop.dependence_chunk_bitmaps.resize(2);
    prop.dependence_chunk_bitmaps[0].Set(0);
    prop.dependence_chunk_bitmaps[0].Set(3);
    prop.dependence_chunk_bitmaps[1].Set(1);
    prop.dependence_chunk_bitmaps[1].Set(2);
    prop.dependence_chunk_bitmaps[1].Set(100);
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
    prop.dependence_chunk_bitmaps.resize(3);
    prop.dependence_chunk_bitmaps[1].Set(4);
    prop.dependence_chunk_bitmaps[1].Set(5);
    edit.AddFile(3, 301, 0, 100, InternalKey("m0", kBig + 3, kTypeValue),
                 InternalKey("m1", kBig + 4, kTypeDeletion), kBig + 3, kBig + 4,
                 false, prop);
  }

  // File C: legacy-style file that does not carry any chunk bitmap at
  // all (empty vector). Decoder must keep it empty (== bitmap
  // unavailable for the whole SST).
  {
    TablePropertyCache prop;
    prop.purpose = 1;
    prop.num_entries = 1;
    prop.raw_key_size = 1;
    prop.raw_value_size = 1;
    prop.dependence = {Dependence{300, 1, 0}};
    // dependence_chunk_bitmaps intentionally left empty.
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
    ASSERT_EQ(2U, p.dependence_chunk_bitmaps.size());
    EXPECT_EQ(uint64_t(2), p.dependence_chunk_bitmaps[0].CountSetBits());
    EXPECT_TRUE(p.dependence_chunk_bitmaps[0].Test(0));
    EXPECT_TRUE(p.dependence_chunk_bitmaps[0].Test(3));
    EXPECT_EQ(uint64_t(3), p.dependence_chunk_bitmaps[1].CountSetBits());
    EXPECT_TRUE(p.dependence_chunk_bitmaps[1].Test(100));
  }
  // File B: mixed case preserved.
  {
    auto& p = files[1].second.prop;
    ASSERT_EQ(3U, p.dependence.size());
    ASSERT_EQ(3U, p.dependence_chunk_bitmaps.size());
    EXPECT_TRUE(p.dependence_chunk_bitmaps[0].empty());
    EXPECT_EQ(uint64_t(2), p.dependence_chunk_bitmaps[1].CountSetBits());
    EXPECT_TRUE(p.dependence_chunk_bitmaps[1].Test(4));
    EXPECT_TRUE(p.dependence_chunk_bitmaps[1].Test(5));
    EXPECT_TRUE(p.dependence_chunk_bitmaps[2].empty());
  }
  // File C: legacy path, full-SST bitmap unavailable.
  {
    auto& p = files[2].second.prop;
    ASSERT_EQ(1U, p.dependence.size());
    EXPECT_TRUE(p.dependence_chunk_bitmaps.empty());
  }

  // Stability: re-encoding the decoded edit should be byte-equal, so
  // a second-generation manifest keeps the exact same format.
  std::string re_encoded;
  ASSERT_TRUE(parsed.EncodeTo(&re_encoded));
  ASSERT_EQ(encoded, re_encoded);
}

// =================================================================
// Phase 10: focused end-to-end tests for the chunk-level blob GC
// pipeline. These tests stitch together every stage of the feature:
//
//   Phase 3  FlushChunkBitmapCollector
//     -> records which blob chunks each SST references at flush time
//   Phase 4  TablePropertyCache::dependence_chunk_bitmaps
//     -> the bitmap travels inside the SST's table property cache
//   Phase 5  VersionEdit manifest encode/decode
//     -> the bitmap survives a full manifest round-trip
//   Phase 6  VersionStorageInfo::AggregateBlobLiveChunkBitmaps
//     -> aggregated per-blob live-chunk view, sticky-clear on legacy
//   Phase 7  VersionStorageInfo::IsChunkLive / IsBlobEntirelyDead
//     -> fast-path decision contract used by ProcessGarbageCollection
//   Phase 8  GC bitmap tickers
//     -> RecordTick block at the end of ProcessGarbageCollection
//
// To keep the tests fast, deterministic, and free of DBImpl/
// background-thread flakiness, we do NOT spin up a real DB. Instead
// each test drives the same sequence of components that
// db/builder.cc, db/version_edit.cc, db/version_set.cc and
// db/compaction_job.cc would drive on the production write->GC
// path. The GC fast-path gate is replayed by a small
// `SimulateGcOnBlob` helper that is a line-for-line mirror of the
// hot section of ProcessGarbageCollection: for each record, consult
// `IsBlobEntirelyDead` once per blob and short-circuit, otherwise
// fall back to the legacy GetKey()-path and count toward
// bitmap_live_bytes.
//
// User-visible data correctness is validated through an explicit
// "ground truth" liveness map built at write time: any record the
// simulator decides to drop (fast path + dead) must also be
// genuinely dead in the ground-truth; any record kept must not be
// corrupted or reordered. This reproduces the real-world contract
// users rely on ("bitmap fast path must never silently drop a live
// record").
// =================================================================

namespace phase10_testutil {

// Minimal record description used by the Phase 10 simulator. Mirrors
// the fields ProcessGarbageCollection's inner loop reads off each
// value-index entry. We keep it intentionally tight (no real LSM
// keys, no seqno) because the Phase 7 gate operates at blob-file
// granularity, not per-key.
struct BlobRecord {
  uint64_t blob_file_number;
  uint64_t chunk_id;
  uint64_t byte_overhead;  // key+value payload size
  bool ground_truth_live;  // what a correct GC MUST keep
};

// Build a FileMetaData that references blob_file with the given
// chunk bitmap. `file_size` and seqno are tuned loosely to pass
// VersionStorageInfo's consistency checks (disabled in tests but
// still required to be sane). Caller owns the returned pointer
// and must release it after the VersionStorageInfo teardown.
FileMetaData* MakeSstReferencingBlob(
    uint64_t sst_file_number, uint64_t blob_file_number,
    std::vector<uint64_t> chunk_ids_referenced, uint64_t file_size = 1024) {
  auto* f = new FileMetaData;
  f->fd = FileDescriptor(sst_file_number, /*path_id=*/0, file_size);
  // Use a per-file, non-overlapping key range so the VersionStorageInfo
  // consistency check at level>0 is satisfied when multiple SSTs are
  // added at the same level.
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
  BlobChunkBitmap bm;
  for (uint64_t cid : chunk_ids_referenced) bm.Set(cid);
  f->prop.dependence_chunk_bitmaps.emplace_back(std::move(bm));
  f->prop.num_entries = static_cast<uint64_t>(chunk_ids_referenced.size());
  f->gc_status = FileMetaData::kGarbageCollectionForbidden;
  return f;
}

// Build a FileMetaData that references blob_file in the LEGACY
// regime (no dependence_chunk_bitmaps). This mimics an SST flushed
// by a pre-Phase-4 binary.
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
  // Intentionally leave dependence_chunk_bitmaps empty -> legacy.
  f->prop.num_entries = 1;
  f->gc_status = FileMetaData::kGarbageCollectionForbidden;
  return f;
}

// Build a blob FileMetaData at hidden level -1 sized to hold
// `num_chunks` logical chunks under the given chunk_size.
FileMetaData* MakeBlobFile(uint64_t blob_file_number, uint64_t chunk_size,
                           uint64_t num_chunks) {
  auto* f = new FileMetaData;
  f->fd =
      FileDescriptor(blob_file_number, 0, chunk_size * num_chunks);
  f->smallest = InternalKey("blobstart", 100, kTypeValue);
  f->largest = InternalKey("blobend", 200, kTypeValue);
  f->fd.smallest_seqno = 100;
  f->fd.largest_seqno = 200;
  f->compensated_file_size = chunk_size * num_chunks;
  f->refs = 0;
  f->prop.num_entries = num_chunks;
  f->gc_status = FileMetaData::kGarbageCollectionForbidden;
  return f;
}

// Counters the simulator produces. These mirror 1:1 the subset of
// the `counter` struct in compaction_job.cc::ProcessGarbageCollection
// that Phase 8 surfaces as observability tickers, so downstream
// assertions can compare them against ticker values after a
// RecordTick replay.
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

// Replays the Phase 7 fast-path gate and the Phase 8 ticker emit
// site over a synthetic record stream. Per-blob IsBlobEntirelyDead
// is consulted exactly once (cached) -- this matches
// compaction_job.cc's `blob_meta_cache`.
//
// For a record where the blob is entirely dead, the simulator
// short-circuits and increments fast-path counters. For all other
// records it walks the "legacy GetKey()" path, consults the
// per-record ground_truth_live flag (our stand-in for
// `input_version->GetKey()`), accumulates live bytes, and either
// keeps or drops the record.
//
// Returns the accumulated counters plus the ordered list of kept
// records, so the caller can assert both the ticker contract AND
// the user-visible data invariants in a single pass.
SimulatedGcCounters SimulateGc(const VersionStorageInfo& vstorage,
                                const std::vector<BlobRecord>& records) {
  SimulatedGcCounters c;
  // (blob_fn -> entirely_dead) cache, exactly like compaction_job.cc.
  struct CacheEntry {
    uint64_t blob_fn;
    bool entirely_dead;
    bool bitmap_aware;
  };
  std::vector<CacheEntry> cache;

  for (const BlobRecord& r : records) {
    ++c.input;
    auto it = std::find_if(cache.begin(), cache.end(),
                           [&](const CacheEntry& e) {
                             return e.blob_fn == r.blob_file_number;
                           });
    bool entirely_dead = false;
    if (it == cache.end()) {
      const auto* info = vstorage.GetBlobLiveChunkInfo(r.blob_file_number);
      bool aware = info != nullptr && info->bitmap_available;
      if (aware) {
        ++c.bitmap_aware_blobs;
        if (info->live_chunk_count == 0) {
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
      // Phase 7 fast path: skip the GetKey() lookup altogether.
      ++c.bitmap_fast_path_skips;
      c.bitmap_skipped_bytes += r.byte_overhead;
      // Record is considered dead and dropped.
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

// Exact replay of compaction_job.cc's Phase 8 RecordTick block.
void EmitPhase10Tickers(Statistics* s, const SimulatedGcCounters& c) {
  RecordTick(s, GC_BITMAP_FAST_PATH_COUNT, c.bitmap_fast_path_skips);
  RecordTick(s, GC_BITMAP_FALLBACK_COUNT, c.bitmap_fallback_blobs);
  RecordTick(s, GC_SKIPPED_DEAD_CHUNK_BYTES, c.bitmap_skipped_bytes);
  RecordTick(s, GC_READ_LIVE_CHUNK_BYTES, c.bitmap_live_bytes);
  RecordTick(s, GC_LOOKUP_AVOIDED_COUNT, c.bitmap_fast_path_skips);
}

// Ground-truth invariant: every record the simulator KEPT must have
// been genuinely live, AND every genuinely live record must have
// ended up in kept_records. This is the "user-visible data is never
// corrupted" assertion Phase 10 is explicitly in charge of.
void AssertNoUserVisibleDataLoss(const std::vector<BlobRecord>& inputs,
                                 const SimulatedGcCounters& out) {
  // Every kept record must be ground-truth live.
  for (const BlobRecord& r : out.kept_records) {
    ASSERT_TRUE(r.ground_truth_live)
        << "GC kept a record that was not ground-truth live "
        << "(blob=" << r.blob_file_number << ", chunk=" << r.chunk_id << ")";
  }
  // Every ground-truth live record must be in kept_records (count +
  // per-record equality, order preserved by the simulator).
  std::vector<BlobRecord> expected_kept;
  for (const BlobRecord& r : inputs) {
    if (r.ground_truth_live) expected_kept.push_back(r);
  }
  ASSERT_EQ(expected_kept.size(), out.kept_records.size())
      << "GC dropped or duplicated records vs. ground truth";
  for (size_t i = 0; i < expected_kept.size(); ++i) {
    EXPECT_EQ(expected_kept[i].blob_file_number,
              out.kept_records[i].blob_file_number);
    EXPECT_EQ(expected_kept[i].chunk_id, out.kept_records[i].chunk_id);
  }
}

// Test fixture factory: new empty VersionStorageInfo.
std::unique_ptr<VersionStorageInfo> MakeVStorage(
    const InternalKeyComparator* icmp, const Comparator* ucmp) {
  Options tmp_opts;
  return std::unique_ptr<VersionStorageInfo>(
      new VersionStorageInfo(icmp, ucmp, tmp_opts.num_levels,
                             kCompactionStyleLevel,
                             /*force_consistency_checks=*/false));
}

}  // namespace phase10_testutil

class BlobValidityBitmapPhase10Test : public testing::Test {
 protected:
  BlobValidityBitmapPhase10Test()
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

// Phase 10 Test 1: the full flush->compaction->GC pipeline. A flush
// writes separated values into blob B1. The FlushChunkBitmapCollector
// builds a chunk bitmap for B1 that Phase 4 puts in the SST's
// TablePropertyCache. Phase 5 round-trips it through a manifest.
// Phase 6 aggregates into VersionStorageInfo. Phase 7 marks the blob
// as entirely dead (no chunks referenced means every chunk is dead
// in the aggregated view). Phase 8 tickers reflect fast-path wins.
// And finally: user-visible data correctness is preserved -- every
// record the simulator drops was genuinely dead; no live record is
// ever silently dropped.
TEST_F(BlobValidityBitmapPhase10Test,
       EndToEnd_FlushCompactionBuildsBitmapThenGcUsesFastPath) {
  using namespace phase10_testutil;
  constexpr uint64_t kChunkSize = 1024;
  constexpr uint64_t kBlobFn = 4200U;
  constexpr uint64_t kSstFn = 5200U;
  constexpr uint64_t kNumChunks = 8;

  // --- Phase 3: flush observes separated-value offsets ---
  FlushChunkBitmapCollector collector(/*enabled=*/true, kChunkSize);
  // Simulate a flush that writes 3 separated values into blob B1,
  // landing in chunks {0, 3, 5}.
  collector.Observe(kBlobFn, /*offset=*/0);
  collector.Observe(kBlobFn, /*offset=*/3 * kChunkSize + 128);
  collector.Observe(kBlobFn, /*offset=*/5 * kChunkSize);

  // --- Phase 4: collector materializes into TablePropertyCache ---
  TablePropertyCache prop;
  prop.dependence = {{kBlobFn, /*ec=*/3, /*bc=*/0}};
  collector.Materialize(prop.dependence, &prop.dependence_chunk_bitmaps);
  ASSERT_EQ(1U, prop.dependence_chunk_bitmaps.size());
  EXPECT_EQ(3U, prop.dependence_chunk_bitmaps[0].CountSetBits());
  EXPECT_TRUE(prop.dependence_chunk_bitmaps[0].Test(0));
  EXPECT_TRUE(prop.dependence_chunk_bitmaps[0].Test(3));
  EXPECT_TRUE(prop.dependence_chunk_bitmaps[0].Test(5));

  // --- Phase 5: VersionEdit encode/decode (manifest round-trip) ---
  VersionEdit edit;
  InternalKey sm("a", 100, kTypeValue);
  InternalKey lg("z", 200, kTypeValue);
  edit.AddFile(/*level=*/1, kSstFn, /*path_id=*/0, /*file_size=*/1024, sm,
               lg, /*smallest_seqno=*/100, /*largest_seqno=*/200,
               /*marked_for_compaction=*/false, prop);
  std::string bytes;
  ASSERT_TRUE(edit.EncodeTo(&bytes));
  VersionEdit recovered;
  ASSERT_OK(recovered.DecodeFrom(bytes));
  ASSERT_EQ(1U, recovered.GetNewFiles().size());
  const auto& rec_prop = recovered.GetNewFiles()[0].second.prop;
  ASSERT_EQ(1U, rec_prop.dependence_chunk_bitmaps.size());
  EXPECT_EQ(3U, rec_prop.dependence_chunk_bitmaps[0].CountSetBits());

  // --- Phase 6: install blob + SST into VersionStorageInfo and
  //     aggregate.
  auto vstorage = MakeVStorage(&icmp_, ucmp_);
  vstorage->AddFile(/*level=*/-1,
                    MakeBlobFile(kBlobFn, kChunkSize, kNumChunks));
  // Attach the recovered SST to level 1 so it contributes its chunk
  // bitmap during aggregation.
  auto* sst_meta = new FileMetaData(recovered.GetNewFiles()[0].second);
  sst_meta->refs = 0;
  vstorage->AddFile(/*level=*/1, sst_meta);
  vstorage->AggregateBlobLiveChunkBitmaps(kChunkSize);

  const auto* info = vstorage->GetBlobLiveChunkInfo(kBlobFn);
  ASSERT_NE(nullptr, info);
  EXPECT_TRUE(info->bitmap_available);
  EXPECT_EQ(3U, info->live_chunk_count);
  EXPECT_EQ(5U, info->dead_chunk_count);  // 8 total - 3 live

  // --- Phase 7 sanity: the 3 live chunks must read as kLive and the
  //     5 dead chunks must read as kDead, so the GC loop can
  //     skip-only-the-dead safely.
  using L = VersionStorageInfo::BlobChunkLiveness;
  for (uint64_t cid : {0U, 3U, 5U}) {
    EXPECT_EQ(L::kLive, vstorage->IsChunkLive(kBlobFn, cid));
  }
  for (uint64_t cid : {1U, 2U, 4U, 6U, 7U}) {
    EXPECT_EQ(L::kDead, vstorage->IsChunkLive(kBlobFn, cid));
  }
  EXPECT_FALSE(vstorage->IsBlobEntirelyDead(kBlobFn));

  // --- End-to-end: drive a GC stream of 12 records across all 8
  //     chunks. ground-truth live iff chunk ∈ {0, 3, 5}. Even though
  //     the Phase 7 gate here cannot short-circuit the *whole* blob
  //     (because some chunks are live), it still correctly answers
  //     per-chunk; the simulator falls back to GetKey() and our
  //     ground-truth drives the kept set.
  std::vector<BlobRecord> records;
  for (uint64_t cid = 0; cid < kNumChunks; ++cid) {
    bool live = (cid == 0 || cid == 3 || cid == 5);
    records.push_back(
        BlobRecord{kBlobFn, cid, /*byte_overhead=*/128, live});
  }
  // Add a few extra records to stress per-record accounting.
  for (uint64_t cid : {0U, 3U, 5U, 1U}) {
    bool live = (cid == 0 || cid == 3 || cid == 5);
    records.push_back(BlobRecord{kBlobFn, cid, 200, live});
  }

  auto stats = CreateDBStatistics();
  auto c = SimulateGc(*vstorage, records);
  EmitPhase10Tickers(stats.get(), c);

  // This blob has live chunks, so the whole-blob fast path does NOT
  // fire; every record goes through the per-record legacy path.
  EXPECT_EQ(1U, c.bitmap_aware_blobs);
  EXPECT_EQ(0U, c.bitmap_fallback_blobs);
  EXPECT_EQ(0U, c.bitmap_entirely_dead_blobs);
  EXPECT_EQ(0U, c.bitmap_fast_path_skips);
  EXPECT_EQ(0U, c.bitmap_skipped_bytes);
  // live_bytes = sum of overheads of all records since none skipped.
  uint64_t expected_live_bytes = 0;
  for (const auto& r : records) expected_live_bytes += r.byte_overhead;
  EXPECT_EQ(expected_live_bytes, c.bitmap_live_bytes);

  // Tickers reflect the same.
  EXPECT_EQ(0U, stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));
  EXPECT_EQ(expected_live_bytes,
            stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));

  // User-visible correctness: every live record kept, no dead kept.
  AssertNoUserVisibleDataLoss(records, c);

  ReleaseFiles(vstorage.get());
}

// Phase 10 Test 2: update-heavy workload. The same user-facing keys
// are updated many times. Older SSTs still reference older blobs
// whose chunks are no longer referenced by anyone. After aggregation
// some blobs end up with ZERO live chunks -> IsBlobEntirelyDead
// returns true -> GC fast-path skips every record in those blobs
// without a GetKey() call. This is the scenario where the feature
// delivers its largest wins, and the test locks that contract.
TEST_F(BlobValidityBitmapPhase10Test,
       EndToEnd_UpdateHeavyWorkloadAvoidsLookupOnGc) {
  using namespace phase10_testutil;
  constexpr uint64_t kChunkSize = 1024;
  constexpr uint64_t kBlobDead = 6000U;   // updated away, no refs
  constexpr uint64_t kBlobLive = 6001U;   // still referenced
  constexpr uint64_t kChunksPerBlob = 8;

  // Drive two separate flushes:
  //   * flush F1 referenced blob kBlobLive chunks {2, 4}
  //   * flush F2 referenced blob kBlobLive chunks {4, 7}
  // Note: nothing in the current version references kBlobDead. This
  // mirrors the real-world update-heavy pattern where user keys were
  // rewritten through newer blobs and an old blob is now 100% dead.
  FlushChunkBitmapCollector c1(true, kChunkSize);
  c1.Observe(kBlobLive, 2 * kChunkSize);
  c1.Observe(kBlobLive, 4 * kChunkSize);
  TablePropertyCache p1;
  p1.dependence = {{kBlobLive, 2, 0}};
  c1.Materialize(p1.dependence, &p1.dependence_chunk_bitmaps);

  FlushChunkBitmapCollector c2(true, kChunkSize);
  c2.Observe(kBlobLive, 4 * kChunkSize);
  c2.Observe(kBlobLive, 7 * kChunkSize);
  TablePropertyCache p2;
  p2.dependence = {{kBlobLive, 2, 0}};
  c2.Materialize(p2.dependence, &p2.dependence_chunk_bitmaps);

  // Also install a reference-less dead blob into the version: no SST
  // touches it. Aggregation must treat it as bitmap_available=true
  // with zero live chunks -> IsBlobEntirelyDead == true.
  auto vstorage = MakeVStorage(&icmp_, ucmp_);
  vstorage->AddFile(-1, MakeBlobFile(kBlobDead, kChunkSize, kChunksPerBlob));
  vstorage->AddFile(-1, MakeBlobFile(kBlobLive, kChunkSize, kChunksPerBlob));
  vstorage->AddFile(1, MakeSstReferencingBlob(7100, kBlobLive, {2, 4}));
  vstorage->AddFile(1, MakeSstReferencingBlob(7101, kBlobLive, {4, 7}));

  vstorage->AggregateBlobLiveChunkBitmaps(kChunkSize);

  // Live blob: chunks {2, 4, 7} are live (union of both SSTs).
  const auto* info_live = vstorage->GetBlobLiveChunkInfo(kBlobLive);
  ASSERT_NE(nullptr, info_live);
  EXPECT_TRUE(info_live->bitmap_available);
  EXPECT_EQ(3U, info_live->live_chunk_count);
  EXPECT_EQ(5U, info_live->dead_chunk_count);
  EXPECT_FALSE(vstorage->IsBlobEntirelyDead(kBlobLive));

  // Dead blob: zero SST references -> the aggregation step never
  // sticky-clears it (no legacy reference); it stays bitmap_available
  // with live_chunk_count=0 -> IsBlobEntirelyDead == true.
  const auto* info_dead = vstorage->GetBlobLiveChunkInfo(kBlobDead);
  ASSERT_NE(nullptr, info_dead);
  EXPECT_TRUE(info_dead->bitmap_available);
  EXPECT_EQ(0U, info_dead->live_chunk_count);
  EXPECT_TRUE(vstorage->IsBlobEntirelyDead(kBlobDead));

  // Drive GC over 200 records against the dead blob (all dead in
  // ground truth) plus 20 records against the live blob (live iff
  // chunk ∈ {2, 4, 7}). The dead blob should be skipped wholesale;
  // the live blob should fall back to the legacy path.
  std::vector<BlobRecord> records;
  for (uint64_t i = 0; i < 200; ++i) {
    records.push_back(BlobRecord{kBlobDead, i % kChunksPerBlob,
                                 /*byte_overhead=*/64,
                                 /*ground_truth_live=*/false});
  }
  for (uint64_t i = 0; i < 20; ++i) {
    uint64_t cid = i % kChunksPerBlob;
    bool live = (cid == 2 || cid == 4 || cid == 7);
    records.push_back(BlobRecord{kBlobLive, cid, 64, live});
  }

  auto stats = CreateDBStatistics();
  auto result = SimulateGc(*vstorage, records);
  EmitPhase10Tickers(stats.get(), result);

  // Hard invariants on ticker values:
  EXPECT_EQ(200U, result.bitmap_fast_path_skips)
      << "every record in the entirely-dead blob must be fast-path-skipped";
  EXPECT_EQ(200U * 64U, result.bitmap_skipped_bytes);
  EXPECT_EQ(2U, result.bitmap_aware_blobs);
  EXPECT_EQ(1U, result.bitmap_entirely_dead_blobs);
  EXPECT_EQ(0U, result.bitmap_fallback_blobs);
  // Live-blob records went through the GetKey() path, all 20 of them.
  EXPECT_EQ(20U * 64U, result.bitmap_live_bytes);

  EXPECT_EQ(200U, stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));
  EXPECT_EQ(200U, stats->getTickerCount(GC_LOOKUP_AVOIDED_COUNT))
      << "GC_LOOKUP_AVOIDED_COUNT must mirror fast-path skips -- that "
         "IS the business value this feature claims";
  EXPECT_EQ(200U * 64U,
            stats->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES));
  EXPECT_EQ(20U * 64U,
            stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));

  // User-visible correctness: nothing live dropped.
  AssertNoUserVisibleDataLoss(records, result);

  ReleaseFiles(vstorage.get());
}

// Phase 10 Test 3: recovery keeps bitmap usable end-to-end. We
// drive the full flush -> manifest -> version-aggregate pipeline,
// then discard the original version and rebuild a fresh one by
// re-decoding the manifest bytes. The rebuilt version must drive
// GC identically to the original -- tickers reflect the same
// fast-path wins and no user-visible record is dropped.
TEST_F(BlobValidityBitmapPhase10Test, EndToEnd_RecoveryKeepsBitmapUsable) {
  using namespace phase10_testutil;
  constexpr uint64_t kChunkSize = 1024;
  constexpr uint64_t kBlobDead = 6200U;  // no refs -> entirely dead
  constexpr uint64_t kChunksPerBlob = 4;

  // We build a VersionEdit that adds the DEAD blob + an unrelated
  // SST that does NOT reference it, then do an encode/decode round.
  // The rebuilt vstorage must still find kBlobDead entirely dead.
  TablePropertyCache prop_unrelated;
  // The SST references a DIFFERENT blob for which we also add a row.
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
    vs->AddFile(-1, MakeBlobFile(kBlobDead, kChunkSize, kChunksPerBlob));
    VersionEdit& e = use_recovered ? recovered : edit;
    auto* sst_meta = new FileMetaData(e.GetNewFiles()[0].second);
    sst_meta->refs = 0;
    vs->AddFile(1, sst_meta);
    vs->AggregateBlobLiveChunkBitmaps(kChunkSize);
    return vs;
  };

  auto vs_orig = build_vstorage(/*use_recovered=*/false);
  auto vs_rcvr = build_vstorage(/*use_recovered=*/true);

  // Both versions must agree: kBlobDead is entirely dead.
  EXPECT_TRUE(vs_orig->IsBlobEntirelyDead(kBlobDead));
  EXPECT_TRUE(vs_rcvr->IsBlobEntirelyDead(kBlobDead));

  std::vector<BlobRecord> records;
  for (uint64_t i = 0; i < 50; ++i) {
    records.push_back(BlobRecord{kBlobDead, i % kChunksPerBlob, 200, false});
  }

  auto stats_orig = CreateDBStatistics();
  auto c_orig = SimulateGc(*vs_orig, records);
  EmitPhase10Tickers(stats_orig.get(), c_orig);

  auto stats_rcvr = CreateDBStatistics();
  auto c_rcvr = SimulateGc(*vs_rcvr, records);
  EmitPhase10Tickers(stats_rcvr.get(), c_rcvr);

  // Ticker contract must be identical across original vs. recovered.
  EXPECT_EQ(c_orig.bitmap_fast_path_skips, c_rcvr.bitmap_fast_path_skips);
  EXPECT_EQ(c_orig.bitmap_skipped_bytes, c_rcvr.bitmap_skipped_bytes);
  EXPECT_EQ(c_orig.bitmap_live_bytes, c_rcvr.bitmap_live_bytes);
  EXPECT_EQ(c_orig.bitmap_aware_blobs, c_rcvr.bitmap_aware_blobs);
  EXPECT_EQ(c_orig.bitmap_entirely_dead_blobs,
            c_rcvr.bitmap_entirely_dead_blobs);

  EXPECT_EQ(stats_orig->getTickerCount(GC_BITMAP_FAST_PATH_COUNT),
            stats_rcvr->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));
  EXPECT_EQ(stats_orig->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES),
            stats_rcvr->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES));
  EXPECT_EQ(50U, stats_rcvr->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));

  // User-visible correctness across both.
  AssertNoUserVisibleDataLoss(records, c_orig);
  AssertNoUserVisibleDataLoss(records, c_rcvr);

  ReleaseFiles(vs_orig.get());
  ReleaseFiles(vs_rcvr.get());
}

// Phase 10 Test 4: a legacy blob (referenced by a pre-Phase-4 SST)
// must fall back SAFELY end-to-end. The GC loop must NOT fast-path
// it, must walk every record through the GetKey() path, and must
// produce a Phase 8 ticker snapshot where GC_BITMAP_FALLBACK_COUNT
// reflects the legacy blob. Critically, ZERO records may be
// dropped incorrectly: the legacy fallback is the safety net that
// keeps the feature opt-safe for mixed-version clusters.
TEST_F(BlobValidityBitmapPhase10Test, EndToEnd_LegacyBlobFallsBackSafely) {
  using namespace phase10_testutil;
  constexpr uint64_t kChunkSize = 1024;
  constexpr uint64_t kBlobLegacy = 6400U;
  constexpr uint64_t kBlobNew = 6401U;
  constexpr uint64_t kChunksPerBlob = 4;

  auto vstorage = MakeVStorage(&icmp_, ucmp_);
  vstorage->AddFile(-1, MakeBlobFile(kBlobLegacy, kChunkSize, kChunksPerBlob));
  vstorage->AddFile(-1, MakeBlobFile(kBlobNew, kChunkSize, kChunksPerBlob));
  // Legacy SST references the legacy blob (no chunk bitmap).
  vstorage->AddFile(1, MakeLegacySstReferencingBlob(7300, kBlobLegacy));
  // New SST references the new blob with chunks {0, 2}.
  vstorage->AddFile(1, MakeSstReferencingBlob(7301, kBlobNew, {0, 2}));
  vstorage->AggregateBlobLiveChunkBitmaps(kChunkSize);

  // Legacy blob: sticky-cleared -> bitmap_available=false. GC path
  // must be forced to the legacy GetKey() path.
  const auto* info_legacy = vstorage->GetBlobLiveChunkInfo(kBlobLegacy);
  ASSERT_NE(nullptr, info_legacy);
  EXPECT_FALSE(info_legacy->bitmap_available);
  using L = VersionStorageInfo::BlobChunkLiveness;
  for (uint64_t cid = 0; cid < kChunksPerBlob; ++cid) {
    EXPECT_EQ(L::kUnknown, vstorage->IsChunkLive(kBlobLegacy, cid));
  }
  EXPECT_FALSE(vstorage->IsBlobEntirelyDead(kBlobLegacy));

  // New blob: bitmap_available=true.
  const auto* info_new = vstorage->GetBlobLiveChunkInfo(kBlobNew);
  ASSERT_NE(nullptr, info_new);
  EXPECT_TRUE(info_new->bitmap_available);
  EXPECT_EQ(2U, info_new->live_chunk_count);

  // Drive GC: 30 legacy-blob records (mixed live/dead per
  // ground-truth) + 10 new-blob records (live iff chunk ∈ {0,2}).
  std::vector<BlobRecord> records;
  for (uint64_t i = 0; i < 30; ++i) {
    // Ground-truth: legacy records are half-alive (realistic).
    bool live = (i % 2 == 0);
    records.push_back(BlobRecord{kBlobLegacy, i % kChunksPerBlob,
                                 /*byte_overhead=*/96, live});
  }
  for (uint64_t i = 0; i < 10; ++i) {
    uint64_t cid = i % kChunksPerBlob;
    bool live = (cid == 0 || cid == 2);
    records.push_back(BlobRecord{kBlobNew, cid, 96, live});
  }

  auto stats = CreateDBStatistics();
  auto c = SimulateGc(*vstorage, records);
  EmitPhase10Tickers(stats.get(), c);

  // Legacy blob NEVER enters fast path; new blob has live chunks so
  // it also does not fast-path the whole blob. Every record walks
  // the legacy path.
  EXPECT_EQ(0U, c.bitmap_fast_path_skips);
  EXPECT_EQ(0U, c.bitmap_skipped_bytes);
  EXPECT_EQ(1U, c.bitmap_aware_blobs);     // kBlobNew
  EXPECT_EQ(1U, c.bitmap_fallback_blobs);  // kBlobLegacy
  EXPECT_EQ(0U, c.bitmap_entirely_dead_blobs);

  EXPECT_EQ(1U, stats->getTickerCount(GC_BITMAP_FALLBACK_COUNT))
      << "exactly one legacy blob must surface in the fallback "
         "ticker";
  EXPECT_EQ(0U, stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT))
      << "fast path must NOT fire when a legacy SST references the "
         "blob -- this is the safety contract";
  EXPECT_EQ((30U + 10U) * 96U,
            stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));

  // User-visible correctness: every live record kept, every dead
  // record dropped, NOTHING LOST.
  AssertNoUserVisibleDataLoss(records, c);

  ReleaseFiles(vstorage.get());
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}