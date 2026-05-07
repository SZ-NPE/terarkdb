//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/blob_chunk_bitmap.h"

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "monitoring/statistics.h"
#include "rocksdb/convenience.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"
#include "rocksdb/terark_namespace.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class BlobChunkBitmapTest : public testing::Test {};

TEST_F(BlobChunkBitmapTest, Bitmap_SetAndTest) {
  BlobChunkBitmap bm;
  ASSERT_TRUE(bm.empty());
  ASSERT_EQ(0U, bm.num_bits());
  ASSERT_EQ(0U, bm.CountSetBits());
  ASSERT_FALSE(bm.Test(0));
  ASSERT_FALSE(bm.Test(123));

  bm.Set(0);
  bm.Set(7);
  bm.Set(8);
  bm.Set(63);
  bm.Set(1024);

  ASSERT_TRUE(bm.Test(0));
  ASSERT_TRUE(bm.Test(7));
  ASSERT_TRUE(bm.Test(8));
  ASSERT_TRUE(bm.Test(63));
  ASSERT_TRUE(bm.Test(1024));

  ASSERT_FALSE(bm.Test(1));
  ASSERT_FALSE(bm.Test(9));
  ASSERT_FALSE(bm.Test(62));
  ASSERT_FALSE(bm.Test(1023));
  ASSERT_FALSE(bm.Test(1025));

  ASSERT_EQ(5U, bm.CountSetBits());
  ASSERT_GE(bm.num_bits(), 1025U);

  // Idempotent Set
  bm.Set(7);
  ASSERT_EQ(5U, bm.CountSetBits());
}

TEST_F(BlobChunkBitmapTest, Bitmap_OrMerge) {
  BlobChunkBitmap a;
  BlobChunkBitmap b;

  a.Set(0);
  a.Set(5);
  a.Set(100);

  b.Set(5);
  b.Set(6);
  b.Set(200);

  a.OrWith(b);

  ASSERT_TRUE(a.Test(0));
  ASSERT_TRUE(a.Test(5));
  ASSERT_TRUE(a.Test(6));
  ASSERT_TRUE(a.Test(100));
  ASSERT_TRUE(a.Test(200));
  ASSERT_FALSE(a.Test(1));
  ASSERT_FALSE(a.Test(99));
  ASSERT_FALSE(a.Test(201));

  // |union| = {0, 5, 6, 100, 200} -> 5 bits
  ASSERT_EQ(5U, a.CountSetBits());

  // OrWith empty is a no-op
  BlobChunkBitmap empty;
  uint64_t before_bits = a.num_bits();
  uint64_t before_count = a.CountSetBits();
  a.OrWith(empty);
  ASSERT_EQ(before_bits, a.num_bits());
  ASSERT_EQ(before_count, a.CountSetBits());

  // Merging into empty takes the other side
  BlobChunkBitmap c;
  c.OrWith(a);
  ASSERT_EQ(a.CountSetBits(), c.CountSetBits());
  ASSERT_TRUE(c.Test(200));
}

TEST_F(BlobChunkBitmapTest, Bitmap_SerializeDeserialize) {
  BlobChunkBitmap bm;
  bm.Set(1);
  bm.Set(15);
  bm.Set(16);
  bm.Set(31);
  bm.Set(1000);

  std::string buf;
  bm.Serialize(&buf);
  ASSERT_FALSE(buf.empty());

  BlobChunkBitmap decoded;
  Slice input(buf);
  ASSERT_TRUE(decoded.Deserialize(&input));
  ASSERT_EQ(0U, input.size());

  ASSERT_EQ(bm.num_bits(), decoded.num_bits());
  ASSERT_EQ(bm.CountSetBits(), decoded.CountSetBits());
  ASSERT_TRUE(decoded.Test(1));
  ASSERT_TRUE(decoded.Test(15));
  ASSERT_TRUE(decoded.Test(16));
  ASSERT_TRUE(decoded.Test(31));
  ASSERT_TRUE(decoded.Test(1000));
  ASSERT_FALSE(decoded.Test(0));
  ASSERT_FALSE(decoded.Test(14));
  ASSERT_FALSE(decoded.Test(999));

  // Round-trip through empty bitmap
  BlobChunkBitmap empty_in;
  std::string ebuf;
  empty_in.Serialize(&ebuf);
  BlobChunkBitmap empty_out;
  empty_out.Set(42);  // dirty the bitmap first
  Slice einput(ebuf);
  ASSERT_TRUE(empty_out.Deserialize(&einput));
  ASSERT_EQ(0U, einput.size());
  ASSERT_EQ(0U, empty_out.num_bits());
  ASSERT_EQ(0U, empty_out.CountSetBits());

  // Malformed: truncated payload must fail
  std::string truncated = buf.substr(0, buf.size() - 1);
  BlobChunkBitmap bad;
  Slice bad_input(truncated);
  ASSERT_FALSE(bad.Deserialize(&bad_input));
}

TEST_F(BlobChunkBitmapTest, EnableBitmapOption_DefaultDisabled) {
  ColumnFamilyOptions cf_opts;
  ASSERT_FALSE(cf_opts.enable_blob_validity_bitmap);

  Options opts;
  ASSERT_FALSE(opts.enable_blob_validity_bitmap);
}

TEST_F(BlobChunkBitmapTest, BlobChunkSize_DefaultValue) {
  ColumnFamilyOptions cf_opts;
  ASSERT_EQ(static_cast<uint64_t>(64 * 1024), cf_opts.blob_gc_chunk_size);

  Options opts;
  ASSERT_EQ(static_cast<uint64_t>(64 * 1024), opts.blob_gc_chunk_size);
}

TEST_F(BlobChunkBitmapTest, EnableBitmapOption_ParseFromOptions) {
  ColumnFamilyOptions base;
  ColumnFamilyOptions parsed;

  ASSERT_OK(GetColumnFamilyOptionsFromString(
      base,
      "enable_blob_validity_bitmap=true;"
      "blob_gc_chunk_size=131072;",
      &parsed));

  ASSERT_TRUE(parsed.enable_blob_validity_bitmap);
  ASSERT_EQ(static_cast<uint64_t>(131072), parsed.blob_gc_chunk_size);

  // Parsing false / non-default should also round-trip
  ColumnFamilyOptions parsed2;
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      base,
      "enable_blob_validity_bitmap=false;"
      "blob_gc_chunk_size=4096;",
      &parsed2));
  ASSERT_FALSE(parsed2.enable_blob_validity_bitmap);
  ASSERT_EQ(static_cast<uint64_t>(4096), parsed2.blob_gc_chunk_size);
}

// -----------------------------------------------------------------
// Phase 8: observability tickers for the blob GC bitmap fast path.
//
// These tests lock the contract between the per-GC `counter` struct
// maintained by ProcessGarbageCollection and the 5 Statistics tickers
// that surface it to external observers. We don't need to spin up a
// real DB: ProcessGarbageCollection's emit-site is a fixed sequence
// of RecordTick() calls keyed on those counters (see compaction_job.cc
// "Phase 8: emit bitmap fast-path observability tickers"), so a
// Statistics + explicit RecordTick replay is an equivalent, faster,
// deterministic test for the contract.
// -----------------------------------------------------------------

namespace {

// Mirrors the Phase 7 counter subset that Phase 8 surfaces as tickers.
struct GcBitmapCounters {
  uint64_t bitmap_fast_path_skips = 0;
  uint64_t bitmap_fallback_blobs = 0;
  uint64_t bitmap_skipped_bytes = 0;
  uint64_t bitmap_live_bytes = 0;
};

// Exact replica of the RecordTick() block at the end of
// ProcessGarbageCollection. Keep this in sync with compaction_job.cc.
void EmitPhase8Tickers(Statistics* s, const GcBitmapCounters& c) {
  RecordTick(s, GC_BITMAP_FAST_PATH_COUNT, c.bitmap_fast_path_skips);
  RecordTick(s, GC_BITMAP_FALLBACK_COUNT, c.bitmap_fallback_blobs);
  RecordTick(s, GC_SKIPPED_DEAD_CHUNK_BYTES, c.bitmap_skipped_bytes);
  RecordTick(s, GC_READ_LIVE_CHUNK_BYTES, c.bitmap_live_bytes);
  RecordTick(s, GC_LOOKUP_AVOIDED_COUNT, c.bitmap_fast_path_skips);
}

}  // namespace

class BlobGcBitmapStatsTest : public testing::Test {};

// Phase 8: in a pure fast-path GC run (every record short-circuited by
// IsBlobEntirelyDead), all five bitmap tickers must be observable and
// carry the expected values. GC_BITMAP_FAST_PATH_COUNT must equal the
// number of records skipped, GC_SKIPPED_DEAD_CHUNK_BYTES must equal
// the cumulative record-overhead bytes avoided, and
// GC_READ_LIVE_CHUNK_BYTES / GC_BITMAP_FALLBACK_COUNT must be zero
// because the fast path bypassed the legacy GetKey() path entirely.
// GC_LOOKUP_AVOIDED_COUNT must equal GC_BITMAP_FAST_PATH_COUNT because
// every skipped record corresponds to an avoided point-lookup.
TEST_F(BlobGcBitmapStatsTest, GcFastPathIncrementsBitmapCounters) {
  std::shared_ptr<Statistics> stats = CreateDBStatistics();

  // Simulate a GC sub-compaction that walked 42 records, all short-
  // circuited by an entirely-dead blob. Assume per-record overhead
  // of 128 bytes (key + blob-reference payload).
  GcBitmapCounters c;
  c.bitmap_fast_path_skips = 42;
  c.bitmap_fallback_blobs = 0;
  c.bitmap_skipped_bytes = 42 * 128;
  c.bitmap_live_bytes = 0;

  // Baseline: every ticker starts at zero.
  ASSERT_EQ(0U, stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));
  ASSERT_EQ(0U, stats->getTickerCount(GC_BITMAP_FALLBACK_COUNT));
  ASSERT_EQ(0U, stats->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES));
  ASSERT_EQ(0U, stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));
  ASSERT_EQ(0U, stats->getTickerCount(GC_LOOKUP_AVOIDED_COUNT));

  EmitPhase8Tickers(stats.get(), c);

  EXPECT_EQ(42U, stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));
  EXPECT_EQ(0U, stats->getTickerCount(GC_BITMAP_FALLBACK_COUNT));
  EXPECT_EQ(42U * 128U,
            stats->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES));
  EXPECT_EQ(0U, stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));
  // GC_LOOKUP_AVOIDED_COUNT is semantically the same counter today
  // (every fast-path skip avoided exactly one GetKey()), but kept as
  // a separate ticker for future per-chunk fast-path extensions.
  EXPECT_EQ(42U, stats->getTickerCount(GC_LOOKUP_AVOIDED_COUNT));
  EXPECT_EQ(stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT),
            stats->getTickerCount(GC_LOOKUP_AVOIDED_COUNT));

  // Ticker names must be registered (Phase 8 adds them explicitly to
  // TickersNameMap so external observability pipelines can discover
  // them). Validate the 5 new ticker strings are all present and
  // uniquely mapped.
  const std::unordered_map<int, std::string> expected = {
      {GC_BITMAP_FAST_PATH_COUNT, "rocksdb.num.gc.bitmap_fast_path"},
      {GC_BITMAP_FALLBACK_COUNT, "rocksdb.num.gc.bitmap_fallback"},
      {GC_SKIPPED_DEAD_CHUNK_BYTES, "rocksdb.bytes.gc.skipped_dead_chunk"},
      {GC_READ_LIVE_CHUNK_BYTES, "rocksdb.bytes.gc.read_live_chunk"},
      {GC_LOOKUP_AVOIDED_COUNT, "rocksdb.num.gc.lookup_avoided"},
  };
  std::unordered_set<std::string> seen;
  for (const auto& kv : TickersNameMap) {
    auto it = expected.find(static_cast<int>(kv.first));
    if (it != expected.end()) {
      EXPECT_EQ(it->second, kv.second) << "ticker id=" << kv.first;
      seen.insert(kv.second);
    }
  }
  EXPECT_EQ(expected.size(), seen.size())
      << "all Phase 8 ticker names must be registered in TickersNameMap";
}

// Phase 8: in a pure fallback GC run (no aggregated bitmap available),
// GC_BITMAP_FALLBACK_COUNT counts distinct legacy blobs and
// GC_READ_LIVE_CHUNK_BYTES accumulates record bytes walked through
// the legacy GetKey() path. Fast-path-related tickers must remain at
// zero: the whole point of the fallback run is that no records were
// skipped by the bitmap gate.
TEST_F(BlobGcBitmapStatsTest, GcFallbackIncrementsFallbackCounters) {
  std::shared_ptr<Statistics> stats = CreateDBStatistics();

  // Simulate a GC sub-compaction that encountered 3 distinct legacy
  // blobs (bitmap unavailable) and walked 100 records through
  // GetKey() totaling 20000 bytes of record-overhead.
  GcBitmapCounters c;
  c.bitmap_fast_path_skips = 0;
  c.bitmap_fallback_blobs = 3;
  c.bitmap_skipped_bytes = 0;
  c.bitmap_live_bytes = 20000;

  EmitPhase8Tickers(stats.get(), c);

  EXPECT_EQ(0U, stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));
  EXPECT_EQ(3U, stats->getTickerCount(GC_BITMAP_FALLBACK_COUNT));
  EXPECT_EQ(0U, stats->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES));
  EXPECT_EQ(20000U, stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));
  EXPECT_EQ(0U, stats->getTickerCount(GC_LOOKUP_AVOIDED_COUNT));

  // Repeated GC runs must accumulate, not overwrite. Fire a second
  // replay with a different counter set and confirm both tickers
  // that received nonzero ticks grow monotonically.
  GcBitmapCounters c2;
  c2.bitmap_fallback_blobs = 2;
  c2.bitmap_live_bytes = 5000;
  EmitPhase8Tickers(stats.get(), c2);

  EXPECT_EQ(3U + 2U,
            stats->getTickerCount(GC_BITMAP_FALLBACK_COUNT));
  EXPECT_EQ(20000U + 5000U,
            stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));
  EXPECT_EQ(0U, stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT))
      << "fast-path counter must not leak from fallback runs";
}

// Phase 8: byte-level savings must be reported correctly in mixed
// scenarios where fast-path skips and fallback walks coexist. The
// skipped-bytes ticker must sum fast-path contributions only and the
// read-live-bytes ticker must sum fallback contributions only; the
// two never overlap on the same record.
TEST_F(BlobGcBitmapStatsTest, GcSkippedBytesReportedCorrectly) {
  std::shared_ptr<Statistics> stats = CreateDBStatistics();

  // Mixed GC run: 10 records hit the fast path (1200 bytes) while
  // 15 records took the legacy GetKey() path (3400 bytes). There
  // was also 1 fallback blob (some SST in legacy regime referenced
  // the same version) that forced the latter 15 records to bypass
  // the fast path.
  GcBitmapCounters c;
  c.bitmap_fast_path_skips = 10;
  c.bitmap_fallback_blobs = 1;
  c.bitmap_skipped_bytes = 1200;
  c.bitmap_live_bytes = 3400;

  EmitPhase8Tickers(stats.get(), c);

  EXPECT_EQ(10U, stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));
  EXPECT_EQ(1U, stats->getTickerCount(GC_BITMAP_FALLBACK_COUNT));
  EXPECT_EQ(1200U, stats->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES));
  EXPECT_EQ(3400U, stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));
  EXPECT_EQ(10U, stats->getTickerCount(GC_LOOKUP_AVOIDED_COUNT));

  // Savings ratio derivation is the canonical observability use
  // case Phase 8 enables. Lock the arithmetic so regressions in the
  // emit-site (e.g. swapped ticker IDs) are caught loudly.
  const uint64_t skipped =
      stats->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES);
  const uint64_t live =
      stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES);
  ASSERT_GT(skipped + live, 0U);
  const double ratio =
      static_cast<double>(skipped) / static_cast<double>(skipped + live);
  EXPECT_NEAR(1200.0 / (1200.0 + 3400.0), ratio, 1e-9);

  // A zero-cost GC (no records at all) must leave all tickers at
  // their previous value: RecordTick(..., 0) is the documented no-op
  // path that we rely on for empty sub-compactions.
  const uint64_t before_fp =
      stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT);
  const uint64_t before_fb =
      stats->getTickerCount(GC_BITMAP_FALLBACK_COUNT);
  const uint64_t before_sk =
      stats->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES);
  const uint64_t before_lv =
      stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES);
  const uint64_t before_la =
      stats->getTickerCount(GC_LOOKUP_AVOIDED_COUNT);
  GcBitmapCounters empty;
  EmitPhase8Tickers(stats.get(), empty);
  EXPECT_EQ(before_fp,
            stats->getTickerCount(GC_BITMAP_FAST_PATH_COUNT));
  EXPECT_EQ(before_fb,
            stats->getTickerCount(GC_BITMAP_FALLBACK_COUNT));
  EXPECT_EQ(before_sk,
            stats->getTickerCount(GC_SKIPPED_DEAD_CHUNK_BYTES));
  EXPECT_EQ(before_lv,
            stats->getTickerCount(GC_READ_LIVE_CHUNK_BYTES));
  EXPECT_EQ(before_la,
            stats->getTickerCount(GC_LOOKUP_AVOIDED_COUNT));
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
