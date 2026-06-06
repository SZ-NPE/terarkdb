//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/blob_block_bitmap.h"

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
#include "rocksdb/types.h"
#include "util/testharness.h"

namespace TERARKDB_NAMESPACE {

class BlobBlockBitmapTest : public testing::Test {};

TEST_F(BlobBlockBitmapTest, Bitmap_SetAndTest) {
  BlobBlockBitmap bm;
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

  // Clear resets all state.
  bm.Clear();
  ASSERT_TRUE(bm.empty());
  ASSERT_EQ(0U, bm.num_bits());
  ASSERT_EQ(0U, bm.CountSetBits());
}

TEST_F(BlobBlockBitmapTest, Bitmap_OrMerge) {
  BlobBlockBitmap a;
  BlobBlockBitmap b;

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
  BlobBlockBitmap empty;
  uint64_t before_bits = a.num_bits();
  uint64_t before_count = a.CountSetBits();
  a.OrWith(empty);
  ASSERT_EQ(before_bits, a.num_bits());
  ASSERT_EQ(before_count, a.CountSetBits());

  // Merging into empty takes the other side
  BlobBlockBitmap c;
  c.OrWith(a);
  ASSERT_EQ(a.CountSetBits(), c.CountSetBits());
  ASSERT_TRUE(c.Test(200));
}

TEST_F(BlobBlockBitmapTest, Bitmap_SerializeDeserialize) {
  BlobBlockBitmap bm;
  bm.Set(1);
  bm.Set(15);
  bm.Set(16);
  bm.Set(31);
  bm.Set(1000);

  std::string buf;
  bm.Serialize(&buf);
  ASSERT_FALSE(buf.empty());

  BlobBlockBitmap decoded;
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
  BlobBlockBitmap empty_in;
  std::string ebuf;
  empty_in.Serialize(&ebuf);
  BlobBlockBitmap empty_out;
  empty_out.Set(42);  // dirty the bitmap first
  Slice einput(ebuf);
  ASSERT_TRUE(empty_out.Deserialize(&einput));
  ASSERT_EQ(0U, einput.size());
  ASSERT_EQ(0U, empty_out.num_bits());
  ASSERT_EQ(0U, empty_out.CountSetBits());

  // Malformed: truncated payload must fail
  std::string truncated = buf.substr(0, buf.size() - 1);
  BlobBlockBitmap bad;
  Slice bad_input(truncated);
  ASSERT_FALSE(bad.Deserialize(&bad_input));
}

// "References no block" (empty bitmap) and "unavailable" are distinct
// concepts. The bitmap class itself only knows emptiness; the
// collectors carry the explicit unavailable flag. This test locks that
// contract so neither path can silently impersonate the other.
TEST_F(BlobBlockBitmapTest, EmptyVsUnavailableNotConfused) {
  // An empty bitmap is NOT unavailable by itself.
  BlobBlockBitmap empty;
  ASSERT_TRUE(empty.empty());

  // A blob that references block 0 only is non-empty and available.
  BlobBlockBitmap referenced;
  referenced.Set(0);
  ASSERT_FALSE(referenced.empty());
  ASSERT_TRUE(referenced.Test(0));

  // The collector distinguishes "observed nothing" from "explicitly
  // unavailable" via MarkUnavailable / IsUnavailable.
  FlushBlockBitmapCollector collector(/*enabled=*/true);
  ASSERT_FALSE(collector.IsUnavailable(7));
  collector.ObserveBlockId(7, /*layout_id=*/70, /*block_id=*/3);
  ASSERT_FALSE(collector.IsUnavailable(7));  // observed != unavailable
  collector.MarkUnavailable(9);
  ASSERT_TRUE(collector.IsUnavailable(9));
  ASSERT_FALSE(collector.IsUnavailable(7));
}

// Materialize must align output to dependence by index, emit an
// unavailable row for blobs marked unavailable, an available row with
// the observed bitmap and layout_id for blobs that were observed, and
// an available-but-empty row (layout_id == kNoBlockLayoutId) for
// dependence entries with no observed reference.
TEST_F(BlobBlockBitmapTest, FlushCollectorMaterialize) {
  FlushBlockBitmapCollector collector(/*enabled=*/true);
  collector.ObserveBlockId(100, /*layout_id=*/1000, /*block_id=*/0);
  collector.ObserveBlockId(100, /*layout_id=*/1000, /*block_id=*/5);
  collector.ObserveBlockId(200, /*layout_id=*/2000, /*block_id=*/2);
  collector.MarkUnavailable(300);

  std::vector<Dependence> dep = {Dependence{100, 1, 0}, Dependence{200, 1, 0},
                                 Dependence{300, 1, 0}, Dependence{400, 1, 0}};
  std::vector<DependenceBlockBitmap> out;
  collector.Materialize(dep, &out);
  ASSERT_EQ(4U, out.size());

  // file 100: available, layout_id=1000, blocks {0,5} live.
  EXPECT_TRUE(out[0].available);
  EXPECT_EQ(1000U, out[0].layout_id);
  EXPECT_TRUE(out[0].bitmap.Test(0));
  EXPECT_TRUE(out[0].bitmap.Test(5));
  EXPECT_FALSE(out[0].bitmap.Test(1));
  EXPECT_EQ(2U, out[0].bitmap.CountSetBits());

  // file 200: available, layout_id=2000, block {2} live.
  EXPECT_TRUE(out[1].available);
  EXPECT_EQ(2000U, out[1].layout_id);
  EXPECT_TRUE(out[1].bitmap.Test(2));
  EXPECT_EQ(1U, out[1].bitmap.CountSetBits());

  // file 300: explicitly unavailable.
  EXPECT_FALSE(out[2].available);
  EXPECT_EQ(kNoBlockLayoutId, out[2].layout_id);

  // file 400: referenced as a dependence but never observed -> available
  // but with no usable layout id and an empty bitmap.
  EXPECT_TRUE(out[3].available);
  EXPECT_EQ(kNoBlockLayoutId, out[3].layout_id);
  EXPECT_TRUE(out[3].bitmap.empty());
}

// A conflicting layout_id for the same blob within one output SST must
// make the whole blob unavailable (stale value-index rows surviving a
// vSST GC rewrite).
TEST_F(BlobBlockBitmapTest, ConflictingLayoutIdMarksUnavailable) {
  FlushBlockBitmapCollector collector(/*enabled=*/true);
  collector.ObserveBlockId(100, /*layout_id=*/1000, /*block_id=*/0);
  collector.ObserveBlockId(100, /*layout_id=*/9999, /*block_id=*/1);

  std::vector<Dependence> dep = {Dependence{100, 1, 0}};
  std::vector<DependenceBlockBitmap> out;
  collector.Materialize(dep, &out);
  ASSERT_EQ(1U, out.size());
  EXPECT_FALSE(out[0].available);
}

// A sentinel layout_id or block_id marks the blob unavailable.
TEST_F(BlobBlockBitmapTest, SentinelObservationMarksUnavailable) {
  FlushBlockBitmapCollector collector(/*enabled=*/true);
  collector.ObserveBlockId(100, kNoBlockLayoutId, /*block_id=*/0);
  collector.ObserveBlockId(200, /*layout_id=*/2000, kNoBlockId);

  std::vector<Dependence> dep = {Dependence{100, 1, 0}, Dependence{200, 1, 0}};
  std::vector<DependenceBlockBitmap> out;
  collector.Materialize(dep, &out);
  ASSERT_EQ(2U, out.size());
  EXPECT_FALSE(out[0].available);
  EXPECT_FALSE(out[1].available);
}

// DependenceBlockBitmap serialization round-trips available + layout_id +
// bitmap, and distinguishes available-empty from unavailable.
TEST_F(BlobBlockBitmapTest, DependenceBlockBitmapRoundTrip) {
  // available, non-empty.
  {
    DependenceBlockBitmap row;
    row.available = true;
    row.layout_id = 12345ULL;
    row.bitmap.Set(0);
    row.bitmap.Set(7);
    row.bitmap.Set(64);

    std::string buf;
    row.Serialize(&buf);

    DependenceBlockBitmap decoded;
    Slice in(buf);
    ASSERT_TRUE(decoded.Deserialize(&in));
    ASSERT_EQ(0U, in.size());
    EXPECT_TRUE(decoded.available);
    EXPECT_EQ(12345ULL, decoded.layout_id);
    EXPECT_TRUE(decoded.bitmap.Test(0));
    EXPECT_TRUE(decoded.bitmap.Test(7));
    EXPECT_TRUE(decoded.bitmap.Test(64));
    EXPECT_EQ(3U, decoded.bitmap.CountSetBits());
  }

  // available, empty bitmap.
  {
    DependenceBlockBitmap row;
    row.available = true;
    row.layout_id = kNoBlockLayoutId;

    std::string buf;
    row.Serialize(&buf);

    DependenceBlockBitmap decoded;
    decoded.available = false;  // dirty
    Slice in(buf);
    ASSERT_TRUE(decoded.Deserialize(&in));
    EXPECT_TRUE(decoded.available);
    EXPECT_EQ(kNoBlockLayoutId, decoded.layout_id);
    EXPECT_TRUE(decoded.bitmap.empty());
  }

  // unavailable.
  {
    DependenceBlockBitmap row;
    row.available = false;
    row.layout_id = kNoBlockLayoutId;

    std::string buf;
    row.Serialize(&buf);

    DependenceBlockBitmap decoded;
    decoded.available = true;  // dirty
    decoded.layout_id = 999;
    Slice in(buf);
    ASSERT_TRUE(decoded.Deserialize(&in));
    EXPECT_FALSE(decoded.available);
  }

  // malformed -> reset to unavailable, returns false.
  {
    DependenceBlockBitmap row;
    row.available = true;
    row.layout_id = 5ULL;
    row.bitmap.Set(3);
    std::string buf;
    row.Serialize(&buf);
    std::string truncated = buf.substr(0, buf.size() - 1);

    DependenceBlockBitmap decoded;
    decoded.available = true;  // dirty
    decoded.layout_id = 7;
    Slice in(truncated);
    ASSERT_FALSE(decoded.Deserialize(&in));
    EXPECT_FALSE(decoded.available);
  }
}

// A disabled collector is a no-op and Materialize() clears its output,
// which the version aggregator treats as the legacy / unavailable
// regime.
TEST_F(BlobBlockBitmapTest, DisabledCollectorIsNoOp) {
  FlushBlockBitmapCollector collector(/*enabled=*/false);
  collector.ObserveBlockId(100, /*layout_id=*/1000, /*block_id=*/0);  // discarded
  collector.MarkUnavailable(200);     // discarded
  ASSERT_FALSE(collector.IsUnavailable(200));

  std::vector<Dependence> dep = {Dependence{100, 1, 0}};
  std::vector<DependenceBlockBitmap> out;
  out.resize(7);  // dirty
  collector.Materialize(dep, &out);
  EXPECT_TRUE(out.empty());
}

TEST_F(BlobBlockBitmapTest, NewBlockBitmapOptionsDefault) {
  ColumnFamilyOptions cf_opts;
  ASSERT_FALSE(cf_opts.enable_blob_block_bitmap);
  ASSERT_TRUE(cf_opts.enable_blob_block_bitmap_gc_fast_path);
  ASSERT_FALSE(cf_opts.enable_blob_block_skip);
  ASSERT_EQ(1U, cf_opts.blob_block_index_version);
  ASSERT_TRUE(cf_opts.blob_block_bitmap_strict_fallback);
  ASSERT_FALSE(cf_opts.blob_block_bitmap_debug);
}

TEST_F(BlobBlockBitmapTest, NewBlockBitmapOptionsParseFromString) {
  ColumnFamilyOptions base;
  ColumnFamilyOptions parsed;
  ASSERT_OK(GetColumnFamilyOptionsFromString(
      base,
      "enable_blob_block_bitmap=true;"
      "enable_blob_block_bitmap_gc_fast_path=false;"
      "enable_blob_block_skip=true;"
      "blob_block_index_version=1;"
      "blob_block_bitmap_strict_fallback=false;"
      "blob_block_bitmap_debug=true;",
      &parsed));
  ASSERT_TRUE(parsed.enable_blob_block_bitmap);
  ASSERT_FALSE(parsed.enable_blob_block_bitmap_gc_fast_path);
  ASSERT_TRUE(parsed.enable_blob_block_skip);
  ASSERT_EQ(1U, parsed.blob_block_index_version);
  ASSERT_FALSE(parsed.blob_block_bitmap_strict_fallback);
  ASSERT_TRUE(parsed.blob_block_bitmap_debug);
}

// -----------------------------------------------------------------
// Observability tickers for the block-bitmap GC fast path.
//
// These tests lock the contract between the per-GC `counter` struct
// maintained by ProcessGarbageCollection and the Statistics tickers
// that surface it to external observers. We don't need to spin up a
// real DB: ProcessGarbageCollection's emit-site is a fixed sequence
// of RecordTick() calls keyed on those counters, so a Statistics +
// explicit RecordTick replay is an equivalent deterministic test.
// -----------------------------------------------------------------

namespace {

struct GcBlockBitmapCounters {
  uint64_t fast_path_skips = 0;
  uint64_t fallback_getkey = 0;
  uint64_t skipped_bytes = 0;
  uint64_t live_blocks = 0;
  uint64_t dead_blocks = 0;
};

// Exact replica of the RecordTick() block at the end of
// ProcessGarbageCollection. Keep this in sync with compaction_job.cc.
void EmitGcTickers(Statistics* s, const GcBlockBitmapCounters& c) {
  RecordTick(s, GC_BLOCK_BITMAP_FAST_PATH_SKIPS, c.fast_path_skips);
  RecordTick(s, GC_BLOCK_BITMAP_FALLBACK_GETKEY, c.fallback_getkey);
  RecordTick(s, GC_BLOCK_BITMAP_SKIPPED_RECORDS, c.fast_path_skips);
  RecordTick(s, GC_BLOCK_BITMAP_SKIPPED_BYTES, c.skipped_bytes);
  RecordTick(s, GC_BLOCK_BITMAP_LIVE_BLOCKS, c.live_blocks);
  RecordTick(s, GC_BLOCK_BITMAP_DEAD_BLOCKS, c.dead_blocks);
}

}  // namespace

class GcBlockBitmapStatsTest : public testing::Test {};

TEST_F(GcBlockBitmapStatsTest, FastPathIncrementsCounters) {
  std::shared_ptr<Statistics> stats = CreateDBStatistics();

  GcBlockBitmapCounters c;
  c.fast_path_skips = 42;
  c.skipped_bytes = 42 * 128;
  c.dead_blocks = 42;

  ASSERT_EQ(0U, stats->getTickerCount(GC_BLOCK_BITMAP_FAST_PATH_SKIPS));
  EmitGcTickers(stats.get(), c);

  EXPECT_EQ(42U, stats->getTickerCount(GC_BLOCK_BITMAP_FAST_PATH_SKIPS));
  EXPECT_EQ(42U, stats->getTickerCount(GC_BLOCK_BITMAP_SKIPPED_RECORDS));
  EXPECT_EQ(42U * 128U, stats->getTickerCount(GC_BLOCK_BITMAP_SKIPPED_BYTES));
  EXPECT_EQ(42U, stats->getTickerCount(GC_BLOCK_BITMAP_DEAD_BLOCKS));
  EXPECT_EQ(0U, stats->getTickerCount(GC_BLOCK_BITMAP_FALLBACK_GETKEY));
  EXPECT_EQ(0U, stats->getTickerCount(GC_BLOCK_BITMAP_LIVE_BLOCKS));
}

TEST_F(GcBlockBitmapStatsTest, FallbackIncrementsCounters) {
  std::shared_ptr<Statistics> stats = CreateDBStatistics();

  GcBlockBitmapCounters c;
  c.fallback_getkey = 100;
  c.live_blocks = 30;

  EmitGcTickers(stats.get(), c);

  EXPECT_EQ(0U, stats->getTickerCount(GC_BLOCK_BITMAP_FAST_PATH_SKIPS));
  EXPECT_EQ(100U, stats->getTickerCount(GC_BLOCK_BITMAP_FALLBACK_GETKEY));
  EXPECT_EQ(30U, stats->getTickerCount(GC_BLOCK_BITMAP_LIVE_BLOCKS));

  // Repeated GC runs accumulate, not overwrite.
  GcBlockBitmapCounters c2;
  c2.fallback_getkey = 5;
  EmitGcTickers(stats.get(), c2);
  EXPECT_EQ(105U, stats->getTickerCount(GC_BLOCK_BITMAP_FALLBACK_GETKEY));
}

TEST_F(GcBlockBitmapStatsTest, TickerNamesRegistered) {
  const std::unordered_map<int, std::string> expected = {
      {GC_BLOCK_BITMAP_FAST_PATH_SKIPS, "rocksdb.num.gc.block_bitmap_fast_path"},
      {GC_BLOCK_BITMAP_FALLBACK_GETKEY,
       "rocksdb.num.gc.block_bitmap_fallback_getkey"},
      {GC_BLOCK_BITMAP_SKIPPED_RECORDS,
       "rocksdb.num.gc.block_bitmap_skipped_records"},
      {GC_BLOCK_BITMAP_SKIPPED_BYTES,
       "rocksdb.bytes.gc.block_bitmap_skipped"},
      {GC_BLOCK_BITMAP_LIVE_BLOCKS, "rocksdb.num.gc.block_bitmap_live_blocks"},
      {GC_BLOCK_BITMAP_DEAD_BLOCKS, "rocksdb.num.gc.block_bitmap_dead_blocks"},
      {GC_BLOCK_BITMAP_BLOCK_SKIP_BYTES,
       "rocksdb.bytes.gc.block_bitmap_block_skip"},
      {BLOB_BLOCK_BITMAP_AGGREGATE_RUNS,
       "rocksdb.num.blob_block_bitmap.aggregate_runs"},
      {BLOB_BLOCK_BITMAP_AGGREGATE_BLOBS,
       "rocksdb.num.blob_block_bitmap.aggregate_blobs"},
      {BLOB_BLOCK_BITMAP_UNAVAILABLE_BLOBS,
       "rocksdb.num.blob_block_bitmap.unavailable_blobs"},
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
      << "all block-bitmap ticker names must be registered in TickersNameMap";
}

}  // namespace TERARKDB_NAMESPACE

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
