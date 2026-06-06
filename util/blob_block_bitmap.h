//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Internal container for vSST data-block-level live bitmap used by the
// blob GC pipeline. A bit indexed by `block_id` (the BlockBasedTable
// data block ordinal of the referenced vSST) is set iff a kSST
// ValueIndex points into that data block. This header is internal-only;
// it is not exported through include/rocksdb/.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/terark_namespace.h"
#include "rocksdb/types.h"

namespace TERARKDB_NAMESPACE {

// Sentinel meaning "no data block id is available for this value-index".
// Callers must treat this as a fallback signal and never assume a block
// is dead based on it.
static constexpr uint64_t kNoBlockId = static_cast<uint64_t>(-1);

// Sentinel meaning "no layout id (physical vSST file number) is bound to
// this value-index". A block_id is only meaningful inside the physical
// vSST layout identified by its layout_id; once the vSST is GC-rewritten,
// the old layout_id no longer matches and the block_id must be ignored.
static constexpr uint64_t kNoBlockLayoutId = static_cast<uint64_t>(-1);

// BlobBlockBitmap is a dense bitmap indexed by block_id. It supports
// growing on demand, bitwise OR merging, population count, and a
// simple varint-length-prefixed serialization format that is stable
// across endianness (byte payload is memcpy'd after a varint64 length
// prefix that stores the number of valid bits, not bytes).
//
// IMPORTANT: an empty bitmap (num_bits()==0) only means "this bitmap
// references no block". It does NOT by itself encode "unavailable".
// Availability is an explicit decision carried by DependenceBlockBitmap
// (see below). Callers must never infer availability from emptiness
// alone.
//
// Thread-safety: Not thread-safe by itself. Callers must provide
// external synchronization if bitmaps from multiple threads are
// concurrently mutated.
class BlobBlockBitmap {
 public:
  BlobBlockBitmap() = default;
  ~BlobBlockBitmap() = default;

  BlobBlockBitmap(const BlobBlockBitmap&) = default;
  BlobBlockBitmap& operator=(const BlobBlockBitmap&) = default;
  BlobBlockBitmap(BlobBlockBitmap&&) noexcept = default;
  BlobBlockBitmap& operator=(BlobBlockBitmap&&) noexcept = default;

  // Marks block_id as live. Bitmap grows automatically when
  // block_id >= num_bits().
  void Set(uint64_t block_id);

  // Returns true iff block_id has been set.
  bool Test(uint64_t block_id) const;

  // Bitwise-OR merge other into *this. The result's logical bit count
  // is max(this->num_bits(), other.num_bits()).
  void OrWith(const BlobBlockBitmap& other);

  // Returns the number of bits currently set to 1.
  uint64_t CountSetBits() const;

  // Logical number of tracked bits. Equal to the highest block_id
  // ever touched by Set() plus one, rounded up to a byte.
  uint64_t num_bits() const { return num_bits_; }

  // Whether the bitmap has no bits tracked yet.
  bool empty() const { return num_bits_ == 0; }

  // Drops all state.
  void Clear() {
    bits_.clear();
    num_bits_ = 0;
  }

  // Serialize into dst. Format:
  //   varint64(num_bits_) || raw_bytes(ceil(num_bits_/8))
  // Appends to dst; does not clear it.
  void Serialize(std::string* dst) const;

  // Deserialize from input. On success, advances input past the
  // consumed bytes and returns true. On failure (truncated or
  // malformed), leaves *this unspecified and returns false.
  bool Deserialize(Slice* input);

 private:
  void EnsureCapacity(uint64_t block_id);

  std::vector<uint8_t> bits_;
  uint64_t num_bits_ = 0;
};

// -----------------------------------------------------------------
// DependenceBlockBitmap: a per-dependence-row, layout-aware wrapper.
//
// One row corresponds to one entry of TablePropertyCache::dependence
// (strictly aligned by index). It encodes three pieces of information
// that GC needs in order to safely reason about a referenced vSST:
//
//   - available : whether the bitmap can be trusted at all. When false,
//                 GC must fall back to the legacy GetKey() lookup path.
//                 An UNAVAILABLE row is NOT the same as an available
//                 row with an empty bitmap.
//   - layout_id : the physical vSST file number (fd.GetNumber()) the
//                 block ids in `bitmap` were stamped against. The
//                 version aggregator rejects the row when this does not
//                 match the blob's *current* physical file number,
//                 because a vSST GC rewrite invalidates old block ids.
//                 Only meaningful when available == true && !bitmap
//                 empty. For an available-but-empty row layout_id is
//                 kNoBlockLayoutId and the aggregator treats it as
//                 "no live references under any known layout" (which,
//                 lacking a verifiable layout, is conservatively folded
//                 into the unavailable regime).
//   - bitmap    : the set of live data block ordinals, under layout_id.
//
// Semantic table:
//   available=false              -> unavailable, GC must fall back.
//   available=true, bitmap empty -> the SST referenced no live block in
//                                   the blob (or referenced it only via
//                                   rows lacking a usable layout id).
//   available=true, bitmap !empty, layout_id valid
//                                -> trustworthy block-level liveness in
//                                   the layout identified by layout_id.
// -----------------------------------------------------------------
struct DependenceBlockBitmap {
  bool available = false;
  uint64_t layout_id = kNoBlockLayoutId;
  BlobBlockBitmap bitmap;

  // Wire format (appended to dst):
  //   [ flags(1B) ]                 bit0 = available
  //   [ layout_id(fixed64) ]
  //   [ BlobBlockBitmap payload ]   (always present; varint num_bits + bytes)
  void Serialize(std::string* dst) const;

  // Parse one row from *input, advancing it past the consumed bytes.
  // On any malformation the row is reset to the safe unavailable state
  // and false is returned so callers fall back to the legacy path.
  bool Deserialize(Slice* input);
};

namespace blob_block_bitmap_detail {

// Shared per-blob accumulator used by both collectors. Tracks the
// observed layout id (with conflict detection) and the live bitmap.
struct BlobAccumulator {
  bool has_layout = false;
  uint64_t layout_id = kNoBlockLayoutId;
  BlobBlockBitmap bitmap;
};

// Common collector body shared by flush and compaction collectors. The
// two public collector types are thin aliases so call sites stay
// self-documenting.
class BlockBitmapCollectorBase {
 public:
  explicit BlockBitmapCollectorBase(bool enabled) : enabled_(enabled) {}

  bool enabled() const { return enabled_; }

  bool empty() const { return per_blob_.empty() && unavailable_.empty(); }

  // Record that one value-index entry in the output SST references data
  // block `block_id` inside vSST `blob_file_number`, stamped under the
  // physical layout `layout_id`. No-op when disabled. When block_id or
  // layout_id is its sentinel, or when a conflicting layout_id is seen
  // for the same blob, the blob is marked unavailable instead.
  void ObserveBlockId(uint64_t blob_file_number, uint64_t layout_id,
                      uint64_t block_id) {
    if (!enabled_) return;
    if (block_id == kNoBlockId || layout_id == kNoBlockLayoutId) {
      MarkUnavailable(blob_file_number);
      return;
    }
    if (unavailable_.count(blob_file_number) > 0) return;
    auto& acc = per_blob_[blob_file_number];
    if (acc.has_layout && acc.layout_id != layout_id) {
      // The same blob is being referenced under two different physical
      // layouts within one output SST (e.g. stale value-index rows that
      // survived a vSST GC rewrite). We cannot reconcile them safely,
      // so make the whole blob unavailable.
      MarkUnavailable(blob_file_number);
      return;
    }
    acc.has_layout = true;
    acc.layout_id = layout_id;
    acc.bitmap.Set(block_id);
  }

  // Mark the entire bitmap for `blob_file_number` as unavailable.
  // Sticky: once unavailable, ObserveBlockId() will not resurrect it.
  void MarkUnavailable(uint64_t blob_file_number) {
    if (!enabled_) return;
    unavailable_.insert(blob_file_number);
    per_blob_.erase(blob_file_number);
  }

  bool IsUnavailable(uint64_t blob_file_number) const {
    return unavailable_.count(blob_file_number) > 0;
  }

  // Produce a per-dependence row vector, strictly aligned to
  // `dependence` by index: out[i] corresponds to dependence[i].
  //
  // When the collector is disabled, `out` is cleared to empty to signal
  // the legacy (whole-SST bitmap-unavailable) regime. Otherwise every
  // dependence row is materialized:
  //   - a blob marked unavailable yields {available=false};
  //   - a blob with observed references yields
  //     {available=true, layout_id, bitmap};
  //   - a dependence with no observed reference yields
  //     {available=true, layout_id=kNoBlockLayoutId, empty bitmap},
  //     which the aggregator conservatively treats as unverifiable.
  void Materialize(const std::vector<Dependence>& dependence,
                   std::vector<DependenceBlockBitmap>* out) const {
    out->clear();
    if (!enabled_) {
      return;
    }
    out->resize(dependence.size());
    for (size_t i = 0; i < dependence.size(); ++i) {
      const uint64_t fn = dependence[i].file_number;
      DependenceBlockBitmap& row = (*out)[i];
      if (unavailable_.count(fn) > 0) {
        row.available = false;
        row.layout_id = kNoBlockLayoutId;
        continue;
      }
      auto it = per_blob_.find(fn);
      if (it == per_blob_.end()) {
        // Referenced as a dependence but no usable block id observed.
        row.available = true;
        row.layout_id = kNoBlockLayoutId;
        continue;
      }
      row.available = true;
      row.layout_id = it->second.layout_id;
      row.bitmap = it->second.bitmap;
    }
  }

 private:
  bool enabled_;
  std::unordered_map<uint64_t, BlobAccumulator> per_blob_;
  std::unordered_set<uint64_t> unavailable_;
};

}  // namespace blob_block_bitmap_detail

// Flush-path block-reference collector. The flush writer observes the
// real data block ordinal returned by the vSST BlockBasedTableBuilder
// for each separated value, together with the physical layout id of the
// vSST it was written into.
using FlushBlockBitmapCollector =
    blob_block_bitmap_detail::BlockBitmapCollectorBase;

// Compaction-path block-reference collector. A compaction output SST
// inherits block references from existing value-index entries (decoded
// block_id + layout_id) and/or from freshly re-written values.
using CompactionBlockBitmapCollector =
    blob_block_bitmap_detail::BlockBitmapCollectorBase;

}  // namespace TERARKDB_NAMESPACE
