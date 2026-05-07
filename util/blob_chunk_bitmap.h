//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Internal container for chunk-level validity bitmap used by the
// Phase 1..Phase 7 chunk-level blob GC pipeline. This header is
// internal-only; it is not exported through include/rocksdb/.

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

// BlobChunkBitmap is a dense bitmap indexed by chunk_id. It supports
// growing on demand, bitwise OR merging, population count, and a
// simple varint-length-prefixed serialization format that is stable
// across endianness (byte payload is memcpy'd after a varint64 length
// prefix that stores the number of valid bits, not bytes).
//
// Thread-safety: Not thread-safe by itself. Callers must provide
// external synchronization if bitmaps from multiple threads are
// concurrently mutated.
class BlobChunkBitmap {
 public:
  BlobChunkBitmap() = default;
  ~BlobChunkBitmap() = default;

  BlobChunkBitmap(const BlobChunkBitmap&) = default;
  BlobChunkBitmap& operator=(const BlobChunkBitmap&) = default;
  BlobChunkBitmap(BlobChunkBitmap&&) noexcept = default;
  BlobChunkBitmap& operator=(BlobChunkBitmap&&) noexcept = default;

  // Marks chunk_id as live. Bitmap grows automatically when
  // chunk_id >= num_bits().
  void Set(uint64_t chunk_id);

  // Returns true iff chunk_id has been set.
  bool Test(uint64_t chunk_id) const;

  // Bitwise-OR merge other into *this. The result's logical bit count
  // is max(this->num_bits(), other.num_bits()).
  void OrWith(const BlobChunkBitmap& other);

  // Returns the number of bits currently set to 1.
  uint64_t CountSetBits() const;

  // Logical number of tracked bits. Equal to the highest chunk_id
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
  void EnsureCapacity(uint64_t chunk_id);

  std::vector<uint8_t> bits_;
  uint64_t num_bits_ = 0;
};

// -----------------------------------------------------------------
// Phase 3: flush-path chunk-reference collector.
//
// Invariant:
//   chunk_id_of(offset) = offset / chunk_size
//
// `chunk_size` is taken from CF option `blob_gc_chunk_size`. When
// the feature switch `enable_blob_validity_bitmap` is off, or when
// chunk_size == 0, every Observe() call is a no-op and Materialize()
// produces an empty output vector. This preserves full backward
// compatibility with DBs that do not opt in to chunk-level GC.
// -----------------------------------------------------------------

// Chunk-id mapping used by both flush/compaction producers and GC
// consumers. Kept inline so that hot paths don't cross a TU boundary.
inline uint64_t ChunkIdOfOffset(uint64_t offset, uint64_t chunk_size) {
  return chunk_size == 0 ? 0 : offset / chunk_size;
}

class FlushChunkBitmapCollector {
 public:
  // When `enabled == false` or `chunk_size == 0`, the collector is a
  // no-op; Observe() discards all input and Materialize() yields an
  // empty result vector.
  FlushChunkBitmapCollector(bool enabled, uint64_t chunk_size)
      : enabled_(enabled && chunk_size > 0), chunk_size_(chunk_size) {}

  bool enabled() const { return enabled_; }

  // Returns true iff no (blob_file_number, chunk_id) pair has ever
  // been observed (either because the collector is disabled, or
  // because flush wrote no separated values).
  bool empty() const { return per_blob_.empty(); }

  // Record that the value we just emitted into blob file
  // `blob_file_number` lives at `blob_offset` bytes inside that file.
  // The collector maps `blob_offset` to a chunk id and sets the
  // corresponding bit in per_blob_[blob_file_number]. No-op when
  // disabled or when chunk_size_ == 0.
  void Observe(uint64_t blob_file_number, uint64_t blob_offset) {
    if (!enabled_) return;
    const uint64_t chunk_id = ChunkIdOfOffset(blob_offset, chunk_size_);
    per_blob_[blob_file_number].Set(chunk_id);
  }

  // Produce a per-dependence chunk bitmap vector, strictly aligned
  // to `dependence` by index: out[i] corresponds to dependence[i].
  // Dependence rows whose `file_number` has not been observed get
  // an empty BlobChunkBitmap (caller treats empty as "no chunk was
  // referenced from this SST into that blob").
  //
  // When the collector is disabled, `out` is cleared to empty to
  // signal the legacy (bitmap-unavailable) regime.
  void Materialize(const std::vector<Dependence>& dependence,
                   std::vector<BlobChunkBitmap>* out) const {
    if (!enabled_) {
      out->clear();
      return;
    }
    out->clear();
    out->resize(dependence.size());
    for (size_t i = 0; i < dependence.size(); ++i) {
      auto it = per_blob_.find(dependence[i].file_number);
      if (it != per_blob_.end()) {
        (*out)[i] = it->second;
      }
    }
  }

 private:
  bool enabled_;
  uint64_t chunk_size_;
  std::unordered_map<uint64_t, BlobChunkBitmap> per_blob_;
};

// -----------------------------------------------------------------
// Phase 4: compaction-path chunk-reference collector.
//
// Unlike the flush collector, a compaction output SST inherits its
// chunk references from *existing* value-index entries rather than
// from freshly written blob bytes. Therefore this collector accepts
// pre-decoded chunk ids directly (ObserveChunkId), and additionally
// exposes MarkUnavailable() which is used whenever a legacy value
// index (one that predates the chunk-id trailer) is encountered for
// a given blob file. Once a blob is marked unavailable, its chunk
// bitmap is force-cleared during Materialize(), serving as the
// explicit "bitmap unavailable" sentinel required by the GC fallback
// contract described in version_edit.h.
//
// Semantics summary at Materialize():
//   - enabled_ == false              -> out->clear() (whole-SST
//                                       unavailable, same regime as
//                                       FlushChunkBitmapCollector)
//   - blob is unavailable            -> out[i] is an empty bitmap
//   - blob was observed (non-legacy) -> out[i] is the accumulated
//                                       bitmap
//   - blob was never observed        -> out[i] is an empty bitmap
//                                       (the SST does not actually
//                                       reference this blob in any
//                                       chunk-aware way)
// -----------------------------------------------------------------
class CompactionChunkBitmapCollector {
 public:
  CompactionChunkBitmapCollector(bool enabled, uint64_t chunk_size)
      : enabled_(enabled && chunk_size > 0), chunk_size_(chunk_size) {}

  bool enabled() const { return enabled_; }
  uint64_t chunk_size() const { return chunk_size_; }

  bool empty() const { return per_blob_.empty() && unavailable_.empty(); }

  // Record that one value-index entry in the compaction output SST
  // references chunk `chunk_id` inside blob file `blob_file_number`.
  // No-op when disabled.
  void ObserveChunkId(uint64_t blob_file_number, uint64_t chunk_id) {
    if (!enabled_) return;
    per_blob_[blob_file_number].Set(chunk_id);
  }

  // Convenience wrapper that accepts a raw byte offset into the blob
  // file and converts it to a chunk id via ChunkIdOfOffset. Intended
  // for test/debug paths; the compaction main path should pass an
  // already-decoded chunk_id via ObserveChunkId().
  void ObserveOffset(uint64_t blob_file_number, uint64_t blob_offset) {
    if (!enabled_) return;
    ObserveChunkId(blob_file_number, ChunkIdOfOffset(blob_offset, chunk_size_));
  }

  // Mark the entire bitmap for `blob_file_number` as unavailable.
  // Called when a legacy value-index entry (one without a chunk-id
  // trailer) is encountered for this blob file. The flag is sticky:
  // even if chunk-aware entries are subsequently observed, the output
  // bitmap for this blob remains empty.
  void MarkUnavailable(uint64_t blob_file_number) {
    if (!enabled_) return;
    unavailable_.insert(blob_file_number);
  }

  // Returns true iff MarkUnavailable() has ever been called for the
  // given blob file.
  bool IsUnavailable(uint64_t blob_file_number) const {
    return unavailable_.count(blob_file_number) > 0;
  }

  // Produce a per-dependence chunk bitmap vector, strictly aligned
  // to `dependence` by index: out[i] corresponds to dependence[i].
  // See semantics summary in the class-level comment.
  void Materialize(const std::vector<Dependence>& dependence,
                   std::vector<BlobChunkBitmap>* out) const {
    if (!enabled_) {
      out->clear();
      return;
    }
    out->clear();
    out->resize(dependence.size());
    for (size_t i = 0; i < dependence.size(); ++i) {
      const uint64_t fn = dependence[i].file_number;
      if (unavailable_.count(fn) > 0) {
        continue;
      }
      auto it = per_blob_.find(fn);
      if (it != per_blob_.end()) {
        (*out)[i] = it->second;
      }
    }
  }

 private:
  bool enabled_;
  uint64_t chunk_size_;
  std::unordered_map<uint64_t, BlobChunkBitmap> per_blob_;
  std::unordered_set<uint64_t> unavailable_;
};

}  // namespace TERARKDB_NAMESPACE
