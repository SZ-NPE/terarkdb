//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// BlobDeathLog records the physical death locations (vSST file number +
// data block ordinal + in-block slot ordinal) of separated values that
// are overwritten or dropped during compaction. Blob GC then builds a
// per-vSST DeathMap from the log to:
//   - physically skip data blocks where every value is proven dead, and
//   - avoid the per-record GetKey() reverse lookup when the death map is
//     complete (every live value can be decided directly).
//
// SAFETY AXIOM: a record is declared dead ONLY when the death map holds
// an explicit (block_id, slot_id) hit. Any missing/incomplete/ambiguous
// state is conservatively treated as live and falls back to the legacy
// GetKey() path. Thus a missing death record can never drop a live value.
//
// This POC keeps the log in memory only. The persistence hooks
// (Persist/Load) are declared but intentionally unimplemented; see the
// TODOs. db_bench can exercise the in-memory fast path end-to-end.

#pragma once

#include <cstdint>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "db/dbformat.h"
#include "rocksdb/terark_namespace.h"

namespace TERARKDB_NAMESPACE {

// Pack a (block_id, slot_id) pair into a single 64-bit key for the dead
// set. block_id occupies the high 40 bits, slot_id the low 24 bits.
// 2^40 data blocks and 2^24 entries/block are far beyond any realistic
// vSST, so collisions cannot happen in practice.
inline uint64_t PackBlockSlot(uint64_t block_id, uint64_t slot_id) {
  return (block_id << 24) | (slot_id & 0xFFFFFFULL);
}

// Read-only snapshot of the death information for a single vSST, built
// by BlobDeathLog::BuildDeathMap(). GC consults this without holding the
// log mutex.
struct BlobDeathMap {
  uint64_t vsst_file_number = static_cast<uint64_t>(-1);
  // Packed (block_id, slot_id) of every value known to be dead.
  std::unordered_set<uint64_t> dead_slots;
  // Number of dead values observed per data block.
  std::unordered_map<uint64_t, uint32_t> dead_count_per_block;
  // `complete` is true only when the log tracked this vSST for its whole
  // lifetime in this process and no ambiguity (layout conflict / missing
  // slot) was observed. Only when complete may GC skip the GetKey()
  // lookup for values NOT in `dead_slots` (treating them as live).
  bool complete = false;

  bool IsValueDead(uint64_t block_id, uint64_t slot_id) const {
    return dead_slots.count(PackBlockSlot(block_id, slot_id)) > 0;
  }
};

class BlobDeathLog {
 public:
  // buffer_limit_bytes: soft memory budget for the in-memory records.
  explicit BlobDeathLog(uint64_t buffer_limit_bytes)
      : buffer_limit_bytes_(buffer_limit_bytes) {}

  // Record that the separated value at `loc` is dead. Idempotent:
  // recording the same location twice is a no-op. No-op when loc is not
  // valid() (e.g. legacy/v1 index without slot id) -- such records are
  // conservatively left as "live" so GC falls back for them.
  void RecordDeath(const ValueLocation& loc);

  // Build a read-only death map for `vsst_fn`. The returned map is a
  // value snapshot; the caller may iterate it without holding any lock.
  // When the vSST was never tracked, returns an empty, complete=false
  // map (GC then falls back entirely).
  BlobDeathMap BuildDeathMap(uint64_t vsst_fn) const;

  // Lifecycle hooks used to decide death-map completeness. A vSST is
  // only `complete` when it was created and tracked from empty within
  // this process (so every overwrite/drop against it was observed).
  void OnVsstCreated(uint64_t vsst_fn);
  void OnVsstObsolete(uint64_t vsst_fn);

  uint64_t records_emitted() const {
    std::lock_guard<std::mutex> lock(mu_);
    return records_emitted_;
  }

  size_t ApproximateMemoryUsage() const;

  // TODO(blob-death-log persistence): persist the per-vSST death records
  // to an append-only sidecar file and record installed segments via a
  // VersionEdit-like mechanism, then reload on open so death maps stay
  // complete across process restarts. Not implemented in the POC.
  // Status Persist(VersionEdit* edit);
  // Status Load(...);

 private:
  struct PerVsstDeath {
    std::unordered_set<uint64_t> dead_slots;          // packed block|slot
    std::unordered_map<uint64_t, uint32_t> dead_count_per_block;
    bool tracked_from_empty = false;                  // created here
  };

  mutable std::mutex mu_;
  std::unordered_map<uint64_t, PerVsstDeath> table_;
  uint64_t buffer_limit_bytes_;
  uint64_t records_emitted_ = 0;
  uint64_t approx_bytes_ = 0;
};

}  // namespace TERARKDB_NAMESPACE
