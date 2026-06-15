//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/blob_death_log.h"

#include <algorithm>

namespace TERARKDB_NAMESPACE {

void BlobDeathLog::RecordDeath(const ValueLocation& loc) {
  // Only block-aware (v2) locations carry a usable slot id. Anything
  // else is conservatively skipped: GC will fall back to GetKey() for
  // those records, which is always safe.
  if (!loc.valid()) {
    return;
  }
  std::lock_guard<std::mutex> lock(mu_);
  PerVsstDeath& d = table_[loc.layout_id];
  const uint64_t packed = PackBlockSlot(loc.block_id, loc.slot_id);
  // Idempotent: inserting an existing element is a no-op.
  auto ins = d.dead_slots.insert(packed);
  if (ins.second) {
    ++d.dead_count_per_block[loc.block_id];
    ++records_emitted_;
    // Rough accounting: two 8-byte hash-set/map slots per record.
    approx_bytes_ += 2 * sizeof(uint64_t);
    // Soft budget: we never block recording (correctness first), but a
    // future persistence step would flush here. TODO: spill to sidecar.
    (void)buffer_limit_bytes_;
  }
}

BlobDeathMap BlobDeathLog::BuildDeathMap(uint64_t vsst_fn) const {
  BlobDeathMap out;
  out.vsst_file_number = vsst_fn;
  std::lock_guard<std::mutex> lock(mu_);
  auto it = table_.find(vsst_fn);
  if (it == table_.end()) {
    // Never tracked: empty, incomplete -> GC falls back entirely.
    return out;
  }
  const PerVsstDeath& d = it->second;
  // Snapshot copy so callers iterate lock-free.
  out.dead_slots = d.dead_slots;
  out.dead_count_per_block = d.dead_count_per_block;
  // complete iff we tracked the vSST from empty within this process.
  // This is the only condition under which it is safe to treat a
  // not-dead value as live without a GetKey() reverse lookup.
  out.complete = d.tracked_from_empty;
  return out;
}

void BlobDeathLog::OnVsstCreated(uint64_t vsst_fn) {
  std::lock_guard<std::mutex> lock(mu_);
  PerVsstDeath& d = table_[vsst_fn];
  d.tracked_from_empty = true;
}

void BlobDeathLog::OnVsstObsolete(uint64_t vsst_fn) {
  std::lock_guard<std::mutex> lock(mu_);
  auto it = table_.find(vsst_fn);
  if (it != table_.end()) {
    approx_bytes_ -= std::min<uint64_t>(
        approx_bytes_, 2 * sizeof(uint64_t) * it->second.dead_slots.size());
    table_.erase(it);
  }
}

size_t BlobDeathLog::ApproximateMemoryUsage() const {
  std::lock_guard<std::mutex> lock(mu_);
  return static_cast<size_t>(approx_bytes_);
}

}  // namespace TERARKDB_NAMESPACE
