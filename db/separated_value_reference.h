#pragma once

#include <stdint.h>

#include <algorithm>
#include <limits>
#include <string>
#include <unordered_set>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/table_properties.h"
#include "rocksdb/terark_namespace.h"
#include "util/coding.h"

namespace TERARKDB_NAMESPACE {

static constexpr char kSeparatedValueReferenceProperty[] =
    "terarkdb.separated-value-reference";

struct SeparatedValueReferenceMetadata {
  static constexpr uint64_t kVersion = 1;
  static constexpr uint64_t kMaxTargetFiles = 1 << 20;

  uint64_t record_count = 0;
  uint64_t separated_record_count = 0;
  uint64_t range_deletion_count = 0;
  bool complete = true;
  std::vector<uint64_t> target_file_numbers;
  std::unordered_set<uint64_t> target_file_number_index;

  void AddReference(const Slice& /* key */, uint64_t target_file_number) {
    ++separated_record_count;
    constexpr size_t kLinearSearchLimit = 8;
    if (target_file_number_index.empty() &&
        target_file_numbers.size() < kLinearSearchLimit) {
      if (std::find(target_file_numbers.begin(), target_file_numbers.end(),
                    target_file_number) == target_file_numbers.end()) {
        target_file_numbers.push_back(target_file_number);
      }
      return;
    }
    if (target_file_number_index.empty()) {
      target_file_number_index.insert(target_file_numbers.begin(),
                                      target_file_numbers.end());
    }
    if (target_file_number_index.insert(target_file_number).second) {
      target_file_numbers.push_back(target_file_number);
    }
  }

  void Finalize() {
    std::sort(target_file_numbers.begin(), target_file_numbers.end());
    target_file_numbers.erase(
        std::unique(target_file_numbers.begin(), target_file_numbers.end()),
        target_file_numbers.end());
  }

  bool IsValid() const {
    return separated_record_count <= record_count &&
           range_deletion_count <= record_count &&
           target_file_numbers.size() <= kMaxTargetFiles &&
           std::is_sorted(target_file_numbers.begin(),
                          target_file_numbers.end()) &&
           std::adjacent_find(target_file_numbers.begin(),
                              target_file_numbers.end()) ==
               target_file_numbers.end() &&
           std::find(target_file_numbers.begin(), target_file_numbers.end(),
                     uint64_t(-1)) == target_file_numbers.end();
  }

};

inline std::string EncodeSeparatedValueReferenceMetadata(
    SeparatedValueReferenceMetadata metadata) {
  metadata.Finalize();
  std::string encoded;
  PutVarint64(&encoded, SeparatedValueReferenceMetadata::kVersion);
  PutVarint64(&encoded, metadata.record_count);
  PutVarint64(&encoded, metadata.separated_record_count);
  PutVarint64(&encoded, metadata.range_deletion_count);
  PutVarint64(&encoded, metadata.complete ? 1 : 0);
  PutVarint64(&encoded, metadata.target_file_numbers.size());
  for (uint64_t target_file_number : metadata.target_file_numbers) {
    PutVarint64(&encoded, target_file_number);
  }
  return encoded;
}

inline Status DecodeSeparatedValueReferenceMetadata(
    const Slice& encoded, SeparatedValueReferenceMetadata* metadata) {
  if (metadata == nullptr) {
    return Status::InvalidArgument("Reference metadata output is null");
  }
  Slice input = encoded;
  uint64_t version = 0;
  uint64_t complete = 0;
  uint64_t target_count = 0;
  SeparatedValueReferenceMetadata parsed;
  if (!GetVarint64(&input, &version) ||
      version != SeparatedValueReferenceMetadata::kVersion ||
      !GetVarint64(&input, &parsed.record_count) ||
      !GetVarint64(&input, &parsed.separated_record_count) ||
      !GetVarint64(&input, &parsed.range_deletion_count) ||
      !GetVarint64(&input, &complete) || complete > 1 ||
      !GetVarint64(&input, &target_count) ||
      target_count > SeparatedValueReferenceMetadata::kMaxTargetFiles) {
    return Status::Corruption("Invalid separated-value reference metadata");
  }
  parsed.complete = complete != 0;
  parsed.target_file_numbers.reserve(static_cast<size_t>(target_count));
  for (uint64_t index = 0; index < target_count; ++index) {
    uint64_t target_file_number = 0;
    if (!GetVarint64(&input, &target_file_number)) {
      return Status::Corruption("Truncated separated-value target metadata");
    }
    parsed.target_file_numbers.push_back(target_file_number);
  }
  if (!input.empty() || !parsed.IsValid()) {
    return Status::Corruption("Inconsistent separated-value reference metadata");
  }
  *metadata = std::move(parsed);
  return Status::OK();
}

inline Status ParseSeparatedValueReferenceProperty(
    const UserCollectedProperties& properties,
    SeparatedValueReferenceMetadata* metadata) {
  auto iter = properties.find(kSeparatedValueReferenceProperty);
  if (iter == properties.end()) {
    if (metadata != nullptr) {
      *metadata = SeparatedValueReferenceMetadata();
      metadata->complete = false;
    }
    return Status::NotFound("Separated-value reference metadata is missing");
  }
  return DecodeSeparatedValueReferenceMetadata(Slice(iter->second), metadata);
}

inline void GetSeparatedValueReferenceTargets(
    const SeparatedValueReferenceMetadata& metadata,
    std::unordered_set<uint64_t>* targets) {
  if (targets == nullptr) {
    return;
  }
  targets->insert(metadata.target_file_numbers.begin(),
                  metadata.target_file_numbers.end());
}

inline bool HasCompleteSeparatedValueReferenceMetadata(
    const TableProperties& table_properties) {
  SeparatedValueReferenceMetadata metadata;
  Status status = ParseSeparatedValueReferenceProperty(
      table_properties.user_collected_properties, &metadata);
  if (!status.ok() || !metadata.complete ||
      metadata.record_count != table_properties.num_entries) {
    return false;
  }
  std::unordered_set<uint64_t> metadata_targets;
  GetSeparatedValueReferenceTargets(metadata, &metadata_targets);
  std::unordered_set<uint64_t> manifest_targets;
  for (const auto& dependence : table_properties.dependence) {
    manifest_targets.insert(dependence.file_number);
  }
  return metadata_targets == manifest_targets;
}

}  // namespace TERARKDB_NAMESPACE
