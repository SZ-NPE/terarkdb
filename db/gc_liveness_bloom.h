#pragma once

#include <stdint.h>

#include <algorithm>
#include <limits>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/terark_namespace.h"
#include "util/coding.h"
#include "util/hash.h"

namespace TERARKDB_NAMESPACE {

static constexpr char kGarbageCollectionLivenessBloomBlock[] =
    "terarkdb.gc-liveness-bloom";

inline uint32_t GarbageCollectionLivenessHash(const ParsedInternalKey& key) {
  uint32_t hash = Hash(key.user_key.data(), key.user_key.size(), 0xbc9f1d34);
  char suffix[sizeof(uint64_t) + 1];
  EncodeFixed64(suffix, key.sequence);
  suffix[sizeof(uint64_t)] =
      key.type == kTypeMerge || key.type == kTypeMergeIndex ? 1 : 0;
  return Hash(suffix, sizeof(suffix), hash);
}

class GarbageCollectionLivenessBloomBuilder {
 public:
  static constexpr uint64_t kVersion = 1;
  static constexpr uint32_t kBitsPerReference = 16;
  static constexpr uint32_t kNumProbes = 11;

  void Add(uint64_t logical_file_number, const ParsedInternalKey& key) {
    hashes_[logical_file_number].push_back(
        GarbageCollectionLivenessHash(key));
  }

  bool empty() const { return hashes_.empty(); }

  std::string Finish() const {
    std::vector<uint64_t> logical_file_numbers;
    logical_file_numbers.reserve(hashes_.size());
    for (const auto& group : hashes_) {
      logical_file_numbers.push_back(group.first);
    }
    std::sort(logical_file_numbers.begin(), logical_file_numbers.end());

    std::string encoded;
    PutVarint64(&encoded, kVersion);
    PutVarint32(&encoded, kBitsPerReference);
    PutVarint32(&encoded, kNumProbes);
    PutVarint64(&encoded, logical_file_numbers.size());
    for (uint64_t logical_file_number : logical_file_numbers) {
      const auto& hashes = hashes_.at(logical_file_number);
      const uint64_t bit_count = std::max<uint64_t>(
          64, static_cast<uint64_t>(hashes.size()) * kBitsPerReference);
      std::string bits((bit_count + 7) / 8, '\0');
      for (uint32_t hash : hashes) {
        AddHash(hash, bit_count, kNumProbes, &bits[0]);
      }
      PutVarint64(&encoded, logical_file_number);
      PutVarint64(&encoded, hashes.size());
      PutVarint64(&encoded, bit_count);
      PutLengthPrefixedSlice(&encoded, Slice(bits));
    }
    return encoded;
  }

 private:
  static void AddHash(uint32_t hash, uint64_t bit_count, uint32_t probes,
                      char* bits) {
    const uint32_t delta = (hash >> 17) | (hash << 15);
    for (uint32_t probe = 0; probe < probes; ++probe) {
      const uint64_t bit = hash % bit_count;
      bits[bit / 8] |= static_cast<char>(uint8_t{1} << (bit % 8));
      hash += delta;
    }
  }

  std::unordered_map<uint64_t, std::vector<uint32_t>> hashes_;
};

class GarbageCollectionLivenessBloomIndex {
 public:
  Status Decode(const Slice& encoded) {
    Slice input = encoded;
    uint64_t version = 0;
    uint32_t bits_per_reference = 0;
    uint32_t probes = 0;
    uint64_t group_count = 0;
    if (!GetVarint64(&input, &version) ||
        version != GarbageCollectionLivenessBloomBuilder::kVersion ||
        !GetVarint32(&input, &bits_per_reference) ||
        bits_per_reference !=
            GarbageCollectionLivenessBloomBuilder::kBitsPerReference ||
        !GetVarint32(&input, &probes) ||
        probes != GarbageCollectionLivenessBloomBuilder::kNumProbes ||
        !GetVarint64(&input, &group_count) ||
        group_count > std::numeric_limits<uint32_t>::max()) {
      return Status::Corruption("Invalid GC liveness Bloom header");
    }

    std::vector<Group> groups;
    groups.reserve(static_cast<size_t>(group_count));
    uint64_t previous_file_number = 0;
    for (uint64_t index = 0; index < group_count; ++index) {
      uint64_t logical_file_number = 0;
      uint64_t entry_count = 0;
      uint64_t bit_count = 0;
      Slice bits;
      if (!GetVarint64(&input, &logical_file_number) ||
          logical_file_number == uint64_t(-1) ||
          (index > 0 && logical_file_number <= previous_file_number) ||
          !GetVarint64(&input, &entry_count) || entry_count == 0 ||
          !GetVarint64(&input, &bit_count) || bit_count < 64 ||
          entry_count >
              std::numeric_limits<uint64_t>::max() /
                  GarbageCollectionLivenessBloomBuilder::kBitsPerReference ||
          bit_count < entry_count *
                          GarbageCollectionLivenessBloomBuilder::
                              kBitsPerReference ||
          bit_count > std::numeric_limits<uint64_t>::max() - 7 ||
          !GetLengthPrefixedSlice(&input, &bits) ||
          bits.size() != (bit_count + 7) / 8) {
        return Status::Corruption("Invalid GC liveness Bloom group");
      }
      groups.push_back(
          {logical_file_number, entry_count, bit_count, probes,
           bits.ToString()});
      previous_file_number = logical_file_number;
    }
    if (!input.empty()) {
      return Status::Corruption("Trailing GC liveness Bloom data");
    }
    groups_ = std::move(groups);
    return Status::OK();
  }

  bool MatchesDependence(const std::vector<Dependence>& dependence) const {
    if (groups_.size() != dependence.size()) {
      return false;
    }
    std::vector<std::pair<uint64_t, uint64_t>> expected;
    expected.reserve(dependence.size());
    for (const auto& item : dependence) {
      expected.emplace_back(item.file_number, item.entry_count);
    }
    std::sort(expected.begin(), expected.end());
    for (size_t index = 0; index < groups_.size(); ++index) {
      if (groups_[index].logical_file_number != expected[index].first ||
          groups_[index].entry_count != expected[index].second) {
        return false;
      }
    }
    return true;
  }

  bool MayContain(const std::vector<uint64_t>& logical_file_numbers,
                  const ParsedInternalKey& key) const {
    const uint32_t hash = GarbageCollectionLivenessHash(key);
    for (uint64_t logical_file_number : logical_file_numbers) {
      auto group = std::lower_bound(
          groups_.begin(), groups_.end(), logical_file_number,
          [](const Group& left, uint64_t right) {
            return left.logical_file_number < right;
          });
      if (group != groups_.end() &&
          group->logical_file_number == logical_file_number &&
          MayContainHash(hash, *group)) {
        return true;
      }
    }
    return false;
  }

 private:
  struct Group {
    uint64_t logical_file_number;
    uint64_t entry_count;
    uint64_t bit_count;
    uint32_t probes;
    std::string bits;
  };

  static bool MayContainHash(uint32_t hash, const Group& group) {
    const uint32_t delta = (hash >> 17) | (hash << 15);
    for (uint32_t probe = 0; probe < group.probes; ++probe) {
      const uint64_t bit = hash % group.bit_count;
      if ((static_cast<uint8_t>(group.bits[bit / 8]) &
           (uint8_t{1} << (bit % 8))) == 0) {
        return false;
      }
      hash += delta;
    }
    return true;
  }

  std::vector<Group> groups_;
};

}  // namespace TERARKDB_NAMESPACE
