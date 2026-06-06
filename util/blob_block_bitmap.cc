//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "util/blob_block_bitmap.h"

#include <algorithm>
#include <cstring>

#include "util/coding.h"

namespace TERARKDB_NAMESPACE {

namespace {

inline size_t BytesForBits(uint64_t num_bits) {
  return static_cast<size_t>((num_bits + 7) / 8);
}

// popcount for a single byte using a compact table.
inline uint8_t PopcountByte(uint8_t v) {
  static const uint8_t kTable[256] = {
      0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 1, 2, 2, 3, 2, 3, 3, 4,
      2, 3, 3, 4, 3, 4, 4, 5, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 1, 2, 2, 3, 2, 3, 3, 4,
      2, 3, 3, 4, 3, 4, 4, 5, 2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
      4, 5, 5, 6, 5, 6, 6, 7, 1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 2, 3, 3, 4, 3, 4, 4, 5,
      3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
      2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6, 3, 4, 4, 5, 4, 5, 5, 6,
      4, 5, 5, 6, 5, 6, 6, 7, 3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
      4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8};
  return kTable[v];
}

}  // namespace

void BlobBlockBitmap::EnsureCapacity(uint64_t block_id) {
  uint64_t required_bits = block_id + 1;
  if (required_bits <= num_bits_) {
    return;
  }
  size_t required_bytes = BytesForBits(required_bits);
  if (required_bytes > bits_.size()) {
    bits_.resize(required_bytes, 0);
  }
  num_bits_ = required_bits;
}

void BlobBlockBitmap::Set(uint64_t block_id) {
  EnsureCapacity(block_id);
  bits_[static_cast<size_t>(block_id >> 3)] |=
      static_cast<uint8_t>(1u << (block_id & 7));
}

bool BlobBlockBitmap::Test(uint64_t block_id) const {
  if (block_id >= num_bits_) {
    return false;
  }
  return (bits_[static_cast<size_t>(block_id >> 3)] &
          static_cast<uint8_t>(1u << (block_id & 7))) != 0;
}

void BlobBlockBitmap::OrWith(const BlobBlockBitmap& other) {
  if (other.num_bits_ == 0) {
    return;
  }
  if (other.num_bits_ > num_bits_) {
    size_t new_bytes = BytesForBits(other.num_bits_);
    if (new_bytes > bits_.size()) {
      bits_.resize(new_bytes, 0);
    }
    num_bits_ = other.num_bits_;
  }
  const size_t n = std::min(bits_.size(), other.bits_.size());
  for (size_t i = 0; i < n; ++i) {
    bits_[i] |= other.bits_[i];
  }
}

uint64_t BlobBlockBitmap::CountSetBits() const {
  uint64_t total = 0;
  const size_t n = BytesForBits(num_bits_);
  const size_t limit = std::min(n, bits_.size());
  for (size_t i = 0; i < limit; ++i) {
    total += PopcountByte(bits_[i]);
  }
  // Mask tail bits that lie beyond num_bits_ (defense in depth for
  // callers that manually toggled bytes via future low-level APIs).
  const uint64_t tail = num_bits_ & 7;
  if (tail != 0 && limit > 0) {
    const uint8_t last = bits_[limit - 1];
    const uint8_t mask = static_cast<uint8_t>((1u << tail) - 1);
    total -= PopcountByte(static_cast<uint8_t>(last & ~mask));
  }
  return total;
}

void BlobBlockBitmap::Serialize(std::string* dst) const {
  PutVarint64(dst, num_bits_);
  const size_t n = BytesForBits(num_bits_);
  if (n == 0) {
    return;
  }
  const size_t src_n = std::min(n, bits_.size());
  dst->append(reinterpret_cast<const char*>(bits_.data()), src_n);
  if (src_n < n) {
    dst->append(n - src_n, '\0');
  }
}

bool BlobBlockBitmap::Deserialize(Slice* input) {
  if (input == nullptr) {
    return false;
  }
  uint64_t num_bits = 0;
  if (!GetVarint64(input, &num_bits)) {
    return false;
  }
  const size_t n = BytesForBits(num_bits);
  if (input->size() < n) {
    return false;
  }
  bits_.assign(reinterpret_cast<const uint8_t*>(input->data()),
               reinterpret_cast<const uint8_t*>(input->data()) + n);
  num_bits_ = num_bits;
  input->remove_prefix(n);
  return true;
}

namespace {
// flags bit layout for DependenceBlockBitmap.
constexpr uint8_t kDepRowAvailableBit = 0x1;
}  // namespace

void DependenceBlockBitmap::Serialize(std::string* dst) const {
  uint8_t flags = 0;
  if (available) {
    flags |= kDepRowAvailableBit;
  }
  dst->push_back(static_cast<char>(flags));
  PutFixed64(dst, layout_id);
  bitmap.Serialize(dst);
}

bool DependenceBlockBitmap::Deserialize(Slice* input) {
  // Default to the safe unavailable state; on any error the row stays
  // unavailable so GC falls back.
  available = false;
  layout_id = kNoBlockLayoutId;
  bitmap.Clear();

  if (input == nullptr || input->size() < 1 + sizeof(uint64_t)) {
    return false;
  }
  const uint8_t flags = static_cast<uint8_t>((*input)[0]);
  input->remove_prefix(1);
  uint64_t decoded_layout = 0;
  if (!GetFixed64(input, &decoded_layout)) {
    return false;
  }
  BlobBlockBitmap decoded_bitmap;
  if (!decoded_bitmap.Deserialize(input)) {
    return false;
  }
  available = (flags & kDepRowAvailableBit) != 0;
  layout_id = decoded_layout;
  bitmap = std::move(decoded_bitmap);
  return true;
}

}  // namespace TERARKDB_NAMESPACE
