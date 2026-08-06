#pragma once

#include <stdint.h>

#include <string>

#include "db/dbformat.h"
#include "rocksdb/terark_namespace.h"
#include "table/internal_iterator.h"
#include "util/iterator_cache.h"

namespace TERARKDB_NAMESPACE {

class StreamingGarbageCollectionValidator {
 public:
  enum class Result {
    kUnknown,
    kDead,
    kSourceMismatch,
    kLive,
  };

  StreamingGarbageCollectionValidator(
      InternalIterator* references, const Comparator* user_comparator,
      const DependenceMap* dependence_map)
      : references_(references),
        user_comparator_(user_comparator),
        dependence_map_(dependence_map) {}

  Result Validate(const ParsedInternalKey& candidate,
                  uint64_t target_file_number) {
    if (references_ == nullptr || user_comparator_ == nullptr ||
        dependence_map_ == nullptr || candidate.type != kTypeValue ||
        !references_->status().ok()) {
      return Result::kUnknown;
    }
    if (!LoadUser(candidate.user_key)) {
      return stream_complete_ && references_->status().ok() ? Result::kDead
                                                            : Result::kUnknown;
    }
    if (!cached_reference_complete_) {
      return Result::kUnknown;
    }
    if (cached_type_ == kTypeMerge || cached_type_ == kTypeMergeIndex) {
      return Result::kUnknown;
    }
    if (cached_sequence_ < candidate.sequence) {
      return Result::kUnknown;
    }
    if (cached_sequence_ > candidate.sequence ||
        cached_type_ != kTypeValueIndex) {
      return Result::kDead;
    }
    return cached_source_file_number_ == target_file_number
               ? Result::kLive
               : Result::kSourceMismatch;
  }

 private:
  bool LoadUser(const Slice& user_key) {
    if (has_cached_user_) {
      const int comparison =
          user_comparator_->Compare(Slice(cached_user_key_), user_key);
      if (comparison == 0) {
        return true;
      }
      if (comparison > 0) {
        stream_complete_ = false;
        return false;
      }
      SkipCachedUser();
    } else if (!stream_initialized_) {
      InternalKey seek_key(user_key, kMaxSequenceNumber, kValueTypeForSeek);
      references_->Seek(seek_key.Encode());
      stream_initialized_ = true;
    }

    while (references_->Valid()) {
      ParsedInternalKey parsed;
      if (!ParseInternalKey(references_->key(), &parsed)) {
        stream_complete_ = false;
        cached_reference_complete_ = false;
        return false;
      }
      const int comparison = user_comparator_->Compare(parsed.user_key, user_key);
      if (comparison < 0) {
        SkipUser(parsed.user_key);
        continue;
      }
      if (comparison > 0) {
        return false;
      }
      return CacheCurrentUser(parsed);
    }
    return false;
  }

  void SkipCachedUser() {
    SkipUser(Slice(cached_user_key_));
    has_cached_user_ = false;
  }

  void SkipUser(const Slice& user_key) {
    std::string key(user_key.data(), user_key.size());
    while (references_->Valid()) {
      ParsedInternalKey parsed;
      if (!ParseInternalKey(references_->key(), &parsed)) {
        stream_complete_ = false;
        break;
      }
      if (user_comparator_->Compare(parsed.user_key, Slice(key)) != 0) {
        break;
      }
      references_->Next();
    }
  }

  bool CacheCurrentUser(const ParsedInternalKey& first) {
    cached_user_key_.assign(first.user_key.data(), first.user_key.size());
    cached_sequence_ = first.sequence;
    cached_type_ = first.type;
    cached_source_file_number_ = uint64_t(-1);
    cached_reference_complete_ =
        DecodeCurrentSource(first.type, &cached_source_file_number_);
    references_->Next();
    while (references_->Valid()) {
      ParsedInternalKey duplicate;
      uint64_t duplicate_source = uint64_t(-1);
      if (!ParseInternalKey(references_->key(), &duplicate)) {
        stream_complete_ = false;
        cached_reference_complete_ = false;
        break;
      }
      if (user_comparator_->Compare(duplicate.user_key,
                                    Slice(cached_user_key_)) != 0 ||
          duplicate.sequence != cached_sequence_) {
        break;
      }
      if (duplicate.type != cached_type_ ||
          !DecodeCurrentSource(duplicate.type, &duplicate_source) ||
          duplicate_source != cached_source_file_number_) {
        cached_reference_complete_ = false;
      }
      references_->Next();
    }
    has_cached_user_ = true;
    return true;
  }

  bool DecodeCurrentSource(ValueType value_type,
                           uint64_t* source_file_number) const {
    if (value_type != kTypeValueIndex && value_type != kTypeMergeIndex) {
      return true;
    }
    LazyBuffer value = references_->value();
    Status status = value.fetch();
    if (!status.ok()) {
      return false;
    }
    SeparateHelper::ValueReference reference;
    if (!SeparateHelper::DecodeValueReference(value.slice(), &reference)) {
      return false;
    }
    auto dependence = dependence_map_->find(reference.file_number);
    if (dependence == dependence_map_->end() || dependence->second == nullptr) {
      return false;
    }
    *source_file_number = dependence->second->fd.GetNumber();
    return true;
  }

  InternalIterator* references_;
  const Comparator* user_comparator_;
  const DependenceMap* dependence_map_;
  bool has_cached_user_ = false;
  bool stream_initialized_ = false;
  bool stream_complete_ = true;
  bool cached_reference_complete_ = false;
  std::string cached_user_key_;
  SequenceNumber cached_sequence_ = kMaxSequenceNumber;
  ValueType cached_type_ = kTypeDeletion;
  uint64_t cached_source_file_number_ = uint64_t(-1);
};

}  // namespace TERARKDB_NAMESPACE
