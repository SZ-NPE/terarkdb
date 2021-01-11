//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#ifndef ROCKSDB_LITE

#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/iterator.h"
#include "rocksdb/table_properties.h"

namespace TERARKDB_NAMESPACE {

// SstFileReader is used to read sst files that are generated by DB or
// SstFileWriter.
class SstFileReader {
 public:
  SstFileReader(const Options& options);

  ~SstFileReader();

  // Prepares to read from the file located at "file_path".
  Status Open(const std::string& file_path);

  // Returns a new iterator over the table contents.
  // Most read options provide the same control as we read from DB.
  // If "snapshot" is nullptr, the iterator returns only the latest keys.
  Iterator* NewIterator(const ReadOptions& options);

  std::shared_ptr<const TableProperties> GetTableProperties() const;

  // Verifies whether there is corruption in this table.
  Status VerifyChecksum();

 private:
  struct Rep;
  std::unique_ptr<Rep> rep_;
};

}  // namespace TERARKDB_NAMESPACE

#endif  // !ROCKSDB_LITE
