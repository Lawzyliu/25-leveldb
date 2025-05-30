// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "leveldb/status.h"

typedef unsigned char u8;

namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

// Build a Table file from the contents of *iter.  The generated file
// will be named according to meta->number.  On success, the rest of
// *meta will be filled with metadata about the generated table.
// If no data is present in *iter, meta->file_size will be set to
// zero, and no Table file will be produced.
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta);

// my code
int splitFile(const std::string& inputFile, const std::string& bash);

void createDirectory(const std::string& path);

}  // namespace leveldb

int gen_redundance_file(int k, int p, int len, const char* filePath1, const char* filePath2, 
 const char* filePath3, const char* filePath4, const char* outputFile1, const char* outputFile2);

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_
