// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

//add
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <sys/stat.h> // 包含mkdir函数
#include "isa-l.h"
#include <stdlib.h>
#include <string.h>

typedef unsigned char u8;
#define MMAX 255
#define KMAX 255

namespace leveldb {

Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();

  std::string fname = TableFileName(dbname, meta->number);
  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);
    meta->smallest.DecodeFrom(iter->key());
    Slice key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      builder->Add(key, iter->value());
    }
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // Finish and check for builder errors
    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // Finish and check for file errors
    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    //my code
    std:: string inputFile = fname;
    std:: string outputFilePrefix = fname;
    if (outputFilePrefix.size() >= 3 && outputFilePrefix.substr(outputFilePrefix.size() - 3) == "ldb") {
        outputFilePrefix.erase(outputFilePrefix.size() - 11);
        splitFile(inputFile, outputFilePrefix);
        std::remove(inputFile.c_str());
    }

    if (s.ok()) {
      // Verify that the table is usable
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number,
                                              meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

//my code
int splitFile(const std::string& inputFile, const std::string& basePath) {
  std::string inputFilename1, inputFilename2, inputFilename3, inputFilename4;
  std::string outputFilename1, outputFilename2;
  for(int i = 1; i <= 6; i++) {
    std::string directory = "/home/user/SSD/disk09/testdb/stripe";
    directory.append(std::to_string(i));
    createDirectory(directory); 
  }
  
    std::ifstream input(inputFile, std::ios::binary);
    if (!input) {
        std::cerr << "Failed to open input file." << std::endl;
        return 0;
    }

    input.seekg(0, std::ios::end);
    std::streampos fileSize = input.tellg();
    input.seekg(0, std::ios::beg);
    //printf("filesize:%d\n",(int)fileSize);
    const std::streampos segmentSize = fileSize / 4 + 1;
    char buffer[1024 * 102];
    for (int i = 0; i < 6; ++i) {
        std::ostringstream outputFilename;
        outputFilename << basePath << "/stripe" << i + 1 << "/" << inputFile.substr(inputFile.size() - 10, 6) << "_" << i + 1 << ".stripe";
        if(i >= 0 && i <= 3) {
          std::ofstream output(outputFilename.str(), std::ios::binary);
          if (!output) {
            std::cerr << "Failed to create output file." << std::endl;
            return 0;
          }
          std::streampos remainingSize = segmentSize;
          if(i == 3) {
            remainingSize = fileSize - segmentSize * 3;
          }
	  /*
	  else
          { remainingSize = segmentSize; }
	  input.read(buffer,remainingSize);
	  output.write(buffer,remainingSize);*/
          while (remainingSize > 0) {
            std::streamsize blockSize = std::min(remainingSize, static_cast<std::streampos>(sizeof(buffer)));
            input.read(buffer, blockSize);
            output.write(buffer, blockSize);
            remainingSize -= blockSize;
          }
	  
        }
        if(i == 0)
        inputFilename1 = outputFilename.str();
        if(i == 1)
        inputFilename2 = outputFilename.str();
        if(i == 2)
        inputFilename3 = outputFilename.str();
        if(i == 3)
        inputFilename4 = outputFilename.str();
        if(i == 4)
        outputFilename1 = outputFilename.str();
        if(i == 5)
        outputFilename2 = outputFilename.str();
    }
    gen_redundance_file(4, 2, segmentSize, inputFilename1.c_str(), inputFilename2.c_str(), 
    inputFilename3.c_str(), inputFilename4.c_str(), outputFilename1.c_str(), outputFilename2.c_str());
    return 1;
}

void createDirectory(const std::string& path) {
    if (mkdir(path.c_str(), 0777) == -1 && errno != EEXIST) {
        std::cerr << "Failed to create directory: " << path << std::endl;
    }
}

}  // namespace leveldb

int gen_redundance_file(int k, int p, int len, const char* filePath1, const char* filePath2, 
 const char* filePath3, const char* filePath4, const char* outputFile1, const char* outputFile2) {
  int i, j, m;
	int nerrs = 2;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];

	// Coefficient matrices
	u8 *encode_matrix;
	u8 *g_tbls;
	m = k + p;

  FILE *file;
  char buffer1[2000000] = {0}; // 用于存储读取的内容
  char buffer2[2000000] = {0}; // 用于存储读取的内容
  char buffer3[2000000] = {0}; // 用于存储读取的内容
  char buffer4[2000000] = {0}; // 用于存储读取的内容
    // 打开文件
    file = fopen(filePath1, "r");
    fread(buffer1, 1, len, file);
    file = fopen(filePath2, "r"); 
    fread(buffer2, 1, len, file);
    file = fopen(filePath3, "r");
    fread(buffer3, 1, len, file);
    file = fopen(filePath4, "r");
    fread(buffer4, 1, len, file);
    // 关闭文件
    fclose(file);

	// Allocate coding matrices
	encode_matrix = (u8 * ) malloc(m * k); //编码矩阵 6 * 4
	g_tbls = (u8 * ) malloc(k * p * 32);

    // Allocate the src & parity buffers
	for (i = 0; i < m; i++) {
		if (NULL == (frag_ptrs[i] = (u8 * ) malloc(len))) {
			printf("alloc error: Fail\n");
			return -1;
		}
	}
	for (i = 0; i < len; i++) {
        frag_ptrs[0][i] = buffer1[i];
        frag_ptrs[1][i] = buffer2[i];
        frag_ptrs[2][i] = buffer3[i];
        frag_ptrs[3][i] = buffer4[i];
	}

	// Pick an encode matrix. A Cauchy matrix is a good choice as even
	// large k are always invertable keeping the recovery rule simple.
	// 生成柯西矩阵
	gf_gen_cauchy1_matrix(encode_matrix, m, k);
	// Initialize g_tbls from encode matrix
	ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);

	// Generate EC parity blocks from sources
	// 生成冗余块
	ec_encode_data(len, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);

    file = fopen(outputFile1, "w"); // 第二个参数 "w" 表示以写入模式打开文件
    if (file == NULL) {
        perror("Error opening file");
        return -1;
    }

    // 写入数据到文件
    if (fwrite(frag_ptrs[4], sizeof(char), len, file) != len) {
        perror("Error writing to file");
        return -1;
    }
    fclose(file);
    file = fopen(outputFile2, "w"); // 第二个参数 "w" 表示以写入模式打开文件
    if (file == NULL) {
        perror("Error opening file");
        return -1;
    }
    

    // 写入数据到文件
    if (fwrite(frag_ptrs[5], sizeof(char), len, file) != len) {
        perror("Error writing to file");
        return -1;
    }
    // 关闭文件
    fclose(file);
	if (nerrs <= 0)
		return 0;
	return 1;
} 
