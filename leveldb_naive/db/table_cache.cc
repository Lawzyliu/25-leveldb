// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

#include <iostream>
#include <fstream>
#include <string>
#include <cstdio>

#include "isa-l.h"
typedef unsigned char u8;
#define MMAX 255
#define KMAX 255

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

//add
/*
 * Generate decode matrix from encode matrix and erasure list
 *
 */

static int gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * decode_index, u8 * frag_err_list, int nerrs, int k,
				       int m)
{
	int i, j, p, r;
	int nsrcerrs = 0;
	u8 s, *b = temp_matrix;
	u8 frag_in_err[MMAX];

	memset(frag_in_err, 0, sizeof(frag_in_err));

	// Order the fragments in erasure for easier sorting
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)
			nsrcerrs++;
		frag_in_err[frag_err_list[i]] = 1;
	}

	// Construct b (matrix that encoded remaining frags) by removing erased rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (frag_in_err[r])
			r++;
		for (j = 0; j < k; j++)
			b[k * i + j] = encode_matrix[k * r + j];
		decode_index[i] = r;
	}

	// Invert matrix to get recovery matrix
	if (gf_invert_matrix(b, invert_matrix, k) < 0)
		return -1;

	// Get decode matrix with only wanted recovery rows
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)	// A src err
			for (j = 0; j < k; j++)
				decode_matrix[k * i + j] =
				    invert_matrix[k * frag_err_list[i] + j];
	}

	// For non-src (parity) erasures need to multiply encode matrix * invert
	for (p = 0; p < nerrs; p++) {
		if (frag_err_list[p] >= k) {	// A parity err
			for (i = 0; i < k; i++) {
				s = 0;
				for (j = 0; j < k; j++)
					s ^= gf_mul(invert_matrix[j * k + i],
						    encode_matrix[k * frag_err_list[p] + j]);
				decode_matrix[k * p + i] = s;
			}
		}
	}
	return 0;
}

//add
int recoverFiles(int c_idx1, int c_idx2, int k, int p, int len, const char * conruptedFile1, const char * conruptedFile2,
 const char * sourceFile1, const char * sourceFile2, const char * sourceFile3, const char * sourceFile4) {
  int stripe_len;
  int lastFileReLen;
  if(len % 4 == 0) {
    stripe_len = len / 4;
  }
  else {
    stripe_len = len / 4 + 1;
  }
  lastFileReLen = stripe_len - len / 4;
  int i, j, m, c, e, ret;
	int nerrs = 2;
  m = k + p;
  FILE *file;
  unsigned char buffer1[1000000] = {0}; // 用于存储读取的内容
  unsigned char buffer2[1000000] = {0}; // 用于存储读取的内容
  unsigned char buffer3[1000000] = {0}; // 用于存储读取的内容
  unsigned char buffer4[1000000] = {0}; // 用于存储读取的内容
  size_t bytesRead;
    // 打开文件
  file = fopen(sourceFile1, "rb"); // 第二个参数 "rb" 表示以二进制只读模式打开文件
  while ((bytesRead = fread(buffer1, 1, sizeof(buffer1), file)) > 0);
  file = fopen(sourceFile2, "rb");
  while ((bytesRead = fread(buffer2, 1, sizeof(buffer2), file)) > 0);
  file = fopen(sourceFile3, "rb");
  while ((bytesRead = fread(buffer3, 1, sizeof(buffer3), file)) > 0);
  file = fopen(sourceFile4, "rb");
  while ((bytesRead = fread(buffer4, 1, sizeof(buffer4), file)) > 0);
  // 关闭文件
  fclose(file);

	// Fragment buffer pointers
	u8 *recover_srcs[KMAX];
	u8 *recover_outp[KMAX];
	u8 frag_err_list[MMAX];

	// Coefficient matrices
	u8 *encode_matrix, *decode_matrix;
	u8 *invert_matrix, *temp_matrix;
	u8 *g_tbls;
	u8 decode_index[MMAX];

	//printf("ec_simple_example:\n");

	// Allocate coding matrices
	encode_matrix = (u8 *) malloc(m * k); //编码矩阵 6 * 4
	decode_matrix = (u8 *) malloc(m * k);
	invert_matrix = (u8 *) malloc(m * k);
	temp_matrix = (u8 *) malloc(m * k);
	g_tbls = (u8 *) malloc(k * p * 32);

	// Allocate buffers for recovered data
	for (i = 0; i < p; i++) {
		if (NULL == (recover_outp[i] = (u8 *) malloc(stripe_len))) {
			printf("alloc error: Fail\n");
			return -1;
		}
	}

	//printf(" encode (m,k,p)=(%d,%d,%d) len=%d\n", m, k, p, stripe_len);

	// Pick an encode matrix. A Cauchy matrix is a good choice as even
	// large k are always invertable keeping the recovery rule simple.
	// 生成柯西矩阵
	gf_gen_cauchy1_matrix(encode_matrix, m, k);
	// Initialize g_tbls from encode matrix
	ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);

	if (nerrs <= 0)
		return 0;

	//printf(" recover %d fragments\n", nerrs);

  frag_err_list[0] = c_idx1;
  frag_err_list[1] = c_idx2;
    
	// Find a decode matrix to regenerate all erasures from remaining frags
	// 生成一个解码矩阵
	ret = gf_gen_decode_matrix_simple(encode_matrix, decode_matrix,
					  invert_matrix, temp_matrix, decode_index,
					  frag_err_list, nerrs, k, m);
  
	if (ret != 0) {
		printf("Fail on generate decode matrix\n");
		return -1;
	}

  for(int i = 0; i < 4; i++) {
    recover_srcs[i] = (u8 *) malloc (stripe_len);
  }
  for(int i = 0; i < stripe_len; i++) {
    recover_srcs[0][i] = buffer1[i];
    recover_srcs[1][i] = buffer2[i];
    recover_srcs[2][i] = buffer3[i];
    recover_srcs[3][i] = buffer4[i];
    /*if(i < 10)
    printf("%d, ", buffer4[i]);*/
  }
  
	// Recover data
	ec_init_tables(k, nerrs, decode_matrix, g_tbls);
	ec_encode_data(stripe_len, k, nerrs, g_tbls, recover_srcs, recover_outp);
    
    // 写入数据到文件
    std::ofstream outfile(conruptedFile1);
    std::ofstream outfile2(conruptedFile2);
    int n = stripe_len;
    if(c_idx1 == 3) {
      n = stripe_len - lastFileReLen;
    }
    for(int i = 0; i < n; i++)
    outfile << recover_outp[0][i];
    outfile.close();

    n = stripe_len;
    if(c_idx2 == 3) {
      n = stripe_len - lastFileReLen; 
    }
    for(int i = 0; i < n; i++) {
      outfile2 << recover_outp[1][i];
      /*if(i < 10) {
        printf("%d ", recover_outp[1][i]);
      }*/
    }
    
    outfile2.close();
	return 0;
}
//add
void mergeFiles(int fileSize, const std::string& file1, const std::string& file2, const std::string& file3,
 const std::string& file4, const std::string& file5, const std::string& file6, const std::string& outputFile) {
    std::ifstream input1(file1);
    std::ifstream input2(file2);
    std::ifstream input3(file3);
    std::ifstream input4(file4);
    std::ifstream input5(file5);
    std::ifstream input6(file6);
    std::ofstream output(outputFile);
    int nerr = 0;
    int idx = 0;
    int b_con[6];
    for(int i = 0; i < 6; i++) {
      b_con[i] = 1;
    }
    std::string con_files_name[6];
    std::string source_files_name[6];
    if(!input1) {
      printf("1丢失了\n");
      b_con[0] = 0;
      con_files_name[nerr] = file1;
      nerr++;
    }
    else {
      source_files_name[idx++] = file1;
    }
    if(!input2) {
      printf("2丢失了\n");
      b_con[1] = 0;
      con_files_name[nerr] = file2;
      nerr++;
    }
    else {
      source_files_name[idx++] = file2;
    }
    if(!input3) {
      printf("3丢失了\n");
      b_con[2] = 0;
      con_files_name[nerr] = file3;
      nerr++;
    }
    else {
      source_files_name[idx++] = file3;
    }
    if(!input4) {
      printf("4丢失了\n");
      b_con[3] = 0;
      con_files_name[nerr] = file4;
      nerr++;
    }
    else {
      source_files_name[idx++] = file4;
    }
    if(!input5) {
      printf("5丢失了\n");
      b_con[4] = 0;
      con_files_name[nerr] = file5;
      nerr++;
    }
    else {
      source_files_name[idx++] = file5;
    }
    if(!input6) {
      printf("6丢失了\n");
      b_con[5] = 0;
      con_files_name[nerr] = file6;
      nerr++;
    }
    else {
      source_files_name[idx++] = file6;
    }
    if(nerr > 2) {
      std::cerr << "There are too many corrupted files to recover." << std::endl;
      return;
    }
    if(nerr == 1) {
      // 1 2 3 4 5 6
      printf("只有一个文件出错\n");
      for(int i = 5; i >= 0; i--) {
        if(b_con[i] == 1) {
          b_con[i] = 0;
          break;
        }
      }
      nerr++;
    }
    if(nerr == 2) {
      int idx[2] = {0};
      int cnt = 0;
      for(int i = 0; i < 6; i++) {
        if(b_con[i] == 0) {
          idx[cnt++] = i;
        }
      }
      recoverFiles(idx[0], idx[1], 4, 2, fileSize, con_files_name[0].c_str(), con_files_name[1].c_str(),
      source_files_name[0].c_str(), source_files_name[1].c_str(), source_files_name[2].c_str(), source_files_name[3].c_str());
    }
    input1.close();
    input2.close();
    input3.close();
    input4.close();
    input1.open(file1);
    input2.open(file2);
    input3.open(file3);
    input4.open(file4);
    output << input1.rdbuf();
    output << input2.rdbuf();
    output << input3.rdbuf();
    output << input4.rdbuf();
    //std::cout << "Files merged successfully!" << std::endl;
}

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname, const Options& options,
                       int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == nullptr) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = nullptr;
    Table* table = nullptr;
    //TODO
    //合并文件
    std::string file1 = TableFileName("/home/user/SSD/disk09/testdb/stripe1", file_number);
    std::string file2 = TableFileName("/home/user/SSD/disk09/testdb/stripe2", file_number);
    std::string file3 = TableFileName("/home/user/SSD/disk09/testdb/stripe3", file_number);
    std::string file4 = TableFileName("/home/user/SSD/disk09/testdb/stripe4", file_number);
    std::string file5 = TableFileName("/home/user/SSD/disk09/testdb/stripe5", file_number);
    std::string file6 = TableFileName("/home/user/SSD/disk09/testdb/stripe6", file_number);
    std::string outputFile = TableFileName("/home/user/SSD/disk09/merge", file_number);
    file1.erase(43);
    file1.append("_1.stripe");
    file2.erase(43);
    file2.append("_2.stripe");
    file3.erase(43);
    file3.append("_3.stripe");
    file4.erase(43);
    file4.append("_4.stripe");
    file5.erase(43);
    file5.append("_5.stripe");
    file6.erase(43);
    file6.append("_6.stripe");
    mergeFiles(file_size, file1, file2, file3, file4, file5, file6, outputFile);
    s = env_->NewRandomAccessFile(outputFile, &file);
    std::remove(outputFile.c_str());

    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options, uint64_t file_number,
                       uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&,
                                             const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
