#include <iostream>
#include <string>
#include <leveldb/db.h>
#include <sys/time.h>

using namespace std;
#define LENGTH 256
#define NUM_STRINGS 1000000
#define NUM_READ 10000

// 生成有序字符序列
void generateOrderedString(char *str, int length, int suffix) {
    int i, j;
    for (i = 0, j = 0; i < length - 1; ++i, ++j) {
        // 根据你的要求排列字符，这里使用了简单的循环
        str[i] = 'a' + (j % 26);
    }
    str[length - 1] = '\0';
    
    // 添加唯一的后缀
    sprintf(str + length - 10, "%09d", suffix); // 为后缀留出9位数字空间
}

int main()
{
    leveldb::DB *db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "/home/user/SSD/disk09/testdb", &db);
    cout <<"status:" << status.ok() << endl;
    if(!status.ok()){
        return 1;
    }

    char **strings = (char **)malloc(NUM_STRINGS * sizeof(char *));
    if (strings == NULL) {
        fprintf(stderr, "Memory allocation failed.\n");
        return 1;
    }
    
    
    // 生成一百万个有序字符串
    for (int i = 0; i < NUM_STRINGS; ++i) {
        strings[i] = (char *)malloc((LENGTH + 1) * sizeof(char));
        if (strings[i] == NULL) {
            fprintf(stderr, "Memory allocation failed.\n");
            return 1;
        }
        generateOrderedString(strings[i], LENGTH, i);
    }
    string get;
    int cnt = 0;
    srand(1);
    struct timeval start,end;
    long mtime,seconds,useconds;
    gettimeofday(&start,NULL);
    for(int i = 0; i < NUM_STRINGS; i++) {
        string key = strings[i];
        string value = strings[i];
        leveldb::Status s = db->Put(leveldb::WriteOptions(), key, value);
        if(s.ok()) {
            cnt++;
        }
    }
    gettimeofday(&end,NULL);
    seconds = end.tv_sec - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
    mtime = ((seconds) * 1000 + useconds / 1000.0) + 0.5;
    std::cout<<"mtime:"<<mtime<<std::endl;
    
    std::string value;
    gettimeofday(&start,NULL);
    for(int i = 0; i < NUM_READ; i++) {
        leveldb::Status s = db->Get(leveldb::ReadOptions(), strings[rand()%NUM_READ], &value);
        if(s.ok()) {
            cnt++;
        }
    }
    gettimeofday(&end,NULL);
    seconds = end.tv_sec - start.tv_sec;
    useconds = end.tv_usec - start.tv_usec;
    mtime = ((seconds) * 1000 + useconds / 1000.0) + 0.5;
    std::cout<<"mtime:"<<mtime<<std::endl;
    
    // 释放内存
    for (int i = 0; i < NUM_STRINGS; ++i) {
        free(strings[i]);
    }
    free(strings);
    cout<<cnt<<endl;
    delete db;

    return 0;
}
