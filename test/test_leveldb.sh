#!/bin/bash

root_path="/home/user/Lzy"
leveldb_version="leveldb"

#leveldb编译
cd "$root_path/$leveldb_version/build"
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 .. && cmake --build .

#测试程序编译
cd "$root_path/test/input"
g++ -o test_leveldb test_leveldb.cpp "$root_path/$leveldb_version/build/libleveldb.a" -I"$root_path/$leveldb_version/include" -lpthread -lsnappy /lib/libisal.a

#执行测试程序
rm ../output/test_leveldb.txt
touch ../output/test_leveldb.txt
for ((i=0;i<5;i++))
    do
        rm -rf /home/user/SSD/disk09/testdb
        echo "This is test $i" >> ../output/test_leveldb.txt
        ./test_leveldb >> ../output/test_leveldb.txt
    done