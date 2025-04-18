#!/bin/bash

root_path="/home/user/Lzy"
leveldb_version="leveldb_efficient"

#leveldb编译
cd "$root_path/$leveldb_version/build"
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 .. && cmake --build .

#测试程序编译
cd "$root_path/test/input"
g++ -o test_efficient test_efficient.cpp "$root_path/$leveldb_version/build/libleveldb.a" -I"$root_path/$leveldb_version/include" -lpthread -lsnappy /lib/libisal.a

#执行测试程序
rm ../output/test_efficient.txt
touch ../output/test_efficient.txt
for ((i=0;i<5;i++))
    do
        rm -rf /home/user/SSD/disk09/testdb
        rm -rf /home/user/SSD/disk10/stripe
        rm -rf /home/user/SSD/disk11/stripe
        rm -rf /home/user/SSD/disk12/stripe
        rm -rf /home/user/SSD/disk13/stripe
        rm -rf /home/user/SSD/disk14/stripe
        rm -rf /home/user/SSD/disk15/stripe
        mkdir /home/user/SSD/disk10/stripe
        mkdir /home/user/SSD/disk11/stripe
        mkdir /home/user/SSD/disk12/stripe
        mkdir /home/user/SSD/disk13/stripe
        mkdir /home/user/SSD/disk14/stripe
        mkdir /home/user/SSD/disk15/stripe
        echo "This is test $i" >> ../output/test_efficient.txt
        ./test_efficient >> ../output/test_efficient.txt
    done