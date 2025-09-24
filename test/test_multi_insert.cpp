// main_mt.cpp
#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include "../include/sb-tree.h"

int main()
{
    SBTree t;

    const int T = 8;     // 线程数
    const int M = 50000; // 每线程插入量
    std::vector<std::thread> ths;

    for (int th = 0; th < T; ++th)
    {
        ths.emplace_back([&, th]
                         {
            uint64_t base = uint64_t(th) << 48; // 让每个线程的 key 各自单调递增
            for (int i = 1; i <= M; ++i) {
                t.insert(base + i, base + i * 10);
            } });
    }
    for (auto &th : ths)
        th.join();

    std::cout << "[OK] multi-thread inserted " << (size_t)T * M << " items\n";
    return 0;
}
