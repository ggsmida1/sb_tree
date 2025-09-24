// main.cpp
#include <cassert>
#include <iostream>
#include <thread>
#include "../include/sb-tree.h"

int main()
{
    SBTree t;

    const int N = 100000;
    for (uint64_t i = 1; i <= N; ++i)
    {
        t.insert(i, i * 10);
    }

    std::cout << "[OK] single-thread inserted " << N << " items\n";
    return 0;
}
