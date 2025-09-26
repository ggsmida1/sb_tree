#include <iostream>
#include <random>
#include "SBTree.h"
#include "KVPair.h"

int main()
{
    SBTree t;

    // 1) 插入顺序键
    const Key N = 10000;
    for (Key i = 1; i <= N; ++i)
    {
        if (i != 10000)
            t.insert(i, static_cast<Value>(i * 10));
        else
            t.insert(i, static_cast<Value>(32423342)); // 故意插入不同的 value
    }

    // 2) 触发转换 + 搜索层构建
    t.flush();

    // 3) 点查：命中
    auto hit = [&](Key k)
    {
        Value v{};
        bool ok = t.lookup(k, &v);
        std::cout << "[lookup] key=" << k
                  << " ok=" << ok
                  << (ok ? (", value=" + std::to_string(v)) : "")
                  << "\n";
    };

    // 4) 点查：未命中
    auto miss = [&](Key k)
    {
        Value v{};
        bool ok = t.lookup(k, &v);
        std::cout << "[lookup] key=" << k << " ok=" << ok << " (expected miss)\n";
    };

    // 一些基本查询
    hit(1);
    hit(N / 2);
    hit(N);
    miss(0);
    miss(N + 123);
    Value q{};
    std::cout << t.lookup(10000, &q) << std::endl;
    std::cout << q << std::endl;

    // 随机抽样命中
    std::mt19937_64 rng(123);
    std::uniform_int_distribution<Key> dist(1, N);
    for (int i = 0; i < 5; ++i)
        hit(dist(rng));

    return 0;
}
