#include "sb-tree.h"
#include <iostream>
#include <vector>
#include <cassert>

int main()
{
    // ==== 原来的 SBTree 测试 ====
    SBTree t;
    const int M = 100000;
    for (int i = 1; i <= M; i++)
    {
        t.insert(i, i * 10);
    }
    std::cout << "[OK] single-thread inserted " << M << " items\n";

    // ==== 新增的 DataBlock 测试 ====
    std::vector<KVPair> kvs;
    for (int i = 1; i <= 50; i++)
    {
        kvs.push_back({static_cast<Key>(i), static_cast<Value>(i * 100)});
    }

    DataBlock db;
    size_t taken = db.build_from_sorted(kvs.data(), kvs.size());
    std::cout << "[TEST] DataBlock built with " << taken << " entries\n";

    // 测试 find()
    Value v;
    bool ok = db.find(10, v);
    assert(ok && v == 1000);
    ok = db.find(999, v);
    assert(!ok); // 不存在

    // 测试 scan_from()
    std::vector<Value> out;
    size_t got = db.scan_from(20, 5, out);
    std::cout << "[TEST] scan_from(20,5) got " << got << " values\n";
    assert(got == 5);
    assert(out[0] == 2000 && out[4] == 2400);

    std::cout << "[OK] DataBlock find/scan tests passed!\n";
    return 0;
}
