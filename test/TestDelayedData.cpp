// test/TestDelayedData.cpp - 建议新增
#include "SBTree.h"
#include <iostream>
#include <vector>

int main() {
    SBTree tree;
    
    // 1. 插入一些顺序数据
    for (int i = 100; i < 200; ++i) {
        tree.insert(i, i * 2);
    }
    
    // 2. 插入延迟数据（小于已插入的key）
    tree.insert(50, 100);  // 历史数据
    tree.insert(150, 300); // 中间数据
    
    // 3. 验证查找
    Value v;
    bool found1 = tree.find(50, v);
    bool found2 = tree.find(150, v);
    
    std::cout << "Delayed data test: " << (found1 && found2 ? "PASS" : "FAIL") << std::endl;
    
    // 4. 扫描测试
    std::vector<Value> results;
    size_t count = tree.scan(45, 155, results);
    std::cout << "Scan returned " << count << " results" << std::endl;
    
    return 0;
}