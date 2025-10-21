#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <chrono>
#include <set>
#include <thread>

int main() {
    std::cout << "=== SB-Tree 数据丢失诊断测试 ===" << std::endl;
    
    SBTree tree;
    
    // 测试1：小规模数据完整性测试
    std::cout << "测试1：小规模数据完整性测试..." << std::endl;
    const int small_count = 100;
    std::set<uint64_t> inserted_keys;
    
    for (int i = 0; i < small_count; ++i) {
        if (tree.Insert(i, i * 10)) {
            inserted_keys.insert(i);
        }
    }
    
    int found_count = 0;
    for (int i = 0; i < small_count; ++i) {
        const uint64_t* value = tree.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            found_count++;
        }
    }
    
    std::cout << "小规模测试: 插入" << inserted_keys.size() << "个，找到" << found_count << "个" << std::endl;
    
    // 测试2：中等规模数据完整性测试
    std::cout << "测试2：中等规模数据完整性测试..." << std::endl;
    SBTree tree2;
    const int medium_count = 1000;
    inserted_keys.clear();
    
    for (int i = 0; i < medium_count; ++i) {
        if (tree2.Insert(i, i * 10)) {
            inserted_keys.insert(i);
        }
    }
    
    found_count = 0;
    for (int i = 0; i < medium_count; ++i) {
        const uint64_t* value = tree2.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            found_count++;
        }
    }
    
    std::cout << "中等规模测试: 插入" << inserted_keys.size() << "个，找到" << found_count << "个" << std::endl;
    
    // 测试3：检查转换器状态
    std::cout << "测试3：等待转换器完成..." << std::endl;
    tree2.WaitForConverterIdle();
    
    found_count = 0;
    for (int i = 0; i < medium_count; ++i) {
        const uint64_t* value = tree2.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            found_count++;
        }
    }
    
    std::cout << "转换器完成后: 找到" << found_count << "个" << std::endl;
    
    // 测试4：分段块扫描测试
    std::cout << "测试4：分段块扫描测试..." << std::endl;
    SBTree tree3;
    const int scan_count = 500;
    
    for (int i = 0; i < scan_count; ++i) {
        tree3.Insert(i, i * 10);
    }
    
    // 检查分段块中的数据
    int segmented_found = 0;
    for (int i = 0; i < scan_count; ++i) {
        uint64_t value;
        if (tree3.LookupInSegmentedBlock(i, &value) && value == static_cast<uint64_t>(i * 10)) {
            segmented_found++;
        }
    }
    
    std::cout << "分段块中: 找到" << segmented_found << "个" << std::endl;
    
    // 测试5：多线程数据完整性测试
    std::cout << "测试5：多线程数据完整性测试..." << std::endl;
    SBTree multi_tree;
    const int thread_count = 2;
    const int per_thread_count = 100;
    std::vector<std::thread> threads;
    std::vector<std::set<uint64_t>> thread_keys(thread_count);
    
    for (int t = 0; t < thread_count; ++t) {
        threads.emplace_back([&multi_tree, &thread_keys, t, per_thread_count]() {
            for (int i = 0; i < per_thread_count; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * per_thread_count + i;
                uint64_t value = key * 10;
                if (multi_tree.Insert(key, value)) {
                    thread_keys[t].insert(key);
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 等待转换器完成
    multi_tree.WaitForConverterIdle();
    
    // 验证多线程结果
    int multi_found = 0;
    int multi_errors = 0;
    for (int t = 0; t < thread_count; ++t) {
        for (int i = 0; i < per_thread_count; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * per_thread_count + i;
            uint64_t expected_value = key * 10;
            
            const uint64_t* value = multi_tree.Lookup(key);
            if (value && *value == expected_value) {
                multi_found++;
            } else {
                multi_errors++;
                if (multi_errors <= 10) {
                    std::cout << "多线程错误: key=" << key << ", expected=" << expected_value;
                    if (value) {
                        std::cout << ", got=" << *value;
                    } else {
                        std::cout << ", got=nullptr";
                    }
                    std::cout << std::endl;
                }
            }
        }
    }
    
    std::cout << "多线程测试: 找到" << multi_found << "/" << (thread_count * per_thread_count) << "个，错误" << multi_errors << "个" << std::endl;
    
    // 测试6：扫描功能测试
    std::cout << "测试6：扫描功能测试..." << std::endl;
    std::vector<KeyValuePair> scan_result;
    size_t scanned = multi_tree.Scan(0, 50, &scan_result);
    std::cout << "扫描结果: 扫描到" << scanned << "个元素" << std::endl;
    
    if (scanned > 0) {
        std::cout << "扫描的前5个元素: ";
        for (size_t i = 0; i < std::min(static_cast<size_t>(5), scan_result.size()); ++i) {
            std::cout << "(" << scan_result[i].key << "," << scan_result[i].value << ") ";
        }
        std::cout << std::endl;
    }
    
    std::cout << "=== 诊断完成 ===" << std::endl;
    return 0;
}
