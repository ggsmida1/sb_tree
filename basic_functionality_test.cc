#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <chrono>
#include <cassert>

int main() {
    std::cout << "=== SB-Tree 基础功能测试 ===" << std::endl;
    
    SBTree tree;
    bool all_tests_passed = true;
    
    // 测试1：基本插入和查找
    std::cout << "测试1：基本插入和查找..." << std::endl;
    for (int i = 0; i < 100; ++i) {
        if (!tree.Insert(i, i * 10)) {
            std::cout << "插入失败: key=" << i << std::endl;
            all_tests_passed = false;
        }
    }
    
    for (int i = 0; i < 100; ++i) {
        const uint64_t* value = tree.Lookup(i);
        if (!value || *value != static_cast<uint64_t>(i * 10)) {
            std::cout << "查找失败: key=" << i << ", expected=" << (i * 10) << std::endl;
            all_tests_passed = false;
        }
    }
    std::cout << "测试1完成" << std::endl;
    
    // 测试2：扫描功能
    std::cout << "测试2：扫描功能..." << std::endl;
    std::vector<KeyValuePair> scan_result;
    size_t scanned = tree.Scan(10, 20, &scan_result);
    if (scanned != 20) {
        std::cout << "扫描失败: 期望20个，实际" << scanned << "个" << std::endl;
        all_tests_passed = false;
    }
    std::cout << "测试2完成，扫描到" << scanned << "个元素" << std::endl;
    
    // 测试3：延迟数据插入
    std::cout << "测试3：延迟数据插入..." << std::endl;
    // 插入一个明显小于当前数据的key
    if (!tree.Insert(5, 50)) {
        std::cout << "延迟数据插入失败: key=5" << std::endl;
        all_tests_passed = false;
    }
    
    const uint64_t* delayed_value = tree.Lookup(5);
    if (!delayed_value || *delayed_value != 50) {
        std::cout << "延迟数据查找失败: key=5, expected=50" << std::endl;
        all_tests_passed = false;
    }
    std::cout << "测试3完成" << std::endl;
    
    // 测试4：多线程插入（小规模）
    std::cout << "测试4：多线程插入..." << std::endl;
    SBTree multi_tree;
    std::vector<std::thread> threads;
    const int num_threads = 2;
    const int inserts_per_thread = 50;
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&multi_tree, t, inserts_per_thread]() {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                uint64_t value = key * 10;
                multi_tree.Insert(key, value);
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证多线程结果
    int verified = 0;
    int errors = 0;
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < inserts_per_thread; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
            uint64_t expected_value = key * 10;
            
            const uint64_t* value = multi_tree.Lookup(key);
            if (value && *value == expected_value) {
                verified++;
            } else {
                errors++;
                if (errors <= 5) {
                    std::cout << "多线程验证错误: key=" << key << ", expected=" << expected_value;
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
    
    std::cout << "多线程验证: " << verified << "/" << (num_threads * inserts_per_thread) << " 成功" << std::endl;
    if (errors > 0) {
        all_tests_passed = false;
    }
    
    // 测试5：性能测试
    std::cout << "测试5：性能测试..." << std::endl;
    SBTree perf_tree;
    const int perf_count = 1000;
    
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < perf_count; ++i) {
        perf_tree.Insert(i, i * 10);
    }
    auto end = std::chrono::high_resolution_clock::now();
    
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    std::cout << "插入" << perf_count << "个元素耗时: " << duration.count() << "微秒" << std::endl;
    
    // 等待转换器完成
    std::cout << "等待转换器完成..." << std::endl;
    perf_tree.WaitForConverterIdle();
    
    // 验证性能测试结果
    int perf_verified = 0;
    int perf_errors = 0;
    for (int i = 0; i < perf_count; ++i) {
        const uint64_t* value = perf_tree.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            perf_verified++;
        } else {
            perf_errors++;
            if (perf_errors <= 10) {
                std::cout << "性能测试错误: key=" << i << ", expected=" << (i * 10);
                if (value) {
                    std::cout << ", got=" << *value;
                } else {
                    std::cout << ", got=nullptr";
                }
                std::cout << std::endl;
            }
        }
    }
    std::cout << "性能测试验证: " << perf_verified << "/" << perf_count << " 成功，错误" << perf_errors << "个" << std::endl;
    
    if (perf_verified != perf_count) {
        all_tests_passed = false;
    }
    
    // 总结
    std::cout << std::endl;
    if (all_tests_passed) {
        std::cout << "=== 所有测试通过 ===" << std::endl;
        return 0;
    } else {
        std::cout << "=== 部分测试失败 ===" << std::endl;
        return 1;
    }
}
