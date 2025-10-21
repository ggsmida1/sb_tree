#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <set>

int main() {
    std::cout << "=== SB-Tree 转换过程分析 ===" << std::endl;
    
    // 测试1：小规模多线程测试
    std::cout << "测试1：小规模多线程测试..." << std::endl;
    SBTree tree1;
    const int num_threads = 4;
    const int inserts_per_thread = 100;
    
    std::vector<std::thread> threads;
    std::atomic<int> successful{0};
    std::atomic<int> failed{0};
    
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                uint64_t value = key * 10;
                
                if (tree1.Insert(key, value)) {
                    successful.fetch_add(1);
                } else {
                    failed.fetch_add(1);
                }
            }
        });
    }
    
    for (auto& thread : threads) {
        thread.join();
    }
    
    std::cout << "插入结果: 成功" << successful.load() << ", 失败" << failed.load() << std::endl;
    
    // 等待转换器完成
    std::cout << "等待转换器完成..." << std::endl;
    tree1.WaitForConverterIdle();
    
    // 验证数据
    int verified = 0;
    int errors = 0;
    for (int t = 0; t < num_threads; ++t) {
        for (int i = 0; i < inserts_per_thread; ++i) {
            uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
            uint64_t expected_value = key * 10;
            
            const uint64_t* value = tree1.Lookup(key);
            if (value && *value == expected_value) {
                verified++;
            } else {
                errors++;
                if (errors <= 5) {
                    std::cout << "小规模错误: key=" << key << ", expected=" << expected_value;
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
    
    std::cout << "小规模验证: " << verified << "/" << (num_threads * inserts_per_thread) << " 成功" << std::endl;
    
    // 测试2：检查转换器状态
    std::cout << "测试2：检查转换器状态..." << std::endl;
    std::cout << "转换器状态: 已等待完成" << std::endl;
    
    // 测试3：分段块数据检查
    std::cout << "测试3：分段块数据检查..." << std::endl;
    int segmented_found = 0;
    for (int i = 0; i < 50; ++i) {
        uint64_t value;
        if (tree1.LookupInSegmentedBlock(i, &value) && value == static_cast<uint64_t>(i * 10)) {
            segmented_found++;
        }
    }
    std::cout << "分段块中前50个: 找到" << segmented_found << "个" << std::endl;
    
    // 测试4：扫描功能
    std::cout << "测试4：扫描功能..." << std::endl;
    std::vector<KeyValuePair> scan_result;
    size_t scanned = tree1.Scan(0, 50, &scan_result);
    std::cout << "扫描结果: " << scanned << "个元素" << std::endl;
    
    if (scanned > 0) {
        std::cout << "扫描的前10个元素: ";
        for (size_t i = 0; i < std::min(static_cast<size_t>(10), scan_result.size()); ++i) {
            std::cout << "(" << scan_result[i].key << "," << scan_result[i].value << ") ";
        }
        std::cout << std::endl;
    }
    
    // 测试5：延迟数据插入测试
    std::cout << "测试5：延迟数据插入测试..." << std::endl;
    SBTree tree2;
    
    // 先插入一些大数据
    for (int i = 100; i < 200; ++i) {
        tree2.Insert(i, i * 10);
    }
    
    // 等待转换器完成
    tree2.WaitForConverterIdle();
    
    // 插入小数据（延迟数据）
    for (int i = 0; i < 50; ++i) {
        tree2.Insert(i, i * 10);
    }
    
    // 验证延迟数据
    int delayed_found = 0;
    for (int i = 0; i < 50; ++i) {
        const uint64_t* value = tree2.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            delayed_found++;
        }
    }
    std::cout << "延迟数据验证: " << delayed_found << "/50 成功" << std::endl;
    
    // 测试6：转换器性能分析
    std::cout << "测试6：转换器性能分析..." << std::endl;
    SBTree tree3;
    
    auto start = std::chrono::high_resolution_clock::now();
    
    // 插入大量数据
    for (int i = 0; i < 1000; ++i) {
        tree3.Insert(i, i * 10);
    }
    
    auto insert_end = std::chrono::high_resolution_clock::now();
    auto insert_duration = std::chrono::duration_cast<std::chrono::microseconds>(insert_end - start);
    
    std::cout << "插入1000个元素耗时: " << insert_duration.count() << "微秒" << std::endl;
    
    // 等待转换器完成
    auto convert_start = std::chrono::high_resolution_clock::now();
    tree3.WaitForConverterIdle();
    auto convert_end = std::chrono::high_resolution_clock::now();
    auto convert_duration = std::chrono::duration_cast<std::chrono::microseconds>(convert_end - convert_start);
    
    std::cout << "转换器完成耗时: " << convert_duration.count() << "微秒" << std::endl;
    
    // 验证转换后的数据
    int converted_found = 0;
    for (int i = 0; i < 1000; ++i) {
        const uint64_t* value = tree3.Lookup(i);
        if (value && *value == static_cast<uint64_t>(i * 10)) {
            converted_found++;
        }
    }
    std::cout << "转换后数据验证: " << converted_found << "/1000 成功" << std::endl;
    
    std::cout << "=== 分析完成 ===" << std::endl;
    return 0;
}
