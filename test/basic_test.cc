#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <cassert>
#include <chrono>
#include <random>

// 基本功能测试
void TestBasicInsertAndLookup() {
    std::cout << "Testing basic insert and lookup..." << std::endl;
    
    SBTree tree;
    
    // 测试插入
    assert(tree.Insert(100, 1000));
    assert(tree.Insert(200, 2000));
    assert(tree.Insert(150, 1500));
    
    // 测试查找
    const uint64_t* value = tree.Lookup(100);
    assert(value != nullptr);
    assert(*value == 1000);
    
    value = tree.Lookup(200);
    assert(value != nullptr);
    assert(*value == 2000);
    
    value = tree.Lookup(150);
    assert(value != nullptr);
    assert(*value == 1500);
    
    // 测试不存在的键
    value = tree.Lookup(300);
    assert(value == nullptr);
    
    std::cout << "Basic insert and lookup test passed!" << std::endl;
}

void TestScan() {
    std::cout << "Testing scan functionality..." << std::endl;
    
    SBTree tree;
    
    // 插入测试数据
    for (int i = 0; i < 100; ++i) {
        assert(tree.Insert(i * 10, i * 100));
    }
    
    // 测试扫描
    std::vector<KeyValuePair> result;
    size_t scanned = tree.Scan(50, 10, &result);
    
    assert(scanned == 10);
    assert(result.size() == 10);
    
    // 验证扫描结果
    for (size_t i = 0; i < result.size(); ++i) {
        assert(result[i].key == 50 + i * 10);
        assert(result[i].value == (50 + i * 10) * 10);
    }
    
    std::cout << "Scan test passed!" << std::endl;
}

void TestDelayedData() {
    std::cout << "Testing delayed data insertion..." << std::endl;
    
    SBTree tree;
    
    // 先插入一些正常数据
    for (int i = 100; i < 200; ++i) {
        assert(tree.Insert(i, i * 10));
    }
    
    // 插入延迟数据（小于当前最大键）
    assert(tree.Insert(50, 500));
    assert(tree.Insert(75, 750));
    
    // 验证延迟数据可以正确查找
    const uint64_t* value = tree.Lookup(50);
    assert(value != nullptr);
    assert(*value == 500);
    
    value = tree.Lookup(75);
    assert(value != nullptr);
    assert(*value == 750);
    
    std::cout << "Delayed data test passed!" << std::endl;
}

void TestConcurrentInsert() {
    std::cout << "Testing concurrent insert..." << std::endl;
    
    SBTree tree;
    const int num_threads = 4;
    const int inserts_per_thread = 1000;
    
    std::vector<std::thread> threads;
    std::vector<std::vector<uint64_t>> thread_keys(num_threads);
    
    // 启动多个线程进行插入
    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&tree, &thread_keys, t, inserts_per_thread]() {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = t * inserts_per_thread + i;
                uint64_t value = key * 10;
                assert(tree.Insert(key, value));
                thread_keys[t].push_back(key);
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 等待转换器完成，确保所有数据都进入搜索层
    tree.WaitForConverterIdle();
    
    // 验证所有插入的数据都能正确查找
    for (int t = 0; t < num_threads; ++t) {
        for (uint64_t key : thread_keys[t]) {
            const uint64_t* value = tree.Lookup(key);
            assert(value != nullptr);
            assert(*value == key * 10);
        }
    }
    
    std::cout << "Concurrent insert test passed!" << std::endl;
}

void TestPerformance() {
    std::cout << "Testing performance..." << std::endl;
    
    SBTree tree;
    const int num_inserts = 100000;
    
    // 测试插入性能
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_inserts; ++i) {
        assert(tree.Insert(i, i * 10));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Inserted " << num_inserts << " items in " << duration.count() << " ms" << std::endl;
    std::cout << "Insert rate: " << (num_inserts * 1000.0 / duration.count()) << " ops/sec" << std::endl;
    
    // 等待转换器完成，确保所有数据都进入搜索层
    tree.WaitForConverterIdle();
    
    // 测试查找性能
    start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_inserts; ++i) {
        const uint64_t* value = tree.Lookup(i);
        assert(value != nullptr);
        assert(*value == i * 10);
    }
    
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Looked up " << num_inserts << " items in " << duration.count() << " ms" << std::endl;
    std::cout << "Lookup rate: " << (num_inserts * 1000.0 / duration.count()) << " ops/sec" << std::endl;
    
    // 测试扫描性能
    start = std::chrono::high_resolution_clock::now();
    
    std::vector<KeyValuePair> result;
    size_t total_scanned = 0;
    for (int i = 0; i < num_inserts; i += 1000) {
        result.clear();
        total_scanned += tree.Scan(i, 1000, &result);
    }
    
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Scanned " << total_scanned << " items in " << duration.count() << " ms" << std::endl;
    std::cout << "Scan rate: " << (total_scanned * 1000.0 / duration.count()) << " ops/sec" << std::endl;
    
    std::cout << "Performance test completed!" << std::endl;
}

int main() {
    std::cout << "Starting SB-Tree basic tests..." << std::endl;
    
    try {
        TestBasicInsertAndLookup();
        TestScan();
        TestDelayedData();
        TestConcurrentInsert();
        TestPerformance();
        
        std::cout << "All basic tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return 1;
    }
}
