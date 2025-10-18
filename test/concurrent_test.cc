#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <cassert>

// 并发测试工具类
class ConcurrentTest {
public:
    static void TestConcurrentInsert() {
        std::cout << "Testing concurrent insert..." << std::endl;
        
        SBTree tree;
        const int num_threads = 16;
        const int inserts_per_thread = 10000;
        
        std::vector<std::thread> threads;
        std::atomic<int> successful_inserts(0);
        std::atomic<int> failed_inserts(0);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 启动多个线程进行插入
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&tree, &successful_inserts, &failed_inserts, t, inserts_per_thread]() {
                for (int i = 0; i < inserts_per_thread; ++i) {
                    uint64_t key = t * inserts_per_thread + i;
                    uint64_t value = key * 10;
                    
                    if (tree.Insert(key, value)) {
                        successful_inserts.fetch_add(1);
                    } else {
                        failed_inserts.fetch_add(1);
                    }
                }
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        int total_successful = successful_inserts.load();
        int total_failed = failed_inserts.load();
        
        std::cout << "Successful inserts: " << total_successful << std::endl;
        std::cout << "Failed inserts: " << total_failed << std::endl;
        std::cout << "Total time: " << duration.count() << " ms" << std::endl;
        std::cout << "Throughput: " << (total_successful * 1000.0 / duration.count()) << " ops/sec" << std::endl;
        
        // 验证数据完整性
        for (int t = 0; t < num_threads; ++t) {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = t * inserts_per_thread + i;
                const uint64_t* value = tree.Lookup(key);
                assert(value != nullptr);
                assert(*value == key * 10);
            }
        }
        
        std::cout << "Concurrent insert test passed!" << std::endl;
    }
    
    static void TestConcurrentLookup() {
        std::cout << "Testing concurrent lookup..." << std::endl;
        
        SBTree tree;
        const int num_inserts = 100000;
        
        // 先插入数据
        for (int i = 0; i < num_inserts; ++i) {
            tree.Insert(i, i * 10);
        }
        
        const int num_threads = 16;
        const int lookups_per_thread = 10000;
        
        std::vector<std::thread> threads;
        std::atomic<int> successful_lookups(0);
        std::atomic<int> failed_lookups(0);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 启动多个线程进行查找
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&tree, &successful_lookups, &failed_lookups, t, lookups_per_thread, num_inserts]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, num_inserts - 1);
                
                for (int i = 0; i < lookups_per_thread; ++i) {
                    int key = dis(gen);
                    const uint64_t* value = tree.Lookup(key);
                    
                    if (value != nullptr) {
                        assert(*value == key * 10);
                        successful_lookups.fetch_add(1);
                    } else {
                        failed_lookups.fetch_add(1);
                    }
                }
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        int total_successful = successful_lookups.load();
        int total_failed = failed_lookups.load();
        
        std::cout << "Successful lookups: " << total_successful << std::endl;
        std::cout << "Failed lookups: " << total_failed << std::endl;
        std::cout << "Total time: " << duration.count() << " ms" << std::endl;
        std::cout << "Throughput: " << (total_successful * 1000.0 / duration.count()) << " ops/sec" << std::endl;
        
        std::cout << "Concurrent lookup test passed!" << std::endl;
    }
    
    static void TestConcurrentScan() {
        std::cout << "Testing concurrent scan..." << std::endl;
        
        SBTree tree;
        const int num_inserts = 100000;
        
        // 先插入数据
        for (int i = 0; i < num_inserts; ++i) {
            tree.Insert(i, i * 10);
        }
        
        const int num_threads = 8;
        const int scans_per_thread = 1000;
        const int scan_size = 100;
        
        std::vector<std::thread> threads;
        std::atomic<int> total_scanned(0);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 启动多个线程进行扫描
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&tree, &total_scanned, t, scans_per_thread, scan_size, num_inserts]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, num_inserts - scan_size);
                
                std::vector<KeyValuePair> result;
                for (int i = 0; i < scans_per_thread; ++i) {
                    int start_key = dis(gen);
                    result.clear();
                    size_t scanned = tree.Scan(start_key, scan_size, &result);
                    total_scanned.fetch_add(scanned);
                }
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        int total_scanned_count = total_scanned.load();
        
        std::cout << "Total scanned items: " << total_scanned_count << std::endl;
        std::cout << "Total time: " << duration.count() << " ms" << std::endl;
        std::cout << "Throughput: " << (total_scanned_count * 1000.0 / duration.count()) << " ops/sec" << std::endl;
        
        std::cout << "Concurrent scan test passed!" << std::endl;
    }
    
    static void TestMixedWorkload() {
        std::cout << "Testing mixed workload (insert + lookup + scan)..." << std::endl;
        
        SBTree tree;
        const int num_threads = 12;
        const int operations_per_thread = 5000;
        
        std::vector<std::thread> threads;
        std::atomic<int> successful_inserts(0);
        std::atomic<int> successful_lookups(0);
        std::atomic<int> total_scanned(0);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 启动混合工作负载线程
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&tree, &successful_inserts, &successful_lookups, &total_scanned, t, operations_per_thread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> op_dis(0, 2); // 0: insert, 1: lookup, 2: scan
                std::uniform_int_distribution<> key_dis(0, 100000);
                
                for (int i = 0; i < operations_per_thread; ++i) {
                    int operation = op_dis(gen);
                    
                    switch (operation) {
                        case 0: { // Insert
                            uint64_t key = key_dis(gen);
                            uint64_t value = key * 10;
                            if (tree.Insert(key, value)) {
                                successful_inserts.fetch_add(1);
                            }
                            break;
                        }
                        case 1: { // Lookup
                            uint64_t key = key_dis(gen);
                            const uint64_t* value = tree.Lookup(key);
                            if (value != nullptr) {
                                successful_lookups.fetch_add(1);
                            }
                            break;
                        }
                        case 2: { // Scan
                            uint64_t start_key = key_dis(gen);
                            std::vector<KeyValuePair> result;
                            size_t scanned = tree.Scan(start_key, 50, &result);
                            total_scanned.fetch_add(scanned);
                            break;
                        }
                    }
                }
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        int total_inserts = successful_inserts.load();
        int total_lookups = successful_lookups.load();
        int total_scanned_count = total_scanned.load();
        
        std::cout << "Successful inserts: " << total_inserts << std::endl;
        std::cout << "Successful lookups: " << total_lookups << std::endl;
        std::cout << "Total scanned items: " << total_scanned_count << std::endl;
        std::cout << "Total time: " << duration.count() << " ms" << std::endl;
        std::cout << "Total operations: " << (total_inserts + total_lookups + total_scanned_count) << std::endl;
        std::cout << "Throughput: " << ((total_inserts + total_lookups + total_scanned_count) * 1000.0 / duration.count()) << " ops/sec" << std::endl;
        
        std::cout << "Mixed workload test passed!" << std::endl;
    }
    
    static void TestStressTest() {
        std::cout << "Testing stress test..." << std::endl;
        
        SBTree tree;
        const int num_threads = 32;
        const int operations_per_thread = 10000;
        
        std::vector<std::thread> threads;
        std::atomic<int> successful_operations(0);
        std::atomic<int> failed_operations(0);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 启动高并发压力测试
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&tree, &successful_operations, &failed_operations, t, operations_per_thread]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> op_dis(0, 2);
                std::uniform_int_distribution<> key_dis(0, 1000000);
                
                for (int i = 0; i < operations_per_thread; ++i) {
                    int operation = op_dis(gen);
                    uint64_t key = key_dis(gen);
                    
                    switch (operation) {
                        case 0: { // Insert
                            uint64_t value = key * 10;
                            if (tree.Insert(key, value)) {
                                successful_operations.fetch_add(1);
                            } else {
                                failed_operations.fetch_add(1);
                            }
                            break;
                        }
                        case 1: { // Lookup
                            const uint64_t* value = tree.Lookup(key);
                            if (value != nullptr) {
                                successful_operations.fetch_add(1);
                            } else {
                                failed_operations.fetch_add(1);
                            }
                            break;
                        }
                        case 2: { // Scan
                            std::vector<KeyValuePair> result;
                            size_t scanned = tree.Scan(key, 10, &result);
                            if (scanned > 0) {
                                successful_operations.fetch_add(1);
                            } else {
                                failed_operations.fetch_add(1);
                            }
                            break;
                        }
                    }
                }
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        int total_successful = successful_operations.load();
        int total_failed = failed_operations.load();
        
        std::cout << "Successful operations: " << total_successful << std::endl;
        std::cout << "Failed operations: " << total_failed << std::endl;
        std::cout << "Total time: " << duration.count() << " ms" << std::endl;
        std::cout << "Throughput: " << (total_successful * 1000.0 / duration.count()) << " ops/sec" << std::endl;
        std::cout << "Success rate: " << (100.0 * total_successful / (total_successful + total_failed)) << "%" << std::endl;
        
        std::cout << "Stress test passed!" << std::endl;
    }
};

int main() {
    std::cout << "Starting SB-Tree concurrent tests..." << std::endl;
    
    try {
        ConcurrentTest::TestConcurrentInsert();
        std::cout << std::endl;
        
        ConcurrentTest::TestConcurrentLookup();
        std::cout << std::endl;
        
        ConcurrentTest::TestConcurrentScan();
        std::cout << std::endl;
        
        ConcurrentTest::TestMixedWorkload();
        std::cout << std::endl;
        
        ConcurrentTest::TestStressTest();
        std::cout << std::endl;
        
        std::cout << "All concurrent tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Concurrent test failed: " << e.what() << std::endl;
        return 1;
    }
}
