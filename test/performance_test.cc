#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <atomic>
#include <random>
#include <algorithm>

// 性能测试工具类
class PerformanceTest {
public:
    static void TestInsertThroughput() {
        std::cout << "Testing insert throughput..." << std::endl;
        
        const int num_threads = 8;
        const int inserts_per_thread = 100000;
        
        SBTree tree;
        std::vector<std::thread> threads;
        std::atomic<int> completed_inserts(0);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 启动多个线程进行插入
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&tree, &completed_inserts, t, inserts_per_thread]() {
                for (int i = 0; i < inserts_per_thread; ++i) {
                    uint64_t key = t * inserts_per_thread + i;
                    uint64_t value = key * 10;
                    tree.Insert(key, value);
                    completed_inserts.fetch_add(1);
                }
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        int total_inserts = completed_inserts.load();
        double throughput = (total_inserts * 1000.0) / duration.count();
        
        std::cout << "Inserted " << total_inserts << " items in " << duration.count() << " ms" << std::endl;
        std::cout << "Insert throughput: " << throughput << " ops/sec" << std::endl;
    }
    
    static void TestLookupThroughput() {
        std::cout << "Testing lookup throughput..." << std::endl;
        
        SBTree tree;
        const int num_inserts = 100000;
        
        // 先插入数据
        for (int i = 0; i < num_inserts; ++i) {
            tree.Insert(i, i * 10);
        }
        
        const int num_lookups = 1000000;
        std::atomic<int> successful_lookups(0);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 多线程查找
        const int num_threads = 8;
        std::vector<std::thread> threads;
        
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&tree, &successful_lookups, t, num_lookups, num_threads]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, 99999);
                
                for (int i = 0; i < num_lookups / num_threads; ++i) {
                    int key = dis(gen);
                    const uint64_t* value = tree.Lookup(key);
                    if (value != nullptr) {
                        successful_lookups.fetch_add(1);
                    }
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        int total_lookups = successful_lookups.load();
        double throughput = (total_lookups * 1000.0) / duration.count();
        
        std::cout << "Performed " << total_lookups << " successful lookups in " << duration.count() << " ms" << std::endl;
        std::cout << "Lookup throughput: " << throughput << " ops/sec" << std::endl;
    }
    
    static void TestScanThroughput() {
        std::cout << "Testing scan throughput..." << std::endl;
        
        SBTree tree;
        const int num_inserts = 100000;
        
        // 先插入数据
        for (int i = 0; i < num_inserts; ++i) {
            tree.Insert(i, i * 10);
        }
        
        const int num_scans = 10000;
        const int scan_size = 100;
        std::atomic<int> total_scanned(0);
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 多线程扫描
        const int num_threads = 8;
        std::vector<std::thread> threads;
        
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&tree, &total_scanned, t, num_scans, num_threads, scan_size]() {
                std::random_device rd;
                std::mt19937 gen(rd());
                std::uniform_int_distribution<> dis(0, 99900);
                
                std::vector<KeyValuePair> result;
                for (int i = 0; i < num_scans / num_threads; ++i) {
                    int start_key = dis(gen);
                    result.clear();
                    size_t scanned = tree.Scan(start_key, scan_size, &result);
                    total_scanned.fetch_add(scanned);
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        int total_scanned_count = total_scanned.load();
        double throughput = (total_scanned_count * 1000.0) / duration.count();
        
        std::cout << "Scanned " << total_scanned_count << " items in " << duration.count() << " ms" << std::endl;
        std::cout << "Scan throughput: " << throughput << " ops/sec" << std::endl;
    }
    
    static void TestDelayedDataPerformance() {
        std::cout << "Testing delayed data performance..." << std::endl;
        
        SBTree tree;
        const int num_inserts = 100000;
        const double delayed_ratio = 0.1; // 10%延迟数据
        
        // 先插入一些正常数据
        for (int i = 0; i < num_inserts / 2; ++i) {
            tree.Insert(i, i * 10);
        }
        
        auto start = std::chrono::high_resolution_clock::now();
        
        // 插入剩余数据，其中一部分是延迟数据
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_real_distribution<> dis(0.0, 1.0);
        
        for (int i = num_inserts / 2; i < num_inserts; ++i) {
            uint64_t key, value;
            if (dis(gen) < delayed_ratio) {
                // 延迟数据：插入到前面的位置
                key = i % (num_inserts / 2);
                value = key * 10;
            } else {
                // 正常数据
                key = i;
                value = i * 10;
            }
            tree.Insert(key, value);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
        
        double throughput = (num_inserts / 2 * 1000.0) / duration.count();
        
        std::cout << "Inserted " << num_inserts / 2 << " items with " << (delayed_ratio * 100) << "% delayed data in " << duration.count() << " ms" << std::endl;
        std::cout << "Throughput with delayed data: " << throughput << " ops/sec" << std::endl;
    }
    
    static void TestMemoryUsage() {
        std::cout << "Testing memory usage..." << std::endl;
        
        SBTree tree;
        const int num_inserts = 1000000;
        
        // 插入大量数据
        for (int i = 0; i < num_inserts; ++i) {
            tree.Insert(i, i * 10);
        }
        
        // 这里可以添加内存使用量统计
        // 由于没有直接的内存统计接口，这里只是示例
        std::cout << "Inserted " << num_inserts << " items" << std::endl;
        std::cout << "Memory usage test completed (actual memory measurement not implemented)" << std::endl;
    }
};

int main() {
    std::cout << "Starting SB-Tree performance tests..." << std::endl;
    
    try {
        PerformanceTest::TestInsertThroughput();
        std::cout << std::endl;
        
        PerformanceTest::TestLookupThroughput();
        std::cout << std::endl;
        
        PerformanceTest::TestScanThroughput();
        std::cout << std::endl;
        
        PerformanceTest::TestDelayedDataPerformance();
        std::cout << std::endl;
        
        PerformanceTest::TestMemoryUsage();
        std::cout << std::endl;
        
        std::cout << "All performance tests completed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Performance test failed: " << e.what() << std::endl;
        return 1;
    }
}
