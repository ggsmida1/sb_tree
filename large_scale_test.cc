#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <random>

class LargeScaleTest {
public:
    static void RunTest() {
        std::cout << "=== SB-Tree 大数据量测试 ===" << std::endl;
        
        // 测试不同的大数据量规模
        const int large_sizes[] = {10000, 50000, 100000, 200000, 500000};
        const int thread_counts[] = {1, 2, 4, 8, 16};
        
        for (int size : large_sizes) {
            std::cout << "\n=== 大数据量测试: " << size << "个元素 ===" << std::endl;
            
            for (int threads : thread_counts) {
                if (threads == 1) {
                    // 单线程大数据量测试
                    TestSingleThreadLarge(size);
                } else {
                    // 多线程大数据量测试
                    TestMultiThreadLarge(size, threads);
                }
            }
        }
        
        // 专门的压力测试
        std::cout << "\n=== 压力测试 ===" << std::endl;
        StressTest();
        
        // 内存使用测试
        std::cout << "\n=== 内存使用测试 ===" << std::endl;
        MemoryUsageTest();
    }
    
private:
    static void TestSingleThreadLarge(int size) {
        SBTree tree;
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int i = 0; i < size; ++i) {
            tree.Insert(i, i * 10);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        tree.WaitForConverterIdle();
        
        // 验证数据完整性
        int verified = 0;
        int sample_size = std::min(1000, size / 10); // 采样验证
        for (int i = 0; i < sample_size; ++i) {
            const uint64_t* value = tree.Lookup(i);
            if (value && *value == static_cast<uint64_t>(i * 10)) {
                verified++;
            }
        }
        
        double throughput = (size * 1000000.0) / duration.count();
        double integrity = (verified * 100.0) / sample_size;
        
        std::cout << "单线程: " << duration.count() << "μs, " 
                  << throughput << " ops/sec, " 
                  << integrity << "% 完整性" << std::endl;
    }
    
    static void TestMultiThreadLarge(int size, int threads) {
        SBTree tree;
        std::vector<std::thread> thread_pool;
        std::atomic<int> successful_inserts{0};
        std::atomic<int> failed_inserts{0};
        
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int t = 0; t < threads; ++t) {
            thread_pool.emplace_back([&, t, size, threads]() {
                int per_thread_size = size / threads;
                for (int i = 0; i < per_thread_size; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * per_thread_size + i;
                    if (tree.Insert(key, key * 10)) {
                        successful_inserts.fetch_add(1);
                    } else {
                        failed_inserts.fetch_add(1);
                    }
                }
            });
        }
        
        for (auto& thread : thread_pool) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        tree.WaitForConverterIdle();
        
        // 验证数据完整性
        int verified = 0;
        int errors = 0;
        int per_thread_size = size / threads;
        int sample_size = std::min(1000, per_thread_size);
        
        for (int t = 0; t < threads; ++t) {
            for (int i = 0; i < sample_size; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * per_thread_size + i;
                const uint64_t* value = tree.Lookup(key);
                if (value && *value == key * 10) {
                    verified++;
                } else {
                    errors++;
                    if (errors <= 5) {
                        std::cout << "错误: key=" << key << ", expected=" << (key * 10);
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
        
        double throughput = (successful_inserts.load() * 1000000.0) / duration.count();
        double integrity = (verified * 100.0) / (threads * sample_size);
        double speedup = (successful_inserts.load() * 1000000.0) / duration.count() / (size * 1000000.0 / duration.count());
        
        std::cout << threads << "线程: " << duration.count() << "μs, " 
                  << throughput << " ops/sec, " 
                  << integrity << "% 完整性, "
                  << "失败: " << failed_inserts.load() << std::endl;
    }
    
    static void StressTest() {
        std::cout << "压力测试: 8线程 × 100000个元素" << std::endl;
        
        SBTree tree;
        const int threads = 8;
        const int per_thread = 100000;
        const int total = threads * per_thread;
        
        std::vector<std::thread> thread_pool;
        std::atomic<int> successful{0};
        std::atomic<int> failed{0};
        
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int t = 0; t < threads; ++t) {
            thread_pool.emplace_back([&, t]() {
                for (int i = 0; i < per_thread; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * per_thread + i;
                    if (tree.Insert(key, key * 10)) {
                        successful.fetch_add(1);
                    } else {
                        failed.fetch_add(1);
                    }
                }
            });
        }
        
        for (auto& thread : thread_pool) {
            thread.join();
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        
        std::cout << "插入完成: " << successful.load() << "成功, " << failed.load() << "失败" << std::endl;
        std::cout << "插入耗时: " << duration.count() << "μs" << std::endl;
        
        // 等待转换器完成
        std::cout << "等待转换器完成..." << std::endl;
        tree.WaitForConverterIdle();
        
        // 验证数据完整性
        int verified = 0;
        int errors = 0;
        int sample_size = 10000; // 采样验证
        
        for (int t = 0; t < threads; ++t) {
            for (int i = 0; i < sample_size; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * per_thread + i;
                const uint64_t* value = tree.Lookup(key);
                if (value && *value == key * 10) {
                    verified++;
                } else {
                    errors++;
                    if (errors <= 10) {
                        std::cout << "验证错误: key=" << key << ", expected=" << (key * 10);
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
        
        double integrity = (verified * 100.0) / (threads * sample_size);
        std::cout << "数据完整性: " << integrity << "% (" << verified << "/" << (threads * sample_size) << ")" << std::endl;
        
        // 扫描测试
        std::vector<KeyValuePair> scan_result;
        scan_result.reserve(1000);
        size_t scanned = tree.Scan(0, 1000, &scan_result);
        std::cout << "扫描测试: " << scanned << "个元素" << std::endl;
    }
    
    static void MemoryUsageTest() {
        std::cout << "内存使用测试: 逐步增加数据量" << std::endl;
        
        const int test_sizes[] = {1000, 10000, 50000, 100000};
        
        for (int size : test_sizes) {
            SBTree tree;
            auto start = std::chrono::high_resolution_clock::now();
            
            for (int i = 0; i < size; ++i) {
                tree.Insert(i, i * 10);
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            
            tree.WaitForConverterIdle();
            
            // 验证数据完整性
            int verified = 0;
            int sample_size = std::min(1000, size / 10);
            for (int i = 0; i < sample_size; ++i) {
                const uint64_t* value = tree.Lookup(i);
                if (value && *value == static_cast<uint64_t>(i * 10)) {
                    verified++;
                }
            }
            
            double throughput = (size * 1000000.0) / duration.count();
            double integrity = (verified * 100.0) / sample_size;
            
            std::cout << size << "个元素: " << duration.count() << "μs, " 
                      << throughput << " ops/sec, " 
                      << integrity << "% 完整性" << std::endl;
        }
    }
};

int main() {
    try {
        LargeScaleTest::RunTest();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "测试未知异常" << std::endl;
        return 1;
    }
}
