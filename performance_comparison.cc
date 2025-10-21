#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>

class PerformanceComparison {
public:
    static void RunComparison() {
        std::cout << "=== SB-Tree 单线程 vs 多线程性能对比 ===" << std::endl;
        
        const int test_sizes[] = {100, 500, 1000, 2000, 5000};
        const int thread_counts[] = {1, 2, 4, 8};
        
        for (int size : test_sizes) {
            std::cout << "\n=== 测试规模: " << size << "个元素 ===" << std::endl;
            
            for (int threads : thread_counts) {
                if (threads == 1) {
                    // 单线程测试
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
                    for (int i = 0; i < size; ++i) {
                        const uint64_t* value = tree.Lookup(i);
                        if (value && *value == static_cast<uint64_t>(i * 10)) {
                            verified++;
                        }
                    }
                    
                    double throughput = (size * 1000000.0) / duration.count();
                    double integrity = (verified * 100.0) / size;
                    
                    std::cout << "单线程: " << duration.count() << "μs, " 
                              << throughput << " ops/sec, " 
                              << integrity << "% 完整性" << std::endl;
                    
                } else {
                    // 多线程测试
                    SBTree tree;
                    std::vector<std::thread> thread_pool;
                    std::atomic<int> successful_inserts{0};
                    
                    auto start = std::chrono::high_resolution_clock::now();
                    
                    for (int t = 0; t < threads; ++t) {
                        thread_pool.emplace_back([&, t, size, threads]() {
                            int per_thread_size = size / threads;
                            for (int i = 0; i < per_thread_size; ++i) {
                                uint64_t key = static_cast<uint64_t>(t) * per_thread_size + i;
                                if (tree.Insert(key, key * 10)) {
                                    successful_inserts.fetch_add(1);
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
                    int per_thread_size = size / threads;
                    for (int t = 0; t < threads; ++t) {
                        for (int i = 0; i < per_thread_size; ++i) {
                            uint64_t key = static_cast<uint64_t>(t) * per_thread_size + i;
                            const uint64_t* value = tree.Lookup(key);
                            if (value && *value == key * 10) {
                                verified++;
                            }
                        }
                    }
                    
                    double throughput = (successful_inserts.load() * 1000000.0) / duration.count();
                    double integrity = (verified * 100.0) / size;
                    double speedup = (threads == 1) ? 1.0 : throughput / (size * 1000000.0 / duration.count());
                    
                    std::cout << threads << "线程: " << duration.count() << "μs, " 
                              << throughput << " ops/sec, " 
                              << integrity << "% 完整性, "
                              << "加速比: " << speedup << "x" << std::endl;
                }
            }
        }
        
        // 详细的小规模测试
        std::cout << "\n=== 小规模详细性能分析 ===" << std::endl;
        DetailedSmallScaleTest();
    }
    
private:
    static void DetailedSmallScaleTest() {
        const int small_size = 1000;
        const int iterations = 5; // 多次测试取平均值
        
        std::cout << "测试规模: " << small_size << "个元素, " << iterations << "次测试" << std::endl;
        
        // 单线程基准
        double single_thread_avg = 0;
        for (int iter = 0; iter < iterations; ++iter) {
            SBTree tree;
            auto start = std::chrono::high_resolution_clock::now();
            
            for (int i = 0; i < small_size; ++i) {
                tree.Insert(i, i * 10);
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            single_thread_avg += duration.count();
        }
        single_thread_avg /= iterations;
        
        std::cout << "单线程平均耗时: " << single_thread_avg << "μs" << std::endl;
        
        // 多线程测试
        for (int threads : {2, 4, 8}) {
            double multi_thread_avg = 0;
            double integrity_avg = 0;
            
            for (int iter = 0; iter < iterations; ++iter) {
                SBTree tree;
                std::vector<std::thread> thread_pool;
                std::atomic<int> successful_inserts{0};
                
                auto start = std::chrono::high_resolution_clock::now();
                
                for (int t = 0; t < threads; ++t) {
                    thread_pool.emplace_back([&, t]() {
                        int per_thread_size = small_size / threads;
                        for (int i = 0; i < per_thread_size; ++i) {
                            uint64_t key = static_cast<uint64_t>(t) * per_thread_size + i;
                            if (tree.Insert(key, key * 10)) {
                                successful_inserts.fetch_add(1);
                            }
                        }
                    });
                }
                
                for (auto& thread : thread_pool) {
                    thread.join();
                }
                
                auto end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                multi_thread_avg += duration.count();
                
                tree.WaitForConverterIdle();
                
                // 验证数据完整性
                int verified = 0;
                int per_thread_size = small_size / threads;
                for (int t = 0; t < threads; ++t) {
                    for (int i = 0; i < per_thread_size; ++i) {
                        uint64_t key = static_cast<uint64_t>(t) * per_thread_size + i;
                        const uint64_t* value = tree.Lookup(key);
                        if (value && *value == key * 10) {
                            verified++;
                        }
                    }
                }
                
                double integrity = (verified * 100.0) / small_size;
                integrity_avg += integrity;
            }
            
            multi_thread_avg /= iterations;
            integrity_avg /= iterations;
            
            double speedup = single_thread_avg / multi_thread_avg;
            double efficiency = speedup / threads * 100;
            
            std::cout << threads << "线程: " << multi_thread_avg << "μs, "
                      << "加速比: " << speedup << "x, "
                      << "效率: " << efficiency << "%, "
                      << "完整性: " << integrity_avg << "%" << std::endl;
        }
    }
};

int main() {
    try {
        PerformanceComparison::RunComparison();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "测试未知异常" << std::endl;
        return 1;
    }
}
