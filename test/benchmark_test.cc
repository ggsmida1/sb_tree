#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <atomic>
#include <random>
#include <algorithm>
#include <iomanip>

// 基准测试工具类
class BenchmarkTest {
public:
    struct BenchmarkResult {
        std::string operation;
        int num_threads;
        int num_operations;
        double duration_ms;
        double throughput_ops_per_sec;
        double avg_latency_us;
    };
    
    static void RunInsertBenchmark() {
        std::cout << "=== Insert Benchmark ===" << std::endl;
        
        std::vector<int> thread_counts = {1, 2, 4, 8, 16, 32, 64};
        std::vector<BenchmarkResult> results;
        
        for (int num_threads : thread_counts) {
            SBTree tree;
            const int operations_per_thread = 100000;
            
            std::vector<std::thread> threads;
            std::atomic<int> completed_operations(0);
            
            auto start = std::chrono::high_resolution_clock::now();
            
            for (int t = 0; t < num_threads; ++t) {
                threads.emplace_back([&tree, &completed_operations, t, operations_per_thread]() {
                    for (int i = 0; i < operations_per_thread; ++i) {
                        uint64_t key = t * operations_per_thread + i;
                        uint64_t value = key * 10;
                        tree.Insert(key, value);
                        completed_operations.fetch_add(1);
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            
            int total_operations = completed_operations.load();
            double duration_ms = duration.count() / 1000.0;
            double throughput = (total_operations * 1000.0) / duration_ms;
            double avg_latency = duration_ms * 1000.0 / total_operations;
            
            BenchmarkResult result;
            result.operation = "Insert";
            result.num_threads = num_threads;
            result.num_operations = total_operations;
            result.duration_ms = duration_ms;
            result.throughput_ops_per_sec = throughput;
            result.avg_latency_us = avg_latency;
            results.push_back(result);
        }
        
        PrintResults(results);
    }
    
    static void RunLookupBenchmark() {
        std::cout << "=== Lookup Benchmark ===" << std::endl;
        
        SBTree tree;
        const int num_inserts = 1000000;
        
        // 先插入数据
        for (int i = 0; i < num_inserts; ++i) {
            tree.Insert(i, i * 10);
        }
        
        std::vector<int> thread_counts = {1, 2, 4, 8, 16, 32, 64};
        std::vector<BenchmarkResult> results;
        
        for (int num_threads : thread_counts) {
            const int operations_per_thread = 100000;
            
            std::vector<std::thread> threads;
            std::atomic<int> completed_operations(0);
            
            auto start = std::chrono::high_resolution_clock::now();
            
            for (int t = 0; t < num_threads; ++t) {
                threads.emplace_back([&tree, &completed_operations, t, operations_per_thread, num_inserts]() {
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<> dis(0, num_inserts - 1);
                    
                    for (int i = 0; i < operations_per_thread; ++i) {
                        int key = dis(gen);
                        const uint64_t* value = tree.Lookup(key);
                        if (value != nullptr) {
                            completed_operations.fetch_add(1);
                        }
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            
            int total_operations = completed_operations.load();
            double duration_ms = duration.count() / 1000.0;
            double throughput = (total_operations * 1000.0) / duration_ms;
            double avg_latency = duration_ms * 1000.0 / total_operations;
            
            BenchmarkResult result;
            result.operation = "Lookup";
            result.num_threads = num_threads;
            result.num_operations = total_operations;
            result.duration_ms = duration_ms;
            result.throughput_ops_per_sec = throughput;
            result.avg_latency_us = avg_latency;
            results.push_back(result);
        }
        
        PrintResults(results);
    }
    
    static void RunScanBenchmark() {
        std::cout << "=== Scan Benchmark ===" << std::endl;
        
        SBTree tree;
        const int num_inserts = 1000000;
        
        // 先插入数据
        for (int i = 0; i < num_inserts; ++i) {
            tree.Insert(i, i * 10);
        }
        
        std::vector<int> thread_counts = {1, 2, 4, 8, 16, 32, 64};
        std::vector<BenchmarkResult> results;
        
        for (int num_threads : thread_counts) {
            const int operations_per_thread = 10000;
            const int scan_size = 100;
            
            std::vector<std::thread> threads;
            std::atomic<int> completed_operations(0);
            std::atomic<int> total_scanned(0);
            
            auto start = std::chrono::high_resolution_clock::now();
            
            for (int t = 0; t < num_threads; ++t) {
                threads.emplace_back([&tree, &completed_operations, &total_scanned, t, operations_per_thread, scan_size, num_inserts]() {
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_int_distribution<> dis(0, num_inserts - scan_size);
                    
                    std::vector<KeyValuePair> result;
                    for (int i = 0; i < operations_per_thread; ++i) {
                        int start_key = dis(gen);
                        result.clear();
                        size_t scanned = tree.Scan(start_key, scan_size, &result);
                        total_scanned.fetch_add(scanned);
                        completed_operations.fetch_add(1);
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            
            int total_operations = completed_operations.load();
            // int total_scanned_count = total_scanned.load();  // 暂时注释掉未使用的变量
            double duration_ms = duration.count() / 1000.0;
            double throughput = (total_operations * 1000.0) / duration_ms;
            double avg_latency = duration_ms * 1000.0 / total_operations;
            
            BenchmarkResult result;
            result.operation = "Scan";
            result.num_threads = num_threads;
            result.num_operations = total_operations;
            result.duration_ms = duration_ms;
            result.throughput_ops_per_sec = throughput;
            result.avg_latency_us = avg_latency;
            results.push_back(result);
        }
        
        PrintResults(results);
    }
    
    static void RunDelayedDataBenchmark() {
        std::cout << "=== Delayed Data Benchmark ===" << std::endl;
        
        std::vector<double> delayed_ratios = {0.0, 0.05, 0.1, 0.2, 0.3, 0.5};
        std::vector<BenchmarkResult> results;
        
        for (double delayed_ratio : delayed_ratios) {
            SBTree tree;
            const int num_inserts = 100000;
            const int num_threads = 8;
            const int operations_per_thread = num_inserts / num_threads;
            
            std::vector<std::thread> threads;
            std::atomic<int> completed_operations(0);
            
            auto start = std::chrono::high_resolution_clock::now();
            
            for (int t = 0; t < num_threads; ++t) {
                threads.emplace_back([&tree, &completed_operations, t, operations_per_thread, delayed_ratio]() {
                    std::random_device rd;
                    std::mt19937 gen(rd());
                    std::uniform_real_distribution<> dis(0.0, 1.0);
                    
                    for (int i = 0; i < operations_per_thread; ++i) {
                        uint64_t key, value;
                        if (dis(gen) < delayed_ratio) {
                            // 延迟数据：插入到前面的位置
                            key = (t * operations_per_thread + i) % (operations_per_thread / 2);
                            value = key * 10;
                        } else {
                            // 正常数据
                            key = t * operations_per_thread + i;
                            value = key * 10;
                        }
                        tree.Insert(key, value);
                        completed_operations.fetch_add(1);
                    }
                });
            }
            
            for (auto& thread : threads) {
                thread.join();
            }
            
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            
            int total_operations = completed_operations.load();
            double duration_ms = duration.count() / 1000.0;
            double throughput = (total_operations * 1000.0) / duration_ms;
            double avg_latency = duration_ms * 1000.0 / total_operations;
            
            BenchmarkResult result;
            result.operation = "DelayedData";
            result.num_threads = num_threads;
            result.num_operations = total_operations;
            result.duration_ms = duration_ms;
            result.throughput_ops_per_sec = throughput;
            result.avg_latency_us = avg_latency;
            results.push_back(result);
        }
        
        PrintResults(results);
    }
    
    static void PrintResults(const std::vector<BenchmarkResult>& results) {
        std::cout << std::fixed << std::setprecision(2);
        std::cout << std::setw(12) << "Threads" 
                  << std::setw(15) << "Operations"
                  << std::setw(15) << "Duration(ms)"
                  << std::setw(20) << "Throughput(ops/s)"
                  << std::setw(20) << "Avg Latency(us)" << std::endl;
        std::cout << std::string(82, '-') << std::endl;
        
        for (const auto& result : results) {
            std::cout << std::setw(12) << result.num_threads
                      << std::setw(15) << result.num_operations
                      << std::setw(15) << result.duration_ms
                      << std::setw(20) << result.throughput_ops_per_sec
                      << std::setw(20) << result.avg_latency_us << std::endl;
        }
        std::cout << std::endl;
    }
    
    static void RunAllBenchmarks() {
        std::cout << "Starting SB-Tree Benchmark Tests..." << std::endl;
        std::cout << "=====================================" << std::endl;
        
        RunInsertBenchmark();
        RunLookupBenchmark();
        RunScanBenchmark();
        RunDelayedDataBenchmark();
        
        std::cout << "All benchmark tests completed!" << std::endl;
    }
};

int main() {
    try {
        BenchmarkTest::RunAllBenchmarks();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Benchmark test failed: " << e.what() << std::endl;
        return 1;
    }
}
