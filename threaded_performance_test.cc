#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <set>
#include <mutex>

class ThreadedPerformanceTest {
public:
    static void RunTest() {
        std::cout << "=== SB-Tree 8线程完整性能测试 ===" << std::endl;
        
        const int num_threads = 8;
        const int inserts_per_thread = 5000;
        const int total_inserts = num_threads * inserts_per_thread;
        
        std::cout << "配置: " << num_threads << "线程, 每线程" << inserts_per_thread << "次插入" << std::endl;
        std::cout << "总插入数: " << total_inserts << std::endl;
        std::cout << std::endl;
        
        // 测试1：并发插入性能
        std::cout << "测试1：并发插入性能..." << std::endl;
        SBTree tree;
        std::vector<std::thread> threads;
        std::atomic<int> successful_inserts{0};
        std::atomic<int> failed_inserts{0};
        std::vector<std::set<uint64_t>> thread_keys(num_threads);
        std::mutex keys_mutex;
        
        auto start_time = std::chrono::high_resolution_clock::now();
        
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                for (int i = 0; i < inserts_per_thread; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                    uint64_t value = key * 10;
                    
                    if (tree.Insert(key, value)) {
                        successful_inserts.fetch_add(1);
                        {
                            std::lock_guard<std::mutex> lock(keys_mutex);
                            thread_keys[t].insert(key);
                        }
                    } else {
                        failed_inserts.fetch_add(1);
                    }
                }
            });
        }
        
        for (auto& thread : threads) {
            thread.join();
        }
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        
        std::cout << "插入完成: 成功" << successful_inserts.load() << ", 失败" << failed_inserts.load() << std::endl;
        std::cout << "插入耗时: " << duration.count() << "微秒" << std::endl;
        std::cout << "吞吐量: " << (successful_inserts.load() * 1000000.0 / duration.count()) << " ops/sec" << std::endl;
        
        // 等待转换器完成
        std::cout << "等待转换器完成..." << std::endl;
        tree.WaitForConverterIdle();
        
        // 测试2：数据完整性验证
        std::cout << "测试2：数据完整性验证..." << std::endl;
        int verified = 0;
        int errors = 0;
        std::vector<int> thread_verified(num_threads, 0);
        std::vector<int> thread_errors(num_threads, 0);
        
        for (int t = 0; t < num_threads; ++t) {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
                uint64_t expected_value = key * 10;
                
                const uint64_t* value = tree.Lookup(key);
                if (value && *value == expected_value) {
                    verified++;
                    thread_verified[t]++;
                } else {
                    errors++;
                    thread_errors[t]++;
                    if (errors <= 10) {
                        std::cout << "验证错误: key=" << key << ", expected=" << expected_value;
                        if (value) {
                            std::cout << ", got=" << *value;
                        } else {
                            std::cout << ", got=nullptr";
                        }
                        std::cout << " (线程" << t << ")" << std::endl;
                    }
                }
            }
        }
        
        std::cout << "验证结果: " << verified << "/" << total_inserts << " 成功" << std::endl;
        std::cout << "错误数: " << errors << std::endl;
        
        // 按线程统计
        std::cout << "按线程统计:" << std::endl;
        for (int t = 0; t < num_threads; ++t) {
            std::cout << "  线程" << t << ": 成功" << thread_verified[t] << ", 错误" << thread_errors[t] << std::endl;
        }
        
        // 测试3：扫描性能测试
        std::cout << "测试3：扫描性能测试..." << std::endl;
        std::vector<KeyValuePair> scan_result;
        scan_result.reserve(1000);
        
        auto scan_start = std::chrono::high_resolution_clock::now();
        size_t scanned = tree.Scan(0, 1000, &scan_result);
        auto scan_end = std::chrono::high_resolution_clock::now();
        auto scan_duration = std::chrono::duration_cast<std::chrono::microseconds>(scan_end - scan_start);
        
        std::cout << "扫描结果: " << scanned << "个元素" << std::endl;
        std::cout << "扫描耗时: " << scan_duration.count() << "微秒" << std::endl;
        
        if (scanned > 0) {
            std::cout << "扫描的前10个元素: ";
            for (size_t i = 0; i < std::min(static_cast<size_t>(10), scan_result.size()); ++i) {
                std::cout << "(" << scan_result[i].key << "," << scan_result[i].value << ") ";
            }
            std::cout << std::endl;
        }
        
        // 测试4：分段块数据检查
        std::cout << "测试4：分段块数据检查..." << std::endl;
        int segmented_found = 0;
        for (int i = 0; i < 100; ++i) {
            uint64_t value;
            if (tree.LookupInSegmentedBlock(i, &value) && value == static_cast<uint64_t>(i * 10)) {
                segmented_found++;
            }
        }
        std::cout << "分段块中前100个: 找到" << segmented_found << "个" << std::endl;
        
        // 测试5：并发查找性能
        std::cout << "测试5：并发查找性能..." << std::endl;
        std::vector<std::thread> lookup_threads;
        std::atomic<int> lookup_success{0};
        std::atomic<int> lookup_failures{0};
        
        auto lookup_start = std::chrono::high_resolution_clock::now();
        
        for (int t = 0; t < num_threads; ++t) {
            lookup_threads.emplace_back([&, t]() {
                for (int i = 0; i < 1000; ++i) {
                    uint64_t key = static_cast<uint64_t>(t) * 1000 + i;
                    const uint64_t* value = tree.Lookup(key);
                    if (value && *value == static_cast<uint64_t>(key * 10)) {
                        lookup_success.fetch_add(1);
                    } else {
                        lookup_failures.fetch_add(1);
                    }
                }
            });
        }
        
        for (auto& thread : lookup_threads) {
            thread.join();
        }
        
        auto lookup_end = std::chrono::high_resolution_clock::now();
        auto lookup_duration = std::chrono::duration_cast<std::chrono::microseconds>(lookup_end - lookup_start);
        
        std::cout << "查找结果: 成功" << lookup_success.load() << ", 失败" << lookup_failures.load() << std::endl;
        std::cout << "查找耗时: " << lookup_duration.count() << "微秒" << std::endl;
        std::cout << "查找吞吐量: " << (lookup_success.load() * 1000000.0 / lookup_duration.count()) << " ops/sec" << std::endl;
        
        // 总结
        std::cout << std::endl;
        std::cout << "=== 性能测试总结 ===" << std::endl;
        std::cout << "插入吞吐量: " << (successful_inserts.load() * 1000000.0 / duration.count()) << " ops/sec" << std::endl;
        std::cout << "查找吞吐量: " << (lookup_success.load() * 1000000.0 / lookup_duration.count()) << " ops/sec" << std::endl;
        std::cout << "数据完整性: " << (verified * 100.0 / total_inserts) << "%" << std::endl;
        
        if (errors == 0) {
            std::cout << "=== 所有测试通过 ===" << std::endl;
        } else {
            std::cout << "=== 发现" << errors << "个数据完整性问题 ===" << std::endl;
        }
    }
};

int main() {
    try {
        ThreadedPerformanceTest::RunTest();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "测试未知异常" << std::endl;
        return 1;
    }
}
