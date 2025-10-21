#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <random>
#include <iomanip>
#include <mutex>

class PerformanceAnalysis {
public:
    static void RunDetailedAnalysis() {
        std::cout << "===============================================" << std::endl;
        std::cout << "    SB-Tree 性能瓶颈分析测试" << std::endl;
        std::cout << "===============================================" << std::endl;
        
        // 测试配置
        const std::vector<int> thread_counts = {1, 2, 4};
        const int inserts_per_thread = 10000;
        
        std::cout << "测试配置:" << std::endl;
        std::cout << "  每线程插入数: " << inserts_per_thread << std::endl;
        std::cout << "  测试线程数: ";
        for (int t : thread_counts) {
            std::cout << t << " ";
        }
        std::cout << std::endl << std::endl;
        
        // 性能结果存储
        std::vector<double> throughputs;
        std::vector<double> latencies;
        std::vector<int> conversion_counts;
        
        for (int num_threads : thread_counts) {
            std::cout << "运行 " << num_threads << " 线程测试..." << std::endl;
            
            // 重置转换计数器
            g_conversion_count = 0;
            
            auto result = RunThreadTest(num_threads, inserts_per_thread);
            throughputs.push_back(result.throughput);
            latencies.push_back(result.avg_latency);
            conversion_counts.push_back(g_conversion_count.load());
            
            std::cout << "  吞吐量: " << std::fixed << std::setprecision(0) 
                      << result.throughput << " ops/sec" << std::endl;
            std::cout << "  平均延迟: " << std::fixed << std::setprecision(3) 
                      << result.avg_latency << " μs" << std::endl;
            std::cout << "  转换次数: " << g_conversion_count.load() << std::endl;
            std::cout << "  成功率: " << result.success_rate << "%" << std::endl;
            std::cout << std::endl;
        }
        
        // 性能分析
        AnalyzePerformance(thread_counts, throughputs, latencies, conversion_counts);
    }
    
private:
    struct TestResult {
        double throughput;
        double avg_latency;
        double success_rate;
        int total_operations;
        int failed_operations;
    };
    
    static std::atomic<int> g_conversion_count;
    
    static TestResult RunThreadTest(int num_threads, int inserts_per_thread) {
        SBTree tree;
        std::vector<std::thread> threads;
        std::atomic<int> completed_ops(0);
        std::atomic<int> failed_ops(0);
        std::vector<std::chrono::high_resolution_clock::time_point> start_times(num_threads);
        std::vector<std::chrono::high_resolution_clock::time_point> end_times(num_threads);
        std::vector<std::vector<double>> latencies(num_threads);
        
        // 启动线程
        for (int t = 0; t < num_threads; ++t) {
            threads.emplace_back([&, t]() {
                start_times[t] = std::chrono::high_resolution_clock::now();
                
                for (int i = 0; i < inserts_per_thread; ++i) {
                    auto op_start = std::chrono::high_resolution_clock::now();
                    
                    uint64_t key = t * inserts_per_thread + i;
                    uint64_t value = key * 10;
                    
                    if (tree.Insert(key, value)) {
                        completed_ops.fetch_add(1);
                    } else {
                        failed_ops.fetch_add(1);
                    }
                    
                    auto op_end = std::chrono::high_resolution_clock::now();
                    auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count() / 1000.0; // μs
                    latencies[t].push_back(latency);
                }
                
                end_times[t] = std::chrono::high_resolution_clock::now();
            });
        }
        
        // 等待所有线程完成
        for (auto& thread : threads) {
            thread.join();
        }
        
        // 等待转换器完成
        tree.WaitForConverterIdle();
        
        // 计算性能指标
        auto total_start = *std::min_element(start_times.begin(), start_times.end());
        auto total_end = *std::max_element(end_times.begin(), end_times.end());
        auto total_duration = std::chrono::duration_cast<std::chrono::microseconds>(total_end - total_start);
        double duration_ms = total_duration.count() / 1000.0;
        if (duration_ms < 0.001) duration_ms = 0.001;
        
        double throughput = (completed_ops.load() * 1000.0) / duration_ms;
        
        // 计算平均延迟
        double total_latency = 0;
        int total_latency_count = 0;
        for (const auto& thread_latencies : latencies) {
            for (double latency : thread_latencies) {
                total_latency += latency;
                total_latency_count++;
            }
        }
        double avg_latency = (total_latency_count > 0) ? (total_latency / total_latency_count) : 0;
        
        // 验证数据完整性
        int verified = 0;
        for (int t = 0; t < num_threads; ++t) {
            for (int i = 0; i < inserts_per_thread; ++i) {
                uint64_t key = t * inserts_per_thread + i;
                const uint64_t* value = tree.Lookup(key);
                if (value && *value == key * 10) {
                    verified++;
                }
            }
        }
        double success_rate = (verified * 100.0) / (num_threads * inserts_per_thread);
        
        return {throughput, avg_latency, success_rate, completed_ops.load(), failed_ops.load()};
    }
    
    static void AnalyzePerformance(const std::vector<int>& thread_counts,
                                 const std::vector<double>& throughputs,
                                 const std::vector<double>& latencies,
                                 const std::vector<int>& conversion_counts) {
        std::cout << "===============================================" << std::endl;
        std::cout << "   性能分析报告" << std::endl;
        std::cout << "===============================================" << std::endl;
        
        std::cout << "线程数    吞吐量(ops/s)   延迟(μs)   转换次数   效率" << std::endl;
        std::cout << "--------------------------------------------------------" << std::endl;
        
        double baseline_throughput = throughputs[0];
        for (size_t i = 0; i < thread_counts.size(); ++i) {
            double efficiency = (thread_counts[i] > 1) ? (throughputs[i] / (baseline_throughput * thread_counts[i])) : 1.0;
            std::cout << std::setw(6) << thread_counts[i] 
                      << std::setw(15) << std::fixed << std::setprecision(0) << throughputs[i]
                      << std::setw(10) << std::fixed << std::setprecision(2) << latencies[i]
                      << std::setw(10) << conversion_counts[i]
                      << std::setw(10) << std::fixed << std::setprecision(2) << efficiency << std::endl;
        }
        
        std::cout << std::endl;
        
        // 性能瓶颈分析
        std::cout << "性能瓶颈分析:" << std::endl;
        
        // 1. 扩展性分析
        bool has_scaling_issues = false;
        for (size_t i = 1; i < thread_counts.size(); ++i) {
            double expected_throughput = throughputs[0] * thread_counts[i];
            double actual_throughput = throughputs[i];
            double scaling_ratio = actual_throughput / expected_throughput;
            
            if (scaling_ratio < 0.5) {
                has_scaling_issues = true;
                std::cout << "  ❌ 扩展性问题: " << thread_counts[i] << "线程时扩展比仅" 
                          << std::fixed << std::setprecision(2) << scaling_ratio << std::endl;
            }
        }
        
        if (!has_scaling_issues) {
            std::cout << "  ✅ 扩展性良好" << std::endl;
        }
        
        // 2. 转换频率分析
        std::cout << "  转换频率分析:" << std::endl;
        for (size_t i = 0; i < thread_counts.size(); ++i) {
            double conversion_rate = (double)conversion_counts[i] / (thread_counts[i] * 50000);
            std::cout << "    " << thread_counts[i] << "线程: " 
                      << std::fixed << std::setprecision(4) << conversion_rate 
                      << " 转换/操作" << std::endl;
        }
        
        // 3. 延迟分析
        std::cout << "  延迟分析:" << std::endl;
        for (size_t i = 0; i < thread_counts.size(); ++i) {
            if (latencies[i] > 10.0) {
                std::cout << "    ⚠️  " << thread_counts[i] << "线程延迟较高: " 
                          << std::fixed << std::setprecision(2) << latencies[i] << "μs" << std::endl;
            }
        }
        
        // 4. 建议
        std::cout << std::endl << "优化建议:" << std::endl;
        if (has_scaling_issues) {
            std::cout << "  1. 检查锁竞争和同步开销" << std::endl;
            std::cout << "  2. 优化转换触发机制" << std::endl;
            std::cout << "  3. 减少全局状态更新频率" << std::endl;
        }
        
        double max_conversion_rate = 0;
        for (size_t i = 0; i < thread_counts.size(); ++i) {
            double rate = (double)conversion_counts[i] / (thread_counts[i] * 50000);
            max_conversion_rate = std::max(max_conversion_rate, rate);
        }
        
        if (max_conversion_rate > 0.01) {
            std::cout << "  4. 转换频率过高，考虑增加转换阈值" << std::endl;
        }
    }
};

std::atomic<int> PerformanceAnalysis::g_conversion_count{0};

int main() {
    PerformanceAnalysis::RunDetailedAnalysis();
    return 0;
}
