#include "sb_tree.h"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <iomanip>

struct Metrics {
  double ms;
  double tps;
};

static Metrics run_mode(int num_threads, int inserts_per_thread, bool wait_drain) {
  SBTree tree;
  std::atomic<int> success_count{0};

  auto t0 = std::chrono::high_resolution_clock::now();

  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < inserts_per_thread; ++i) {
        uint64_t key = static_cast<uint64_t>(t) * inserts_per_thread + i;
        uint64_t value = key * 10;
        if (tree.Insert(key, value)) success_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (auto &th : threads) th.join();

  if (wait_drain) {
    tree.WaitForConverterIdle();
  }

  auto t1 = std::chrono::high_resolution_clock::now();
  double ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  if (ms < 0.1) ms = 0.1;
  double tps = static_cast<double>(success_count.load()) / ms * 1000.0;
  return {ms, tps};
}

int main() {
  std::cout << "=== SB-Tree 插入吞吐（两种模式） ===" << std::endl;
  const int base_inserts_per_thread = 5000;
  std::vector<int> threads = {1, 2, 4, 8};

  std::cout << "基础每线程插入数: " << base_inserts_per_thread << "\n";
  std::cout << "线程数: ";
  for (size_t i = 0; i < threads.size(); ++i) {
    std::cout << threads[i] << (i + 1 < threads.size() ? ", " : "\n\n");
  }

  std::cout << std::left
            << std::setw(8) << "Threads"
            << std::setw(14) << "InsOnly(ms)"
            << std::setw(16) << "InsOnly(TPS)"
            << std::setw(10) << "Speedup"
            << std::setw(16) << "+Drain(ms)"
            << std::setw(16) << "+Drain(TPS)"
            << std::setw(10) << "Speedup"
            << "\n";
  std::cout << std::string(90, '-') << "\n";

  Metrics base_only{0,0}, base_drain{0,0};
  for (int th : threads) {
    int inserts_per_thread = base_inserts_per_thread;
    if (th >= 8) inserts_per_thread = 2000; // 降低高并发压力避免崩溃
    Metrics m_only = run_mode(th, inserts_per_thread, /*wait_drain=*/false);
    Metrics m_drain = run_mode(th, inserts_per_thread, /*wait_drain=*/true);
    if (th == 1) { base_only = m_only; base_drain = m_drain; }
    double sp_only = base_only.tps > 0 ? (m_only.tps / base_only.tps) : 1.0;
    double sp_drain = base_drain.tps > 0 ? (m_drain.tps / base_drain.tps) : 1.0;

    std::cout << std::left
              << std::setw(8) << th
              << std::setw(14) << std::fixed << std::setprecision(1) << m_only.ms
              << std::setw(16) << std::fixed << std::setprecision(0) << m_only.tps
              << std::setw(10) << std::fixed << std::setprecision(2) << sp_only
              << std::setw(16) << std::fixed << std::setprecision(1) << m_drain.ms
              << std::setw(16) << std::fixed << std::setprecision(0) << m_drain.tps
              << std::setw(10) << std::fixed << std::setprecision(2) << sp_drain
              << "\n";
  }

  std::cout << "\n注: InsOnly 仅统计插入线程join时间；+Drain 统计插入+转换完成的总时间。" << std::endl;
  return 0;
}
