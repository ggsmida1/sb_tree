// test/TestInsertBenchmark.cpp
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <iomanip>
#include "../include/SBTree.h"

#define LOG(x) std::cout << "[Benchmark] " << x << std::endl;

static void RunBenchmark(int num_threads, int per_thread_inserts)
{
  SBTree tree;
  std::atomic<int> ready_count{0};
  std::atomic<bool> start_flag{false};
  std::vector<std::thread> workers;

  auto worker = [&](int tid)
  {
    ready_count.fetch_add(1);
    while (!start_flag.load())
      std::this_thread::yield();

    uint64_t base = static_cast<uint64_t>(tid) * 100000000ULL;
    for (int i = 0; i < per_thread_inserts; ++i)
    {
      Key k = base + static_cast<Key>(i);
      Value v = static_cast<Value>(k * 2);
      tree.insert(k, v);
    }
  };

  for (int i = 0; i < num_threads; ++i)
    workers.emplace_back(worker, i);

  while (ready_count.load() < num_threads)
    std::this_thread::yield();

  auto t0 = std::chrono::high_resolution_clock::now();
  start_flag.store(true);
  for (auto &t : workers)
    t.join();
  auto t1 = std::chrono::high_resolution_clock::now();

  double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  double inserts = static_cast<double>(num_threads) * per_thread_inserts;
  double throughput = inserts / (ms / 1000.0);

  // flush + wait
  tree.flush();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  std::cout << std::setw(3) << num_threads
            << " threads | "
            << std::setw(10) << std::fixed << std::setprecision(3) << ms
            << " ms | "
            << std::setw(12) << std::fixed << std::setprecision(2)
            << throughput / 1e6 << " M inserts/s"
            << std::endl;
}

int main()
{
  LOG("SBTree Multithreaded Insert Benchmark (1–8 threads)");
  const int kPerThreadInserts = 50000;

  std::cout << "---------------------------------------------" << std::endl;
  std::cout << "Threads |   Time (ms)  |  Throughput (M/s)" << std::endl;
  std::cout << "---------------------------------------------" << std::endl;

  for (int t = 1; t <= 8; ++t)
  {
    RunBenchmark(t, kPerThreadInserts);
  }

  std::cout << "---------------------------------------------" << std::endl;
  LOG("Benchmark completed.");
  return 0;
}
