// test/TestInsertBasic.cpp
#include <iostream>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <atomic>
#include "SBTree.h" // assume include dir is added in CMake

// 简单打印辅助
#define LOG(x) std::cout << "[Test] " << x << std::endl;

int main()
{
  LOG("SBTree basic insert/find test starting...");

  SBTree tree;

  constexpr int kNumThreads = 4;
  constexpr int kPerThreadInserts = 20000;

  std::atomic<int> ready_count{0};
  std::atomic<bool> start_flag{false};
  std::vector<std::thread> workers;

  auto worker = [&](int tid)
  {
    ready_count.fetch_add(1);
    while (!start_flag.load())
      std::this_thread::yield();

    uint64_t base = static_cast<uint64_t>(tid) * 1000000ULL;
    for (int i = 0; i < kPerThreadInserts; ++i)
    {
      Key k = base + static_cast<Key>(i);
      Value v = static_cast<Value>(k * 2);
      tree.insert(k, v);
    }
  };

  // 启动线程
  for (int i = 0; i < kNumThreads; ++i)
    workers.emplace_back(worker, i);

  // 等待全部线程就绪
  while (ready_count.load() < kNumThreads)
    std::this_thread::yield();

  auto t0 = std::chrono::high_resolution_clock::now();
  start_flag.store(true);
  for (auto &t : workers)
    t.join();
  auto t1 = std::chrono::high_resolution_clock::now();

  double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
  LOG("All threads finished inserts in " << ms << " ms");

  // 强制 flush 以便后台线程转换所有段
  tree.flush();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // 简单验证：查找部分 key
  bool ok = true;
  for (int tid = 0; tid < kNumThreads; ++tid)
  {
    uint64_t base = static_cast<uint64_t>(tid) * 1000000ULL;
    for (int i = 0; i < 5; ++i)
    {
      Key k = base + static_cast<Key>(i * 100);
      Value v = 0;
      if (!tree.find(k, v) || v != static_cast<Value>(k * 2))
      {
        LOG("Mismatch at key " << k << " got " << v);
        ok = false;
        break;
      }
    }
    if (!ok)
      break;
  }

  if (ok)
  {
    LOG("Find() basic verification passed!");
  }
  else
  {
    LOG("Find() failed!");
  }

  // 扫描测试
  std::vector<Value> out;
  Key L = 100, R = 1000;
  size_t cnt = tree.scan(L, R, out);
  LOG("Scan(" << L << "," << R << ") returned " << cnt << " results");

  LOG("All tests finished.");
  return ok ? 0 : 1;
}
