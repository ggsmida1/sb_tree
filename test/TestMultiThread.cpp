#include "SBTree.h"
#include <thread>
#include <vector>
#include <chrono>
#include <iostream>
#include <random>

// 打印测试开始信息
void print_header(const std::string &title)
{
  std::cout << "\n===========================================\n";
  std::cout << title << std::endl;
  std::cout << "===========================================\n";
}

// 多线程插入测试
void run_multi_thread_insert_test(size_t num_threads, size_t total_inserts)
{
  SBTree tree;

  std::cout << "\n[多线程写入测试] 启动 " << num_threads << " 个线程，总插入 "
            << total_inserts << " 条记录..." << std::endl;

  auto start_time = std::chrono::high_resolution_clock::now();

  size_t per_thread = total_inserts / num_threads;
  std::vector<std::thread> workers;
  workers.reserve(num_threads);

  for (size_t t = 0; t < num_threads; ++t)
  {
    workers.emplace_back([&, t]()
                         {
            Key base = t * per_thread;
            for (size_t i = 0; i < per_thread; ++i)
            {
                tree.insert(base + i, base + i);
            } });
  }

  for (auto &th : workers)
    th.join();

  auto end_time = std::chrono::high_resolution_clock::now();
  double seconds = std::chrono::duration<double>(end_time - start_time).count();
  double throughput = total_inserts / (seconds * 1e6);

  std::cout << "  - 总耗时:   " << seconds << " 秒" << std::endl;
  std::cout << "  - 吞吐量:   " << throughput << " Mops/sec" << std::endl;

  // 简单查找验证（抽样检查）
  std::default_random_engine eng{1234};
  std::uniform_int_distribution<size_t> dist(0, total_inserts - 1);

  size_t correct = 0;
  for (int i = 0; i < 1000; ++i)
  {
    Key k = dist(eng);
    Value v;
    if (tree.find(k, v) && v == k)
      ++correct;
  }

  std::cout << "  - 查找验证: 成功 " << correct << " / 1000 条\n";
}

int main()
{
  print_header("SB-Tree 多线程写入性能测试");

  // （1线程相当于单线程基线）
  run_multi_thread_insert_test(1, 1'000'000);

  // （多线程对比）
  run_multi_thread_insert_test(2, 2'000'000);
  run_multi_thread_insert_test(4, 4'000'000);
  run_multi_thread_insert_test(8, 8'000'000);

  std::cout << "\n测试结束。\n";
  return 0;
}
