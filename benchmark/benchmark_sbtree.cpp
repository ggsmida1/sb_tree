#include <benchmark/benchmark.h>
#include <vector>
#include <random>
#include <algorithm>
#include <thread>
#include <atomic>
#include "SBTree.h" // 引入你的 SB-Tree 头文件

// =================================================================
// 辅助函数
// =================================================================

// 生成测试数据
static std::vector<uint64_t> generate_keys(size_t count, bool shuffle = false)
{
  std::vector<uint64_t> keys(count);
  for (size_t i = 0; i < count; ++i)
  {
    keys[i] = i;
  }
  if (shuffle)
  {
    std::random_device rd;
    // **【关键修正】**：修复拼写错误，mt19937 是正确的名称
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);
  }
  return keys;
}

// =================================================================
// 插入性能测试 (Insert Performance)
// =================================================================

// 测试顺序插入 (Fast Path) - 单线程
static void BM_SBTree_Sequential_Insert(benchmark::State &state)
{
  for (auto _ : state)
  {
    state.PauseTiming(); // 暂停计时，准备数据
    SBTree tree;
    auto num_keys = state.range(0);
    state.ResumeTiming(); // 恢复计时

    // 执行测试
    for (uint64_t i = 0; i < num_keys; ++i)
    {
      tree.insert(i, i * 10);
    }
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_SBTree_Sequential_Insert)->Range(1 << 12, 1 << 16);

// =================================================================
// 查询与扫描性能测试 (预填充数据)
// =================================================================
// 使用一个结构体来管理只初始化一次的共享资源（树和数据）
struct QueryBenchmarkState
{
  SBTree tree;
  std::vector<uint64_t> keys_to_lookup;
  bool initialized = false;

  QueryBenchmarkState(size_t num_keys)
  {
    if (!initialized)
    {
      auto keys_to_insert = generate_keys(num_keys, false);
      for (const auto &key : keys_to_insert)
      {
        tree.insert(key, key * 10);
      }
      tree.flush();
      tree.flush_index();
      keys_to_lookup = generate_keys(num_keys, true); // 用于查找的键是随机的
      initialized = true;
    }
  }
};

// 点查询 (Lookup)
static void BM_SBTree_Lookup(benchmark::State &state)
{
  state.PauseTiming();
  auto num_keys = state.range(0);
  static QueryBenchmarkState benchmark_state(num_keys);
  auto &tree = benchmark_state.tree;
  auto &keys_to_lookup = benchmark_state.keys_to_lookup;
  state.ResumeTiming();

  for (auto _ : state)
  {
    // 每次循环随机查一部分key，避免缓存效应过于理想化
    for (size_t i = 0; i < 1000; ++i)
    {
      uint64_t value;
      benchmark::DoNotOptimize(tree.lookup(keys_to_lookup[i], &value));
    }
  }
  state.SetItemsProcessed(state.iterations() * 1000);
}
BENCHMARK(BM_SBTree_Lookup)->Range(1 << 16, 1 << 20);

// 范围扫描 (Scan)
static void BM_SBTree_Scan(benchmark::State &state)
{
  state.PauseTiming();
  auto num_keys = state.range(0);
  auto scan_size = state.range(1);
  static QueryBenchmarkState benchmark_state(num_keys);
  auto &tree = benchmark_state.tree;

  std::vector<uint64_t> result;
  result.reserve(scan_size);

  std::mt19937 rng(123); // 使用固定种子以保证可重复性
  std::uniform_int_distribution<uint64_t> dist(0, num_keys - scan_size);
  state.ResumeTiming();

  for (auto _ : state)
  {
    uint64_t start_key = dist(rng);
    result.clear();
    tree.scan(start_key, start_key + scan_size - 1, result); // scan是闭区间
    benchmark::ClobberMemory();                              // 确保结果不被优化掉
  }
  state.SetItemsProcessed(state.iterations());
}
BENCHMARK(BM_SBTree_Scan)->ArgsProduct({
    {1 << 16, 1 << 20}, // 树的总大小
    {100, 1000}         // 扫描范围大小
});

// =================================================================
// 并发性能测试 (Concurrent Performance) - 【已修正】
// =================================================================
// 多线程并发插入
static void BM_SBTree_Concurrent_Insert(benchmark::State &state)
{
  auto num_keys_total = state.range(0);

  // 外层循环是 benchmark 的多次重复测量
  for (auto _ : state)
  {
    state.PauseTiming();
    // **【关键修正 1】**：为每次测量创建一个全新的、干净的树实例。
    // 这确保了每次运行都测量的是从空树插入相同数量key的性能，保证了测试的公平性和可重复性。
    SBTree tree;
    auto num_threads = state.threads();
    const size_t keys_per_thread = num_keys_total / num_threads;
    std::vector<std::thread> threads;
    state.ResumeTiming();

    // 创建并运行所有工作线程
    for (int tid = 0; tid < num_threads; ++tid)
    {
      threads.emplace_back([&, tid]()
                           {
                for (size_t i = 0; i < keys_per_thread; ++i)
                {
                    // **【关键修正 2】**：使用交错键(interleaved key)模式。
                    // 这比分区模式更能模拟真实负载，能对 SegmentedBlock 的切换和合并逻辑产生更大压力。
                    uint64_t key = (i * num_threads) + tid;
                    tree.insert(key, key * 10);
                } });
    }
    for (auto &th : threads)
    {
      th.join();
    }
  }
  state.SetItemsProcessed(state.iterations() * num_keys_total);
}
BENCHMARK(BM_SBTree_Concurrent_Insert)
    ->Arg(1 << 18)                                                // 每个 benchmark run 插入的总 key 数量
    ->DenseThreadRange(1, std::thread::hardware_concurrency(), 2) // 测试 1, 3, 5... 个线程
    ->UseRealTime();                                              // 对于多线程测试，使用真实墙上时间

// 运行 benchmark
BENCHMARK_MAIN();
