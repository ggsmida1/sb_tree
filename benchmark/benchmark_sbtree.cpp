#include <benchmark/benchmark.h>
#include <vector>
#include <random>
#include <algorithm>
#include <thread>
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
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);
  }
  return keys;
}

// =================================================================
// 插入性能测试 (Insert Performance)
// =================================================================

// 测试顺序插入 (Fast Path)
static void BM_SBTree_Sequential_Insert(benchmark::State &state)
{
  for (auto _ : state)
  {
    state.PauseTiming(); // 暂停计时，准备数据
    SBTree tree;
    auto num_keys = state.range(0);
    auto keys = generate_keys(num_keys, false);
    state.ResumeTiming(); // 恢复计时

    // 执行测试
    for (uint64_t i = 0; i < num_keys; ++i)
    {
      tree.insert(keys[i], i);
    }
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}
// 修正：使用极小的范围来快速验证
BENCHMARK(BM_SBTree_Sequential_Insert)->Range(1 << 8, 1 << 10);

// 测试完全随机插入 (Fallback Path)
static void BM_SBTree_Random_Insert(benchmark::State &state)
{
  for (auto _ : state)
  {
    state.PauseTiming();
    SBTree tree;
    auto num_keys = state.range(0);
    auto keys = generate_keys(num_keys, true); // 数据是随机的
    state.ResumeTiming();

    for (uint64_t i = 0; i < num_keys; ++i)
    {
      tree.insert(keys[i], i);
    }
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}
BENCHMARK(BM_SBTree_Random_Insert)->Range(1 << 8, 1 << 10);

// =================================================================
// 查询性能测试 (Query Performance) - 优化后
// =================================================================

// 点查询 (Lookup)
static void BM_SBTree_Lookup(benchmark::State &state)
{
  state.PauseTiming();
  auto num_keys = state.range(0);

  // 优化：使用 static 变量，让树和测试数据只被构建一次
  static SBTree tree;
  static bool initialized = false;
  static std::vector<uint64_t> keys_to_lookup;

  if (!initialized)
  {
    auto keys_to_insert = generate_keys(num_keys, true);
    for (const auto &key : keys_to_insert)
    {
      tree.insert(key, key);
    }
    keys_to_lookup = generate_keys(num_keys, true);
    initialized = true;
  }
  state.ResumeTiming();

  for (auto _ : state)
  {
    for (const auto &key : keys_to_lookup)
    {
      uint64_t value;
      benchmark::DoNotOptimize(tree.lookup(key, &value));
    }
  }
  state.SetItemsProcessed(state.iterations() * num_keys);
}
// 修正：使用较小但有意义的范围
BENCHMARK(BM_SBTree_Lookup)->Range(1 << 10, 1 << 14);

// 范围扫描 (Scan)
static void BM_SBTree_Scan(benchmark::State &state)
{
  state.PauseTiming();
  auto num_keys = state.range(0);
  auto scan_size = state.range(1);

  static SBTree tree;
  static bool initialized = false;

  if (!initialized)
  {
    auto keys_to_insert = generate_keys(num_keys, false);
    for (const auto &key : keys_to_insert)
    {
      tree.insert(key, key);
    }
    initialized = true;
  }

  std::vector<uint64_t> result;
  result.reserve(scan_size);

  std::mt19937 rng(std::random_device{}());
  std::uniform_int_distribution<uint64_t> dist(0, num_keys - scan_size);
  state.ResumeTiming();

  for (auto _ : state)
  {
    state.PauseTiming();
    uint64_t start_key = dist(rng);
    result.clear();
    state.ResumeTiming();

    tree.scan(start_key, start_key + scan_size, result);
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * scan_size);
}
BENCHMARK(BM_SBTree_Scan)->Ranges({{1 << 14, 1 << 14}, {100, 1000}});

// =================================================================
// 并发性能测试 (Concurrent Performance)
// =================================================================

static void BM_SBTree_Concurrent_Insert(benchmark::State &state)
{
  static SBTree tree;
  auto num_keys_per_thread = state.range(0);
  uint64_t start_key = state.thread_index() * num_keys_per_thread;
  uint64_t end_key = start_key + num_keys_per_thread;

  for (auto _ : state)
  {
    for (uint64_t k = start_key; k < end_key; ++k)
    {
      tree.insert(k, k);
    }
  }
  state.SetItemsProcessed(state.iterations() * num_keys_per_thread);
}
BENCHMARK(BM_SBTree_Concurrent_Insert)->Arg(1000)->DenseThreadRange(1, 8, 2)->UseRealTime();

// 主函数
BENCHMARK_MAIN();
