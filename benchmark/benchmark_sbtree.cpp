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
    std::mt19937 g(rd());
    std::shuffle(keys.begin(), keys.end(), g);
  }
  return keys;
}

// =================================================================
// 插入性能测试
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
// 查询与扫描性能测试的 Fixture
// =================================================================
class QueryScanFixture : public benchmark::Fixture
{
public:
  SBTree tree;
  std::vector<uint64_t> keys_to_lookup;

  void SetUp(const benchmark::State &state) override
  {
    const size_t num_keys = state.range(0);

    keys_to_lookup.clear();

    // 填充树
    auto keys_to_insert = generate_keys(num_keys, false);
    for (const auto &key : keys_to_insert)
    {
      tree.insert(key, key * 10);
    }
    // 确保所有数据都已从 SegmentedBlock 转换并进入 DataBlock
    tree.flush();
    // 确保索引已完全建立
    tree.flush_index();

    // 准备用于查找的随机键
    keys_to_lookup = generate_keys(num_keys, true);
  }

  void TearDown(const benchmark::State &state) override
  {
  }
};

// =================================================================
// 使用 Fixture 进行点查询 (Lookup)
// =================================================================
BENCHMARK_F(QueryScanFixture, BM_SBTree_Lookup)(benchmark::State &state)
{
  for (auto _ : state)
  {
    // 每次循环随机查一部分key，避免缓存效应过于理想化
    for (size_t i = 0; i < 1000 && i < keys_to_lookup.size(); ++i)
    {
      uint64_t value;
      bool found = tree.lookup(keys_to_lookup[i], &value);
      benchmark::DoNotOptimize(found);
      benchmark::DoNotOptimize(value);
    }
  }
  state.SetItemsProcessed(state.iterations() * 1000);
}
// 【MVP 修正】将测试起点从 1<<16 (65,536) 降低到 1<<12 (4,096)，上限暂时降到 1<<16
BENCHMARK_REGISTER_F(QueryScanFixture, BM_SBTree_Lookup)->Range(1 << 12, 1 << 16);

// =================================================================
// 使用 Fixture 进行范围扫描 (Scan)
// =================================================================
BENCHMARK_F(QueryScanFixture, BM_SBTree_Scan)(benchmark::State &state)
{
  auto num_keys = state.range(0);
  auto scan_size = state.range(1);

  std::vector<uint64_t> result;
  result.reserve(scan_size);

  std::mt19937 rng(123); // 使用固定种子以保证可重复性
  std::uniform_int_distribution<uint64_t> dist(0, num_keys - scan_size);

  for (auto _ : state)
  {
    uint64_t start_key = dist(rng);
    result.clear();
    tree.scan(start_key, start_key + scan_size - 1, result); // scan是闭区间
    benchmark::ClobberMemory();                              // 确保结果不被优化掉
  }
  state.SetItemsProcessed(state.iterations());
}
// 【MVP 修正】将树的总大小测试范围的起点也降低到 1<<12
BENCHMARK_REGISTER_F(QueryScanFixture, BM_SBTree_Scan)->ArgsProduct({
    {1 << 12, 1 << 16}, // 树的总大小
    {100, 1000}         // 扫描范围大小
});

// =================================================================
// 并发性能测试
// =================================================================
static void BM_SBTree_Concurrent_Insert(benchmark::State &state)
{
  auto num_keys_total = state.range(0);

  for (auto _ : state)
  {
    state.PauseTiming();
    SBTree tree;
    auto num_threads = state.threads();
    const size_t keys_per_thread = num_keys_total / num_threads;
    std::vector<std::thread> threads;
    state.ResumeTiming();

    for (int tid = 0; tid < num_threads; ++tid)
    {
      threads.emplace_back([&, tid]()
                           {
                for (size_t i = 0; i < keys_per_thread; ++i)
                {
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
    ->Arg(1 << 18)
    ->DenseThreadRange(1, std::thread::hardware_concurrency(), 2)
    ->UseRealTime();

// 运行 benchmark
BENCHMARK_MAIN();