#include <iostream>
#include <vector>
#include <chrono>
#include <iomanip>
#include <random>
#include <algorithm>
#include "SBTree.h" // 包含您的 SBTree 主头文件

// ======================= 测试参数 =======================
// 您可以调整这些值来改变测试负载
// 插入一千万条数据
const size_t NUM_INSERTS = 10'000'000;
// 查找一百万次
const size_t NUM_LOOKUPS = 1'000'000;
// ========================================================

int main()
{
  std::cout << "--- SB-Tree 单线程 MVP 性能测试 ---" << std::endl;
  std::cout << "即将开始测试，请稍候..." << std::endl;

  // 设置输出格式
  std::cout << std::fixed << std::setprecision(2);

  // 1. 创建 SBTree 实例
  SBTree tree;

  // 2. 插入性能测试
  {
    std::cout << "\n[阶段 1: 插入测试]" << std::endl;
    std::cout << "正在插入 " << NUM_INSERTS << " 条记录..." << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    for (Key i = 0; i < NUM_INSERTS; ++i)
    {
      // 插入单调递增的 key，value 设为 key * 10
      tree.insert(i, i * 10);
    }
    // 确保所有在缓冲区的数据都已落盘
    tree.flush();

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;

    double mops = (static_cast<double>(NUM_INSERTS) / duration.count()) / 1'000'000.0;

    std::cout << "插入测试完成。" << std::endl;
    std::cout << "  - 总耗时:   " << duration.count() << " 秒" << std::endl;
    std::cout << "  - 吞吐量:   " << mops << " Mops/sec (每秒百万次操作)" << std::endl;
  }

  // 3. 查找性能测试
  {
    std::cout << "\n[阶段 2: 查找测试]" << std::endl;
    std::cout << "准备 " << NUM_LOOKUPS << " 个随机key用于查找..." << std::endl;

    std::vector<Key> lookup_keys;
    lookup_keys.reserve(NUM_LOOKUPS);

    // 使用随机数引擎生成在 [0, NUM_INSERTS-1] 范围内的key
    std::mt19937_64 rng(std::chrono::steady_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<Key> dist(0, NUM_INSERTS - 1);

    for (size_t i = 0; i < NUM_LOOKUPS; ++i)
    {
      lookup_keys.push_back(dist(rng));
    }

    std::cout << "随机key准备完毕，开始查找..." << std::endl;

    auto start = std::chrono::high_resolution_clock::now();

    Value found_value;
    for (const auto key : lookup_keys)
    {
      tree.lookup(key, &found_value);
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;

    double mops = (static_cast<double>(NUM_LOOKUPS) / duration.count()) / 1'000'000.0;

    std::cout << "查找测试完成。" << std::endl;
    std::cout << "  - 总耗时:   " << duration.count() << " 秒" << std::endl;
    std::cout << "  - 吞吐量:   " << mops << " Mops/sec (每秒百万次操作)" << std::endl;
  }

  std::cout << "\n测试结束。" << std::endl;

  return 0;
}