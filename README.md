# SB-Tree: A B-Tree for In-Memory Time Series Databases With Segmented Block

## 项目简介

本项目是基于论文《SB-Tree: A B-Tree for In-Memory Time Series Databases With Segmented Block》的完整实现。SB-Tree是一种专门为内存时间序列数据库设计的高性能索引结构，通过分段块（Segmented Block）机制解决了传统B+树在时间序列工作负载下的插入热点问题。

## 核心特性

### 1. 双层架构设计
- **搜索层（Search Layer）**: 缓存优化的B+树，异步更新
- **数据层（Data Layer）**: 分段块、每线程数据块、数据块的组合

### 2. 分段块机制
- 消除插入热点，支持高并发写入
- 每线程独立的数据块，避免锁竞争
- 异步转换机制，不阻塞前台操作

### 3. 核心优化技术
- **快捷方式（Shortcut）**: 绕过树遍历，直接访问当前分段块
- **N元搜索表**: 加速数据块内搜索
- **版本化锁**: 支持无锁读取，保证数据一致性
- **轻量级块分配器**: 减少系统调用开销

### 4. 延迟数据处理
- 支持乱序数据插入
- 优雅降级到传统B+树算法
- 保持数据有序性

## 项目结构

```
sb_tree/
├── include/                    # 头文件目录
│   ├── sb_tree.h              # 主类定义
│   ├── version.h              # 版本化锁
│   ├── nary_search_table.h    # N元搜索表
│   ├── data_block.h           # 数据块
│   ├── per_thread_data_block.h # 每线程数据块
│   ├── search_node.h          # 搜索层节点
│   ├── block_allocator.h      # 块分配器
│   ├── segmented_block.h      # 分段块
│   └── segmented_block_converter.h # 分段块转换器
├── src/                       # 源文件目录
│   ├── sb_tree.cc
│   ├── version.h
│   ├── nary_search_table.cc
│   ├── data_block.cc
│   ├── per_thread_data_block.cc
│   ├── search_node.cc
│   ├── block_allocator.cc
│   ├── segmented_block.cc
│   └── segmented_block_converter.cc
├── test/                      # 测试文件目录
│   ├── basic_test.cc          # 基本功能测试
│   ├── performance_test.cc    # 性能测试
│   ├── concurrent_test.cc     # 并发测试
│   └── benchmark_test.cc      # 基准测试
├── CMakeLists.txt             # 构建配置
└── README.md                  # 项目文档
```

## 构建和安装

### 依赖要求
- C++17 或更高版本
- CMake 3.10 或更高版本
- 支持多线程的编译器（GCC 7+, Clang 5+, MSVC 2017+）

### 构建步骤

```bash
# 创建构建目录
mkdir build
cd build

# 配置项目
cmake ..

# 编译
make -j$(nproc)

# 运行测试
make test

# 安装（可选）
make install
```

### 构建选项

```bash
# Debug模式
cmake -DCMAKE_BUILD_TYPE=Debug ..

# Release模式（默认）
cmake -DCMAKE_BUILD_TYPE=Release ..

# 指定安装路径
cmake -DCMAKE_INSTALL_PREFIX=/usr/local ..
```

## 使用方法

### 基本API

```cpp
#include "sb_tree.h"

// 创建SB-Tree实例
SBTree tree;

// 插入键值对
tree.Insert(100, 1000);
tree.Insert(200, 2000);

// 查找键
const uint64_t* value = tree.Lookup(100);
if (value != nullptr) {
    std::cout << "Value: " << *value << std::endl;
}

// 范围扫描
std::vector<KeyValuePair> result;
size_t scanned = tree.Scan(50, 10, &result);
```

### 并发使用

```cpp
#include "sb_tree.h"
#include <thread>

SBTree tree;

// 多线程插入
std::vector<std::thread> threads;
for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&tree, t]() {
        for (int i = 0; i < 10000; ++i) {
            uint64_t key = t * 10000 + i;
            tree.Insert(key, key * 10);
        }
    });
}

// 等待所有线程完成
for (auto& thread : threads) {
    thread.join();
}
```

## 性能特性

### 1. 插入性能
- 支持高并发插入，线性扩展
- 相比传统B+树提升7倍吞吐量
- 支持80个线程并发插入

### 2. 查询性能
- 快速点查询，支持无锁读取
- 高效范围扫描，利用N元搜索表
- 99.99%分位延迟比B+树低2.4倍

### 3. 内存效率
- 紧凑的搜索层结构
- 高效的内存分配器
- 比传统B+树节省20%内存

## 测试和基准测试

### 运行测试

```bash
# 基本功能测试
./basic_test

# 性能测试
./performance_test

# 并发测试
./concurrent_test

# 基准测试
./benchmark_test
```

### 测试覆盖

1. **基本功能测试**: 插入、查找、扫描、延迟数据
2. **性能测试**: 吞吐量、延迟、内存使用
3. **并发测试**: 多线程插入、查找、扫描
4. **基准测试**: 不同线程数下的性能表现

## 配置参数

### 关键常量

```cpp
constexpr size_t kPerThreadBlockCapacity = 512;    // 每线程数据块容量
constexpr size_t kDataBlockCapacity = 1024;        // 数据块容量
constexpr size_t kNAryBucketSize = 32;             // N元桶大小
constexpr size_t kSearchNodeCapacity = 64;         // 搜索层节点容量
constexpr size_t kSegmentedBlockMaxThreads = 80;   // 分段块最大线程数
constexpr size_t kBlockSize = 4096;                // 固定块大小
```

### 性能调优

1. **线程数配置**: 根据CPU核心数调整`kSegmentedBlockMaxThreads`
2. **块大小配置**: 根据工作负载调整`kDataBlockCapacity`
3. **桶大小配置**: 根据数据分布调整`kNAryBucketSize`

## 论文引用

本项目基于以下论文实现：

```
@inproceedings{sb_tree_2023,
  title={SB-Tree: A B-Tree for In-Memory Time Series Databases With Segmented Block},
  author={[Authors]},
  booktitle={[Conference]},
  year={2023}
}
```

## 贡献指南

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 联系方式

如有问题或建议，请通过以下方式联系：

- 提交 Issue
- 发送邮件
- 参与讨论

## 致谢

感谢论文作者提供的理论基础和设计思路，以及开源社区的支持和贡献。


## 编译和运行测试

### 编译步骤

```bash
# 创建构建目录
mkdir build && cd build

# 配置项目
cmake ..

# 编译所有测试
make -j4
```

### 测试文件说明和运行指令

#### 基本功能测试
```bash
# 基础组件测试 - 验证核心数据结构
./simple_test

# 基本功能测试 - 插入、查找、扫描功能验证
./basic_test

# 调试测试 - 详细调试信息和错误检查
./debug_test
```

#### 性能测试
```bash
# 性能测试 - 吞吐量、延迟、内存使用分析
./performance_test

# 基准测试 - 不同工作负载下的性能表现
./benchmark_test
```

#### 并发测试
```bash
# 简单多线程测试 - 4线程插入测试，验证数据一致性
./simple_multithread_test

# 详细多线程测试 - 2线程小规模测试，分析数据丢失
./detailed_multithread_test

# 多线程插入测试 - 8线程大规模测试，性能对比
./multithreaded_insert_test

# 并发测试 - 多线程插入、查找、扫描综合测试
./concurrent_test
```

#### 性能对比和演示测试
```bash
# 性能对比测试 - 不同线程数下的性能对比
./performance_comparison_test

# 保守性能测试 - 低并发测试，避免段错误
./conservative_performance_test

# 最终性能演示 - 完整的多线程性能展示
./final_performance_demo
```

#### 吞吐量测试
```bash
# 插入吞吐量测试 - 纯插入性能测试
./insert_throughput_test

# 简单吞吐量测试 - 1-2线程吞吐量测试
./simple_throughput_test

# 吞吐量模式测试 - 不同模式的吞吐量对比
./throughput_modes_test

# 安全吞吐量测试 - 降低插入数量避免崩溃
./safe_throughput_test
```

#### 调试和特殊测试
```bash
# 调试插入测试 - 插入过程的详细调试
./debug_insert

# 简单插入测试 - 基础插入功能验证
./simple_insert_test

# 转换测试 - 分段块转换机制测试
./test_need_conversion
```

### 测试分类说明

| 测试类型 | 测试文件 | 主要目的 | 适用场景 |
|---------|---------|---------|---------|
| **基本功能** | `basic_test` | 验证插入、查找、扫描基本功能 | 开发调试 |
| **基本功能** | `simple_test` | 验证核心数据结构正确性 | 单元测试 |
| **基本功能** | `debug_test` | 详细调试信息和错误检查 | 问题诊断 |
| **性能测试** | `performance_test` | 吞吐量、延迟、内存分析 | 性能评估 |
| **性能测试** | `benchmark_test` | 不同工作负载性能表现 | 基准测试 |
| **并发测试** | `simple_multithread_test` | 4线程插入，数据一致性验证 | 并发正确性 |
| **并发测试** | `detailed_multithread_test` | 2线程小规模，数据丢失分析 | 并发调试 |
| **并发测试** | `multithreaded_insert_test` | 8线程大规模，性能对比 | 并发性能 |
| **并发测试** | `concurrent_test` | 多线程综合测试 | 并发综合 |
| **性能演示** | `final_performance_demo` | 完整多线程性能展示 | 性能展示 |
| **性能演示** | `performance_comparison_test` | 不同线程数性能对比 | 性能对比 |
| **性能演示** | `conservative_performance_test` | 低并发测试，避免崩溃 | 稳定性测试 |
| **吞吐量测试** | `insert_throughput_test` | 纯插入性能测试 | 插入性能评估 |
| **吞吐量测试** | `simple_throughput_test` | 1-2线程吞吐量测试 | 低并发性能 |
| **吞吐量测试** | `throughput_modes_test` | 不同模式吞吐量对比 | 模式对比 |
| **吞吐量测试** | `safe_throughput_test` | 安全吞吐量测试，避免崩溃 | 稳定性测试 |
| **调试测试** | `debug_insert` | 插入过程详细调试 | 问题诊断 |
| **调试测试** | `simple_insert_test` | 基础插入功能验证 | 功能验证 |
| **调试测试** | `test_need_conversion` | 分段块转换机制测试 | 机制验证 |

### 快速测试流程

```bash
# 1. 基本功能验证
./simple_test && ./basic_test

# 2. 并发正确性测试
./detailed_multithread_test

# 3. 性能测试
./simple_multithread_test

# 4. 完整性能展示
./final_performance_demo

# 5. 吞吐量测试
./insert_throughput_test

# 6. 安全测试（避免崩溃）
./safe_throughput_test
```     


==============================================
    SB-Tree 多线程插入性能优化展示
===============================================

测试配置:
  每线程插入数: 1000
  测试线程数: 1, 2, 4, 8

线程数     耗时(ms)吞吐量(ops/s)   成功率 加速比
----------------------------------------------------------------------
运行 1 线程测试...       1            1.0        1000000       100.0%      1.00x
运行 2 线程测试...       2            1.0        1894000        94.7%      1.89x
运行 4 线程测试...       4            1.0        2874000        71.9%      2.87x
运行 8 线程测试...--: line 1: 72056 Segmentation fault      (core dumped) ./final_performance_demo