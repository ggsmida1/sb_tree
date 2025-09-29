# SB-Tree

**SB-Tree (Segmented-Block Tree)** 是一种面向内存时间序列数据库的高吞吐索引结构。  
本项目实现了论文提出的核心机制，使用 **C++17 + CMake + GoogleTest**。

---

## 已实现功能

-   **两层结构**

    -   **搜索层 (Search Layer)**：维护已转换数据块 (DataBlock) 的有序 runs，并支持追加。
    -   **数据层 (Data Layer)**：由有序的 DataBlock 构成，支持点查与范围扫描。

-   **插入操作 (Insert)**

    -   新写入首先定位当前活跃的分段块 (Segmented Block)，落到对应线程的 Per-Thread Block (PTB)。
    -   当 PTB 填满时，整个分段块立即封印并转换：收集所有 PTB → 排序 → 切分为 DataBlock → 尾插到数据层 → 搜索层追加 run。
    -   CAS 保证只有一个线程执行转换，其他线程立即切换到新分段块继续写入。
    -   这样实现了 **写入与转换解耦**，提高了并发写入效率。

-   **并发语义**

    -   **Insert vs Insert**：每线程写 PTB，避免锁竞争；封印-转换由单线程负责，保证全局有序。
    -   **Insert vs Lookup/Scan**：
        -   转换完成后，搜索层会发布新的快照 (snapshot)。
        -   查询 (`lookup` / `scan`) 无锁读取快照，访问不可变的 DataBlock。
        -   写线程独占发布新快照，保证快照切换一致性。
    -   当前实现支持 **高并发插入** 与 **无锁查询**，在多线程下保持正确性和稳定性。

-   **查询接口**

    -   `lookup(key)`：点查。
    -   `scan(L, R)`：范围扫描，支持跨块。

-   **单元测试覆盖**
    -   run 接缝正确性（无重复/遗漏）。
    -   转换过程中只允许一个线程负责。
    -   多轮插入后全局有序性验证。
    -   异步索引正确性。
    -   并发插入与查询场景下的稳定性。
    -   全部测试均已通过 ✅。

---

## 未来可考虑功能

-   **块级优化**

    -   DataBlock 内部支持预取与向量化查找。
    -   4KB 对齐以改善缓存命中率。
    -   N-ary 搜索表参数的自适应调优。

-   **工程优化**
    -   内存分配器优化（NUMA-aware、thread-local freelist）。
    -   长时间基准测试，验证写入吞吐与读延迟。
    -   CI/CD 集成，自动化测试与分析。

---

## 使用说明

### 构建

```bash
# 克隆仓库
git clone https://github.com/yourname/sb_tree.git
cd sb_tree

# 配置与构建
cmake -B build
cmake --build build -j
```
