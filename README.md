# SB-Tree

**SB-Tree (Segmented-Block Tree)** 是一种面向内存时间序列数据库的高吞吐索引结构。  
本项目实现了论文提出的核心机制，使用 **C++17 + CMake + GoogleTest**。  
目标是为高频写入、范围查询和并发访问提供高效、低延迟的索引方案。

---

## 架构概览

-   **两层结构**

    -   **搜索层 (Search Layer)**  
        维护已转换的 DataBlock runs，并通过后台线程异步追加索引。  
        查询线程访问只读快照 (snapshot)，保证一致性和无锁查询。

    -   **数据层 (Data Layer)**  
        由不可变的 DataBlock 构成的有序链表。  
        支持快速点查、范围扫描与延迟数据插入。

-   **分段块 (Segmented Block)**

    -   管理多个每线程独占的 PTB（Per-Thread Block）。
    -   任一 PTB 写满即触发“封印”：  
        收集所有 PTB → 排序 → 切分为 DataBlock → 尾插到数据层。
    -   转换完成后由后台线程更新搜索层索引。

-   **每线程数据块 (Per-Thread Block, PTB)**

    -   每个线程独占，完全无锁。
    -   顺序写入，达到容量后等待统一转换。

-   **数据块 (DataBlock)**
    -   固定大小（默认 4KB），存储有序 `(Key, Value)` 对。
    -   内建 **N-ary 索引表**，块内二级搜索。
    -   支持延迟插入与自动分裂 (`split()`)，保证局部有序性。
    -   一旦封装完成即不可变，允许无锁并发查询。

---

## 已实现功能

### 🔹 插入路径（Insert Pipeline）

SB-Tree 支持两种写入路径：

| 类型                          | 触发条件          | 行为                                                                              |
| ----------------------------- | ----------------- | --------------------------------------------------------------------------------- |
| **Fast Path（顺序数据）**     | `key > max_key_`  | 写入当前活跃 SegmentedBlock 的 PTB；PTB 满时触发转换与 DataBlock 构建             |
| **Fallback Path（延迟数据）** | `key <= max_key_` | 通过 SearchLayer 精确定位目标 DataBlock，在块内有序插入，必要时自动分裂并更新索引 |

关键机制：

-   `max_key_` 表示**已落盘数据层的最大 Key**，只在 `convert_and_append()` 更新；
-   插入延迟数据时加锁保护链表修改，避免并发 split 冲突；
-   每次 `DataBlock::split()` 自动重建左右块的 N-ary 索引结构；
-   保证延迟数据插入后，数据层仍保持全局递增顺序。

### 🔹 并发语义

| 操作组合                 | 同步机制                              | 保证                |
| ------------------------ | ------------------------------------- | ------------------- |
| Insert vs Insert         | 各线程独占 PTB，转换通过 CAS 竞态解决 | 无锁写入 + 唯一转换 |
| Insert vs Delayed Insert | 延迟路径持有 `data_layer_lock_`       | 防止并发链表断裂    |
| Insert vs Lookup / Scan  | 查询使用快照访问只读 DataBlock        | 无锁读取，一致视图  |
| Split vs Append          | 加锁保证链表更新与索引任务一致        | 数据层全局有序      |

### 🔹 查询接口

-   `lookup(key)`：点查指定键；通过 SearchLayer 快速定位目标块；
-   `scan(L, R)`：范围扫描，自动跨块拼接结果。

### 🔹 DataBlock 内部机制

-   `insert_sorted()`：保持块内有序插入；当插入首位或首条记录时自动更新 `min_key_`；
-   `split()`：在块满时等分拆分，重建 N-ary 索引；左右块的 `min_key_` 分别重置为首元素；
-   `build_nary_()`：块内构建稀疏索引表，提升局部查找速度；
-   `find()`：基于 N-ary 索引 + 局部线性搜索的高效点查。

### 🔹 索引维护（SearchLayer）

-   后台线程异步执行 `append_run()`，将新的 DataBlock runs 晋升到索引层；
-   查询使用稳定快照（snapshot）；
-   线程间通过任务队列 `enqueue_index_task_()` 通信。

---

## 关键设计要点总结

-   `max_key_` 表示**已提交数据层最大 key**（非当前写入最大 key）；
-   延迟数据仅当 `key <= max_key_ && data_head_ != nullptr` 时触发；
-   `DataBlock::min_key_` 必须在插入与分裂后同步维护；
-   延迟插入、split、链表更新均受 `data_layer_lock_` 保护；
-   所有已封印块不可变，查询可并发无锁访问。

---

## 未来可考虑功能

-   **块级优化**

    -   DataBlock 内 SIMD 加速与向量化查找；
    -   4KB 对齐，提高缓存局部性；
    -   动态 N-ary 桶参数调整。

-   **工程优化**

    -   自定义内存分配器（NUMA-aware、thread-local freelist）；
    -   更大规模 benchmark 测试，统计延迟写比例与吞吐曲线；
    -   CI/CD 自动化测试与性能可视化。

-   **功能扩展**
    -   多版本或覆盖写入语义（重复 key 处理策略）；
    -   分布式分片与持久化支持；
    -   结合异步压缩/归档机制的时序落盘。

---

## 论文参考

> _SB-Tree: A B+Tree for In-Memory Time-Series Databases with Segmented Blocks_  
> IEEE Access, 2025.

---

✅ **当前版本状态：稳定（全测试通过）**  
支持顺序与延迟写入的高并发 SB-Tree 原型实现。

cd ~/sb_tree

rm -rf build

mkdir build
cd build

cmake -DCMAKE_BUILD_TYPE=Release ..

make sbtree_benchmark

./benchmark/sbtree_benchmark
