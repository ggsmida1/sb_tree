# SB-Tree

**SB-Tree (Segmented-Block Tree)** 是一种面向内存时间序列数据库的高吞吐索引结构。  
本项目是一个参考实现，使用 **C++17 + CMake + GoogleTest**。

---

## 已实现的功能

-   **两层结构**

    -   **搜索层 (Search Layer)**：维护已转换数据块 (DataBlock) 的有序 runs，并支持追加。
    -   **数据层 (Data Layer)**：由有序的 DataBlock 构成，支持点查与范围扫描。

-   **插入操作 (Insert)**

    -   新写入首先通过 **Shortcut** 找到当前活跃的分段块 (Segmented Block)。
    -   写入落到该分段块中对应线程的 **Per-Thread Block (PTB)**。
    -   如果 PTB **未满**：直接写入成功。
    -   如果 PTB **填满**：
        1. 当前写入仍会成功落到该 PTB；
        2. 分段块立即被 **封印 (seal)**，不再接受新写入；
        3. CAS 保证只有一个线程负责：收集所有 PTB → 排序 → 切分为 DataBlock → 尾插到数据层 → 更新搜索层；
        4. 其余线程自动切换到新的分段块继续写入。
    -   这样实现了 **写入与转换解耦**，保证高并发下写入不断流。

-   **分段块 (Segmented Block) 管理**

    -   每个分段块包含多个 **PTB**，线程本地写入，减少写入竞争。
    -   **填满即封印 (seal-on-fill)**：当某个 PTB 被写满时，该分段块立即被封印，不再接受新写入。

-   **转换机制**

    -   被封印的分段块会收集所有 PTB 的数据，排序后切分成 DataBlock。
    -   DataBlock 会尾插到数据层，并异步入队到搜索层。
    -   保证数据层整体有序，支持 scan/lookup 的正确性。

-   **只一个转换者**

    -   使用 CAS 保证同时只有一个线程负责切段与转换，避免重复工作。

-   **查询接口**

    -   `lookup(key)`：点查。
    -   `scan(L, R)`：范围扫描，支持跨块。

-   **完整单元测试**
    -   接缝无重复 (`RunSeam.NoDuplicateAtBoundary`)。
    -   填满只触发一次转换 (`Conversion.OnlyOneConverterOnFill`)。
    -   端到端正确性 (`EndToEnd.MultipleRunsStillCorrect`)。
    -   异步索引一致性 (`AsyncIndexing.LookupScanCorrectBeforeAndAfterBarrier`)。
    -   自探测 PTB 容量并验证语义 (`AutoDetect.SealOnFill`)。
    -   全部通过 ✅。

---

## 未来可扩展功能

-   **延迟 / 乱序写入支持** (可不考虑)  
    当前实现仅支持单调递增写入。后续可加入乱序数据缓冲与合并策略。

-   **读侧优化 (ROWEX)**  
    将搜索层的读操作改为无锁读、写独占，提高多线程查询吞吐。

-   **块分配器与 NUMA 优化**  
    自定义内存分配器，支持 NUMA-aware 分配和回收，降低内存管理开销。

-   **DataBlock 优化**

    -   预取 / 向量化查找。
    -   4KB 对齐，提升 cache 命中率。
    -   自适应 N-ary 搜索表参数。

-   **混合负载测试与基准**  
    添加长时间高并发基准，验证写吞吐、读延迟、尾延迟。

---

## 构建与运行

```bash
# 配置与构建
cmake -B build
cmake --build build -j

# 运行全部测试
cd build
ctest --output-on-failure
```
