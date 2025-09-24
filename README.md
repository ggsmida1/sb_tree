# SB-Tree 项目进度总结

## ✅ 已完成的工作

### 核心数据结构

-   **PerThreadDataBlock**

    -   `Insert(key, value)`：向线程本地缓冲块追加 KV，容量满返回 false。
    -   `GetData()` / `GetNumEntries()`：提供当前写入的数据视图，用于后续转换。

-   **SegmentedBlock**

    -   `append_ordered(key, value)`（在你现有的逻辑里）：负责将顺序写入分派到 PTB。
    -   `should_convert()`：判断是否需要触发转换（PTB 满或槽位用尽）。
    -   `collect_and_sort()`：收集所有 PTB 的 KV，并全局排序。
    -   `seal()`：标记当前段不再接受写入。

-   **DataBlock**
    -   `build_from_sorted(src, n)`：从已排序 KV 构建定长数据块（容量 4KB，超出部分需切片到下一块）。
    -   `find(key, out)`：基于 N-ary 桶定位并线性查找，返回是否找到。
    -   `scan_from(startKey, count, out)`：从某个 key 开始向后扫描，最多返回 count 条。

### 控制逻辑

-   **SBTree**
    -   `insert(key, value)`：顺序插入接口；当当前段满时，新建一个分段块，并触发旧分段块转换。
    -   `convert_segment_to_datablocks_(seg)`：将旧分段块的数据收集、排序并生成 DataBlock（当前先简单处理为单块）。

### 测试验证

-   单线程插入测试：插入 100,000 条数据，运行成功。
-   多线程插入测试：并发插入 400,000 条数据，运行成功。
-   DataBlock 单元测试：验证了 `find` 与 `scan_from` 的正确性。
-   DataBlock 并发读测试：多线程同时进行 find/scan，结果一致，线程安全（只读场景）。

---

## 🚧 还缺的部分（顺序插入路径要补完）

1. **分段块 → 数据块的切片转换**

    - 按照 `DataBlock::kCapacity` 分割排序后的 entries。
    - 构建多个 DataBlock，并用 `set_next` 串起来。
    - 更新 `data_head_ / data_tail_`，保证数据层链条完整。

2. **SBTree 对外 scan 接口**

    - 提供 `scan(start_key, count)`：
        - 遍历 DataBlock 链，找到包含 start_key 的块。
        - 逐块调用 `scan_from` 收集结果，直到满足数量。
    - 用例：顺序插入 [1..N] 后，任意范围扫描返回升序结果。

3. **SBTree 查找接口（可选）**
    - 提供 `lookup(key)`，顺着 DataBlock 链调用 `find`，先保证正确性。

---
