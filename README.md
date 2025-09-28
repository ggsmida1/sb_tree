# SB-Tree：当前进度 & 下一步计划

> 目标：面向时序/递增键的内存索引。数据层只追加，搜索层批量晋升（B+ 风格）。

---

## ✅ 已完成（不含延迟/乱序数据）

### 数据层（DataBlock）

-   顺序插入（键单调递增）→ 收集排序 → 切成 `DataBlock` → **尾插**到全局链表。
-   块内：N-ary 表 + 二分，支持 `find`、区间扫描切片。
-   PTB（每线程缓冲）已从 **4KB** 调到 **16KB**，单次转换的 run 更大。

### 搜索层（SearchLayer，append-only）

-   叶层 L0：`{min_key, DataBlock*}`。
-   内层 L1/L2/...：**凑满扇出 F(默认 64)** 才做**批量晋升**；尾巴不足 F 保留到下次（不分裂/不合并）。
-   `find_candidate(k)`：父层 floor → 下钻到叶层。

### 查询与扫描

-   `lookup(k, &v)`：**搜索层候选 + 块内 N-ary**；若未命中，**沿 next() 右移兜底**（覆盖尾巴未晋升）。
-   `scan(l, r, out)`：区间扫描，块内二分裁剪 + 跨块续扫。
-   **游标/迭代器**：`open_range_cursor(l, r)` + `RangeCursor::next/next_batch`（流式/分页/低内存）。

### 异步索引构建（索引专线线程）

-   前台线程：完成数据层尾插后 **enqueue** 本次 run；  
    后台线程：串行 `append_run`（独占写），读侧 `find_candidate` 走共享锁。
-   提供 `flush_index()` barrier；统计指标（入队/应用批次数与条数、`levels()`）可读取。

### 测试覆盖（gtest）

-   晋升规则（凑满 F 才晋升）、尾巴未晋升候选命中、端到端查找与扫描。
-   游标与批量拉取：跨块、严格 ≤ r 截断、耗尽语义。
-   **异步验证**：不等索引追平也能查对；`flush_index()` 后结果一致。

---

## 🧩 公开接口（核心）

-   插入：`insert(Key k, Value v)`, `flush()`
-   点查：`bool lookup(Key k, Value* out) const`
-   扫描：`size_t scan(Key l, Key r, std::vector<Value>& out) const`
-   游标：`RangeCursor open_range_cursor(Key l, Key r) const`
    -   `bool next(KVPair* out)`, `size_t next_batch(std::vector<KVPair>& out, size_t limit)`
-   异步索引：`void flush_index()`（测试/基准用）

---

## 🔜 下一步（建议按顺序推进）

### 1) 并发插入——最小可用版（正确优先）

-   数据层读写锁：`std::shared_mutex data_layer_mu_`
    -   追加链表：`unique_lock`
    -   读取链表：`shared_lock`
-   转换粗锁：`std::mutex convert_mu_`（PTB 满 → 收集/排序/切块 → 追加 → 入队）
-   多线程小测：2–4 线程、各插 5–10 万；`flush()` + `flush_index()` 后抽查 `lookup/scan`。

### 2) 并发插入——迭代优化

-   **PTB 双缓冲/小池**：满了先换新，旧的批量收集转换，缩短阻塞。
-   **轻锁快照**：收集/排序放锁外；仅在“发布指针/尾插/入队”时短锁。

---

## 🧭 后续路线（功能与工程化）

-   **延迟/乱序数据**：per-thread delta/merge 策略，归并到数据层；与索引批量晋升对接。
-   **搜索层抽象化**：从 `vector` 版过渡到 `SearchBlock`（统一元数据、为持久化/RCU 做准备）。
-   **持久化与恢复**：WAL + 快照；启动时重建或加载搜索层。
-   **API 增强**：游标分页游标/返回键值、可选回调版 `scan_cb`。
-   **指标与调优**：扇出 F、块大小、N-ary 阶数；levels/批次/命中率/吞吐监控。
-   **健壮性测试**：随机多 run、边界键/稀疏键、Fuzz、压力与失败注入。

---

## 📌 现状总结

-   已完成**顺序插入 → 异步索引 → 查找/扫描/游标**的单线程闭环，并通过异步一致性测试。
-   现在可安全启动**并发插入（粗锁版）**，再逐步优化为更细粒度的并发实现。
