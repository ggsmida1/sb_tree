🚨 1️⃣ SearchLayer 顺序断言触发（min_key 逆序）

在多线程测试中触发：

Assertion `L0_.back().min_key <= blocks.front()->min_key()` failed.

📍原因：

后台线程并没有保证多个 SegmentedBlock 的 flush 顺序；

window_id 没有被严格线性化；

导致不同线程同时 enqueue 的 Segment 出现乱序发布。

例如：

Thread A -> flush window 5 (key ~ 5000)

Thread B -> flush window 4 (key ~ 4000)

SearchLayer 期望顺序为 [4000 -> 5000]，

但你当前的 background_loop 是按批次随机顺序处理 convert_queue_，最终 [5000 -> 4000] 触发断言。

🚨 2️⃣ Delayed Map 被一次性清空导致混乱

你在 background_loop 中写道：

// clear delayed_map_ entirely because we merged all delayed entries into grouped.

delayed_map_.clear();

这会导致：

未来窗口的 delayed 数据被提前合并；

下次 flush 时可能重复或缺失；

破坏 SearchLayer 的全局有序性。

正确做法应当是：

仅合并当前窗口及其之前的 delayed 数据；

未来窗口的数据保留。

🚨 3️⃣ 多线程同时触发 flush 时，窗口编号无法全局单调递增

当前逻辑：

if (block->IsFull()) newseg = create_segmented_block_for_window(seg_wid + 1)

问题：

线程 A 可能 flush 出 window_id = 2

线程 B 也几乎同时 CAS 成功，得到 window_id = 3

结果 2、3 两个窗口的排序不确定；

最终 SearchLayer append_run 顺序随机。

🚨 4️⃣ background_loop 缺乏全局窗口协调

当前逻辑按如下方式运行：

// group by window_id

std::unordered_map<uint64_t, std::vector<KVPair>> grouped;

std::sort(wids.begin(), wids.end());

for (uint64_t wid : wids)

​    publish_blocks(wid, blocks);

这里虽然排序了窗口，但多个 flush 线程同时运行时，仍可能出现“提前发布未来窗口”的情况。

论文中要求：

“Only one writer thread (background thread) updates the search layer in strict key order.”

但当前实现的 background_loop 是以「批次」为单位，非严格顺序。




✅ 阶段 1（修复当前错误）

给 SegmentedBlock 增加 window_id；

在 SBTree 中维护 std::atomic<uint64_t> last_published_window_id_；

在 convert_segmented_block() 里检查是否能发布：

while (window_id > last_published_window_id_ + 1)
    std::this_thread::yield();


确保 SearchLayer append_run 的顺序性。

⚙️ 阶段 2（加入后台 flush 线程）

将 convert_segmented_block() 改为后台队列任务；

flush 线程根据 window_id 顺序从队列发布。

🚀 阶段 3（延迟数据支持）

引入 delayed buffer；

按 key 判断是否落入当前窗口，否则放入 delay 队列。